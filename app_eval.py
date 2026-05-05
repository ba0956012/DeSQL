"""
Eval Debug UI — 從 BIRD 題庫選題，跑 pipeline 並顯示完整 log

啟動：
  PROMPT_PROFILE=qwen3_en LLM_PROVIDER=bedrock LLM_MODEL=qwen.qwen3-next-80b-a3b \
  SQL_LLM_PROVIDER=bedrock SQL_LLM_MODEL=qwen.qwen3-coder-30b-a3b-v1:0 \
  CODE_LLM_PROVIDER=bedrock CODE_LLM_MODEL=qwen.qwen3-coder-30b-a3b-v1:0 \
  streamlit run app_eval.py --server.port 8502
"""

import json
import time
import os
import sys
from pathlib import Path

import streamlit as st

st.set_page_config(page_title="DeSQL Eval Debug", page_icon="🔬", layout="wide")

# ── Load env BEFORE any pipeline imports ──
# Save CLI env vars (LLM settings from run_eval_debug.sh)
_CLI_ENV_KEYS = [
    "LLM_PROVIDER",
    "LLM_MODEL",
    "SQL_LLM_PROVIDER",
    "SQL_LLM_MODEL",
    "CODE_LLM_PROVIDER",
    "CODE_LLM_MODEL",
    "PROMPT_PROFILE",
    "BEDROCK_API_TOKEN",
    "BEDROCK_BASE_URL",
    "ENABLE_CHART",
    "DATABASE_URL",
    "PG_BASE_URL",
    "DOMAIN_RULES",
]
_cli_env = {k: os.environ[k] for k in _CLI_ENV_KEYS if k in os.environ}

# Load eval debug env (sets DATABASE_URL, PG_BASE_URL)
from dotenv import load_dotenv

load_dotenv(".env.eval_debug", override=True)

# Restore CLI env vars (LLM settings take priority)
os.environ.update(_cli_env)

# ── Load BIRD dataset ──
EVAL_DIR = Path("eval")


@st.cache_data
def load_questions():
    """Load all BIRD questions from dev.json"""
    with open(EVAL_DIR / "dev.json") as f:
        data = json.load(f)
    return data


@st.cache_data
def load_gold_results(db_id):
    """Load gold SQL results for comparison"""
    from sqlalchemy import create_engine, text as sa_text
    from dotenv import load_dotenv

    load_dotenv()
    load_dotenv(str(EVAL_DIR / ".env.eval"), override=True)
    pg_base = os.environ.get("PG_BASE_URL", "")
    if not pg_base:
        return {}
    engine = create_engine(f"{pg_base}/bird_{db_id}")
    results = {}
    return results


all_questions = load_questions()

# ── DB is set at startup via DATABASE_URL env var ──
PG_BASE_URL = os.environ.get("PG_BASE_URL", "")

# ── Shared state for LLM call tracking ──
_THIS_MODULE = "desql_eval_state"
if _THIS_MODULE not in sys.modules:
    import types

    _mod = types.ModuleType(_THIS_MODULE)
    _mod.call_log = []
    sys.modules[_THIS_MODULE] = _mod
_shared = sys.modules[_THIS_MODULE]


# ── TrackedLLM (same as app.py) ──
class _TrackedLLM:
    def __init__(self, real_llm, label):
        object.__setattr__(self, "_real", real_llm)
        object.__setattr__(self, "_label", label)

    _CALLER_MAP = {
        "analyze_conditions": "retrieval: 條件分析",
        "tokenize": "retrieval: 拆詞",
        "expand_synonyms": "retrieval: 同義詞擴展",
        "_classify_question": "QA: pitfall 偵測",
        "question_analysis": "QA: 問題分解",
        "_review_plan": "QA: plan review",
        "_refine_aggregation": "QA: aggregation refine",
        "_enhance_hint": "QA: hint enhance",
        "schema_filter": "schema filter",
        "_filter_big_tables": "schema filter: 大表過濾",
        "generate_sql": "SQL 生成",
        "validate_sql_result": "SQL 驗證",
        "generate_code": "Python code 生成",
        "_check_code_task": "code: 資料驗證",
        "_judge_answer": "eval: 答案判斷",
        "format_answer": "答案格式化",
        "generate_chart": "chart 生成",
    }

    def _detect_purpose(self):
        import inspect

        for frame_info in inspect.stack():
            fname = frame_info.function
            if fname in self._CALLER_MAP:
                return self._CALLER_MAP[fname]
        return "unknown"

    def invoke(self, messages, *args, **kwargs):
        purpose = self._detect_purpose()
        input_text = "\n".join(m.content for m in messages if hasattr(m, "content"))
        result = self._real.invoke(messages, *args, **kwargs)
        output_text = result.content if hasattr(result, "content") else str(result)
        sys.modules[_THIS_MODULE].call_log.append(
            {
                "label": object.__getattribute__(self, "_label"),
                "purpose": purpose,
                "input": input_text,
                "output": output_text,
                "input_len": len(input_text),
                "output_len": len(output_text),
            }
        )
        return result

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_real"), name)


# Patch LLMs
import llm as _llm_mod

if not isinstance(_llm_mod.llm, _TrackedLLM):
    _llm_mod.llm = _TrackedLLM(_llm_mod.llm, "main")
    _llm_mod.sql_llm = _TrackedLLM(_llm_mod.sql_llm, "sql")
    _llm_mod.code_llm = _TrackedLLM(_llm_mod.code_llm, "code")
    _llm_mod.qa_llm = _TrackedLLM(_llm_mod.qa_llm, "qa")
    _llm_mod.schema_llm = _TrackedLLM(_llm_mod.schema_llm, "schema")

from pipeline import app as pipeline_app
from logger import init_run_logger

# Force-patch node refs
import nodes.sql, nodes.code, nodes.answer, nodes.question_analysis, nodes.schema_filter

_tracked_set = {
    id(_llm_mod.llm),
    id(_llm_mod.sql_llm),
    id(_llm_mod.code_llm),
    id(_llm_mod.qa_llm),
    id(_llm_mod.schema_llm),
}
if id(nodes.sql.llm) not in _tracked_set:
    nodes.sql.llm = _llm_mod.sql_llm
if id(nodes.code.llm) not in _tracked_set:
    nodes.code.llm = _llm_mod.code_llm
if id(nodes.answer.llm) not in _tracked_set:
    nodes.answer.llm = _llm_mod.llm
if id(nodes.question_analysis.llm) not in _tracked_set:
    nodes.question_analysis.llm = _llm_mod.qa_llm
if id(nodes.question_analysis.review_llm) not in _tracked_set:
    nodes.question_analysis.review_llm = _llm_mod.schema_llm
if id(nodes.schema_filter.llm) not in _tracked_set:
    nodes.schema_filter.llm = _llm_mod.schema_llm
try:
    import nodes.chart_echarts

    if id(nodes.chart_echarts.llm) not in _tracked_set:
        nodes.chart_echarts.llm = _llm_mod.llm
except ImportError:
    pass

# ── Sidebar: Question Selector ──
with st.sidebar:
    st.subheader("📋 題目選擇")

    # DB is fixed at startup (set by run_eval_debug.sh)
    db_name = os.environ.get("DATABASE_URL", "").split("/")[-1].replace("bird_", "")
    st.text(f"Current DB: {db_name}")
    st.caption("Change DB: restart with ./run_eval_debug.sh <db>")

    # Filter questions by current DB
    db_questions = [q for q in all_questions if q["db_id"] == db_name]

    # Difficulty filter
    difficulties = ["all"] + sorted(
        set(q.get("difficulty", "unknown") for q in db_questions)
    )
    selected_diff = st.selectbox("Difficulty", difficulties)
    if selected_diff != "all":
        db_questions = [q for q in db_questions if q.get("difficulty") == selected_diff]

    # Question selector
    q_options = {
        f"#{q['question_id']} ({q.get('difficulty','?')}): {q['question'][:60]}": q
        for q in db_questions
    }
    selected_label = st.selectbox("Question", list(q_options.keys()))
    selected_q = q_options[selected_label] if selected_label else None

    st.divider()
    st.subheader("⚙️ Config")
    provider = os.getenv("LLM_PROVIDER", "azure")
    st.text(f"Provider: {provider}")
    st.text(f"Model: {os.getenv('LLM_MODEL', 'N/A')}")
    st.text(f"SQL: {os.getenv('SQL_LLM_MODEL', 'N/A')}")
    st.text(f"Code: {os.getenv('CODE_LLM_MODEL', 'N/A')}")
    st.text(f"Profile: {os.getenv('PROMPT_PROFILE', 'default')}")

# ── Main Area ──
if selected_q:
    # Show question details
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(
            f"### #{selected_q['question_id']} — {selected_q.get('difficulty', '?')}"
        )
        st.write(f"**Question:** {selected_q['question']}")
        evidence = selected_q.get("evidence", "")
        if evidence:
            st.info(f"**Hint:** {evidence}")
    with col2:
        st.code(f"DB: {selected_q['db_id']}", language="text")
        st.code(f"Gold SQL:\n{selected_q.get('SQL', 'N/A')[:200]}", language="sql")

    # Run button
    if st.button("▶️ Run Pipeline", type="primary"):
        _shared.call_log = []

        # Build question with hint
        question = selected_q["question"]
        if selected_q.get("evidence"):
            question += f"\n(Hint: {selected_q['evidence']})"

        init_run_logger(question)
        start_time = time.time()

        steps = []
        merged = {}
        progress = st.empty()

        for event in pipeline_app.stream({"question": question, "retry": 0}):
            for node_name, node_output in event.items():
                steps.append(node_name)
                if isinstance(node_output, dict):
                    merged.update(node_output)
            with progress.container():
                st.caption(f"處理中... ({len(steps)} nodes)")
                for i, name in enumerate(steps):
                    icon = "✅" if i < len(steps) - 1 else "⏳"
                    st.write(f"{icon} {name}")

        elapsed = time.time() - start_time
        with progress.container():
            st.caption(f"完成（{elapsed:.1f}s, {len(steps)} nodes）")
            for name in steps:
                st.write(f"✅ {name}")

        # ── Answer comparison + Judging ──
        st.divider()
        answer = (
            merged.get("display_answer") or merged.get("final_answer") or "無法回答"
        )

        # Execute gold SQL on SQLite to get expected result
        gold_sql = selected_q.get("SQL", "")
        expected_result = None
        if gold_sql:
            try:
                import sqlite3

                sqlite_path = (
                    EVAL_DIR
                    / "databases"
                    / selected_q["db_id"]
                    / f"{selected_q['db_id']}.sqlite"
                )
                conn = sqlite3.connect(str(sqlite_path))
                cursor = conn.cursor()
                cursor.execute(gold_sql)
                cols = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                conn.close()
                expected_result = [dict(zip(cols, r)) for r in rows]
            except Exception as e:
                expected_result = f"Gold SQL error: {e}"

        # Judge: use LLM to compare
        is_correct = None
        judge_reason = ""
        if expected_result and not isinstance(expected_result, str):
            try:
                # Simplify expected: if single row single column, extract the value directly
                if len(expected_result) == 1 and len(expected_result[0]) == 1:
                    expected_str = str(list(expected_result[0].values())[0])
                elif len(expected_result) <= 20:
                    expected_str = json.dumps(
                        expected_result, ensure_ascii=False, default=str
                    )
                else:
                    expected_str = json.dumps(
                        expected_result[:10], ensure_ascii=False, default=str
                    )
                    expected_str += (
                        f"\n... ({len(expected_result)} rows total, showing first 10)"
                    )

                judge_prompt = f"""Compare the predicted answer with the expected answer. Are they equivalent?

Criteria:
- Compare semantic meaning and values, not format
- Allow minor rounding differences in numbers
- If expected is a list, predicted just needs to contain the same items (order doesn't matter)

Question: {selected_q['question']}
Expected (from gold SQL): {expected_str[:500]}
Predicted: {str(answer)[:500]}

Answer ONLY "correct" or "incorrect" followed by a brief reason."""

                def _judge_answer(prompt):
                    from langchain_core.messages import HumanMessage

                    return _llm_mod.llm.invoke([HumanMessage(content=prompt)])

                judge_res = _judge_answer(judge_prompt)
                judge_text = (
                    judge_res.content.strip().lower()
                    if hasattr(judge_res, "content")
                    else str(judge_res).lower()
                )
                is_correct = judge_text.startswith("correct")
                judge_reason = judge_text
            except Exception as e:
                judge_reason = f"Judge error: {e}"

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**Pipeline Answer:**")
            if is_correct is True:
                st.success(f"✅ {answer}")
            elif is_correct is False:
                st.error(f"❌ {answer}")
            else:
                st.warning(answer)
            if judge_reason:
                st.caption(f"Judge: {judge_reason[:200]}")
        with col_b:
            st.markdown("**Expected (Gold SQL):**")
            st.code(gold_sql, language="sql")
            if expected_result and not isinstance(expected_result, str):
                if len(expected_result) <= 10:
                    st.json(expected_result)
                else:
                    st.json(expected_result[:5])
                    st.caption(f"... {len(expected_result)} rows total")
            elif isinstance(expected_result, str):
                st.error(expected_result)

        # ── Pipeline Details ──
        st.divider()

        # Retrieval
        with st.expander("🔍 Retrieval"):
            st.write(f"Strategy: {merged.get('strategy', 'N/A')}")
            conds = merged.get("conditions", [])
            if conds:
                st.json(conds)

        # Schema Filter
        filtered = merged.get("filtered_schema", "")
        if filtered:
            with st.expander("🔧 Filtered Schema"):
                st.code(filtered, language="sql")

        # Task Plan
        task_plan = merged.get("task_plan", "")
        if task_plan:
            with st.expander("🧠 Task Plan (QA)", expanded=True):
                try:
                    st.json(json.loads(task_plan))
                except:
                    st.code(task_plan)

        # SQL
        with st.expander("🗄️ SQL", expanded=True):
            st.code(merged.get("sql", "N/A"), language="sql")
            sql_result = merged.get("sql_result", [])
            st.caption(f"Rows: {len(sql_result)}")
            if merged.get("sql_validation"):
                st.warning(f"Validation: {merged['sql_validation']}")
            if sql_result and len(sql_result) <= 30:
                st.dataframe(sql_result)
            elif sql_result:
                st.dataframe(sql_result[:10])
                st.caption(f"... showing 10/{len(sql_result)} rows")

        # Python Code
        if merged.get("code"):
            with st.expander("🐍 Python Code", expanded=True):
                st.code(merged["code"], language="python")

        # Error
        if merged.get("error"):
            st.error(f"❌ {merged['error']}")

        # LLM Calls
        _call_log = _shared.call_log
        _deduped = []
        _seen = set()
        for c in _call_log:
            key = (c["label"], c["input_len"], c["output_len"], c["input"][:200])
            if key not in _seen:
                _deduped.append(c)
                _seen.add(key)
        _call_log = _deduped

        total_in = sum(c["input_len"] for c in _call_log)
        total_out = sum(c["output_len"] for c in _call_log)
        c1, c2, c3 = st.columns(3)
        c1.metric("⏱️ Time", f"{elapsed:.1f}s")
        c2.metric("🤖 LLM Calls", f"{len(_call_log)}")
        c3.metric("📝 I/O Chars", f"{(total_in + total_out):,}")

        with st.expander(f"🤖 LLM Call Details ({len(_call_log)})", expanded=True):
            for i, call in enumerate(_call_log):
                purpose = call.get("purpose", "")
                st.markdown(
                    f"**#{i+1}** `{call['label']}` — {purpose} "
                    f"({call['input_len']:,} → {call['output_len']:,})"
                )
                tabs = st.tabs(["Input", "Output"])
                with tabs[0]:
                    inp = call["input"]
                    st.code(inp[:10000] if len(inp) > 10000 else inp, language="text")
                with tabs[1]:
                    out = call["output"]
                    st.code(out[:10000] if len(out) > 10000 else out, language="text")
                if i < len(_call_log) - 1:
                    st.divider()
