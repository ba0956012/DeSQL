"""
LangGraph SQL+Python Pipeline — Streamlit UI

啟動：./run.sh
"""

import json
import time
import base64
import os

import streamlit as st

st.set_page_config(page_title="DeSQL Pipeline", page_icon="🔍", layout="wide")
st.title("🔍 DeSQL — Text-to-SQL Pipeline")

# ── Global call log ──
# Use a mutable container on the module so TrackedLLM always appends to the
# *current* list even after Streamlit re-runs the script (which re-binds names).
import sys as _sys

_THIS_MODULE = "desql_app_state"
if _THIS_MODULE not in _sys.modules:
    # First run: create a tiny namespace module to hold shared state
    import types

    _mod = types.ModuleType(_THIS_MODULE)
    _mod.call_log = []
    _sys.modules[_THIS_MODULE] = _mod

_shared = _sys.modules[_THIS_MODULE]


class _TrackedLLM:
    """Wrapper that delegates to real LLM but logs invoke calls."""

    def __init__(self, real_llm, label):
        object.__setattr__(self, "_real", real_llm)
        object.__setattr__(self, "_label", label)

    # Map caller function → human-readable purpose
    _CALLER_MAP = {
        "analyze_conditions": "retrieval: 條件分析",
        "tokenize": "retrieval: 拆詞",
        "expand_synonyms": "retrieval: 同義詞擴展",
        "_classify_question": "QA: pitfall 偵測",
        "question_analysis": "QA: 問題分解",
        "_review_plan": "QA: plan review",
        "_analyze_hint": "QA: hint 分析",
        "schema_filter": "schema filter: 表/欄位篩選",
        "generate_sql": "SQL 生成",
        "_check_code_task": "code: 資料驗證",
        "generate_code": "Python code 生成",
        "format_answer": "答案格式化",
        "generate_chart": "chart: 視覺化判斷/生成",
        "generate_schema_summary": "schema: 描述精煉",
        "llm_summarize_desc": "schema: 動態描述",
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
        _sys.modules[_THIS_MODULE].call_log.append(
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


# Patch LLM module BEFORE importing pipeline.
# Guard: only wrap once (check if already tracked).
import llm as _llm_mod

if not isinstance(_llm_mod.llm, _TrackedLLM):
    _orig_llm = _llm_mod.llm
    _orig_sql = _llm_mod.sql_llm
    _orig_code = _llm_mod.code_llm
    _orig_qa = _llm_mod.qa_llm
    _orig_schema = _llm_mod.schema_llm

    _llm_mod.llm = _TrackedLLM(_orig_llm, "main")
    _llm_mod.sql_llm = _TrackedLLM(_orig_sql, "sql")
    _llm_mod.code_llm = _TrackedLLM(_orig_code, "code")
    _llm_mod.qa_llm = _TrackedLLM(_orig_qa, "qa")
    _llm_mod.schema_llm = _TrackedLLM(_orig_schema, "schema")

# NOW import pipeline — nodes will do `from llm import sql_llm as llm` etc.
from pipeline import app as pipeline_app
from logger import init_run_logger

# Force-patch node-level references to use tracked LLMs.
# On first run, nodes already got tracked versions from the patched module.
# On Streamlit re-run, modules are cached so we must ensure node refs are tracked.
import nodes.sql
import nodes.code
import nodes.answer
import nodes.question_analysis
import nodes.schema_filter

# Use identity check: only replace if the node's llm is NOT already one of our tracked instances
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

# ── Sidebar: config display ──
with st.sidebar:
    st.subheader("⚙️ Pipeline Config")
    from config import LLM_MODEL, PROMPT_PROFILE, DEBUG

    provider = os.getenv("LLM_PROVIDER", "azure")
    st.text(f"Provider: {provider}")
    st.text(f"Model: {LLM_MODEL}")
    st.text(f"Profile: {PROMPT_PROFILE}")
    sql_model = os.getenv("SQL_LLM_MODEL", "")
    code_model = os.getenv("CODE_LLM_MODEL", "")
    if sql_model:
        st.text(f"SQL LLM: {sql_model}")
    if code_model:
        st.text(f"Code LLM: {code_model}")
    st.text(f"Debug: {DEBUG}")
    st.divider()
    db_name = os.getenv("DATABASE_URL", "N/A").split("/")[-1]
    st.caption(f"DB: {db_name}")

# ── Main ──
question = st.chat_input("請輸入問題（自然語言查詢）")

if question:
    with st.chat_message("user"):
        st.write(question)

    # Clear call log for this run
    _shared.call_log = []

    init_run_logger(question)
    start_time = time.time()

    steps = []
    merged = {}
    progress_container = st.empty()

    # Pipeline graph structure for Mermaid visualization
    _GRAPH_NODES = [
        "retrieval",
        "schema_filter",
        "question_analysis",
        "generate_sql",
        "execute_sql",
        "validate_sql_result",
        "generate_code",
        "run_code",
        "validate_result",
        "format_answer",
        "generate_chart",
    ]
    _NODE_LABELS = {
        "retrieval": "📥 資料檢索",
        "schema_filter": "🔍 Schema 篩選",
        "question_analysis": "🧠 問題分析",
        "generate_sql": "🔧 SQL 生成",
        "execute_sql": "⚡ SQL 執行",
        "validate_sql_result": "🔍 SQL 驗證",
        "generate_code": "💻 Code 生成",
        "run_code": "▶️ Code 執行",
        "validate_result": "✔️ 結果驗證",
        "format_answer": "📝 答案格式化",
        "generate_chart": "📊 圖表生成",
    }

    # Detect disabled nodes from env
    from nodes.result_validator import ENABLE_RESULT_VALIDATION

    _DISABLED_NODES = set()
    if not ENABLE_RESULT_VALIDATION:
        _DISABLED_NODES.add("validate_result")
    # Edges: (from, to, label)
    _GRAPH_EDGES = [
        ("retrieval", "schema_filter", ""),
        ("schema_filter", "question_analysis", ""),
        ("question_analysis", "generate_sql", ""),
        ("generate_sql", "execute_sql", ""),
        ("execute_sql", "validate_sql_result", ""),
        ("execute_sql", "generate_sql", "retry"),
        ("validate_sql_result", "generate_code", ""),
        ("validate_sql_result", "generate_sql", "retry"),
        ("validate_sql_result", "format_answer", "無資料"),
        ("generate_code", "run_code", ""),
        ("run_code", "validate_result", ""),
        ("run_code", "generate_code", "retry"),
        ("run_code", "generate_sql", "資料不足"),
        ("validate_result", "format_answer", ""),
        ("validate_result", "generate_code", "retry"),
        ("format_answer", "generate_chart", ""),
    ]

    def _render_mermaid(steps_done, running=True):
        """Render pipeline graph with Graphviz (natively supported by Streamlit)."""
        done_set = set(steps_done)
        last_step = steps_done[-1] if steps_done else ""

        dot_lines = [
            "digraph pipeline {",
            "    rankdir=LR;",
            '    node [shape=box, style="rounded,filled", fontsize=10, fontname="Arial"];',
            '    edge [fontsize=8, color="#999999"];',
        ]

        # Define nodes with colors
        for node_id in _GRAPH_NODES:
            label = _NODE_LABELS.get(node_id, node_id)

            # Disabled nodes
            if node_id in _DISABLED_NODES:
                color = "#ffffff"
                fontcolor = "#adb5bd"
                penwidth = "1"
                label = f"⊘ {label}"
                style = "rounded,filled,dashed"
            elif not running and node_id in done_set:
                color = "#d4edda"
                fontcolor = "#155724"
                penwidth = "1"
                style = "rounded,filled"
            elif running and node_id == last_step:
                color = "#fff3cd"
                fontcolor = "#856404"
                penwidth = "2"
                style = "rounded,filled"
            elif node_id in done_set:
                color = "#d4edda"
                fontcolor = "#155724"
                penwidth = "1"
                style = "rounded,filled"
            else:
                color = "#f8f9fa"
                fontcolor = "#6c757d"
                penwidth = "1"
                style = "rounded,filled"
            dot_lines.append(
                f'    {node_id} [label="{label}", fillcolor="{color}", '
                f'fontcolor="{fontcolor}", penwidth={penwidth}, style="{style}"];'
            )

        # Define edges
        for src, dst, label in _GRAPH_EDGES:
            edge_style = ""
            if label:
                edge_style = f'label="{label}", style=dashed, color="#dc3545"'
            else:
                if src in done_set and dst in done_set:
                    edge_style = 'color="#28a745", penwidth=2'
                else:
                    edge_style = 'color="#dee2e6"'
            dot_lines.append(f"    {src} -> {dst} [{edge_style}];")

        dot_lines.append("}")
        return "\n".join(dot_lines)

    # Track LLM sub-calls for fine-grained progress
    _prev_call_count = len(_shared.call_log)

    for event in pipeline_app.stream({"question": question, "retry": 0}):
        for node_name, node_output in event.items():
            steps.append(node_name)
            if isinstance(node_output, dict):
                merged.update(node_output)
        _prev_call_count = len(_shared.call_log)

        with progress_container.container():
            elapsed_so_far = time.time() - start_time
            # Current status line with last sub-call
            all_calls = _shared.call_log
            last_purpose = all_calls[-1]["purpose"] if all_calls else ""
            st.caption(
                f"⏳ 處理中... ({len(steps)} steps, {elapsed_so_far:.1f}s) → {last_purpose}"
            )
            dot_code = _render_mermaid(steps, running=True)
            st.graphviz_chart(dot_code, use_container_width=True)

    elapsed = time.time() - start_time
    # Final state: graph + summary of all LLM calls
    all_calls = _shared.call_log
    with progress_container.container():
        st.caption(
            f"✅ 完成（{elapsed:.1f}s, {len(steps)} nodes, {len(all_calls)} LLM calls）"
        )
        dot_code = _render_mermaid(steps, running=False)
        st.graphviz_chart(dot_code, use_container_width=True)

    # ── Answer ──
    with st.chat_message("assistant"):
        st.markdown(
            merged.get("display_answer") or merged.get("final_answer") or "無法回答"
        )

    # ── Chart ──
    chart_reason = merged.get("chart_reason", "")
    if chart_reason:
        st.caption(f"📈 圖表判斷：{chart_reason}")
    chart_html = merged.get("chart_html", "")
    chart_b64 = merged.get("chart_image", "")
    if chart_html:
        import streamlit.components.v1 as components

        components.html(chart_html, height=550, scrolling=True)
    elif chart_b64:
        st.image(base64.b64decode(chart_b64), width=700)

    # ── Metrics ──
    _call_log = _shared.call_log
    # Deduplicate: same input+output at consecutive positions = duplicate
    _deduped = []
    _seen = set()
    for c in _call_log:
        key = (c["label"], c["input_len"], c["output_len"], c["input"][:200])
        if key not in _seen:
            _deduped.append(c)
            _seen.add(key)
    _call_log = _deduped
    total_input_chars = sum(c["input_len"] for c in _call_log)
    total_output_chars = sum(c["output_len"] for c in _call_log)
    col1, col2, col3 = st.columns(3)
    col1.metric("⏱️ 耗時", f"{elapsed:.1f}s")
    col2.metric("🤖 LLM 呼叫", f"{len(_call_log)} 次")
    col3.metric("📝 I/O 字元", f"{(total_input_chars + total_output_chars):,}")

    # ── Detail expanders (按 pipeline 執行順序) ──

    # 1. 檢索
    with st.expander("🔍 檢索"):
        st.write(f"策略：{merged.get('strategy', '無')}")
        conds = merged.get("conditions", [])
        if conds:
            st.json(conds)
        retrieved = merged.get("retrieved_docs", [])
        if retrieved:
            st.write("檢索到的值：", retrieved)

    # 2. Schema Filter
    filtered = merged.get("filtered_schema", "")
    if filtered:
        with st.expander("🔧 Filtered Schema"):
            st.code(filtered, language="sql")

    # 3. Task Plan (QA)
    task_plan = merged.get("task_plan", "")
    if task_plan:
        with st.expander("🧠 Task Plan (QA)"):
            try:
                st.json(json.loads(task_plan))
            except (json.JSONDecodeError, TypeError):
                st.code(task_plan)

    # 4. SQL
    with st.expander("🗄️ SQL"):
        st.code(merged.get("sql", "N/A"), language="sql")
        sql_result = merged.get("sql_result", [])
        st.caption(f"查詢結果：{len(sql_result)} 筆")
        if sql_result and len(sql_result) <= 20:
            st.dataframe(sql_result)

    # 5. Python Code
    if merged.get("code"):
        with st.expander("🐍 Python Code"):
            st.code(merged["code"], language="python")

    # 6. Chart
    if merged.get("chart_code"):
        with st.expander("📈 Chart Code"):
            st.code(merged["chart_code"], language="python")
    if merged.get("chart_option"):
        with st.expander("📊 ECharts Option JSON"):
            st.code(merged["chart_option"], language="json")

    # 7. LLM Calls Detail
    if _call_log:
        with st.expander(f"🤖 LLM 呼叫明細（{len(_call_log)} 次）", expanded=False):
            for i, call in enumerate(_call_log):
                label = call["label"]
                purpose = call.get("purpose", "")
                purpose_str = f" — {purpose}" if purpose else ""
                st.markdown(
                    f"**Call #{i+1}** `{label}`{purpose_str} "
                    f"({call['input_len']:,} → {call['output_len']:,} chars)"
                )
                tabs = st.tabs(["Input", "Output"])
                with tabs[0]:
                    inp = call["input"]
                    if len(inp) > 8000:
                        st.code(
                            inp[:8000] + f"\n\n... ({len(inp):,} chars total)",
                            language="text",
                        )
                    else:
                        st.code(inp, language="text")
                with tabs[1]:
                    out = call["output"]
                    if len(out) > 8000:
                        st.code(
                            out[:8000] + f"\n\n... ({len(out):,} chars total)",
                            language="text",
                        )
                    else:
                        st.code(out, language="text")
                if i < len(_call_log) - 1:
                    st.divider()

    # ── Error ──
    if merged.get("error"):
        st.error(f"❌ {merged['error']}")
