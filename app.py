"""
LangGraph SQL+Python Pipeline — Streamlit UI

啟動：./run.sh
"""

import time
import base64
import streamlit as st
from langchain_community.callbacks import get_openai_callback
from pipeline import app
from logger import init_run_logger
from config import PRICE_INPUT, PRICE_OUTPUT

st.set_page_config(page_title="SQL+Python Pipeline", page_icon="🔍", layout="wide")
st.title("🔍 Text-to-SQL Pipeline")

question = st.chat_input("請輸入問題")

if question:
    with st.chat_message("user"):
        st.write(question)

    init_run_logger(question)
    start_time = time.time()
    process_status = st.status("處理中...", expanded=True)

    with get_openai_callback() as cb:
        steps = []
        merged = {}
        token_per_node = []
        prev_total = 0

        for event in app.stream({"question": question, "retry": 0}):
            for node_name, node_output in event.items():
                node_tokens = cb.total_tokens - prev_total
                node_prompt = cb.prompt_tokens - sum(t[1] for t in token_per_node)
                node_completion = cb.completion_tokens - sum(
                    t[2] for t in token_per_node
                )
                token_per_node.append(
                    (node_name, node_prompt, node_completion, node_tokens)
                )
                prev_total = cb.total_tokens

                steps.append((node_name, node_output))
                if isinstance(node_output, dict):
                    merged.update(node_output)

                with process_status:
                    for i, (name, _) in enumerate(steps):
                        tok = token_per_node[i][3] if i < len(token_per_node) else 0
                        icon = "✅" if i < len(steps) - 1 else "⏳"
                        tok_str = f" ({tok:,} tokens)" if tok > 0 else ""
                        st.write(f"{icon} {name}{tok_str}")

    elapsed = time.time() - start_time
    process_status.update(
        label=f"完成（{elapsed:.1f}s）", state="complete", expanded=False
    )

    # 回答
    with st.chat_message("assistant"):
        st.markdown(
            merged.get("display_answer") or merged.get("final_answer") or "無法回答"
        )

    # 圖表
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

    # 指標
    actual_cost = (
        cb.prompt_tokens * PRICE_INPUT + cb.completion_tokens * PRICE_OUTPUT
    ) / 1_000_000
    col1, col2, col3 = st.columns(3)
    col1.metric("⏱️ 耗時", f"{elapsed:.1f}s")
    col2.metric("🔤 Tokens", f"{cb.total_tokens:,}")
    col3.metric("💰 Cost", f"${actual_cost:.6f}")

    # 詳細資訊
    with st.expander("🗄️ SQL"):
        st.code(merged.get("sql", "N/A"), language="sql")
        st.caption(f"查詢結果：{len(merged.get('sql_result', []))} 筆")

    with st.expander("🔍 檢索"):
        st.write(f"策略：{merged.get('strategy', '無')}")
        conds = merged.get("conditions", [])
        if conds:
            st.json(conds)
        retrieved = merged.get("retrieved_docs", [])
        if retrieved:
            st.write("檢索到的值：", retrieved)

    if merged.get("code"):
        with st.expander("🐍 Python Code"):
            st.code(merged["code"], language="python")

    if merged.get("chart_code"):
        with st.expander("📈 Chart Code"):
            st.code(merged["chart_code"], language="python")

    if merged.get("chart_option"):
        with st.expander("📊 ECharts Option JSON"):
            st.code(merged["chart_option"], language="json")

    with st.expander("📊 Token 明細"):
        st.write(
            f"**總計** — Prompt: {cb.prompt_tokens:,} / Completion: {cb.completion_tokens:,} / Total: {cb.total_tokens:,}"
        )
        st.divider()
        for name, p, c, t in token_per_node:
            if t > 0:
                st.write(
                    f"**{name}** — Prompt: {p:,} / Completion: {c:,} / Total: {t:,}"
                )

    if merged.get("error"):
        st.error(f"❌ {merged['error']}")
