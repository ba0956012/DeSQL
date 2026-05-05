"""
Context Merger Node

在 schema_filter 之後、QA 之前，整合所有上下文資訊：
- question + hint
- filtered schema (with column descs)
- retrieval conditions (enum/keyword matches)
- semantic_notes (column facts)

產生一份精簡的 qa_context 供 QA 使用。
"""

import json
from langchain_core.messages import HumanMessage

from db import SCHEMA_INFO
from llm import qa_llm as llm
from utils import debug_log
from config import PROMPT_LANG
from retrieval_subgraph import format_enum_info, build_conditions_context
from db import ENUM_VALUES


def context_merger(state):
    """整合所有上下文，產生精簡的 qa_context"""
    question = state["question"]

    # 收集所有可用資訊
    conditions_context = build_conditions_context(state)

    # schema（優先用 filtered，否則原始）
    base_schema = state.get("filtered_schema") or SCHEMA_INFO

    # column descs 嵌入 schema
    column_descs = state.get("column_descs")
    if column_descs:
        from nodes.question_analysis import _embed_descs

        schema_text = _embed_descs(base_schema, column_descs)
    else:
        schema_text = base_schema

    semantic_notes = state.get("semantic_notes", "")

    # 組合所有資訊讓 LLM 整合
    parts = [f"Database Schema:\n{schema_text}"]
    if conditions_context:
        parts.append(f"Retrieved Values:\n{conditions_context}")
    if semantic_notes:
        parts.append(f"Column Facts:\n{semantic_notes}")

    all_context = "\n\n".join(parts)

    if PROMPT_LANG == "en":
        prompt = f"""You are preparing context for a data analyst who will create a query plan.

Given the user's question and all available database information below, produce a concise context summary that includes:
1. Which tables and columns are relevant (with their meanings)
2. Any confirmed values from the database that match entities in the question
3. Key column definitions or formulas from the descriptions
4. Any potential pitfalls (e.g., similar column names with different meanings)

Be factual. Do not guess or infer beyond what the schema and descriptions state.

User Question: {question}

Available Information:
{all_context}

Output a concise context summary (not JSON, just clear text):"""
    else:
        prompt = f"""你正在為資料分析師準備上下文，他將根據這些資訊建立查詢計畫。

根據使用者的問題和以下所有資料庫資訊，產生一份精簡的上下文摘要，包含：
1. 哪些表和欄位相關（附上含義）
2. 從資料庫中確認的、與問題中實體匹配的值
3. 欄位描述中的關鍵定義或公式
4. 潛在的陷阱（如名稱相似但含義不同的欄位）

只陳述事實，不要猜測或推論超出 schema 和描述所述的內容。

使用者問題：{question}

可用資訊：
{all_context}

輸出精簡的上下文摘要（不要 JSON，用清楚的文字）："""

    debug_log("context_merger", prompt=prompt[:300])
    try:
        res = llm.invoke([HumanMessage(content=prompt)])
        qa_context = res.content.strip()
        debug_log("context_merger", qa_context=qa_context[:300])
        return {"qa_context": qa_context}
    except Exception as e:
        debug_log("context_merger", error=str(e))
        return {"qa_context": ""}
