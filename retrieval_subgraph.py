"""
關鍵字漸進式檢索 Subgraph

提供 analyze_conditions + 漸進式 keyword 檢索（PHRASE → AND → SYNONYM → OR）。
可被任何 LangGraph pipeline import 使用。

用法：
    from keyword_retrieval_subgraph import build_retrieval_subgraph, RetrievalState

    # 建立 subgraph（注入依賴）
    retrieval_graph = build_retrieval_subgraph(
        llm=your_llm,
        engine=your_engine,
        schema_info=your_schema_info,
        enum_values=your_enum_values,
    )

    # 在 parent graph 中當作一個 node 使用
    parent_graph.add_node("retrieval", retrieval_graph)
"""

import json
import logging
from typing import TypedDict

from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, END
from sqlalchemy import text as sa_text

_logger = logging.getLogger("langgraph_sql_python")


def _clean_llm_json(text: str) -> dict:
    """清除 LLM 回傳中的 markdown 包裹，解析 JSON"""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return json.loads(text.strip())


def _debug_log(node_name: str, **kwargs):
    sep = "=" * 60
    _logger.debug(f"\n{sep}")
    _logger.debug(f"🐛 [{node_name}]")
    _logger.debug(sep)
    for key, value in kwargs.items():
        val_str = str(value)
        if len(val_str) > 500:
            val_str = val_str[:500] + "... (truncated)"
        _logger.debug(f"  {key}: {val_str}")
    _logger.debug(sep)


# =========================
# 📦 Subgraph State
# =========================
class RetrievalState(TypedDict):
    question: str
    # 條件分析結果
    conditions: list  # [{"type": "enum"|"keyword"|"range"|"none", ...}]
    # 檢索用（keyword 類條件）
    keyword: str
    search_table: str
    search_column: str
    tokens: list
    synonyms: list
    retrieved_docs: list
    strategy: str
    schema_desc: str


def format_enum_info(enum_values: dict) -> str:
    """格式化 enum 資訊供 prompt 使用"""
    if not enum_values:
        return ""
    lines = ["以下欄位的所有可能值（可直接用於 WHERE 條件，不需要模糊搜尋）："]
    for col, vals in enum_values.items():
        lines.append(f"  {col}: {vals}")
    return "\n".join(lines)


def build_conditions_context(state: RetrievalState) -> str:
    """把 conditions 和 retrieved_docs 組合成 SQL 生成的上下文"""
    parts = []
    conditions = state.get("conditions", [])

    # enum 條件
    enum_conds = [c for c in conditions if c.get("type") == "enum"]
    if enum_conds:
        lines = ["以下是已確認的精確值條件（直接用於 WHERE）："]
        for c in enum_conds:
            lines.append(f"  {c['table']}.{c['column']} = '{c['value']}'")
        parts.append("\n".join(lines))

    # keyword 檢索結果
    retrieved = state.get("retrieved_docs", [])
    keyword_cond = next((c for c in conditions if c.get("type") == "keyword"), None)
    if retrieved and keyword_cond:
        kw = keyword_cond.get("keyword", "")
        tbl = keyword_cond.get("table", "")
        col = keyword_cond.get("column", "")
        tokens = state.get("tokens", [])

        # 判斷是否有精確匹配（檢索結果中有值完全包含關鍵字）
        exact_match = next((v for v in retrieved if kw.lower() == v.lower()), None)
        if not exact_match:
            exact_match = next((v for v in retrieved if kw.lower() in v.lower() and len(v) < len(kw) + 10), None)

        if exact_match:
            # 精確匹配 → 用 = 
            parts.append(
                f"以下是用關鍵字「{kw}」從 {tbl}.{col} 搜尋到的精確匹配值：\n"
                + json.dumps(retrieved, ensure_ascii=False)
                + f"\nSQL 的 WHERE 條件應使用精確匹配：WHERE {col} = '{exact_match}'"
            )
        else:
            # 模糊匹配 → 用 ILIKE
            if tokens:
                ilike_parts = " AND ".join([f"{col} ILIKE '%{t}%'" for t in tokens])
                token_hint = f"\n建議的 WHERE 寫法：WHERE {ilike_parts}"
            else:
                token_hint = f"\n建議的 WHERE 寫法：WHERE {col} ILIKE '%{kw}%'"
            parts.append(
                f"以下是用關鍵字「{kw}」從 {tbl}.{col} 模糊搜尋到的樣本（僅供參考，實際可能更多）：\n"
                + json.dumps(retrieved, ensure_ascii=False)
                + "\n"
                "SQL 的 WHERE 條件應使用 ILIKE 模糊搜尋，將關鍵字拆成多個詞分別匹配（用 AND 連接），不要用 IN 精確匹配。"
                + token_hint
            )

    # range 條件
    range_conds = [c for c in conditions if c.get("type") == "range"]
    if range_conds:
        lines = ["以下是數值/時間範圍條件（轉換為 SQL WHERE 條件）："]
        for c in range_conds:
            lines.append(f"  {c.get('description', '')}")
        parts.append("\n".join(lines))

    return "\n\n".join(parts)


# =========================
# 🏗️ Subgraph Builder
# =========================
def build_retrieval_subgraph(llm, engine, schema_info: str, enum_values: dict):
    """
    建立關鍵字檢索 subgraph。

    Args:
        llm: LangChain ChatModel（用於條件分析、拆詞、同義詞）
        engine: SQLAlchemy engine（用於 DB 查詢）
        schema_info: DB schema 字串
        enum_values: {table.column: [values]} 低基數欄位值

    Returns:
        compiled subgraph，可直接當 node 加入 parent graph
    """
    enum_info = format_enum_info(enum_values)

    # ----- Node functions（閉包，捕獲注入的依賴）-----

    def analyze_conditions(state: RetrievalState):
        schema_desc = state.get("schema_desc", "")
        schema_desc_section = f"\n欄位說明：\n{schema_desc}\n" if schema_desc else ""

        prompt = f"""根據使用者問題和資料庫 schema，分析問題中涉及的所有篩選條件。

每個條件歸類為以下 type 之一：
- "enum"：使用者提到的值可以在「已知欄位值」中找到精確或近似匹配，直接用已知值。注意：使用者的用詞可能和資料庫不完全一致（如「電器類」對應「家電商品類」），請用語意判斷找出最接近的已知值
- "keyword"：使用者提到特定名稱（如商品名、品牌名）需要模糊搜尋才能找到
- "range"：數值或時間範圍條件（如「2000元以上」「上個月」）
- "none"：純計算/統計問題，不需要任何值篩選

規則：
- 一個問題可能有多個條件，全部列出
- 如果完全不需要篩選，回傳一個 type=none 的條件即可
- enum 類條件必須附上從已知欄位值中匹配到的精確值
- 優先嘗試 enum 匹配，只有在已知欄位值中完全找不到相關值時才歸類為 keyword

資料庫 Schema：
{schema_info}
{schema_desc_section}
{enum_info}

使用者問題：{state["question"]}

只輸出純 JSON，不要 markdown 格式、不要 ```、不要任何解釋：
{{"conditions": [
  {{"type": "enum", "table": "表名", "column": "欄位名", "value": "精確值"}},
  {{"type": "keyword", "table": "表名", "column": "欄位名", "keyword": "搜尋關鍵字"}},
  {{"type": "range", "description": "條件描述"}},
  {{"type": "none"}}
]}}
"""
        _debug_log("analyze_conditions", prompt=prompt)
        res = llm.invoke([HumanMessage(content=prompt)])
        _debug_log("analyze_conditions", llm_response=res.content)

        try:
            parsed = _clean_llm_json(res.content)
            conditions = parsed.get("conditions", [])
        except (json.JSONDecodeError, KeyError) as e:
            _debug_log("analyze_conditions", parse_error=str(e))
            conditions = [
                {
                    "type": "keyword",
                    "table": "product",
                    "column": "product_name",
                    "keyword": state["question"],
                }
            ]

        keyword_cond = next((c for c in conditions if c.get("type") == "keyword"), None)
        result = {"conditions": conditions}
        if keyword_cond:
            result["keyword"] = keyword_cond.get("keyword", "")
            result["search_table"] = keyword_cond.get("table", "")
            result["search_column"] = keyword_cond.get("column", "")

        _debug_log("analyze_conditions", output=result)
        return result

    def route_after_analyze(state: RetrievalState):
        conditions = state.get("conditions", [])
        has_keyword = any(c.get("type") == "keyword" for c in conditions)
        return "retrieve_phrase" if has_keyword else END

    def retrieve_phrase(state: RetrievalState):
        keyword = state.get("keyword", "")
        table = state.get("search_table", "")
        column = state.get("search_column", "")

        if not keyword or not table or not column:
            _debug_log("retrieve_phrase", skip="missing keyword/table/column")
            return {"retrieved_docs": [], "strategy": "PHRASE"}

        sql = f"SELECT {column} FROM {table} WHERE {column} ILIKE '%{keyword}%' LIMIT 5"
        _debug_log("retrieve_phrase", sql=sql)
        try:
            with engine.connect() as conn:
                rows = conn.execute(sa_text(sql)).fetchall()
            docs = [r[0] for r in rows]
        except Exception as e:
            _debug_log("retrieve_phrase", error=str(e))
            docs = []

        _debug_log("retrieve_phrase", docs=docs)
        return {"retrieved_docs": docs, "strategy": "PHRASE"}

    def check_phrase(state: RetrievalState):
        return END if state.get("retrieved_docs") else "tokenize"

    def tokenize(state: RetrievalState):
        prompt = f"""
請將以下詞拆成關鍵詞（最小單位）：

{state.get("keyword", "")}

只輸出純 JSON，不要 markdown 格式、不要 ```、不要任何解釋：
{{"tokens": []}}
"""
        _debug_log("tokenize", prompt=prompt)
        res = llm.invoke([HumanMessage(content=prompt)])
        _debug_log("tokenize", llm_response=res.content)

        try:
            tokens = _clean_llm_json(res.content)["tokens"]
        except (json.JSONDecodeError, KeyError):
            tokens = []

        _debug_log("tokenize", tokens=tokens)
        return {"tokens": tokens}

    def retrieve_and(state: RetrievalState):
        tokens = state.get("tokens", [])
        table = state.get("search_table", "")
        column = state.get("search_column", "")

        if not tokens:
            _debug_log("retrieve_and", skip="no tokens")
            return {"retrieved_docs": []}

        conds = " AND ".join([f"{column} ILIKE '%{t}%'" for t in tokens])
        sql = f"SELECT {column} FROM {table} WHERE {conds} LIMIT 5"
        _debug_log("retrieve_and", sql=sql)
        try:
            with engine.connect() as conn:
                rows = conn.execute(sa_text(sql)).fetchall()
            docs = [r[0] for r in rows]
        except Exception as e:
            _debug_log("retrieve_and", error=str(e))
            docs = []

        _debug_log("retrieve_and", docs=docs)
        return {"retrieved_docs": docs, "strategy": "AND"}

    def check_and(state: RetrievalState):
        return END if state.get("retrieved_docs") else "expand_synonyms"

    def expand_synonyms(state: RetrievalState):
        prompt = f"""
請為以下詞產生搜尋同義詞：

{state.get("keyword", "")}

只輸出純 JSON，不要 markdown 格式、不要 ```、不要任何解釋：
{{"keywords": []}}
"""
        _debug_log("expand_synonyms", prompt=prompt)
        res = llm.invoke([HumanMessage(content=prompt)])
        _debug_log("expand_synonyms", llm_response=res.content)

        try:
            synonyms = _clean_llm_json(res.content)["keywords"]
        except (json.JSONDecodeError, KeyError):
            synonyms = []

        _debug_log("expand_synonyms", synonyms=synonyms)
        return {"synonyms": synonyms}

    def retrieve_synonym(state: RetrievalState):
        synonyms = state.get("synonyms", [])
        table = state.get("search_table", "")
        column = state.get("search_column", "")

        if not synonyms:
            _debug_log("retrieve_synonym", skip="no synonyms")
            return {"retrieved_docs": []}

        conds = " OR ".join([f"{column} ILIKE '%{k}%'" for k in synonyms])
        sql = f"SELECT {column} FROM {table} WHERE ({conds}) LIMIT 5"
        _debug_log("retrieve_synonym", sql=sql)
        try:
            with engine.connect() as conn:
                rows = conn.execute(sa_text(sql)).fetchall()
            docs = [r[0] for r in rows]
        except Exception as e:
            _debug_log("retrieve_synonym", error=str(e))
            docs = []

        _debug_log("retrieve_synonym", docs=docs)
        return {"retrieved_docs": docs, "strategy": "SYNONYM"}

    def check_synonym(state: RetrievalState):
        return END if state.get("retrieved_docs") else "retrieve_or"

    def retrieve_or(state: RetrievalState):
        tokens = state.get("tokens", [])
        table = state.get("search_table", "")
        column = state.get("search_column", "")

        if not tokens:
            _debug_log("retrieve_or", skip="no tokens")
            return {"retrieved_docs": []}

        conds = " OR ".join([f"{column} ILIKE '%{t}%'" for t in tokens])
        sql = f"SELECT {column} FROM {table} WHERE {conds} LIMIT 5"
        _debug_log("retrieve_or", sql=sql)
        try:
            with engine.connect() as conn:
                rows = conn.execute(sa_text(sql)).fetchall()
            docs = [r[0] for r in rows]
        except Exception as e:
            _debug_log("retrieve_or", error=str(e))
            docs = []

        _debug_log("retrieve_or", docs=docs)
        return {"retrieved_docs": docs, "strategy": "OR"}

    # ----- 組裝 subgraph -----

    sg = StateGraph(RetrievalState)

    sg.add_node("analyze_conditions", analyze_conditions)
    sg.add_node("retrieve_phrase", retrieve_phrase)
    sg.add_node("tokenize", tokenize)
    sg.add_node("retrieve_and", retrieve_and)
    sg.add_node("expand_synonyms", expand_synonyms)
    sg.add_node("retrieve_synonym", retrieve_synonym)
    sg.add_node("retrieve_or", retrieve_or)

    sg.set_entry_point("analyze_conditions")

    sg.add_conditional_edges("analyze_conditions", route_after_analyze)
    sg.add_conditional_edges("retrieve_phrase", check_phrase)
    sg.add_edge("tokenize", "retrieve_and")
    sg.add_conditional_edges("retrieve_and", check_and)
    sg.add_edge("expand_synonyms", "retrieve_synonym")
    sg.add_conditional_edges("retrieve_synonym", check_synonym)
    sg.add_conditional_edges(
        "retrieve_or", lambda s: END
    )  # OR 是最後一步，無論如何都結束

    return sg.compile()
