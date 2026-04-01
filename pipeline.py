"""
LangGraph Pipeline — Graph 組裝

所有節點邏輯已拆分至 nodes/ 目錄，
本檔只負責定義 State、routing、組裝 graph。
"""

from typing import TypedDict, Any
import os

from langgraph.graph import StateGraph, END

from db import engine, SCHEMA_INFO, ENUM_VALUES
from llm import llm
from retrieval_subgraph import build_retrieval_subgraph
from nodes.sql import generate_sql, execute_sql, validate_sql_result
from nodes.code import check_need_code, generate_code, run_code
from nodes.answer import format_answer
CHART_ENGINE = os.getenv("CHART_ENGINE", "matplotlib")  # matplotlib | echarts
if CHART_ENGINE == "echarts":
    from nodes.chart_echarts import generate_chart
else:
    from nodes.chart import generate_chart
from utils import debug_log


# =========================
# 📦 State
# =========================
class State(TypedDict):
    question: str
    conditions: list
    keyword: str
    search_table: str
    search_column: str
    tokens: list
    synonyms: list
    retrieved_docs: list
    strategy: str
    plan: str
    sql: str
    sql_result: Any
    sample: Any
    needs_code: bool
    code: str
    error: str
    retry: int
    sql_retry: int
    final_answer: str
    display_answer: str
    chart_data: list
    chart_reason: str
    chart_code: str
    chart_option: str
    chart_image: str
    chart_html: str
    schema_desc: str
    sql_validation: str


# =========================
# 🔀 Routing
# =========================
def route_after_retrieval(state: State):
    return "generate_sql"


def route_after_execute(state: State):
    if state.get("error") and state.get("sql_retry", 0) < 2:
        debug_log(
            "route_after_execute", action="retry SQL", sql_retry=state.get("sql_retry")
        )
        return "generate_sql"
    # 查詢成功但 0 筆結果 → 可能 WHERE 條件太嚴格，重試一次
    if not state.get("error") and not state.get("sql_result") and state.get("sql_retry", 0) <= 1:
        debug_log(
            "route_after_execute", action="retry SQL (empty result)", sql_retry=state.get("sql_retry")
        )
        return "generate_sql"
    return "validate_sql_result"


def route_after_validate(state: State):
    if state.get("error") and state.get("sql_validation"):
        debug_log(
            "route_after_validate", action="retry SQL (incomplete)", missing=state.get("sql_validation")
        )
        return "generate_sql"
    return "check_need_code"


def route_after_check(state: State):
    if state.get("final_answer"):
        return "format_answer"
    return "generate_code"


def should_retry(state: State):
    if state.get("error") and state.get("retry", 0) < 2:
        return "generate_code"
    return "format_answer"


# =========================
# 🧠 Retrieval Subgraph
# =========================
retrieval_subgraph = build_retrieval_subgraph(
    llm=llm,
    engine=engine,
    schema_info=SCHEMA_INFO,
    enum_values=ENUM_VALUES,
)

# =========================
# 🔗 Graph 組裝
# =========================
graph = StateGraph(State)

graph.add_node("retrieval", retrieval_subgraph)
graph.add_node("generate_sql", generate_sql)
graph.add_node("execute_sql", execute_sql)
graph.add_node("validate_sql_result", validate_sql_result)
graph.add_node("check_need_code", check_need_code)
graph.add_node("generate_code", generate_code)
graph.add_node("run_code", run_code)
graph.add_node("format_answer", format_answer)

ENABLE_CHART = os.getenv("ENABLE_CHART", "true").lower() in ("true", "1", "yes")
if ENABLE_CHART:
    graph.add_node("generate_chart", generate_chart)

graph.set_entry_point("retrieval")
graph.add_conditional_edges("retrieval", route_after_retrieval)
graph.add_edge("generate_sql", "execute_sql")
graph.add_conditional_edges("execute_sql", route_after_execute)
graph.add_conditional_edges("validate_sql_result", route_after_validate)
graph.add_conditional_edges("check_need_code", route_after_check)
graph.add_edge("generate_code", "run_code")
graph.add_conditional_edges("run_code", should_retry)

if ENABLE_CHART:
    graph.add_edge("format_answer", "generate_chart")
    graph.add_edge("generate_chart", END)
else:
    graph.add_edge("format_answer", END)

app = graph.compile()
