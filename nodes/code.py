"""
Python code 生成、sandbox 執行節點
"""

import ast
import json
from datetime import datetime, timedelta, date
from collections import Counter, defaultdict
from decimal import Decimal

from langchain_core.messages import HumanMessage

from llm import llm
from utils import debug_log, clean_llm_json, strip_code_fences

# 安全的 builtins 白名單
SAFE_BUILTINS = {
    "len": len,
    "sum": sum,
    "min": min,
    "max": max,
    "sorted": sorted,
    "reversed": reversed,
    "enumerate": enumerate,
    "zip": zip,
    "map": map,
    "filter": filter,
    "range": range,
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
    "list": list,
    "dict": dict,
    "set": set,
    "tuple": tuple,
    "round": round,
    "abs": abs,
    "any": any,
    "all": all,
    "isinstance": isinstance,
    "hasattr": hasattr,
    "getattr": getattr,
    "type": type,
    "print": print,
    "Counter": Counter,
    "defaultdict": defaultdict,
    "Decimal": Decimal,
    "datetime": datetime,
    "timedelta": timedelta,
    "date": date,
}


def _validate_and_run(code, data):
    """在受限環境中執行 Python code"""
    lines = code.split("\n")
    lines = [ln for ln in lines if not ln.strip().startswith(("import ", "from "))]
    code = "\n".join(lines)
    try:
        ast.parse(code)
    except Exception as e:
        return False, str(e), None, None
    namespace = {"__builtins__": SAFE_BUILTINS, "data": data}
    try:
        exec(code, namespace)
    except Exception as e:
        return False, str(e), None, None
    if "result" not in namespace:
        return False, "missing result", None, None
    return True, "OK", namespace["result"], namespace.get("chart_data", [])


def check_need_code(state):
    if state.get("error"):
        debug_log("check_need_code", skip="has error")
        return {"needs_code": True}
    sql_result = state.get("sql_result", [])
    if not sql_result:
        return {"needs_code": False, "final_answer": "查無資料"}

    sample_str = json.dumps(state["sample"], indent=2, ensure_ascii=False, default=str)
    prompt = f"""根據使用者問題和 SQL 查詢結果，判斷是否需要進一步用 Python 處理。

判斷標準：
- 結果只有 1 行且欄位名稱已清楚表達答案 → 直接回答
- 結果是簡單列表且問題只是「有哪些」→ 直接回答
- 需要計算、比較、排名、篩選 → 需要 Python

使用者問題：{state["question"]}
SQL：{state["sql"]}
結果筆數：{len(sql_result)}
結果樣本：
{sample_str}

只輸出純 JSON：
{{"needs_code": true/false, "direct_answer": "如果不需要 code，直接給出答案；否則空字串"}}
"""
    debug_log("check_need_code", prompt=prompt)
    res = llm.invoke([HumanMessage(content=prompt)])
    try:
        parsed = clean_llm_json(res.content)
        needs_code = parsed.get("needs_code", True)
        direct_answer = parsed.get("direct_answer", "")
    except (json.JSONDecodeError, KeyError):
        needs_code = True
        direct_answer = ""

    if not needs_code and direct_answer:
        has_data = any(c.isdigit() for c in str(direct_answer)) or len(str(direct_answer)) > 100
        if has_data:
            return {"needs_code": False, "final_answer": direct_answer}
        debug_log("check_need_code", override="direct_answer too vague")
    return {"needs_code": True}


def generate_code(state):
    error_context = ""
    if state.get("error"):
        error_context = (
            f"\n上次的 code 執行失敗：\n錯誤：{state['error']}\n"
            f"上次的 code：\n{state['code']}\n請修正。\n"
        )

    prompt = f"""
問題：{state["question"]}

SQL 查詢已執行完成，data 變數中包含完整結果：
SQL：{state["sql"]}

資料樣本（共 {len(state.get("sql_result", []))} 筆）：
{json.dumps(state["sample"], indent=2, ensure_ascii=False, default=str)}
{f"處理規劃：{chr(10)}{state['plan']}{chr(10)}" if state.get("plan") else ""}
{error_context}
請寫 Python code 處理 data 變數中的資料：
1. 將最終答案存入 result 變數
2. 另外將適合畫圖的結構化資料存入 chart_data 變數（list of dict）
   - chart_data 的 key 必須用使用者看得懂的名稱，不要用 ID
   - 如果結果不適合畫圖（如單一數值、純列表、沒有數值比較），chart_data 設為空 list []

只輸出純 Python code，不要 markdown。
注意：
- 不能使用 import 語句
- Counter、defaultdict、Decimal、datetime、timedelta、date 已可直接使用
- SQL 回傳的日期/時間欄位可能是 datetime 物件或字串，使用前先確認型別
- 不要對 datetime 物件做字串解析，直接使用
"""
    debug_log("generate_code", prompt=prompt)
    res = llm.invoke([HumanMessage(content=prompt)])
    code = strip_code_fences(res.content)
    return {"code": code}


def run_code(state):
    debug_log("run_code", code=state["code"])
    ok, msg, result, chart_data = _validate_and_run(state["code"], state["sql_result"])
    debug_log("run_code", ok=ok, msg=msg, result=result)
    if not ok:
        return {"error": msg, "retry": state.get("retry", 0) + 1}
    output = {"final_answer": str(result), "error": ""}
    if chart_data:
        output["chart_data"] = chart_data
    return output
