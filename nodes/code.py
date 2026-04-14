"""
Python code 生成、sandbox 執行節點
"""

import ast
import json
import os
from datetime import datetime, timedelta, date
from collections import Counter, defaultdict
from decimal import Decimal

from langchain_core.messages import HumanMessage

from llm import llm
from utils import debug_log, clean_llm_json, strip_code_fences

def _safe_import(name, *args, **kwargs):
    """只允許已在 SAFE_BUILTINS 中的模組的內部 import"""
    _ALLOWED = {"datetime", "collections", "decimal", "_datetime", "time"}
    if name in _ALLOWED:
        return __import__(name, *args, **kwargs)
    raise ImportError(f"import '{name}' 已停用，請使用已提供的內建函式")


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
    "next": next,
    "iter": iter,
    "Counter": Counter,
    "defaultdict": defaultdict,
    "Decimal": Decimal,
    "datetime": datetime,
    "timedelta": timedelta,
    "date": date,
    "ValueError": ValueError,
    "TypeError": TypeError,
    "KeyError": KeyError,
    "IndexError": IndexError,
    "AttributeError": AttributeError,
    "ZeroDivisionError": ZeroDivisionError,
    "__import__": _safe_import,
}


def _normalize_dates(data):
    """把 SQL 回傳的 datetime.datetime 統一轉成 datetime.date，避免型別比較錯誤"""
    if not isinstance(data, list):
        return data
    for row in data:
        if not isinstance(row, dict):
            continue
        for k, v in row.items():
            if isinstance(v, datetime) and not isinstance(v, date):
                row[k] = v.date()
    return data


def _validate_and_run(code, data):
    """在受限環境中執行 Python code"""
    data = _normalize_dates(data)
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
    except NameError as e:
        return False, f"{e}。請直接使用已提供的內建函式，不要使用 import", None, None
    except ImportError as e:
        return False, f"{e}。請直接使用已提供的內建函式，不要使用 import", None, None
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
    if not isinstance(sql_result, list):
        sql_result = [sql_result] if sql_result else []
    if not sql_result:
        return {"needs_code": False, "final_answer": "查無資料"}

    # 如果 QA node 已經決定不需要 Python，直接信任
    qa_needs = state.get("qa_needs_python")
    if qa_needs is False:
        # QA 說不需要 Python，讓 LLM 直接從 SQL 結果給答案
        sample = state.get("sample", [])
        if not isinstance(sample, list):
            sample = [sample] if sample else []
        sample_str = json.dumps(sample, indent=2, ensure_ascii=False, default=str)
        result_count = len(sql_result) if isinstance(sql_result, list) else 1
        prompt = f"""根據使用者問題和 SQL 查詢結果，直接給出答案。

使用者問題：{state["question"]}
SQL：{state["sql"]}
結果筆數：{result_count}
結果：
{sample_str}

只輸出純 JSON：
{{"direct_answer": "直接回答問題的答案"}}
"""
        debug_log("check_need_code", mode="qa_direct")
        res = llm.invoke([HumanMessage(content=prompt)])
        try:
            parsed = clean_llm_json(res.content)
            direct_answer = parsed.get("direct_answer", "")
        except (json.JSONDecodeError, KeyError):
            direct_answer = ""
        if direct_answer:
            return {"needs_code": False, "final_answer": direct_answer}
        return {"needs_code": True}

    # 如果 QA 說需要 Python，直接走 code 路徑，不再問 LLM
    if qa_needs is True:
        debug_log("check_need_code", mode="qa_force_code")
        return {"needs_code": True}

    # 沒有 QA 結果時，走原本的 LLM 判斷
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
        # 放寬條件：只要 LLM 給了 direct_answer 就信任它
        return {"needs_code": False, "final_answer": direct_answer}
    return {"needs_code": True}


def generate_code(state):
    # 如果沒有 SQL 結果，直接回傳查無資料
    sql_result = state.get("sql_result", [])
    if not isinstance(sql_result, list):
        sql_result = [sql_result] if sql_result else []
    if not sql_result:
        return {"final_answer": "查無資料", "error": ""}

    enable_chart = os.environ.get("ENABLE_CHART", "true").lower() in ("true", "1", "yes")

    error_context = ""
    if state.get("error") and state.get("code"):
        error_context = (
            f"\n上次的 code 執行失敗：\n錯誤：{state['error']}\n"
            f"上次的 code：\n{state['code']}\n請修正。\n"
        )

    # 從 task_plan 提取預期結果格式和 Python 任務
    task_plan = state.get("task_plan", "")
    result_guidance = ""
    if task_plan:
        try:
            plan = json.loads(task_plan)
            er = plan.get("expected_result", {})
            pt = plan.get("python_task", "")
            parts = []
            if er:
                parts.append(f"預期結果格式：{er.get('type', '')} — {er.get('description', '')}")
            if pt:
                parts.append(f"Python 任務：{pt}")
            if parts:
                result_guidance = "\n".join(parts)
        except (json.JSONDecodeError, KeyError):
            pass

    result_guidance_section = f"\n{result_guidance}\n" if result_guidance else ""

    sql_result = state.get("sql_result", [])
    sample = state.get("sample", [])
    if not isinstance(sql_result, list):
        sql_result = [sql_result] if sql_result else []
    if not isinstance(sample, list):
        sample = [sample] if sample else []

    chart_instruction = ""
    if enable_chart:
        chart_instruction = """4. 另外將適合畫圖的結構化資料存入 chart_data 變數（list of dict）
   - chart_data 的 key 必須用使用者看得懂的名稱，不要用 ID
   - 如果結果不適合畫圖（如單一數值、純列表、沒有數值比較），chart_data 設為空 list []"""

    prompt = f"""
問題：{state["question"]}

SQL 查詢已執行完成，data 變數中包含完整結果：
SQL：{state["sql"]}

資料筆數：{len(sql_result)}
資料欄位：{list(sample[0].keys()) if sample and isinstance(sample[0], dict) else []}
資料樣本：
{json.dumps(sample, indent=2, ensure_ascii=False, default=str)}
{result_guidance_section}
{error_context}
請寫 Python code 處理 data 變數中的資料：
1. 根據任務需求處理 data（計算、篩選、排序、整理格式等）
2. 將最終答案存入 result 變數
   - 如果 SQL 已經用 GROUP BY / ORDER BY / LIMIT / 聚合函數算出結果，不要再重複聚合，直接提取答案
   - 如果 SQL 回傳原始資料，按照 Python 任務描述進行處理
   - result 應該是完整的答案，包含所有需要的數值和名稱
3. 如果資料中有 None 值，做數學運算前要先過濾掉，不要省略
{chart_instruction}

重要提醒：
- data 是 list of dict，每個 dict 的 key 就是 SQL SELECT 的欄位名（小寫）
- 如果問題問的是「數量」或「有多少」，通常是 len(filtered_data) 或 sum，不要直接用 SQL 回傳的筆數
- 如果問題問的是「比例」或「百分比」，確認分子和分母都從 data 中正確計算
- 不能使用 import 語句
- Counter、defaultdict、Decimal、datetime、timedelta、date 已可直接使用
- SQL 回傳的日期欄位已統一為 datetime.date 物件，直接用 < > == 比較即可
- 不要對 date 物件呼叫 .date() 方法或做字串解析
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
