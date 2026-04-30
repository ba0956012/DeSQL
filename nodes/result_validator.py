"""
結果驗證節點 — 檢查 code 計算結果是否合理，不合理則觸發重試
"""

import json
import os
from langchain_core.messages import HumanMessage

from llm import llm as validator_llm
from utils import debug_log

ENABLE_RESULT_VALIDATION = os.getenv("ENABLE_RESULT_VALIDATION", "false").lower() in ("true", "1", "yes")


def _build_data_profile(sql_result):
    """Build a concise data profile with distinct values for the validator."""
    if not sql_result or not isinstance(sql_result, list):
        return ""
    if not isinstance(sql_result[0], dict):
        return ""

    cols = list(sql_result[0].keys())
    total = len(sql_result)
    lines = []

    for col in cols:
        vals = [r.get(col) for r in sql_result if r.get(col) is not None]
        unique_vals = set(str(v) for v in vals)
        unique_count = len(unique_vals)
        null_count = total - len(vals)

        parts = [f"{col}: {unique_count} unique, {null_count} nulls"]

        # Show distinct values if few enough (categorical columns)
        if unique_count <= 15:
            sorted_vals = sorted(unique_vals)
            parts.append(f"values=[{', '.join(sorted_vals[:15])}]")
        else:
            # Show min/max for numeric-looking columns
            numeric_vals = []
            for v in vals[:100]:
                try:
                    numeric_vals.append(float(v))
                except (ValueError, TypeError):
                    break
            if len(numeric_vals) > 5:
                parts.append(f"range=[{min(numeric_vals):.2f} ~ {max(numeric_vals):.2f}]")

        lines.append(", ".join(parts))

    return "Data profile (full dataset):\n" + "\n".join(f"  {line}" for line in lines)


VALIDATE_PROMPT = """你是一個資料分析結果的驗證器。請檢查以下 Python 計算結果是否有明顯的邏輯錯誤。

重要：你只能看到前 5 筆資料樣本，但下方提供了完整資料的 profile（包含各欄位的 distinct values）。請根據 data profile 做判斷，不要只看 sample。

問題：{question}

SQL 查詢：{sql}
資料筆數：{data_len}
資料樣本（前 5 筆）：
{sample}

{data_profile}

Python code：
{code}

計算結果：{result}

只在以下情況判定 valid=false（必須非常確定）：
1. 分類統計遺漏：問題明確問「A 和 B 各有多少」或「how many X and Y」，但 code 只回傳一個總數（如 len(data) 或單一數字），沒有分別列出各類別的數量
2. 方向性明確錯誤：問題說「A 比 B 多多少」，code 卻算了 B-A（或反之）
3. 百分比公式明確錯誤：分子分母明顯搞反，或忘了乘 100
4. 結果為 0 或 None 但 data profile 顯示該欄位確實有值（filter 條件和資料不匹配）

不要判定為 invalid 的情況：
- 結果數值和你預期不同（你無法驗算完整資料）
- code 沒有處理 null（如果結果看起來合理就放行）
- 結果格式不夠完美但包含了正確資訊
- 你不確定是否有問題（寧可放過）

回覆 JSON 格式：
{{"valid": true/false, "issue": "問題描述（如果 valid=false）", "suggestion": "修正建議（如果 valid=false）"}}

只輸出 JSON，不要其他文字。"""


def validate_result(state):
    """驗證 code 執行結果是否合理。只在有結果且未驗證過時執行。"""
    # 功能開關
    if not ENABLE_RESULT_VALIDATION:
        return {}

    # 如果已經驗證重試過，不再驗證（避免無限循環）
    if state.get("result_validated", False):
        return {}

    final_answer = state.get("final_answer", "")
    code = state.get("code", "")
    if not final_answer or not code:
        return {}

    sample = state.get("sample", [])
    sample_str = json.dumps(sample, indent=2, ensure_ascii=False, default=str)

    # Build richer data context for validator
    sql_result = state.get("sql_result", [])
    data_profile = _build_data_profile(sql_result)

    prompt = VALIDATE_PROMPT.format(
        question=state["question"],
        sql=state.get("sql", ""),
        data_len=len(sql_result),
        sample=sample_str,
        data_profile=data_profile,
        code=code,
        result=final_answer[:500],
    )

    debug_log("validate_result", prompt=prompt[:400])

    try:
        res = validator_llm.invoke([HumanMessage(content=prompt)])
        content = res.content.strip()
        # 清除 markdown fences
        if content.startswith("```"):
            content = content.split("\n", 1)[1] if "\n" in content else content[3:]
        if content.endswith("```"):
            content = content[:-3]
        result = json.loads(content.strip())
    except (json.JSONDecodeError, Exception) as e:
        debug_log("validate_result", error=str(e))
        # 解析失敗就放行
        return {"result_validated": True}

    debug_log("validate_result", valid=result.get("valid"), issue=result.get("issue", ""))

    if result.get("valid", True):
        return {"result_validated": True, "error": ""}

    # 驗證失敗 → 設定 error 讓 code 重試，帶上修正建議
    issue = result.get("issue", "結果可能有誤")
    suggestion = result.get("suggestion", "")
    error_msg = f"結果驗證失敗：{issue}"
    if suggestion:
        error_msg += f"\n修正建議：{suggestion}"

    debug_log("validate_result", action="trigger_retry", error_msg=error_msg)

    return {
        "error": error_msg,
        "retry": state.get("retry", 0),  # 不增加 retry，讓 should_retry 判斷
        "result_validated": True,  # 標記已驗證過，避免循環
        "final_answer": "",  # 清除錯誤結果
    }
