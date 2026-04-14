"""
答案格式化節點
"""

from langchain_core.messages import HumanMessage

from llm import llm
from utils import debug_log


def format_answer(state):
    final = state.get("final_answer", "")
    if not final:
        return {"display_answer": "無法回答此問題。"}
    final = str(final)  # 確保是字串

    # 如果 final_answer 是簡短的數值或短文字，直接用，不經過 LLM 重新詮釋
    # 避免 LLM 把數值誤解成其他意思（如 5.0 → 「5筆」）
    stripped = final.strip()
    try:
        float(stripped)
        is_short_value = True
    except ValueError:
        is_short_value = len(stripped) <= 50 and "\n" not in stripped

    if is_short_value:
        # 短答案：用 LLM 組成自然語言，但強調「分析結果就是答案本身」
        prompt = f"""你是一個資料分析助手。根據使用者的問題和分析結果，用自然、易懂的繁體中文回答。

規則：
- 分析結果就是最終答案的數值或名稱，直接用它回答問題
- 不要重新解讀或推測分析結果的含義
- 直接回答問題，不要提及 SQL、Python、資料庫等技術細節
- 保持簡潔

使用者問題：{state["question"]}
分析結果（即答案）：{stripped}
"""
    else:
        # 長答案：原本的邏輯
        if len(final) <= 2000:
            truncated = final
        else:
            truncated = final[:2000] + f"...\n（共 {len(final)} 字元）"

        prompt = f"""你是一個資料分析助手。根據使用者的問題和分析結果，用自然、易懂的繁體中文回答。

規則：
- 直接回答問題，不要提及 SQL、Python、資料庫等技術細節
- 必須保留分析結果中的具體數值，不要用「約」「大約」「數千」等模糊描述
- 如果結果是列表，列出所有項目（或至少前 20 筆）
- 如果結果是單一數值，直接回答該數值
- 保持簡潔但完整

使用者問題：{state["question"]}
分析結果：{truncated}
"""
    debug_log("format_answer", prompt=prompt)
    res = llm.invoke([HumanMessage(content=prompt)])
    return {"display_answer": res.content.strip()}
