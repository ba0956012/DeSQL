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

    # 如果 final_answer 夠短，直接傳給 LLM 格式化，不截斷
    # 避免 LLM 摘要化導致具體數值丟失
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
