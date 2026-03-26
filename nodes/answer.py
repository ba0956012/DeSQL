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
    truncated = (
        final[:500] + f"...\n（共 {len(final)} 字元）" if len(final) > 500 else final
    )

    prompt = f"""你是一個資料分析助手。根據使用者的問題和分析結果，用自然、易懂的繁體中文回答。

規則：
- 直接回答問題，不要提及 SQL、Python、資料庫等技術細節
- 如果結果包含數字，適當加上單位或格式化
- 如果資料筆數很多，摘要說明重點即可
- 保持簡潔

使用者問題：{state["question"]}
分析結果：{truncated}
"""
    debug_log("format_answer", prompt=prompt)
    res = llm.invoke([HumanMessage(content=prompt)])
    return {"display_answer": res.content.strip()}
