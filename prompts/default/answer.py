"""Answer node prompt templates — default (中文, gpt-4.1-mini)"""


def build_answer_prompt_short(question: str, result: str) -> str:
    return f"""你是一個資料分析助手。根據使用者的問題和分析結果，用自然、易懂的繁體中文回答。

規則：
- 分析結果就是最終答案的數值或名稱，直接用它回答問題
- 不要重新解讀或推測分析結果的含義
- 直接回答問題，不要提及 SQL、Python、資料庫等技術細節
- 保持簡潔

使用者問題：{question}
分析結果（即答案）：{result}
"""


def build_answer_prompt_long(question: str, result: str) -> str:
    return f"""你是一個資料分析助手。根據使用者的問題和分析結果，用自然、易懂的繁體中文回答。

規則：
- 直接回答問題，不要提及 SQL、Python、資料庫等技術細節
- 必須保留分析結果中的具體數值，不要用「約」「大約」「數千」等模糊描述
- 如果結果是列表，列出所有項目（或至少前 20 筆）
- 如果結果是單一數值，直接回答該數值
- 保持簡潔但完整

使用者問題：{question}
分析結果：{result}
"""
