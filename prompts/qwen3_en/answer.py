"""Answer node prompt templates — qwen3_en (English, Qwen3)"""


def build_answer_prompt_short(question: str, result: str) -> str:
    return f"""You are a data analysis assistant. Answer the user's question based on the analysis result.

Rules:
- The analysis result IS the answer, use it directly
- Do not mention SQL, Python, or database details
- Be concise

User Question: {question}
Analysis Result: {result}
"""


def build_answer_prompt_long(question: str, result: str) -> str:
    return f"""You are a data analysis assistant. Answer the user's question based on the analysis result.

Rules:
- Answer directly with specific values
- If the result is a list, include all items (or at least first 20)
- Do not mention SQL, Python, or database details

User Question: {question}
Analysis Result: {result}
"""
