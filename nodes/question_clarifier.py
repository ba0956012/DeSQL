"""
Question Clarifier — 強化原始問題描述
在 QA 之前，根據 schema 把問題裡的模糊描述明確化。
例如：
- "merged schools" → 明確指出 StatusType = 'Merged'
- "mailing city" → 明確指出用 MailCity 欄位而非 City
- "K-12 enrollment" → 明確指出用 frpm."Enrollment (K-12)" 而非其他 enrollment 欄位
"""

import os
from langchain_core.messages import HumanMessage

from llm import llm
from utils import debug_log

ENABLE_QUESTION_CLARIFIER = os.getenv("ENABLE_QUESTION_CLARIFIER", "false").lower() in (
    "true",
    "1",
    "yes",
)

CLARIFY_PROMPT = """Rewrite the following question by explicitly listing all conditions and requirements mentioned in it. Do NOT add new conditions or interpret beyond what is stated. Just make implicit conditions explicit.

Question: {question}

Rules:
- Keep the original question as the first line
- Below it, add "Conditions:" followed by a numbered list of every condition mentioned
- Each condition should specify: what entity, what column/attribute, what value/comparison
- If the question mentions a Hint, include the Hint's conditions too
- Do NOT guess or add conditions not mentioned in the question
- Do NOT reference the database schema — just parse the question text

Example:
Question: How many schools in merged Alameda have number of test takers less than 100?
Output:
How many schools in merged Alameda have number of test takers less than 100?
Conditions:
1. County = Alameda
2. School status = merged
3. Number of test takers < 100
4. Count the number of schools meeting all conditions"""


def clarify_question(state):
    """Clarify ambiguous terms in the question using schema context."""
    if not ENABLE_QUESTION_CLARIFIER:
        return {}

    question = state.get("question", "")
    if not question:
        return {}

    prompt = CLARIFY_PROMPT.format(question=question)

    try:
        res = llm.invoke([HumanMessage(content=prompt)])
        clarified = res.content.strip()
        debug_log(
            "question_clarifier", original=question[:100], clarified=clarified[:200]
        )

        # Only update if clarifier added something (longer output)
        if clarified and len(clarified) > len(question):
            return {"question": clarified}
        return {}

    except Exception as e:
        debug_log("question_clarifier", error=str(e))
        return {}
