"""
答案格式化節點
"""

from langchain_core.messages import HumanMessage

from llm import llm
from utils import debug_log
from prompts import load_profile


def format_answer(state):
    profile = load_profile()
    final = state.get("final_answer", "")
    if not final:
        return {"display_answer": "無法回答此問題。"}
    final = str(final)

    stripped = final.strip()
    try:
        float(stripped)
        is_short_value = True
    except ValueError:
        is_short_value = len(stripped) <= 50 and "\n" not in stripped

    if is_short_value:
        prompt = profile.build_answer_prompt_short(state["question"], stripped)
    else:
        if len(final) <= 2000:
            truncated = final
        else:
            truncated = final[:2000] + f"...\n(total {len(final)} chars)"
        prompt = profile.build_answer_prompt_long(state["question"], truncated)

    debug_log("format_answer", prompt=prompt)
    res = llm.invoke([HumanMessage(content=prompt)])
    return {"display_answer": res.content.strip()}
