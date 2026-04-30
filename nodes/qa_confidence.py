"""
QA Confidence Scorer — 評估 QA task_plan 的正確性信心分數
只記錄 log，不影響 pipeline 行為。用於收集數據，之後分析信心分數和實際正確率的相關性。
"""

import json
import os
from langchain_core.messages import HumanMessage

from llm import llm as reviewer_llm, _build_llm
from utils import debug_log

ENABLE_QA_CONFIDENCE = os.getenv("ENABLE_QA_CONFIDENCE", "false").lower() in ("true", "1", "yes")
ENABLE_QA_REPLAN = os.getenv("ENABLE_QA_REPLAN", "false").lower() in ("true", "1", "yes")
QA_REPLAN_THRESHOLD = int(os.getenv("QA_REPLAN_THRESHOLD", "70"))

# Support independent model for confidence scoring
_conf_provider = os.getenv("CONFIDENCE_LLM_PROVIDER", "")
_conf_model = os.getenv("CONFIDENCE_LLM_MODEL", "")
if _conf_provider and _conf_model:
    reviewer_llm = _build_llm(
        provider=_conf_provider,
        model=_conf_model,
        deployment=os.getenv("CONFIDENCE_LLM_DEPLOYMENT", _conf_model),
    )

CONFIDENCE_PROMPT = """You are reviewing a query plan. Your job is to check: if this plan is executed exactly as described, will it produce the correct answer to the original question?

Original Question: {question}

Query Plan:
- Target: {target}
- Tables needed: {tables}
- SQL task: {sql_task}
- Python task: {python_task}
- Filters: {filters}

Database Schema:
{schema_summary}

Think step by step:
1. Read the question carefully. What exactly is being asked? What conditions are mentioned?
2. Does the plan capture ALL conditions from the question? (e.g., "merged schools" → is there a filter for merged status? "mailing city" → is it using mailing address column, not city column?)
3. Does the plan use the correct columns? (e.g., if the question says "enrollment for K-12", is it using the K-12 enrollment column, not a different enrollment column?)
4. If the question has a Hint with a formula, does the plan follow it exactly?
5. After SQL fetches data and Python processes it, will the result actually answer the question?

Rate your confidence (0-100) that this plan will produce the correct answer.
- 90-100: Plan clearly covers all conditions, correct tables and columns
- 70-89: Plan looks reasonable but might miss a subtle condition
- 50-69: Plan has a likely issue (missing filter, wrong column, incomplete logic)
- 0-49: Plan is clearly wrong or missing critical information

Output JSON only:
{{"confidence": 0-100, "issues": ["list any missing conditions or potential problems"], "suggestion": "one-line fix if confidence < 70"}}"""


def score_qa_confidence(state):
    """Score QA plan confidence. Log only, no state changes."""
    if not ENABLE_QA_CONFIDENCE:
        return {}

    task_plan = state.get("task_plan", "")
    if not task_plan:
        return {}

    try:
        plan = json.loads(task_plan)
    except (json.JSONDecodeError, KeyError):
        return {}

    target = plan.get("target", "")
    tables = plan.get("tables_needed", [])
    sql_task = plan.get("sql_task", "")
    python_task = plan.get("python_task", "")
    filters = plan.get("filters", [])
    filters_str = json.dumps(filters, ensure_ascii=False)[:200] if filters else "none"

    # Use full raw schema (without column descriptions) for cleaner review
    # Descriptions make DDL too noisy and LLM may miss tables
    from db import SCHEMA_INFO
    schema = SCHEMA_INFO

    prompt = CONFIDENCE_PROMPT.format(
        question=state["question"],
        target=target,
        tables=", ".join(tables),
        sql_task=sql_task[:300],
        python_task=python_task[:200],
        filters=filters_str,
        schema_summary=schema[:1500],
    )

    try:
        res = reviewer_llm.invoke([HumanMessage(content=prompt)])
        content = res.content.strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[1] if "\n" in content else content[3:]
        if content.endswith("```"):
            content = content[:-3]
        result = json.loads(content.strip())

        confidence = result.get("confidence", -1)
        issues = result.get("issues", [])
        suggestion = result.get("suggestion", "")

        debug_log("qa_confidence",
                  confidence=confidence,
                  issues=issues[:3] if issues else [],
                  suggestion=suggestion[:100])

    except Exception as e:
        debug_log("qa_confidence", error=str(e))
        return {}

    # Store confidence in state for eval logging
    output = {"qa_confidence_score": confidence, "qa_confidence_issues": issues[:3]}

    # If re-plan enabled and confidence is low, pass feedback for re-planning
    if ENABLE_QA_REPLAN and confidence < QA_REPLAN_THRESHOLD and not state.get("qa_replanned"):
        feedback_parts = []
        if issues:
            feedback_parts.append("Issues found: " + "; ".join(issues[:2]))
        if suggestion:
            feedback_parts.append("Suggestion: " + suggestion)
        if feedback_parts:
            output["qa_review_feedback"] = "\n".join(feedback_parts)
            debug_log("qa_confidence", action="trigger_replan", confidence=confidence)

    return output
