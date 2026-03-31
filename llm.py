"""
LLM 初始化
"""

import os
from langchain_openai import AzureChatOpenAI
from config import LLM_MODEL, LLM_DEPLOYMENT, LLM_TEMPERATURE

_kwargs = {}
_reasoning_effort = os.getenv("LLM_REASONING_EFFORT", "")
if _reasoning_effort:
    _kwargs["reasoning_effort"] = _reasoning_effort

llm = AzureChatOpenAI(
    model=LLM_MODEL,
    azure_deployment=LLM_DEPLOYMENT,
    temperature=LLM_TEMPERATURE,
    model_kwargs=_kwargs,
)
