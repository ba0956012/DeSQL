"""
LLM 初始化
"""

from langchain_openai import AzureChatOpenAI
from config import LLM_MODEL, LLM_DEPLOYMENT, LLM_TEMPERATURE

llm = AzureChatOpenAI(
    model=LLM_MODEL,
    azure_deployment=LLM_DEPLOYMENT,
    temperature=LLM_TEMPERATURE,
)
