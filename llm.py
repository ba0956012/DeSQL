"""
LLM 初始化 — 支援 Azure OpenAI 和 AWS Bedrock，支援 per-node 模型配置

環境變數：
  LLM_PROVIDER=azure|bedrock        主 LLM provider
  LLM_MODEL / LLM_DEPLOYMENT        主 LLM 模型

  CODE_LLM_PROVIDER                 Code node 用的 provider（留空則用主 LLM）
  CODE_LLM_MODEL                    Code node 用的模型
"""

import os
import requests
from langchain_openai import AzureChatOpenAI
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage
from config import LLM_MODEL, LLM_DEPLOYMENT, LLM_TEMPERATURE

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "azure")


class BedrockChat(BaseChatModel):
    """Minimal Bedrock Converse API wrapper compatible with LangChain."""

    endpoint_url: str = ""
    api_token: str = ""
    model_id: str = ""
    temperature: float = 0.0

    @property
    def _llm_type(self) -> str:
        return "bedrock-converse"

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        from langchain_core.outputs import ChatResult, ChatGeneration
        import time as _time

        bedrock_messages = []
        for msg in messages:
            role = "user" if isinstance(msg, HumanMessage) else "assistant"
            bedrock_messages.append({
                "role": role,
                "content": [{"text": msg.content}]
            })

        body = {"messages": bedrock_messages}
        if self.temperature > 0:
            body["inferenceConfig"] = {"temperature": self.temperature}

        max_retries = 5
        for attempt in range(max_retries):
            try:
                resp = requests.post(
                    self.endpoint_url,
                    headers={
                        "Authorization": f"Bearer {self.api_token}",
                        "Content-Type": "application/json",
                    },
                    json=body,
                    timeout=180,
                )
                resp.raise_for_status()
                break
            except requests.HTTPError as e:
                status = getattr(e.response, 'status_code', 0)
                if (status >= 500 or status == 429) and attempt < max_retries - 1:
                    wait = 2 ** (attempt + 1)
                    print(f"  ⚠️ Bedrock {status} retry {attempt+1}/{max_retries} after {wait}s", flush=True)
                    _time.sleep(wait)
                elif attempt < max_retries - 1:
                    wait = 2 ** attempt
                    print(f"  ⚠️ Bedrock retry {attempt+1}/{max_retries} after {wait}s: {e}", flush=True)
                    _time.sleep(wait)
                else:
                    raise
            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt < max_retries - 1:
                    wait = 2 ** attempt
                    print(f"  ⚠️ Bedrock retry {attempt+1}/{max_retries} after {wait}s: {e}", flush=True)
                    _time.sleep(wait)
                else:
                    raise

        data = resp.json()
        # 提取回應文字（跳過 reasoningContent，只取 text）
        content_parts = data["output"]["message"]["content"]
        text = ""
        for part in content_parts:
            if "text" in part:
                text = part["text"]
                break
        if not text:
            text = str(content_parts)
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content=text))])


def _build_llm(provider=None, model=None, deployment=None):
    provider = provider or LLM_PROVIDER
    model = model or LLM_MODEL
    deployment = deployment or LLM_DEPLOYMENT

    if provider == "bedrock":
        base_url = os.getenv("BEDROCK_BASE_URL", "https://bedrock-runtime.us-east-1.amazonaws.com")
        token = os.getenv("BEDROCK_API_TOKEN", "")
        # 用 model name 組出 endpoint URL
        endpoint_url = f"{base_url}/model/{model}/converse"
        return BedrockChat(
            endpoint_url=endpoint_url,
            api_token=token,
            model_id=model,
            temperature=LLM_TEMPERATURE,
        )
    else:
        _kwargs = {}
        _reasoning_effort = os.getenv("LLM_REASONING_EFFORT", "")
        if _reasoning_effort:
            _kwargs["reasoning_effort"] = _reasoning_effort
        return AzureChatOpenAI(
            model=model,
            azure_deployment=deployment,
            temperature=LLM_TEMPERATURE,
            model_kwargs=_kwargs,
        )


# 主 LLM（retrieval, schema_filter, QA, format_answer 等）
llm = _build_llm()

# SQL LLM（generate_sql）— 留空則用主 LLM
_sql_provider = os.getenv("SQL_LLM_PROVIDER", "")
_sql_model = os.getenv("SQL_LLM_MODEL", "")
if _sql_provider and _sql_model:
    sql_llm = _build_llm(
        provider=_sql_provider,
        model=_sql_model,
        deployment=os.getenv("SQL_LLM_DEPLOYMENT", _sql_model),
    )
else:
    sql_llm = llm

# Code LLM（generate_code）— 留空則用主 LLM
_code_provider = os.getenv("CODE_LLM_PROVIDER", "")
_code_model = os.getenv("CODE_LLM_MODEL", "")
if _code_provider and _code_model:
    code_llm = _build_llm(
        provider=_code_provider,
        model=_code_model,
        deployment=os.getenv("CODE_LLM_DEPLOYMENT", _code_model),
    )
else:
    code_llm = llm

# QA LLM（question_analysis, schema_filter）— 留空則用主 LLM
_qa_provider = os.getenv("QA_LLM_PROVIDER", "")
_qa_model = os.getenv("QA_LLM_MODEL", "")
if _qa_provider and _qa_model:
    qa_llm = _build_llm(
        provider=_qa_provider,
        model=_qa_model,
        deployment=os.getenv("QA_LLM_DEPLOYMENT", _qa_model),
    )
else:
    qa_llm = llm

# Schema LLM（schema_filter）— 留空則用 QA LLM
_schema_provider = os.getenv("SCHEMA_LLM_PROVIDER", "")
_schema_model = os.getenv("SCHEMA_LLM_MODEL", "")
if _schema_provider and _schema_model:
    schema_llm = _build_llm(
        provider=_schema_provider,
        model=_schema_model,
        deployment=os.getenv("SCHEMA_LLM_DEPLOYMENT", _schema_model),
    )
else:
    schema_llm = qa_llm
