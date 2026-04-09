"""
共用工具函式
"""

import json
import logging

from config import DEBUG

_logger = logging.getLogger("pipeline")


def clean_llm_json(text: str) -> dict:
    """清除 LLM 回傳中的 markdown 包裹，解析 JSON"""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return json.loads(text.strip())


def strip_code_fences(text: str) -> str:
    """清除 LLM 回傳中的 markdown code fences 和尾部說明"""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    # 找到第一個結尾的 ``` 並截斷（移除後面的 markdown 說明）
    if "```" in text:
        text = text[:text.index("```")]
    return text.strip()


def debug_log(node_name: str, **kwargs):
    """Debug 用的結構化 log"""
    if not DEBUG:
        return
    sep = "=" * 60
    _logger.debug(f"\n{sep}\n🐛 [{node_name}]\n{sep}")
    for k, v in kwargs.items():
        val_str = str(v)
        if len(val_str) > 500:
            val_str = val_str[:500] + "... (truncated)"
        _logger.debug(f"  {k}: {val_str}")
    _logger.debug(sep)
