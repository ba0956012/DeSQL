"""
共用工具函式
"""

import json
import logging

from config import DEBUG

_logger = logging.getLogger("pipeline")


def clean_llm_json(text: str) -> dict:
    """清除 LLM 回傳中的 markdown 包裹，解析 JSON。

    支援：
    1. 純 JSON（無 code fence）
    2. 整段被 ```json ... ``` 包裹
    3. 前後有說明文字，中間夾 ```json ... ``` block
    4. 不合法 JSON（換行、trailing comma、comment 等）透過 json_repair 修復
    """
    import re
    text = text.strip()

    # 嘗試從 code fence 中提取 JSON block
    m = re.search(r'```(?:json)?\s*\n(.*?)```', text, re.DOTALL)
    if m:
        text = m.group(1).strip()
    else:
        # fallback: 去掉頭尾 code fence（相容舊行為）
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        from json_repair import repair_json
        repaired = repair_json(text, return_objects=True)
        if isinstance(repaired, dict):
            return repaired
        raise


def strip_code_fences(text: str) -> str:
    """清除 LLM 回傳中的 markdown code fences 和尾部說明。
    
    支援兩種格式：
    1. 純 code block: ```python\ncode\n```
    2. 分析文字 + code block: 分析...\n```python\ncode\n```\n說明...
    """
    text = text.strip()
    
    # 找到最後一個 code block（LLM 可能先寫分析再寫 code）
    # 尋找 ```python 或 ``` 開頭的 code block
    import re
    # 匹配 ```(python)?\n...code...\n```
    blocks = list(re.finditer(r'```(?:python)?\s*\n(.*?)```', text, re.DOTALL))
    if blocks:
        # 取最後一個 code block 的內容
        return blocks[-1].group(1).strip()
    
    # fallback: 原本的邏輯
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
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
