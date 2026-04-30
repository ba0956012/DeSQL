"""
Prompt Profile Loader

用 PROMPT_PROFILE 環境變數選擇 prompt profile。
每個 profile 是 prompts/ 下的一個子目錄，包含各 node 的 prompt template。

用法：
    from prompts import load_profile
    p = load_profile()  # 讀取 PROMPT_PROFILE 環境變數，預設 "default"
    p.qa_system_prompt   # question_analysis 的 system prompt
    p.sql_rules          # SQL 生成規則列表
    ...
"""

import importlib
import os


def load_profile(profile_name: str = None):
    """載入指定的 prompt profile module。"""
    if profile_name is None:
        profile_name = os.getenv("PROMPT_PROFILE", "default")
    module = importlib.import_module(f"prompts.{profile_name}")
    return module
