"""Property-based tests for chart_service.sandbox module.

Uses hypothesis to verify correctness properties 8, 9, 10
across all valid/invalid inputs.
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis.strategies import sampled_from, lists

from chart_service.sandbox import (
    SAFE_BUILTINS,
    strip_imports,
    execute_matplotlib_code,
    execute_echarts_code,
)


# ── Dangerous builtins that must NOT be in SAFE_BUILTINS ─────────────

DANGEROUS_BUILTINS = [
    "open", "exec", "eval", "__import__", "compile",
    "globals", "locals", "breakpoint", "exit", "quit",
    "input", "memoryview", "vars", "dir",
]


# ── Strategies ───────────────────────────────────────────────────────

_import_prefixes = sampled_from([
    "import os",
    "import sys",
    "from os import path",
    "from collections import OrderedDict",
    "import json",
    "from pathlib import Path",
    "import subprocess",
])

_safe_code_lines = sampled_from([
    "x = 1",
    "y = len([1,2,3])",
    "z = sum([1,2,3])",
    "result = 'hello'",
    "a = list(range(5))",
])

_error_code = sampled_from([
    "1/0",
    "raise ValueError('test')",
    "raise RuntimeError('boom')",
    "int('not_a_number')",
    "[][999]",
    "{}['missing_key']",
])


# ── Property 8: 沙箱僅允許白名單內建函式 ────────────────────────────
# Feature: chart-microservice, Property 8: 沙箱僅允許白名單內建函式
# **Validates: Requirements 5.1**


@settings(max_examples=100)
@given(dangerous=sampled_from(DANGEROUS_BUILTINS))
def test_property8_safe_builtins_excludes_dangerous(dangerous: str) -> None:
    """SAFE_BUILTINS 不應包含任何危險的內建函式。"""
    assert dangerous not in SAFE_BUILTINS


# ── Property 9: 沙箱移除所有 import 語句 ─────────────────────────────
# Feature: chart-microservice, Property 9: 沙箱移除所有 import 語句
# **Validates: Requirements 5.2**


@settings(max_examples=100)
@given(
    imports=lists(_import_prefixes, min_size=1, max_size=5),
    safe_lines=lists(_safe_code_lines, min_size=0, max_size=5),
)
def test_property9_strip_imports_removes_all_imports(
    imports: list[str], safe_lines: list[str]
) -> None:
    """經 strip_imports 處理後，結果不應包含任何 import 語句。"""
    # Interleave import lines with safe code lines
    all_lines = imports + safe_lines
    code = "\n".join(all_lines)
    result = strip_imports(code)
    for line in result.split("\n"):
        stripped = line.strip()
        assert not stripped.startswith("import "), f"Found import line: {line}"
        assert not stripped.startswith("from "), f"Found from-import line: {line}"


@settings(max_examples=100)
@given(safe_lines=lists(_safe_code_lines, min_size=1, max_size=5))
def test_property9_strip_imports_preserves_non_import_lines(
    safe_lines: list[str],
) -> None:
    """strip_imports 應保留所有非 import 的程式碼行。"""
    code = "\n".join(safe_lines)
    result = strip_imports(code)
    for line in safe_lines:
        assert line in result


# ── Property 10: 沙箱捕獲所有執行例外 ───────────────────────────────
# Feature: chart-microservice, Property 10: 沙箱捕獲所有執行例外
# **Validates: Requirements 5.4**


@settings(max_examples=100)
@given(error_code=_error_code)
def test_property10_matplotlib_catches_all_exceptions(error_code: str) -> None:
    """execute_matplotlib_code 應捕獲所有例外，回傳結構化錯誤結果。"""
    result = execute_matplotlib_code(error_code, [])
    assert result["success"] is False
    assert result["image"] == ""
    assert isinstance(result["error"], str)
    assert len(result["error"]) > 0


@settings(max_examples=100)
@given(error_code=_error_code)
def test_property10_echarts_catches_all_exceptions(error_code: str) -> None:
    """execute_echarts_code 應捕獲所有例外，回傳結構化錯誤結果。"""
    result = execute_echarts_code(error_code, [])
    assert result["success"] is False
    assert result["option_json"] == ""
    assert result["html"] == ""
    assert isinstance(result["error"], str)
    assert len(result["error"]) > 0
