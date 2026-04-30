"""
Qwen3 English prompt profile — Qwen3 Next 80B + Qwen3 Coder 30B

Tuned for Qwen3 models on Bedrock. Key differences from default:
- English prompts (Qwen3 performs better with EN)
- Stronger guidance against over-JOINing
- Explicit Hint formula handling
- Code prompt warns against re-filtering SQL results
"""

from prompts.qwen3_en.question_analysis import *
from prompts.qwen3_en.sql import *
from prompts.qwen3_en.code import *
from prompts.qwen3_en.answer import *
from prompts.qwen3_en.schema_filter import *
