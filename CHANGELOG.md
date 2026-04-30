# Changelog

## [Unreleased] — 2026-04-23

### Commit: Improve retrieval, QA context, empty retry, and disable harmful validator

**Summary:** Multiple improvements to retrieval accuracy, QA planning, and SQL retry. Disable validate_sql_result (net harmful). Average score: 185 (±3), same as baseline but with specific bug fixes for wrong-column issues.

**Eval results:**
- test_qaenum: 186/259 (71.8%) — best single run
- test_qaenum2: 184/259 (71.0%) — confirmation run
- Baseline (test_bugfix): 185/259 (71.4%)
- Without validator (test_novalidate): 188/259 (72.6%) — confirmed validator is harmful

---

### Changes by file:

#### `retrieval_subgraph.py` — Enum pre-match + broad search fallback
1. **Enum pre-match (keyword → enum fix):** After LLM classifies conditions, programmatically check if any "keyword" value exists in `ENUM_VALUES`. If found, auto-correct to "enum" with the correct table.column. Fixes cases like `POPLATEK PO OBRATU` being misclassified as `trans.k_symbol` when it's actually in `account.frequency`.
2. **Broad search fallback:** When `retrieve_phrase` finds nothing in the LLM-specified column, scan ALL text columns across all tables. If found elsewhere, update `search_table`/`search_column` to the correct location.
3. **Type safety:** All `.lower()` calls now handle non-string values (int enum values) via `str()` conversion. Condition fields are sanitized to strings before returning.
4. **Removed:** Question-text enum scanning (step 2) — caused too many false matches with short common words like "gold", "OWNER".

#### `nodes/schema_filter.py` — Pass conditions context to QA
- Compute `_conditions_context` from retrieval results (enum matches + keyword search locations) and store in state.
- Format: clean English lines like `account.frequency = 'POPLATEK PO OBRATU'` and `'Colusa' found in schools.county`.
- No ILIKE suggestions or JSON arrays — only table.column confirmation for QA.

#### `nodes/question_analysis.py` — QA receives retrieval context + enum values
- Pass `_conditions_context` (confirmed value locations) to QA prompt.
- Pass `ENUM_VALUES` summary (via `format_enum_info`) to QA prompt — helps QA pick correct columns when multiple tables have similar column names.

#### `prompts/qwen3_en/question_analysis.py` — QA prompt enhancements
- Added `conditions_context` parameter: "Confirmed values from database" section.
- Added `enum_info` parameter: "Known column values" section.
- New guideline: "If Confirmed values are provided, use the EXACT table and column specified."
- New guideline: "Use Known column values to identify which column contains the values mentioned in the question."

#### `prompts/default/question_analysis.py` — Signature update
- Added `conditions_context` and `enum_info` parameters (backward compatible, default empty).

#### `nodes/sql.py` — Disable validate_sql_result
- `validate_sql_result` now returns `{"sql_validation": ""}` immediately (pass-through).
- Original v10 (task_plan compare) code preserved but bypassed.
- Reason: 15+ prompt versions tested, precision never exceeded 50%. Net effect always negative (retry hurts more than helps).

#### `pipeline.py` — State update
- Added `_conditions_context: str` to State TypedDict.

#### `prompts/qwen3_en/sql.py` — Improved empty retry prompt
- `build_error_context_empty`: More specific guidance for 0-row retries:
  - Value format mismatch (leading zeros, casing)
  - Wrong column/table suggestion
  - Too many filters
  - Date range may not exist in DB
- Confirmed fix for #98 (POPLATEK TYDNE) and #109 (Jesenik branch).

#### `eval/run_eval.py` — Validation tracking fix
- `run_pipeline_with_state`: Track `validation_history` — captures all non-empty `sql_validation` values before they get overwritten by subsequent passes.
- Record `sql_validation_count` in eval logs for accurate analysis.
- Fixes bug where validator triggers were invisible in eval results (always showed 0).

---

### Experiments conducted (this session):

| Tag | Score | Notes |
|-----|-------|-------|
| test_programmer (validate_sql programmer perspective) | 179/259 | Validator never triggered, abandoned |
| test_v10_confirm (validate_sql v10 task_plan) | 181-184 | Validator precision 48%, net -2 |
| test_v10b (validate_sql column-only) | 179 | Precision 10%, worse |
| test_novalidate (validator disabled) | 188 | Best baseline without validator |
| test_emptyretry (improved empty prompt) | 184 | Fixed #98, #109 from 0-rows |
| test_retrieval (enum fix + broad search) | 180 | Crash fixed, enum scan removed |
| test_retrieval2 (no enum scan) | 181 | Clean, no crashes |
| test_qacontext (conditions → QA, enum only) | 183 | #97 fixed |
| test_qacontext2 (conditions → QA, enum+keyword) | 182 | #97 fixed |
| test_qaenum (+ enum_values → QA) | 186 | Best with all changes |
| test_qaenum2 (confirmation) | 184 | Confirmed stable |

### Key findings:
- validate_sql_result is net harmful at current precision (<50%). Disabled.
- Retrieval enum pre-match fixes wrong-column bugs (#97, #98) reliably.
- Passing enum_values to QA helps financial DB (+2-5 questions).
- Empty retry prompt improvement safely fixes 2 additional 0-row questions.
- All improvements combined: ~185 average, same as baseline but with fewer "unable to answer" cases.
