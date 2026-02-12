# Repro checklist: failure category to script mapping

Source: FAILURE_CATEGORIES.md (from `python scripts/analyze_robin_failures.py`).

| Category ID | Failure key | Repro script | Notes |
|-------------|-------------|--------------|--------|
| op_filter | Operations: filter | 04_filter_eqNullSafe.py, 06_filter_between.py | eqNullSafe / between; Robin 0.8 may have these |
| parity_assertion | AssertionError / DataFrames not equivalent | 05_parity_string_soundex.py | soundex; Robin 0.8 has F.soundex |
| op_join | Operations: join | 01_join_same_name.py | Robin supports same-name join |
| op_select | select / col_not_found | 03_select_alias.py | Alias preservation |
| op_withColumn | Operations: withColumn | 02_withColumn_expression.py | Cast, when/otherwise |
| col_not_found | Column not found (alias/schema) | 03_select_alias.py; repro_robin_limitations/02_case_sensitivity.py | Case sensitivity is in repro_robin_limitations |
| other | Type strictness (string vs numeric) | repro_robin_limitations/01_type_strictness.py | Issue #235 |
| other | not found: name / ID (case) | repro_robin_limitations/02_case_sensitivity.py | Issue #194 |
| other | lengths don't match (split limit) | 07_split_limit.py | New repro for split with limit |
| other | array requires at least one column | repro_robin_limitations/06_map_array.py | map/array |
| other | to_timestamp / datetime conversion | — | Candidate; no dedicated repro in robin_parity_repros |

Run all: `python scripts/robin_parity_repros/run_all_repros.py`. Then run `scripts/repro_robin_limitations/*.py` separately for full coverage.
