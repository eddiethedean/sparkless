# Repro results with robin-sparkless 0.6.0

| Script | Robin | PySpark | Limitation confirmed |
|--------|-------|---------|----------------------|
| 01_type_strictness | FAILED | OK | Yes — string vs numeric |
| 02_case_sensitivity | OK | OK | No |
| 03_row_values_nested | OK | OK | No (list supported) |
| 04_case_when | FAILED (.otherwise() missing) | OK | Yes — CaseWhen API |
| 05_window_in_select | FAILED (row_number/Window not found) | OK | Yes — Window in Python API |
| 06_map_array | OK | OK | No |
| 07_filter_complex | OK | OK | No |
| 08_concat_literal | FAILED (F.concat not found) | OK | Yes — concat |
| 09_chained_arithmetic | OK | OK | No |
| 10_datetime_row | FAILED (datetime not in row types) | OK | Yes — datetime in row |

Existing robin-sparkless issues (closed): #201 (type strictness), #196 (concat), #195 (expression), #187 (Window), #198 (map/array/row).
