"""
Generate TEST_FAILURE_ANALYSIS.md from parsed Robin results JSON.

Usage:
  python tests/tools/generate_failure_report.py
  python tests/tools/generate_failure_report.py -i tests/robin_results_parsed.json -o tests/TEST_FAILURE_ANALYSIS.md

Reads the JSON produced by parse_robin_results.py and writes the markdown report.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path


# Section 1: tests removed (from plan)
DELETED_FILES = [
    "tests/unit/backend/test_robin_unsupported_raises.py",
    "tests/unit/backend/test_robin_optional.py",
    "tests/unit/backend/test_robin_materializer.py",
    "tests/test_backend_capability_model.py",
    "tests/test_issue_160_actual_bug_reproduction.py",
    "tests/test_issue_160_cache_key_reproduction.py",
    "tests/test_issue_160_exact_150_rows.py",
    "tests/test_issue_160_force_bug_reproduction.py",
    "tests/test_issue_160_lazy_frame_execution_plan.py",
    "tests/test_issue_160_manual_cache_manipulation.py",
    "tests/test_issue_160_reproduce_actual_bug.py",
    "tests/test_issue_160_nested_operations.py",
    "tests/test_issue_160_without_fix.py",
    "tests/test_issue_160_lazy_polars_expr.py",
    "tests/test_issue_160_with_cache_enabled.py",
    "tests/test_issue_160_reproduce_bug.py",
    "tests/test_issue_160_dropped_column_execution_plan.py",
]
MOVED_TO_ARCHIVE = "tests/unit/dataframe/test_inferschema_parity.py → tests/archive/unit/dataframe/test_inferschema_parity.py"


def _theme_for_fix_sparkless(message: str) -> str:
    """Assign a theme label for Section 3 grouping."""
    if re.search(r"PyColumn.*has no attribute", message):
        return "PyColumn (missing methods)"
    if re.search(r"unsupported operand type.*PyColumn", message):
        return "PyColumn (reverse operators)"
    if re.search(r"PyColumn.*is not callable", message):
        return "fillna / subset"
    if re.search(r"RobinDataFrameReader.*option", message):
        return "Reader API"
    if re.search(r"RobinSparkSession.*stop", message) or "_has_active_session" in message or "No active SparkSession" in message:
        return "Session / lifecycle"
    if re.search(r"module.*has no attribute ['\"]first['\"]", message) or re.search(r"module.*has no attribute ['\"]rank['\"]", message):
        return "Functions module (F.)"
    if re.search(r"NotImplementedError.*GroupedData", message) or re.search(r"RobinGroupedData.*pivot", message):
        return "GroupedData / pivot"
    if re.search(r"tuple.*cannot be converted to PyDict", message):
        return "withField / struct conversion"
    if re.search(r"NoneType.*fields", message):
        return "Schema / NoneType"
    if "Regex pattern did not match" in message or "can not infer schema" in message or "createDataFrame requires schema" in message:
        return "Error message parity"
    if re.search(r"PyColumn.*is not iterable", message):
        return "Join column handling"
    return "Other (fix_sparkless)"


def _normalize_pattern(msg: str, max_len: int = 72) -> str:
    """Shorten message to a readable pattern for table cells."""
    s = (msg or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def _short_test_id(test_id: str) -> str:
    """e.g. tests/unit/dataframe/test_foo.py::TestBar::test_baz -> test_foo (test_baz) or TestBar::test_baz."""
    if "::" in test_id:
        parts = test_id.split("::")
        return parts[-1] if len(parts) > 1 else test_id
    return Path(test_id).name if test_id else test_id


def render_report(data: dict) -> str:
    """Produce markdown report from parsed data."""
    results_file = data.get("results_file", "")
    # Use relative path for display if under repo
    if "sparkless" in results_file:
        try:
            idx = results_file.index("sparkless")
            results_file = results_file[idx:]
        except ValueError:
            pass
    run_cmd = "SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh -n 12"
    # Try to get date from filename results_robin_YYYYMMDD_HHMMSS.txt
    date_note = ""
    if "results_robin_" in results_file:
        m = re.search(r"results_robin_(\d{8})_\d{6}", results_file)
        if m:
            d = m.group(1)
            date_note = f" ({d[:4]}-{d[4:6]}-{d[6:8]})"

    out = []
    out.append("# Test Failure Analysis (Robin Backend)\n")
    out.append(f"**Run:** `{run_cmd}`  ")
    out.append(f"**Results file:** `{results_file}`{date_note}\n")
    out.append("## Summary\n")
    u = data.get("summary_unit", {})
    p = data.get("summary_parity", {})
    out.append("| Phase     | Passed | Failed | Errors | Skipped |")
    out.append("|-----------|--------|--------|--------|---------|")
    out.append(f"| Unit      | {u.get('passed', 0)}    | {u.get('failed', 0)}    | {u.get('errors', 0)}    | {u.get('skipped', 0)}       |")
    out.append(f"| Parity    | {p.get('passed', 0)}     | {p.get('failed', 0)}    | {p.get('errors', 0)}     | {p.get('skipped', 0)}       |")
    total = (u.get("failed", 0) + u.get("errors", 0)) + (p.get("failed", 0) + p.get("errors", 0))
    out.append(f"\n**Total failures/errors:** {u.get('failed', 0) + u.get('errors', 0)} unit + {p.get('failed', 0) + p.get('errors', 0)} parity = **{total}** (excluding passed/skipped).\n")
    out.append("---\n")

    # Section 1 – Tests removed
    out.append("## 1. Tests removed\n\n")
    out.append("The following obsolete tests were deleted or moved so they no longer run.\n\n")
    out.append("### 1.1 Deleted files\n\n")
    out.append("| File | Reason |")
    out.append("|------|--------|")
    for f in DELETED_FILES[:4]:
        out.append(f"| `{f}` | Obsolete backend/BackendFactory/Polars. |")
    out.append(f"| `tests/test_issue_160_*.py` ({len(DELETED_FILES) - 4} files) | Polars/BackendFactory/cache; backend removed. |")
    out.append("\n### 1.2 Moved to archive\n\n")
    out.append(f"- `{MOVED_TO_ARCHIVE}` (ignored by pytest `--ignore=tests/archive`).\n\n")
    out.append("---\n")

    failures = data.get("failures", [])
    robin = [x for x in failures if x.get("category") == "robin_sparkless"]
    fix = [x for x in failures if x.get("category") == "fix_sparkless"]
    other = [x for x in failures if x.get("category") == "other"]

    # Section 2 – Robin-sparkless
    out.append("## 2. Robin-sparkless (upstream crate)\n\n")
    out.append("Failures that indicate the **robin-sparkless** crate does not match PySpark semantics. Fix or track upstream.\n\n")
    if not robin:
        out.append("(No failures in this category.)\n\n")
    else:
        # Group by normalized pattern (first line / truncated message)
        by_pattern: dict[str, list[dict]] = defaultdict(list)
        for r in robin:
            pat = _normalize_pattern(r.get("message", ""), 80)
            by_pattern[pat].append(r)
        out.append("| Error pattern | Example tests | Action |")
        out.append("|---------------|----------------|--------|")
        for pat, items in sorted(by_pattern.items(), key=lambda kv: -len(kv[1])):
            examples = ", ".join(_short_test_id(x["test_id"]) for x in items[:4])
            if len(items) > 4:
                examples += f" (+{len(items) - 4})"
            out.append(f"| {pat} | {examples} | Crate: fix semantics or track upstream. |")
        out.append("")
    out.append("---\n")

    # Section 3 – Fix Sparkless
    out.append("## 3. Fix Sparkless (Python / Robin integration layer)\n\n")
    out.append("Failures that should be fixed in **this repo** (Sparkless): missing APIs, wrong conversion, or session/reader implementation.\n\n")
    if not fix:
        out.append("(No failures in this category.)\n\n")
    else:
        by_theme: dict[str, list[dict]] = defaultdict(list)
        for f in fix:
            by_theme[_theme_for_fix_sparkless(f.get("message", ""))].append(f)
        theme_order = [
            "PyColumn (missing methods)",
            "PyColumn (reverse operators)",
            "fillna / subset",
            "Reader API",
            "Session / lifecycle",
            "Functions module (F.)",
            "GroupedData / pivot",
            "withField / struct conversion",
            "Schema / NoneType",
            "Error message parity",
            "Join column handling",
            "Other (fix_sparkless)",
        ]
        section_num = 0
        for theme in theme_order:
            items = by_theme.get(theme, [])
            if not items:
                continue
            section_num += 1
            out.append(f"### 3.{section_num} {theme}\n\n")
            by_pat: dict[str, list[dict]] = defaultdict(list)
            for x in items:
                by_pat[_normalize_pattern(x.get("message", ""), 70)].append(x)
            out.append("| Error pattern | Example tests | Fix |")
            out.append("|---------------|----------------|-----|")
            for pat, pat_items in sorted(by_pat.items(), key=lambda kv: -len(kv[1])):
                examples = ", ".join(_short_test_id(x["test_id"]) for x in pat_items[:3])
                if len(pat_items) > 3:
                    examples += f" (+{len(pat_items) - 3})"
                fix_hint = "Implement or fix in Sparkless (see current analysis)."
                out.append(f"| {pat} | {examples} | {fix_hint} |")
            out.append("")
    out.append("---\n")

    # Section 4 – Other
    out.append("## 4. Other\n\n")
    out.append(f"Failures not categorized as robin_sparkless or fix_sparkless: **{len(other)}**.\n\n")
    if other:
        out.append("Sample (first 15):\n\n")
        for x in other[:15]:
            out.append(f"- `{x.get('test_id', '')}` — {_normalize_pattern(x.get('message', ''), 60)}\n")
        if len(other) > 15:
            out.append(f"\n… and {len(other) - 15} more.\n")
    out.append("\n---\n")

    # Section 5 – Suggested order of work
    out.append("## 5. Suggested order of work\n\n")
    out.append("1. **Delete** obsolete tests (section 1) — done.\n")
    out.append("2. **Sparkless fixes** that unblock many tests: PyColumn (astype, desc/asc, isin, getItem, substr, reverse operators), RobinDataFrameReader.option(), RobinSparkSession.stop(), Functions (first, rank), fillna(subset=...), withField/createDataFrame tuple→dict conversion.\n")
    out.append("3. **Robin-sparkless** (upstream): create_dataframe_from_rows (map, array, struct), select/Column conversion, comparison/union/join coercion, temp view lookup.\n")
    out.append("4. **Re-run** with the command below and iterate.\n\n")
    out.append("---\n")

    # Section 6 – How to re-run and regenerate
    out.append("## 6. How to re-run and regenerate\n\n")
    out.append("Run the full suite and save output:\n\n")
    out.append("```bash\n")
    out.append("SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh -n 12 2>&1 | tee tests/results_robin_$(date +%Y%m%d_%H%M%S).txt\n")
    out.append("```\n\n")
    out.append("Then parse the new results file and regenerate this report:\n\n")
    out.append("```bash\n")
    out.append("python tests/tools/parse_robin_results.py tests/results_robin_<timestamp>.txt -o tests/robin_results_parsed.json\n")
    out.append("python tests/tools/generate_failure_report.py -i tests/robin_results_parsed.json -o tests/TEST_FAILURE_ANALYSIS.md\n")
    out.append("```\n")

    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate TEST_FAILURE_ANALYSIS.md from parsed JSON.")
    parser.add_argument("-i", "--input", type=str, default=None, help="Input JSON path (default: tests/robin_results_parsed.json)")
    parser.add_argument("-o", "--output", type=str, default=None, help="Output markdown path (default: tests/TEST_FAILURE_ANALYSIS.md)")
    args = parser.parse_args()
    base = Path(__file__).resolve().parent.parent
    in_path = Path(args.input) if args.input else base / "robin_results_parsed.json"
    out_path = Path(args.output) if args.output else base / "TEST_FAILURE_ANALYSIS.md"
    data = json.loads(in_path.read_text())
    report = render_report(data)
    out_path.write_text(report)
    print(f"Wrote {out_path}", file=__import__("sys").stderr)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
