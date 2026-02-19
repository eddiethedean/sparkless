"""
Parse pytest Robin results file and categorize failures for the test failure report.

Usage:
  python tests/tools/parse_robin_results.py tests/results_robin_20260218_194747.txt
  python tests/tools/parse_robin_results.py results.txt -o tests/robin_results_parsed.json

Reads a results file from run_all_tests.sh (Robin backend), extracts summary and
FAILED/ERROR lines, categorizes each as robin_sparkless / fix_sparkless / other,
and writes JSON for generate_failure_report.py.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


def _categorize(message: str) -> str:
    """Categorize failure/error message as robin_sparkless, fix_sparkless, or other."""
    if not message:
        return "other"
    # robin_sparkless
    if re.search(r"collect failed:", message):
        return "robin_sparkless"
    if re.search(r"create_dataframe_from_rows failed:", message):
        return "robin_sparkless"
    if re.search(r"select expects Column or str|cannot convert to Column", message):
        return "robin_sparkless"
    if re.search(r"Table or view .* not found", message):
        return "robin_sparkless"
    if "AssertionError" in message and (
        "tuple" in message and "set" in message
        or "incompatible with expected type" in message
        or "==" in message and "1234" in message
    ):
        return "robin_sparkless"
    # fix_sparkless
    if re.search(r"PyColumn.*has no attribute", message):
        return "fix_sparkless"
    if re.search(r"RobinDataFrameReader.*option", message):
        return "fix_sparkless"
    if re.search(r"RobinSparkSession.*stop", message):
        return "fix_sparkless"
    if re.search(r"tuple.*cannot be converted to.*PyDict", message):
        return "fix_sparkless"
    if re.search(r"NotImplementedError.*GroupedData|GroupedData\.agg\(\) not yet", message):
        return "fix_sparkless"
    if re.search(r"RobinGroupedData.*pivot", message):
        return "fix_sparkless"
    if re.search(r"module.*has no attribute ['\"]first['\"]", message):
        return "fix_sparkless"
    if re.search(r"module.*has no attribute ['\"]rank['\"]", message):
        return "fix_sparkless"
    if re.search(r"PyColumn.*is not callable", message):
        return "fix_sparkless"
    if re.search(r"NoneType.*fields", message):
        return "fix_sparkless"
    if "_has_active_session" in message:
        return "fix_sparkless"
    if "No active SparkSession" in message:
        return "fix_sparkless"
    if "Regex pattern did not match" in message or "can not infer schema from empty" in message or "createDataFrame requires schema" in message:
        return "fix_sparkless"
    if re.search(r"PyColumn.*is not iterable", message):
        return "fix_sparkless"
    if "RobinSparkSession" in message and ("_storage" in message or "stop" in message):
        return "fix_sparkless"
    # unsupported operand (reverse operators) -> fix_sparkless
    if re.search(r"unsupported operand type.*PyColumn", message):
        return "fix_sparkless"
    return "other"


def parse_results(results_path: str | Path) -> dict:
    """Parse results file and return structured data."""
    path = Path(results_path)
    text = path.read_text()
    lines = text.splitlines()

    # Summary lines: "=== 746 failed, 271 passed, 4 skipped, 118 errors in 21.24s ==="
    # Or without errors: "=== 670 failed, 229 passed, 4 skipped in 17.31s ==="
    # First occurrence = unit, second = parity.
    summary_re = re.compile(
        r"=+\s*(\d+)\s+failed,\s*(\d+)\s+passed,\s*(\d+)\s+skipped(?:,\s*(\d+)\s+errors)?"
    )
    summaries: list[dict] = []
    for line in lines:
        m = summary_re.search(line)
        if m:
            summaries.append({
                "failed": int(m.group(1)),
                "passed": int(m.group(2)),
                "skipped": int(m.group(3)),
                "errors": int(m.group(4)) if m.group(4) is not None else 0,
            })
    summary_unit = summaries[0] if len(summaries) >= 1 else {"passed": 0, "failed": 0, "errors": 0, "skipped": 0}
    summary_parity = summaries[1] if len(summaries) >= 2 else {"passed": 0, "failed": 0, "errors": 0, "skipped": 0}

    failures: list[dict] = []
    fail_re = re.compile(r"^(FAILED|ERROR)\s+(.+?)(?:\s+-\s+(.+))?$")
    for line in lines:
        line = line.strip()
        if not line.startswith("FAILED ") and not line.startswith("ERROR "):
            continue
        m = fail_re.match(line)
        if not m:
            continue
        kind = m.group(1)  # FAILED or ERROR
        test_id = m.group(2).strip()
        rest = (m.group(3) or "").strip()
        # rest is "ExceptionType: message"
        if ": " in rest:
            exc_type, _, msg = rest.partition(": ")
            exception_type = exc_type.strip()
            message = msg.strip()
        else:
            exception_type = rest or "Unknown"
            message = rest
        phase = "unit" if test_id.startswith("tests/unit/") else "parity"
        category = _categorize(message)
        failures.append({
            "kind": kind,
            "test_id": test_id,
            "exception_type": exception_type,
            "message": message,
            "phase": phase,
            "category": category,
        })

    return {
        "results_file": str(path.resolve()),
        "summary_unit": summary_unit,
        "summary_parity": summary_parity,
        "failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse Robin pytest results and output JSON.")
    parser.add_argument("results_file", type=str, help="Path to results .txt file")
    parser.add_argument("-o", "--output", type=str, default=None, help="Output JSON path (default: tests/robin_results_parsed.json)")
    args = parser.parse_args()
    out_path = args.output
    if not out_path:
        out_path = str(Path(__file__).resolve().parent.parent / "robin_results_parsed.json")
    data = parse_results(args.results_file)
    Path(out_path).write_text(json.dumps(data, indent=2))
    print(f"Wrote {out_path} ({len(data['failures'])} failures/errors)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
