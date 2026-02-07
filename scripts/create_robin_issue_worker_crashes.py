#!/usr/bin/env python3
"""
Create the robin-sparkless GitHub issue for worker crashes under pytest-xdist.

Run from repo root:
  python scripts/create_robin_issue_worker_crashes.py [--dry-run]

Requires: gh CLI authenticated with access to eddiethedean/robin-sparkless
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO = "eddiethedean/robin-sparkless"
TITLE = "[Bug] Worker processes crash (node down: Not properly terminated) when used from pytest-xdist (forked workers)"
BODY_FILE = Path(__file__).resolve().parent.parent / "docs" / "robin_github_issue_worker_crashes.md"


def main() -> int:
    parser = argparse.ArgumentParser(description="Create robin-sparkless issue for worker crashes under xdist")
    parser.add_argument("--dry-run", action="store_true", help="Print title and body path, do not create")
    args = parser.parse_args()

    if not BODY_FILE.exists():
        print(f"Body file not found: {BODY_FILE}", file=sys.stderr)
        return 1

    print(f"Title: {TITLE}")
    print(f"Body file: {BODY_FILE}")
    if args.dry_run:
        print("Dry run: would run gh issue create -R {} --title ... --body-file {}".format(REPO, BODY_FILE))
        return 0

    cmd = [
        "gh", "issue", "create",
        "-R", REPO,
        "--title", TITLE,
        "--body-file", str(BODY_FILE),
    ]
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout.strip() or f"Created: {TITLE[:60]}...")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"Failed: {e.stderr or str(e)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
