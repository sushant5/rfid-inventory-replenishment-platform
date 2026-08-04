"""Fail when coverage.py reports less than the required branch coverage."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


def branch_coverage_percentage(report: Mapping[str, Any]) -> float:
    totals = report.get("totals")
    if not isinstance(totals, Mapping):
        raise ValueError("coverage report has no totals object")
    covered = totals.get("covered_branches")
    total = totals.get("num_branches")
    if isinstance(covered, bool) or not isinstance(covered, int):
        raise ValueError("coverage report has no covered_branches count")
    if isinstance(total, bool) or not isinstance(total, int) or total <= 0:
        raise ValueError("coverage report has no branches")
    return covered / total * 100


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path, nargs="?", default=Path("coverage.json"))
    parser.add_argument("--minimum", type=float, default=80.0)
    arguments = parser.parse_args(argv)
    report = json.loads(arguments.report.read_text(encoding="utf-8"))
    percentage = branch_coverage_percentage(report)
    print(f"Branch coverage: {percentage:.2f}% (required: {arguments.minimum:.2f}%)")
    return 0 if percentage >= arguments.minimum else 1


if __name__ == "__main__":
    raise SystemExit(main())
