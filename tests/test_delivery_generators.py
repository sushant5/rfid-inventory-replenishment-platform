from __future__ import annotations

import csv
import io
import json
from pathlib import Path

import pytest
from scripts.check_branch_coverage import branch_coverage_percentage
from scripts.check_branch_coverage import main as coverage_main
from scripts.generate_showcase_catalog import (
    PRIMARY_EPCS,
    build_showcase_catalog,
    epcs_for_sku,
    upc_for_sku,
)

from abacus.services.catalog import parse_catalog_csv


def test_showcase_catalog_is_deterministic_and_reviewer_sized() -> None:
    content = build_showcase_catalog()
    rows = list(csv.DictReader(io.StringIO(content.decode())))

    assert build_showcase_catalog() == content
    assert len({row["sku"] for row in rows}) == 100
    assert len({row["epc"] for row in rows}) == 208
    assert {row["epc"] for row in rows if row["sku"] == "SKU-TRAIL-BLUE-M"} == set(PRIMARY_EPCS)
    assert all(json.loads(row["style_attributes"])["category"] for row in rows)
    assert len({row["upc"] for row in rows}) == 100
    assert len(epcs_for_sku(2)) == len(epcs_for_sku(3)) == 4
    assert len(epcs_for_sku(4)) == 4
    assert upc_for_sku(1) == "036000291452"
    parsed = parse_catalog_csv(content)
    assert parsed.issues == []
    assert len(parsed.rows) == 208
    assert all(row.normalized is not None and row.issues == [] for row in parsed.rows)


@pytest.mark.parametrize("sku_count", [2, 501])
def test_showcase_catalog_rejects_unhelpful_sizes(sku_count: int) -> None:
    with pytest.raises(ValueError, match="between 3 and 500"):
        build_showcase_catalog(sku_count)


def test_branch_coverage_gate_uses_branch_counts_not_combined_coverage(
    tmp_path: Path,
) -> None:
    report = {
        "totals": {
            "covered_branches": 80,
            "num_branches": 100,
            "percent_covered": 95,
        }
    }
    path = tmp_path / "coverage.json"
    path.write_text(json.dumps(report), encoding="utf-8")

    assert branch_coverage_percentage(report) == 80
    assert coverage_main([str(path), "--minimum", "80"]) == 0
    assert coverage_main([str(path), "--minimum", "81"]) == 1


@pytest.mark.parametrize(
    "report, message",
    [
        ({}, "no totals"),
        ({"totals": {"covered_branches": True, "num_branches": 1}}, "covered_branches"),
        ({"totals": {"covered_branches": 0, "num_branches": 0}}, "no branches"),
    ],
)
def test_branch_coverage_gate_rejects_invalid_reports(
    report: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        branch_coverage_percentage(report)
