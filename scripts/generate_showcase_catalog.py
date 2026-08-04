"""Generate a deterministic 100-SKU apparel catalog for the runnable demo."""

from __future__ import annotations

import argparse
import csv
import io
import json
from collections.abc import Sequence
from pathlib import Path

PRIMARY_EPCS = (
    "3074257BF7194E4000001A85",
    "3074257BF7194E4000001A86",
    "3074257BF7194E4000001A87",
    "3074257BF7194E4000001A88",
)
FIELDNAMES = (
    "style_code",
    "style_name",
    "sku",
    "upc",
    "color",
    "size",
    "epc",
    "style_attributes",
    "attributes",
)
COLORS = ("Blue", "Black", "Orange", "Green", "Red")
SIZES = ("XS", "S", "M", "L", "XL")
CATEGORIES = ("SHIRTS", "JACKETS", "TROUSERS", "DRESSES")


def upc_for_sku(sku_number: int) -> str:
    if sku_number == 1:
        return "036000291452"
    body = f"036{sku_number:08d}"
    weighted_sum = sum(
        int(digit) * (3 if index % 2 == 0 else 1) for index, digit in enumerate(body)
    )
    return body + str((-weighted_sum) % 10)


def epcs_for_sku(sku_number: int) -> tuple[str, ...]:
    if sku_number == 1:
        return PRIMARY_EPCS
    count = 4 if sku_number in {2, 3, 4} else 2
    return tuple(f"30{sku_number:06X}{tag_number:016X}" for tag_number in range(1, count + 1))


def sku_for_number(sku_number: int) -> str:
    return (
        "SKU-TRAIL-BLUE-M"
        if sku_number == 1
        else f"SKU-DEMO-{sku_number:03d}-{SIZES[(sku_number - 1) % len(SIZES)]}"
    )


def build_showcase_catalog(sku_count: int = 100) -> bytes:
    if not 3 <= sku_count <= 500:
        raise ValueError("sku_count must be between 3 and 500")
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=FIELDNAMES, lineterminator="\n")
    writer.writeheader()
    for number in range(1, sku_count + 1):
        style_number = ((number - 1) // len(SIZES)) + 1
        color = COLORS[(style_number - 1) % len(COLORS)]
        size = "M" if number == 1 else SIZES[(number - 1) % len(SIZES)]
        category = CATEGORIES[(style_number - 1) % len(CATEGORIES)]
        sku = sku_for_number(number)
        upc = upc_for_sku(number)
        for epc in epcs_for_sku(number):
            writer.writerow(
                {
                    "style_code": "ST-TRAIL" if number == 1 else f"STYLE-{style_number:03d}",
                    "style_name": "Trail Shirt"
                    if number == 1
                    else f"Demo Style {style_number:03d}",
                    "sku": sku,
                    "upc": upc,
                    "color": color,
                    "size": size,
                    "epc": epc,
                    "style_attributes": json.dumps({"category": category}, separators=(",", ":")),
                    "attributes": json.dumps({"material": "cotton-blend"}, separators=(",", ":")),
                }
            )
    return output.getvalue().encode()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sku-count", type=int, default=100)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args(argv)
    content = build_showcase_catalog(arguments.sku_count)
    if arguments.output is None:
        print(content.decode(), end="")
    else:
        arguments.output.write_bytes(content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
