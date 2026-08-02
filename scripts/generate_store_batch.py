"""Generate a deterministic 1-500 store onboarding request on stdout."""

import argparse
import json
import sys


def _store_count(value: str) -> int:
    count = int(value)
    if not 1 <= count <= 500:
        raise argparse.ArgumentTypeError("count must be between 1 and 500")
    return count


def _store(number: int) -> dict[str, object]:
    code = f"store{number:03d}"
    serial_prefix = f"ORANGE-{number:04d}"
    return {
        "code": code,
        "name": f"Orange Store {number:03d}",
        "timezone": "America/Los_Angeles",
        "organization_path": [
            {"code": "us", "name": "United States", "unit_type": "COUNTRY"},
            {"code": "west", "name": "West", "unit_type": "REGION"},
        ],
        "zones": [
            {"code": "floor", "name": "Sales Floor", "kind": "SALES_FLOOR"},
            {"code": "backroom", "name": "Backroom", "kind": "BACKROOM"},
        ],
        "devices": [
            {
                "serial_number": f"{serial_prefix}-FLOOR",
                "display_name": f"{code} Floor Reader",
                "zone_code": "floor",
            },
            {
                "serial_number": f"{serial_prefix}-BACK",
                "display_name": f"{code} Backroom Reader",
                "zone_code": "backroom",
            },
        ],
        "configuration": {"rfid_enabled": True},
    }


def build_store_batch(count: int = 100) -> dict[str, object]:
    """Build the assignment-sized payload for tests or command-line output."""

    if not 1 <= count <= 500:
        raise ValueError("count must be between 1 and 500")
    return {"stores": [_store(number) for number in range(1, count + 1)]}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=_store_count, default=100)
    arguments = parser.parse_args()
    json.dump(build_store_batch(arguments.count), sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
