"""Generate deterministic RFID scenarios and optionally submit them to Abacus.

The simulator intentionally uses only the Python standard library so it can run
before the project dependencies are installed.  Device credentials are supplied
only as HTTP headers and are never included in generated output.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Final
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

DEFAULT_DEVICE_ID: Final = uuid.UUID("11111111-1111-4111-8111-111111111111")
DEFAULT_SECOND_DEVICE_ID: Final = uuid.UUID("22222222-2222-4222-8222-222222222222")
DEFAULT_EPC: Final = "3034257BF7194E4000000001"
DEFAULT_UNKNOWN_EPC: Final = "3034257BF7194E40FFFFFFFF"
SCENARIOS: Final = (
    "normal",
    "duplicate-retry",
    "event-id-conflict",
    "repeated-reads",
    "late-out-of-order",
    "adjacent-zone-conflict",
    "unknown-epc",
    "gateway-outage-replay",
    "large-stationary-burst",
)


@dataclass(frozen=True, slots=True)
class Observation:
    event_id: uuid.UUID
    epc: str
    observed_at: datetime
    rssi: float
    antenna_id: str | None = None

    def as_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "event_id": str(self.event_id),
            "epc": self.epc,
            "observed_at": self.observed_at.isoformat(),
            "rssi": self.rssi,
        }
        if self.antenna_id is not None:
            payload["antenna_id"] = self.antenna_id
        return payload


@dataclass(frozen=True, slots=True)
class ObservationBatch:
    device_id: uuid.UUID
    observations: tuple[Observation, ...]
    backlog_drained: bool = True
    reader_coverage_ok: bool = True
    credential_slot: str = "primary"
    expected_http_status: int = 202

    def as_payload(self) -> dict[str, object]:
        return {
            "device_id": str(self.device_id),
            "observations": [item.as_payload() for item in self.observations],
            "backlog_drained": self.backlog_drained,
            "reader_coverage_ok": self.reader_coverage_ok,
        }


class EventFactory:
    """Create reproducible UUID event IDs while preserving realistic timestamps."""

    def __init__(self, seed: int | None) -> None:
        # IDs are synthetic idempotency keys, not security credentials.
        self._random = random.Random(seed)  # noqa: S311

    def event(
        self,
        *,
        epc: str,
        observed_at: datetime,
        rssi: float,
        antenna_id: str | None = None,
    ) -> Observation:
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        event_id = uuid.UUID(int=self._random.getrandbits(128), version=4)
        return Observation(event_id, epc, observed_at, rssi, antenna_id)


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone offset")
    return parsed.astimezone(UTC)


def build_scenario(
    scenario: str,
    *,
    device_id: uuid.UUID = DEFAULT_DEVICE_ID,
    second_device_id: uuid.UUID = DEFAULT_SECOND_DEVICE_ID,
    epc: str = DEFAULT_EPC,
    unknown_epc: str = DEFAULT_UNKNOWN_EPC,
    count: int = 1000,
    seed: int | None = None,
    start_at: datetime | None = None,
) -> list[ObservationBatch]:
    """Build canonical observation-batch requests for one simulator scenario."""

    if scenario not in SCENARIOS:
        raise ValueError(f"unknown scenario: {scenario}")
    if not 1 <= count <= 1000:
        raise ValueError("count must be between 1 and 1000")
    base = start_at or datetime.now(UTC)
    if base.tzinfo is None or base.utcoffset() is None:
        raise ValueError("start_at must be timezone-aware")
    base = base.astimezone(UTC)
    factory = EventFactory(seed)
    primary = device_id
    epc = epc.strip().upper()
    unknown_epc = unknown_epc.strip().upper()

    if scenario == "normal":
        events = tuple(
            factory.event(
                epc=epc,
                observed_at=base + timedelta(seconds=offset),
                rssi=-47.0 + offset / 10,
                antenna_id="floor-1",
            )
            for offset in range(3)
        )
        return [ObservationBatch(primary, events)]

    if scenario == "duplicate-retry":
        event = factory.event(
            epc=epc,
            observed_at=base,
            rssi=-48.0,
            antenna_id="floor-1",
        )
        # The canonical API rejects duplicate IDs inside one batch. A real retry
        # is therefore represented by the same event in two separate batches.
        return [
            ObservationBatch(primary, (event,)),
            ObservationBatch(primary, (event,)),
        ]

    if scenario == "event-id-conflict":
        event = factory.event(
            epc=epc,
            observed_at=base,
            rssi=-48.0,
            antenna_id="floor-1",
        )
        conflicting = Observation(
            event_id=event.event_id,
            epc=event.epc,
            observed_at=event.observed_at,
            rssi=-61.0,
            antenna_id=event.antenna_id,
        )
        # The second request deliberately violates the immutable event contract.
        # A conforming API accepts the first batch and rejects the second with 409.
        return [
            ObservationBatch(primary, (event,)),
            ObservationBatch(primary, (conflicting,), expected_http_status=409),
        ]

    if scenario == "repeated-reads":
        events = tuple(
            factory.event(
                epc=epc,
                observed_at=base + timedelta(milliseconds=index * 100),
                rssi=-46.0 - (index % 3) / 10,
                antenna_id="floor-1",
            )
            for index in range(min(count, 1000))
        )
        return [ObservationBatch(primary, events)]

    if scenario == "late-out-of-order":
        offsets = (0, 3, -90, 1, 6)
        events = tuple(
            factory.event(
                epc=epc,
                observed_at=base + timedelta(seconds=offset),
                rssi=-49.0,
                antenna_id="floor-1",
            )
            for offset in offsets
        )
        return [ObservationBatch(primary, events)]

    if scenario == "adjacent-zone-conflict":
        batches: list[ObservationBatch] = []
        for index in range(6):
            use_primary = index % 2 == 0
            event = factory.event(
                epc=epc,
                observed_at=base + timedelta(seconds=index),
                rssi=-53.0 if use_primary else -53.2,
                antenna_id="zone-a" if use_primary else "zone-b",
            )
            batches.append(
                ObservationBatch(
                    primary if use_primary else second_device_id,
                    (event,),
                    credential_slot="primary" if use_primary else "secondary",
                )
            )
        return batches

    if scenario == "unknown-epc":
        events = tuple(
            factory.event(
                epc=unknown_epc,
                observed_at=base + timedelta(seconds=offset),
                rssi=-45.0,
                antenna_id="floor-1",
            )
            for offset in range(3)
        )
        return [ObservationBatch(primary, events)]

    if scenario == "gateway-outage-replay":
        buffered = tuple(
            factory.event(
                epc=epc,
                observed_at=base - timedelta(minutes=10) + timedelta(seconds=index),
                rssi=-50.0,
                antenna_id="floor-1",
            )
            for index in range(3)
        )
        live = tuple(
            factory.event(
                epc=epc,
                observed_at=base + timedelta(seconds=index),
                rssi=-48.0,
                antenna_id="floor-1",
            )
            for index in range(3)
        )
        return [
            ObservationBatch(
                primary,
                buffered,
                backlog_drained=False,
                reader_coverage_ok=False,
            ),
            ObservationBatch(primary, live),
        ]

    events = tuple(
        factory.event(
            epc=epc,
            observed_at=base + timedelta(milliseconds=index * 5),
            rssi=-44.0 - (index % 5) / 10,
            antenna_id="floor-1",
        )
        for index in range(count)
    )
    return [ObservationBatch(primary, events)]


def validate_base_url(base_url: str) -> str:
    candidate = base_url.rstrip("/")
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("base URL must be an absolute http(s) URL")
    return candidate


def post_batches(
    batches: list[ObservationBatch],
    *,
    base_url: str,
    primary_token: str | None,
    secondary_token: str | None = None,
    timeout: float = 10.0,
) -> list[dict[str, object]]:
    """Submit batches, returning response metadata without exposing credentials."""

    root = validate_base_url(base_url)
    results: list[dict[str, object]] = []
    for index, batch in enumerate(batches, start=1):
        token = primary_token if batch.credential_slot == "primary" else secondary_token
        if not token:
            raise ValueError(f"missing {batch.credential_slot} device token")
        request = Request(  # noqa: S310 - validate_base_url permits only HTTP(S).
            f"{root}/v1/rfid/observation-batches",
            data=json.dumps(batch.as_payload()).encode("utf-8"),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Device-Token": token,
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=timeout) as response:  # noqa: S310
                response_body = json.loads(response.read().decode("utf-8"))
                if response.status != batch.expected_http_status:
                    raise RuntimeError(
                        f"batch {index} returned {response.status}; "
                        f"expected {batch.expected_http_status}"
                    )
                results.append(
                    {
                        "batch_number": index,
                        "http_status": response.status,
                        "response": response_body,
                    }
                )
        except HTTPError as exc:
            raw_body = exc.read().decode("utf-8", errors="replace")
            if exc.code != batch.expected_http_status:
                raise RuntimeError(f"batch {index} was rejected ({exc.code}): {raw_body}") from exc
            try:
                response_body = json.loads(raw_body)
            except json.JSONDecodeError:
                response_body = {"detail": raw_body}
            results.append(
                {
                    "batch_number": index,
                    "http_status": exc.code,
                    "response": response_body,
                }
            )
        except URLError as exc:
            raise RuntimeError(f"could not reach Abacus for batch {index}: {exc.reason}") from exc
    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=SCENARIOS, default="normal")
    parser.add_argument("--device-id", type=uuid.UUID, default=DEFAULT_DEVICE_ID)
    parser.add_argument("--second-device-id", type=uuid.UUID, default=DEFAULT_SECOND_DEVICE_ID)
    parser.add_argument("--epc", default=DEFAULT_EPC)
    parser.add_argument("--unknown-epc", default=DEFAULT_UNKNOWN_EPC)
    parser.add_argument("--count", type=int, default=1000, help="1..1000 reads for burst scenarios")
    parser.add_argument("--seed", type=int, help="seed used for reproducible UUID event IDs")
    parser.add_argument("--start-at", type=parse_timestamp, help="ISO-8601 scenario start time")
    parser.add_argument("--base-url", help="when set, POST batches instead of printing them")
    parser.add_argument(
        "--device-token",
        default=os.getenv("ABACUS_DEVICE_TOKEN"),
        help="primary token (or ABACUS_DEVICE_TOKEN); never printed",
    )
    parser.add_argument(
        "--second-device-token",
        default=os.getenv("ABACUS_SECOND_DEVICE_TOKEN"),
        help="second-zone token (or ABACUS_SECOND_DEVICE_TOKEN); never printed",
    )
    parser.add_argument("--timeout", type=float, default=10.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        batches = build_scenario(
            args.scenario,
            device_id=args.device_id,
            second_device_id=args.second_device_id,
            epc=args.epc,
            unknown_epc=args.unknown_epc,
            count=args.count,
            seed=args.seed,
            start_at=args.start_at,
        )
        if args.base_url:
            output: object = {
                "scenario": args.scenario,
                "submissions": post_batches(
                    batches,
                    base_url=args.base_url,
                    primary_token=args.device_token,
                    secondary_token=args.second_device_token,
                    timeout=args.timeout,
                ),
            }
        else:
            output = {
                "scenario": args.scenario,
                "batches": [batch.as_payload() for batch in batches],
            }
    except (RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
