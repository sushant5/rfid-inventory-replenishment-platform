import json
import uuid
from datetime import UTC, datetime

import pytest
from scripts.rfid_simulator import (
    DEFAULT_DEVICE_ID,
    DEFAULT_EPC,
    DEFAULT_SECOND_DEVICE_ID,
    DEFAULT_UNKNOWN_EPC,
    SCENARIOS,
    build_scenario,
    main,
    post_batches,
)

START = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_every_scenario_emits_canonical_timezone_aware_batches(scenario: str) -> None:
    batches = build_scenario(scenario, seed=73, start_at=START, count=12)

    assert batches
    for batch in batches:
        payload = batch.as_payload()
        assert set(payload) == {
            "device_id",
            "observations",
            "backlog_drained",
            "reader_coverage_ok",
        }
        uuid.UUID(str(payload["device_id"]))
        observations = payload["observations"]
        assert isinstance(observations, list)
        assert observations
        event_ids: list[str] = []
        for observation in observations:
            assert isinstance(observation, dict)
            assert {"event_id", "epc", "observed_at", "rssi"} <= observation.keys()
            event_ids.append(str(observation["event_id"]))
            uuid.UUID(str(observation["event_id"]))
            parsed = datetime.fromisoformat(str(observation["observed_at"]))
            assert parsed.utcoffset() is not None
        assert len(event_ids) == len(set(event_ids))


def test_seed_and_start_time_make_generation_reproducible() -> None:
    first = build_scenario("normal", seed=7, start_at=START)
    second = build_scenario("normal", seed=7, start_at=START)

    assert [batch.as_payload() for batch in first] == [batch.as_payload() for batch in second]


def test_duplicate_retry_reuses_exact_event_in_a_separate_batch() -> None:
    batches = build_scenario("duplicate-retry", seed=1, start_at=START)

    assert len(batches) == 2
    assert batches[0].observations == batches[1].observations
    assert batches[0].as_payload() == batches[1].as_payload()


def test_event_id_conflict_reuses_identity_but_changes_immutable_payload() -> None:
    batches = build_scenario("event-id-conflict", seed=1, start_at=START)

    assert len(batches) == 2
    first = batches[0].observations[0]
    conflicting = batches[1].observations[0]
    assert first.event_id == conflicting.event_id
    assert first.epc == conflicting.epc
    assert first.observed_at == conflicting.observed_at
    assert first.rssi != conflicting.rssi
    assert batches[0].expected_http_status == 202
    assert batches[1].expected_http_status == 409


def test_repeated_and_stationary_scenarios_represent_one_physical_epc() -> None:
    repeated = build_scenario("repeated-reads", seed=2, start_at=START, count=27)
    burst = build_scenario("large-stationary-burst", seed=2, start_at=START, count=1000)

    assert len(repeated[0].observations) == 27
    assert len(burst[0].observations) == 1000
    assert {event.epc for event in repeated[0].observations} == {DEFAULT_EPC}
    assert {event.epc for event in burst[0].observations} == {DEFAULT_EPC}
    assert len({event.event_id for event in burst[0].observations}) == 1000


def test_late_scenario_is_deliberately_not_in_observed_time_order() -> None:
    batch = build_scenario("late-out-of-order", seed=3, start_at=START)[0]
    observed_times = [event.observed_at for event in batch.observations]

    assert observed_times != sorted(observed_times)
    assert min(observed_times) < START


def test_adjacent_zone_conflict_alternates_two_registered_devices() -> None:
    batches = build_scenario("adjacent-zone-conflict", seed=4, start_at=START)

    assert [batch.device_id for batch in batches] == [
        DEFAULT_DEVICE_ID,
        DEFAULT_SECOND_DEVICE_ID,
        DEFAULT_DEVICE_ID,
        DEFAULT_SECOND_DEVICE_ID,
        DEFAULT_DEVICE_ID,
        DEFAULT_SECOND_DEVICE_ID,
    ]
    assert {batch.credential_slot for batch in batches} == {"primary", "secondary"}
    assert {batch.observations[0].epc for batch in batches} == {DEFAULT_EPC}


def test_unknown_epc_and_outage_replay_are_explicit() -> None:
    unknown = build_scenario("unknown-epc", seed=5, start_at=START)[0]
    outage = build_scenario("gateway-outage-replay", seed=5, start_at=START)

    assert {event.epc for event in unknown.observations} == {DEFAULT_UNKNOWN_EPC}
    assert len(outage) == 2
    assert outage[0].backlog_drained is False
    assert outage[0].reader_coverage_ok is False
    assert outage[1].backlog_drained is True
    assert outage[1].reader_coverage_ok is True
    assert max(event.observed_at for event in outage[0].observations) < min(
        event.observed_at for event in outage[1].observations
    )


@pytest.mark.parametrize("count", [0, 1001])
def test_count_outside_api_batch_limit_is_rejected(count: int) -> None:
    with pytest.raises(ValueError, match="between 1 and 1000"):
        build_scenario("large-stationary-burst", count=count, start_at=START)


def test_post_mode_validates_credentials_before_network_access() -> None:
    batch = build_scenario("normal", seed=6, start_at=START)

    with pytest.raises(ValueError, match="missing primary device token"):
        post_batches(batch, base_url="http://localhost:1", primary_token=None)


def test_generation_output_never_prints_device_token(capsys: pytest.CaptureFixture[str]) -> None:
    secret = "do-not-print-this-token"
    exit_code = main(
        [
            "--scenario",
            "normal",
            "--seed",
            "9",
            "--start-at",
            START.isoformat(),
            "--device-token",
            secret,
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert secret not in captured.out
    assert secret not in captured.err
    parsed = json.loads(captured.out)
    assert parsed["scenario"] == "normal"
