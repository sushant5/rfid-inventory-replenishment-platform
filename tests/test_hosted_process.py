from __future__ import annotations

import signal
import subprocess
from collections.abc import Callable
from typing import cast
from unittest.mock import MagicMock

import pytest

from abacus.processes import hosted


def _process_mock(*, return_code: int | None = None) -> MagicMock:
    process = MagicMock(spec=subprocess.Popen)
    process.poll.return_value = return_code
    process.wait.return_value = return_code or 0
    return process


def test_hosted_port_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PORT", raising=False)
    assert hosted._port() == "8000"
    monkeypatch.setenv("PORT", "9123")
    assert hosted._port() == "9123"
    monkeypatch.setenv("PORT", "not-a-number")
    with pytest.raises(ValueError, match="integer"):
        hosted._port()
    monkeypatch.setenv("PORT", "70000")
    with pytest.raises(ValueError, match="between 1 and 65535"):
        hosted._port()


def test_hosted_startup_runs_checked_command(monkeypatch: pytest.MonkeyPatch) -> None:
    run = MagicMock()
    monkeypatch.setattr("abacus.processes.hosted.subprocess.run", run)
    hosted._run_startup(("python", "-m", "alembic"), "migration-test")
    run.assert_called_once_with(["python", "-m", "alembic"], check=True)


def test_hosted_terminate_kills_child_that_ignores_graceful_stop() -> None:
    process = _process_mock()
    process.wait.side_effect = [subprocess.TimeoutExpired("worker", 10), 0]

    hosted._terminate([cast(subprocess.Popen[bytes], process)])

    process.terminate.assert_called_once_with()
    process.kill.assert_called_once_with()
    assert process.wait.call_count == 2


def test_hosted_run_supervises_children_and_handles_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    startup = MagicMock()
    processes = [_process_mock() for _ in range(3)]
    launch = MagicMock(side_effect=processes)
    handlers: dict[int, Callable[[int, object], None]] = {}

    monkeypatch.setattr(hosted, "_run_startup", startup)
    monkeypatch.setattr(hosted, "_port", lambda: "8123")
    monkeypatch.setattr("abacus.processes.hosted.subprocess.Popen", launch)
    monkeypatch.setattr(
        "abacus.processes.hosted.signal.signal",
        lambda signum, handler: handlers.__setitem__(signum, handler),
    )
    monkeypatch.setattr(
        "abacus.processes.hosted.time.sleep",
        lambda _seconds: handlers[signal.SIGTERM](signal.SIGTERM, None),
    )

    assert hosted.run() == 0
    assert startup.call_count == 2
    assert launch.call_count == 3
    assert launch.call_args_list[-1].args[0][-2:] == ("--port", "8123")
    for process in processes:
        process.terminate.assert_called_once_with()


def test_hosted_run_propagates_unexpected_child_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processes = [_process_mock(return_code=7), _process_mock(), _process_mock()]
    monkeypatch.setattr(hosted, "_run_startup", MagicMock())
    monkeypatch.setattr(
        "abacus.processes.hosted.subprocess.Popen",
        MagicMock(side_effect=processes),
    )
    monkeypatch.setattr("abacus.processes.hosted.signal.signal", MagicMock())

    assert hosted.run() == 7
    processes[0].terminate.assert_not_called()
    processes[1].terminate.assert_called_once_with()
    processes[2].terminate.assert_called_once_with()


def test_hosted_main_returns_process_exit_code(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hosted, "run", lambda: 3)
    with pytest.raises(SystemExit) as exc_info:
        hosted.main()
    assert exc_info.value.code == 3
