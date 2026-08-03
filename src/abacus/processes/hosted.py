"""Supervise the complete demo topology inside one hosted web container.

Docker Compose keeps the API and workers as independent processes. Some hosted
hosting tiers provide only one free web process, so this launcher starts the same
three entry points as children and treats any unexpected child exit as fatal.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from collections.abc import Sequence

import structlog

from abacus.logging import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)


def _port() -> str:
    raw = os.environ.get("PORT", "8000")
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError("PORT must be an integer") from exc
    if not 1 <= value <= 65_535:
        raise ValueError("PORT must be between 1 and 65535")
    return str(value)


def _run_startup(command: Sequence[str], name: str) -> None:
    logger.info("hosted_startup_step_started", step=name)
    subprocess.run(list(command), check=True)  # noqa: S603
    logger.info("hosted_startup_step_completed", step=name)


def _terminate(children: Sequence[subprocess.Popen[bytes]]) -> None:
    for child in children:
        if child.poll() is None:
            child.terminate()

    deadline = time.monotonic() + 10
    for child in children:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            child.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)


def run() -> int:
    """Run migrations, seed the reviewer login, and supervise all demo processes."""

    executable = sys.executable
    _run_startup((executable, "-m", "alembic", "upgrade", "head"), "migrations")
    _run_startup((executable, "-m", "abacus.cli", "bootstrap-admin"), "reviewer_seed")

    commands = (
        (executable, "-m", "abacus.processes.catalog_worker"),
        (executable, "-m", "abacus.processes.event_worker"),
        (
            executable,
            "-m",
            "uvicorn",
            "abacus.main:app",
            "--host",
            "0.0.0.0",  # noqa: S104 - required by the hosting load balancer.
            "--port",
            _port(),
        ),
    )
    children: list[subprocess.Popen[bytes]] = []
    stop_requested = False

    def request_stop(signum: int, _frame: object) -> None:
        nonlocal stop_requested
        logger.info("hosted_shutdown_requested", signal=signum)
        stop_requested = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    try:
        for launch_command in commands:
            children.append(subprocess.Popen(launch_command))  # noqa: S603
        while not stop_requested:
            for watched_command, child in zip(commands, children, strict=True):
                return_code = child.poll()
                if return_code is not None:
                    logger.error(
                        "hosted_child_exited",
                        process=(
                            watched_command[3] if len(watched_command) > 3 else watched_command[-1]
                        ),
                        return_code=return_code,
                    )
                    return return_code or 1
            time.sleep(0.2)
        return 0
    finally:
        _terminate(children)


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
