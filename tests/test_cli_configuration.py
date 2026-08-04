from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from pytest import CaptureFixture, MonkeyPatch

from abacus.cli import BootstrapSettings, main
from abacus.models.architecture import CanonicalIdentityRole


def test_bootstrap_settings_load_repository_env_file(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    variable_names = (
        "BOOTSTRAP_TENANT_CODE",
        "BOOTSTRAP_TENANT_NAME",
        "BOOTSTRAP_ADMIN_EMAIL",
        "BOOTSTRAP_ADMIN_DISPLAY_NAME",
        "BOOTSTRAP_ADMIN_PASSWORD",
        "BOOTSTRAP_PUBLIC_REVIEWER_ENABLED",
        "BOOTSTRAP_PUBLIC_REVIEWER_EMAIL",
        "BOOTSTRAP_PUBLIC_REVIEWER_DISPLAY_NAME",
        "BOOTSTRAP_PUBLIC_REVIEWER_PASSWORD",
    )
    for name in variable_names:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "\n".join(
            (
                "BOOTSTRAP_TENANT_CODE=orange",
                "BOOTSTRAP_TENANT_NAME=Orange",
                "BOOTSTRAP_ADMIN_EMAIL=reviewer@orange.example",
                "BOOTSTRAP_ADMIN_DISPLAY_NAME=Assignment Reviewer",
                "BOOTSTRAP_ADMIN_PASSWORD=a-strong-reviewer-password",
                "BOOTSTRAP_PUBLIC_REVIEWER_ENABLED=true",
                "BOOTSTRAP_PUBLIC_REVIEWER_EMAIL=demo-reader@orange.example",
                "BOOTSTRAP_PUBLIC_REVIEWER_DISPLAY_NAME=Public API Reviewer",
                "BOOTSTRAP_PUBLIC_REVIEWER_PASSWORD=Orange-Demo-ReadOnly-2026!",
            )
        ),
        encoding="utf-8",
    )

    settings = BootstrapSettings()

    assert settings.bootstrap_tenant_code == "orange"
    assert settings.bootstrap_tenant_name == "Orange"
    assert settings.bootstrap_admin_email == "reviewer@orange.example"
    assert settings.bootstrap_admin_display_name == "Assignment Reviewer"
    assert settings.bootstrap_admin_password == "a-strong-reviewer-password"
    assert settings.bootstrap_public_reviewer_enabled is True
    assert settings.bootstrap_public_reviewer_email == "demo-reader@orange.example"
    assert settings.bootstrap_public_reviewer_display_name == "Public API Reviewer"
    assert settings.bootstrap_public_reviewer_password == "Orange-Demo-ReadOnly-2026!"


def test_bootstrap_validation_error_does_not_echo_password(
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    rejected_password = "secret-7"
    monkeypatch.setenv("BOOTSTRAP_TENANT_CODE", "orange")
    monkeypatch.setenv("BOOTSTRAP_TENANT_NAME", "Orange")
    monkeypatch.setenv("BOOTSTRAP_ADMIN_EMAIL", "reviewer@orange.example")
    monkeypatch.setenv("BOOTSTRAP_ADMIN_DISPLAY_NAME", "Assignment Reviewer")
    monkeypatch.setenv("BOOTSTRAP_ADMIN_PASSWORD", rejected_password)

    assert main(["bootstrap-admin"]) == 2
    captured = capsys.readouterr()
    assert rejected_password not in captured.err
    assert "password" in captured.err
    assert "at least 12" in captured.err


def test_public_reviewer_bootstrap_is_disabled_by_default(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("BOOTSTRAP_PUBLIC_REVIEWER_ENABLED", raising=False)
    session_factory = MagicMock()
    monkeypatch.setattr("abacus.cli.SessionLocal", session_factory)

    assert main(["bootstrap-public-reviewer"]) == 0
    assert "disabled" in capsys.readouterr().out.lower()
    session_factory.assert_not_called()


def test_public_reviewer_cli_builds_only_the_read_only_role(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    password = "Orange-Demo-ReadOnly-2026!"
    monkeypatch.setenv("BOOTSTRAP_TENANT_CODE", "orange")
    monkeypatch.setenv("BOOTSTRAP_ADMIN_EMAIL", "reviewer@orange.example")
    monkeypatch.setenv("BOOTSTRAP_PUBLIC_REVIEWER_ENABLED", "true")
    monkeypatch.setenv("BOOTSTRAP_PUBLIC_REVIEWER_EMAIL", "demo-reader@orange.example")
    monkeypatch.setenv(
        "BOOTSTRAP_PUBLIC_REVIEWER_DISPLAY_NAME",
        "Public API Reviewer",
    )
    monkeypatch.setenv("BOOTSTRAP_PUBLIC_REVIEWER_PASSWORD", password)
    db = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = db
    monkeypatch.setattr("abacus.cli.SessionLocal", MagicMock(return_value=context))
    bootstrap = MagicMock(
        return_value=SimpleNamespace(
            user=SimpleNamespace(email="demo-reader@orange.example", id="reviewer-id")
        )
    )
    monkeypatch.setattr("abacus.cli.bootstrap_public_reviewer", bootstrap)

    assert main(["bootstrap-public-reviewer"]) == 0

    call = bootstrap.call_args
    assert call.kwargs["tenant_code"] == "orange"
    request = call.kwargs["request"]
    assert request.roles == [CanonicalIdentityRole.CORPORATE_USER]
    assert request.store_ids == []
    assert password not in capsys.readouterr().out


def test_public_reviewer_validation_error_does_not_echo_password(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    rejected_password = "secret-7"
    monkeypatch.setenv("BOOTSTRAP_TENANT_CODE", "orange")
    monkeypatch.setenv("BOOTSTRAP_PUBLIC_REVIEWER_ENABLED", "true")
    monkeypatch.setenv("BOOTSTRAP_PUBLIC_REVIEWER_EMAIL", "demo-reader@orange.example")
    monkeypatch.setenv(
        "BOOTSTRAP_PUBLIC_REVIEWER_DISPLAY_NAME",
        "Public API Reviewer",
    )
    monkeypatch.setenv("BOOTSTRAP_PUBLIC_REVIEWER_PASSWORD", rejected_password)

    assert main(["bootstrap-public-reviewer"]) == 2
    captured = capsys.readouterr()
    assert rejected_password not in captured.err
    assert "password" in captured.err
    assert "at least 12" in captured.err
