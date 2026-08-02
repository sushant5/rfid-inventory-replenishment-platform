from pathlib import Path

from pytest import MonkeyPatch

from abacus.cli import BootstrapSettings


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
