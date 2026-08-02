import argparse
import sys
from collections.abc import Sequence

from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from abacus.api.errors import ApiError
from abacus.db import SessionLocal
from abacus.models.identity import IdentityRole
from abacus.schemas.identity import RoleAssignmentCreate, UserCreate
from abacus.schemas.tenancy import TenantCreate
from abacus.services.identity import bootstrap_corporate_admin


class BootstrapSettings(BaseSettings):
    """Bootstrap inputs from process variables or the repository-local `.env`."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    bootstrap_tenant_code: str = ""
    bootstrap_tenant_name: str = ""
    bootstrap_admin_email: str = ""
    bootstrap_admin_display_name: str = ""
    bootstrap_admin_password: str = ""


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Abacus deployment operations")
    commands = parser.add_subparsers(dest="command", required=True)
    bootstrap = commands.add_parser(
        "bootstrap-admin",
        help="idempotently create the first tenant and corporate administrator",
    )
    bootstrap.add_argument(
        "--if-configured",
        action="store_true",
        help="exit successfully when a required BOOTSTRAP_* variable is absent",
    )
    return parser


def _bootstrap_from_environment(*, if_configured: bool) -> int:
    settings = BootstrapSettings()
    values = {
        "BOOTSTRAP_TENANT_CODE": settings.bootstrap_tenant_code.strip(),
        "BOOTSTRAP_TENANT_NAME": settings.bootstrap_tenant_name.strip(),
        "BOOTSTRAP_ADMIN_EMAIL": settings.bootstrap_admin_email.strip(),
        "BOOTSTRAP_ADMIN_DISPLAY_NAME": settings.bootstrap_admin_display_name.strip(),
        # Do not normalize a password: spaces may be intentional secret material.
        "BOOTSTRAP_ADMIN_PASSWORD": settings.bootstrap_admin_password,
    }
    missing = [name for name, value in values.items() if not value]
    if missing:
        if if_configured:
            print(f"Bootstrap skipped; missing: {', '.join(missing)}")
            return 0
        print(f"Missing required environment variables: {', '.join(missing)}", file=sys.stderr)
        return 2

    try:
        tenant_request = TenantCreate(
            code=values["BOOTSTRAP_TENANT_CODE"],
            name=values["BOOTSTRAP_TENANT_NAME"],
        )
        user_request = UserCreate(
            email=values["BOOTSTRAP_ADMIN_EMAIL"],
            display_name=values["BOOTSTRAP_ADMIN_DISPLAY_NAME"],
            password=values["BOOTSTRAP_ADMIN_PASSWORD"],
            role_assignments=[RoleAssignmentCreate(role=IdentityRole.CORPORATE_ADMIN)],
        )
    except ValidationError as exc:
        print(f"Invalid bootstrap configuration: {exc}", file=sys.stderr)
        return 2

    with SessionLocal() as db:
        try:
            record = bootstrap_corporate_admin(db, tenant_request, user_request)
        except ApiError as exc:
            print(f"Bootstrap failed [{exc.status_code}]: {exc.detail}", file=sys.stderr)
            return 1
    print(f"Corporate administrator ready: {record.user.email} ({record.user.id})")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    if arguments.command == "bootstrap-admin":
        return _bootstrap_from_environment(if_configured=arguments.if_configured)
    raise AssertionError(f"Unhandled command: {arguments.command}")


if __name__ == "__main__":
    raise SystemExit(main())
