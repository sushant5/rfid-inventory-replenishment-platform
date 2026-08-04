import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

pytestmark = pytest.mark.integration


def test_runtime_identity_audit_acl_is_append_only(
    application_session_factory: sessionmaker[Session],
) -> None:
    with application_session_factory() as db:
        privileges = {
            privilege: bool(
                db.scalar(
                    text(
                        "SELECT has_table_privilege("
                        "current_user, 'public.identity_audit_records', :privilege)"
                    ),
                    {"privilege": privilege},
                )
            )
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE")
        }

    assert privileges == {
        "SELECT": True,
        "INSERT": True,
        "UPDATE": False,
        "DELETE": False,
    }
