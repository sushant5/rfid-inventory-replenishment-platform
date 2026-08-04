import uuid

import pytest
from sqlalchemy import delete, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from abacus.models.architecture import (
    PolicyDefinition,
    PolicyRule,
    PolicyVersion,
    PolicyVersionStatus,
)
from abacus.models.tenancy import Tenant

pytestmark = pytest.mark.integration


def test_activated_policy_cannot_be_demoted_changed_or_move_rules(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    tenant_id = uuid.uuid4()
    policy_id = uuid.uuid4()
    active_version_id = uuid.uuid4()
    draft_version_id = uuid.uuid4()
    rule_id = uuid.uuid4()

    with postgres_session_factory() as db:
        db.add(Tenant(id=tenant_id, code=f"trigger-{tenant_id.hex}", name="Trigger test"))
        db.flush()
        db.add(
            PolicyDefinition(
                id=policy_id,
                tenant_id=tenant_id,
                name="Immutable active policy",
                description=None,
            )
        )
        db.flush()
        db.add_all(
            [
                PolicyVersion(
                    id=active_version_id,
                    tenant_id=tenant_id,
                    policy_id=policy_id,
                    version_number=1,
                    status=PolicyVersionStatus.DRAFT,
                ),
                PolicyVersion(
                    id=draft_version_id,
                    tenant_id=tenant_id,
                    policy_id=policy_id,
                    version_number=2,
                    status=PolicyVersionStatus.DRAFT,
                ),
            ]
        )
        db.flush()
        db.add(
            PolicyRule(
                id=rule_id,
                tenant_id=tenant_id,
                version_id=active_version_id,
                min_floor_qty=1,
                target_floor_qty=3,
                priority=0,
            )
        )
        db.flush()
        active = db.get(PolicyVersion, active_version_id)
        assert active is not None
        active.status = PolicyVersionStatus.ACTIVE
        db.commit()

        forbidden_statements = (
            (
                "UPDATE replenishment_policy_rules SET target_floor_qty = 4 WHERE id = :id",
                {"id": rule_id},
            ),
            (
                "UPDATE replenishment_policy_rules SET version_id = :draft WHERE id = :id",
                {"id": rule_id, "draft": draft_version_id},
            ),
            (
                "UPDATE replenishment_policy_versions SET status = 'DRAFT' WHERE id = :id",
                {"id": active_version_id},
            ),
            (
                "UPDATE replenishment_policy_versions SET version_number = 99 WHERE id = :id",
                {"id": active_version_id},
            ),
        )
        for statement, parameters in forbidden_statements:
            with pytest.raises(DBAPIError, match=r"immutable|cannot move"):
                db.execute(text(statement), parameters)
                db.commit()
            db.rollback()

        # Retirement is a lifecycle transition, not a content mutation.
        db.execute(
            text("UPDATE replenishment_policy_versions SET status = 'RETIRED' WHERE id = :id"),
            {"id": active_version_id},
        )
        db.commit()

        # Tenant deletion remains possible and cascades immutable policy history.
        db.execute(delete(Tenant).where(Tenant.id == tenant_id))
        db.commit()
