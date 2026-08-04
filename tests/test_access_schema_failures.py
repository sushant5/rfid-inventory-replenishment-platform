from __future__ import annotations

import uuid

import pytest
from pydantic import BaseModel, ValidationError

from abacus.models.architecture import CanonicalIdentityRole, ReplenishmentTaskStatus
from abacus.models.identity import IdentityRole
from abacus.schemas.identity import (
    RoleAssignmentCreate,
    UserAccessReplace,
    UserCreate,
    UserCreateRequest,
    UserRolesReplace,
    UserStoreAssignmentsReplace,
)
from abacus.schemas.replenishment import (
    PolicyCreate,
    PolicyRuleWrite,
    ReplenishmentEvaluationCreate,
    ReplenishmentTaskPatch,
)

STORE_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
OTHER_STORE_ID = uuid.UUID("00000000-0000-0000-0000-000000000002")


@pytest.mark.parametrize(
    ("model", "payload", "message"),
    [
        (
            RoleAssignmentCreate,
            {"role": IdentityRole.CORPORATE_ADMIN, "store_id": STORE_ID},
            "cannot specify store_id",
        ),
        (
            RoleAssignmentCreate,
            {"role": IdentityRole.STORE_MANAGER},
            "require store_id",
        ),
        (
            UserRolesReplace,
            {"roles": [CanonicalIdentityRole.TENANT_ADMIN] * 2},
            "duplicates",
        ),
        (
            UserStoreAssignmentsReplace,
            {"store_ids": [STORE_ID, STORE_ID]},
            "duplicates",
        ),
        (
            UserAccessReplace,
            {"roles": [CanonicalIdentityRole.STORE_MANAGER] * 2, "store_ids": [STORE_ID]},
            "roles cannot contain duplicates",
        ),
        (
            UserAccessReplace,
            {
                "roles": [CanonicalIdentityRole.STORE_MANAGER],
                "store_ids": [STORE_ID, STORE_ID],
            },
            "store_ids cannot contain duplicates",
        ),
        (
            UserAccessReplace,
            {"roles": [CanonicalIdentityRole.STORE_MANAGER]},
            "require at least one store_id",
        ),
        (
            UserAccessReplace,
            {"roles": [CanonicalIdentityRole.CORPORATE_USER], "store_ids": [STORE_ID]},
            "store_ids require a store-scoped role",
        ),
        (
            UserCreate,
            {
                "email": "user@example.com",
                "display_name": "   ",
                "password": "valid-password-2026",
                "role_assignments": [{"role": IdentityRole.CORPORATE_ADMIN}],
            },
            "cannot be blank",
        ),
        (
            UserCreate,
            {
                "email": "user@example.com",
                "display_name": "User",
                "password": "valid-password-2026",
                "role_assignments": [
                    {"role": IdentityRole.STORE_ASSOCIATE, "store_id": STORE_ID},
                    {"role": IdentityRole.STORE_ASSOCIATE, "store_id": STORE_ID},
                ],
            },
            "assignments cannot contain duplicates",
        ),
        (
            UserCreateRequest,
            {
                "email": "user@example.com",
                "display_name": "   ",
                "password": "valid-password-2026",
                "roles": [CanonicalIdentityRole.CORPORATE_USER],
            },
            "cannot be blank",
        ),
        (
            UserCreateRequest,
            {
                "email": "user@example.com",
                "display_name": "User",
                "password": "valid-password-2026",
                "roles": [CanonicalIdentityRole.STORE_ASSOCIATE] * 2,
                "store_ids": [STORE_ID],
            },
            "roles cannot contain duplicates",
        ),
        (
            UserCreateRequest,
            {
                "email": "user@example.com",
                "display_name": "User",
                "password": "valid-password-2026",
                "roles": [CanonicalIdentityRole.STORE_ASSOCIATE],
                "store_ids": [STORE_ID, STORE_ID],
            },
            "store_ids cannot contain duplicates",
        ),
        (
            UserCreateRequest,
            {
                "email": "user@example.com",
                "display_name": "User",
                "password": "valid-password-2026",
                "roles": [CanonicalIdentityRole.STORE_ASSOCIATE],
            },
            "require at least one store_id",
        ),
        (
            UserCreateRequest,
            {
                "email": "user@example.com",
                "display_name": "User",
                "password": "valid-password-2026",
                "roles": [CanonicalIdentityRole.CORPORATE_USER],
                "store_ids": [STORE_ID],
            },
            "store_ids require a store-scoped role",
        ),
        (
            PolicyRuleWrite,
            {"size": "M", "min_floor_qty": 1, "target_floor_qty": 2},
            "size requires sku_id",
        ),
        (
            PolicyRuleWrite,
            {"min_floor_qty": 3, "target_floor_qty": 2},
            "target_floor_qty must be at least",
        ),
        (
            PolicyCreate,
            {
                "name": "   ",
                "rules": [{"min_floor_qty": 1, "target_floor_qty": 2}],
            },
            "name cannot be blank",
        ),
        (
            ReplenishmentEvaluationCreate,
            {"store_id": STORE_ID, "sku_ids": [OTHER_STORE_ID, OTHER_STORE_ID]},
            "sku_ids cannot contain duplicates",
        ),
        (
            ReplenishmentTaskPatch,
            {"status": ReplenishmentTaskStatus.CLAIMED, "version": 1, "note": "   "},
            "note cannot be blank",
        ),
    ],
)
def test_invalid_access_and_policy_combinations_are_rejected(
    model: type[BaseModel],
    payload: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        model.model_validate(payload)
