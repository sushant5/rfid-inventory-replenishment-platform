import uuid

import pytest
from pydantic import ValidationError

from abacus.api.errors import ApiError
from abacus.models.architecture import CanonicalIdentityRole
from abacus.models.identity import IdentityRole
from abacus.schemas.identity import UserRolesReplace, UserStoreAssignmentsReplace
from abacus.schemas.tenancy import StoreDeviceCreate
from abacus.security import Principal, RoleScope
from abacus.services.identity import _compatibility_grant_specs, replace_user_roles


def _store_manager() -> Principal:
    return Principal(
        user_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        email="manager@orange.example",
        display_name="Store Manager",
        role_scopes=(RoleScope(IdentityRole.STORE_MANAGER, uuid.uuid4()),),
    )


def test_canonical_command_bodies_reject_client_supplied_tenant_scope() -> None:
    tenant_id = uuid.uuid4()

    with pytest.raises(ValidationError):
        StoreDeviceCreate.model_validate(
            {
                "tenant_id": str(tenant_id),
                "serial_number": "reader-1",
                "display_name": "Reader 1",
                "zone_id": str(uuid.uuid4()),
            }
        )
    with pytest.raises(ValidationError):
        UserRolesReplace.model_validate({"tenant_id": str(tenant_id), "roles": ["STORE_ASSOCIATE"]})
    with pytest.raises(ValidationError):
        UserStoreAssignmentsReplace.model_validate({"tenant_id": str(tenant_id), "store_ids": []})


def test_access_replace_bodies_reject_duplicates() -> None:
    store_id = uuid.uuid4()

    with pytest.raises(ValidationError):
        UserRolesReplace(
            roles=[
                CanonicalIdentityRole.STORE_MANAGER,
                CanonicalIdentityRole.STORE_MANAGER,
            ]
        )
    with pytest.raises(ValidationError):
        UserStoreAssignmentsReplace(store_ids=[store_id, store_id])


def test_corporate_user_compatibility_never_escalates_to_admin() -> None:
    store_id = uuid.uuid4()

    specs = _compatibility_grant_specs(
        frozenset({CanonicalIdentityRole.CORPORATE_USER}),
        frozenset({store_id}),
    )

    assert specs == frozenset({(IdentityRole.STORE_ASSOCIATE, store_id)})
    assert (IdentityRole.CORPORATE_ADMIN, None) not in specs


def test_only_tenant_admin_can_replace_roles() -> None:
    principal = _store_manager()

    with pytest.raises(ApiError) as error:
        replace_user_roles(
            None,  # type: ignore[arg-type]
            principal,
            uuid.uuid4(),
            UserRolesReplace(roles=[CanonicalIdentityRole.STORE_ASSOCIATE]),
        )

    assert error.value.status_code == 403
    assert error.value.code == "role_assignment_forbidden"
