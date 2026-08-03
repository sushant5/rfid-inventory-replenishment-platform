import uuid

import pytest

from abacus.api.errors import ApiError
from abacus.models.architecture import CanonicalIdentityRole
from abacus.models.identity import IdentityRole
from abacus.security import Permission, Principal, RoleScope, ensure_can_assign_roles


def _principal(*scopes: RoleScope) -> Principal:
    return Principal(
        user_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        email="person@example.com",
        display_name="Test Person",
        role_scopes=scopes,
    )


def test_corporate_admin_has_tenant_wide_user_access() -> None:
    principal = _principal(RoleScope(IdentityRole.CORPORATE_ADMIN, None))

    assert principal.has_tenant_permission(Permission.USERS_CREATE)
    assert principal.can_access_store(Permission.USERS_READ, uuid.uuid4())
    assert principal.has_permission(Permission.IDENTITY_AUDIT_READ)


def test_corporate_user_can_read_every_store_but_cannot_manage_inventory() -> None:
    principal = _principal(RoleScope(CanonicalIdentityRole.CORPORATE_USER, None))

    assert principal.can_access_store(Permission.INVENTORY_READ, uuid.uuid4())
    assert principal.can_access_store(Permission.REPLENISHMENT_READ, uuid.uuid4())
    assert not principal.has_permission(Permission.REPLENISHMENT_MANAGE)
    assert not principal.has_permission(Permission.USERS_CREATE)


def test_store_manager_access_is_limited_to_assigned_store() -> None:
    assigned_store = uuid.uuid4()
    another_store = uuid.uuid4()
    principal = _principal(RoleScope(IdentityRole.STORE_MANAGER, assigned_store))

    assert principal.can_access_store(Permission.USERS_READ, assigned_store)
    assert not principal.can_access_store(Permission.USERS_READ, another_store)
    assert not principal.has_tenant_permission(Permission.USERS_READ)
    assert not principal.has_permission(Permission.IDENTITY_AUDIT_READ)


def test_store_associate_cannot_administer_users_or_policies() -> None:
    principal = _principal(RoleScope(IdentityRole.STORE_ASSOCIATE, uuid.uuid4()))

    assert not principal.has_permission(Permission.USERS_CREATE)
    assert not principal.has_permission(Permission.USERS_READ)
    assert not principal.has_permission(Permission.USERS_SUSPEND)
    assert not principal.has_permission(Permission.POLICY_MANAGE)
    assert principal.has_permission(Permission.REPLENISHMENT_EXECUTE)


@pytest.mark.parametrize(
    "assignment",
    [
        RoleScope(IdentityRole.CORPORATE_ADMIN, None),
        RoleScope(IdentityRole.STORE_MANAGER, uuid.uuid4()),
        RoleScope(IdentityRole.STORE_ASSOCIATE, uuid.uuid4()),
    ],
)
def test_store_manager_cannot_delegate_privilege_or_another_store(
    assignment: RoleScope,
) -> None:
    managed_store = uuid.uuid4()
    principal = _principal(RoleScope(IdentityRole.STORE_MANAGER, managed_store))

    with pytest.raises(ApiError) as error:
        ensure_can_assign_roles(principal, [assignment])

    assert error.value.status_code == 403
    assert error.value.code == "role_assignment_forbidden"


def test_store_manager_can_create_associate_only_in_managed_store() -> None:
    managed_store = uuid.uuid4()
    principal = _principal(RoleScope(IdentityRole.STORE_MANAGER, managed_store))

    ensure_can_assign_roles(
        principal,
        [RoleScope(IdentityRole.STORE_ASSOCIATE, managed_store)],
    )


def test_corporate_admin_can_delegate_all_role_types() -> None:
    principal = _principal(RoleScope(IdentityRole.CORPORATE_ADMIN, None))

    ensure_can_assign_roles(
        principal,
        [
            RoleScope(IdentityRole.CORPORATE_ADMIN, None),
            RoleScope(IdentityRole.STORE_MANAGER, uuid.uuid4()),
            RoleScope(IdentityRole.STORE_ASSOCIATE, uuid.uuid4()),
        ],
    )
