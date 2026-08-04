import uuid

from abacus.models.architecture import CanonicalIdentityRole
from abacus.models.identity import IdentityRole
from abacus.security import Permission, Principal, RoleScope


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
    assert not principal.has_permission(Permission.INVENTORY_ADJUST)
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
    assert principal.can_access_store(Permission.INVENTORY_ADJUST, assigned_store)
    assert not principal.can_access_store(Permission.INVENTORY_ADJUST, another_store)


def test_store_associate_cannot_administer_users_or_policies() -> None:
    principal = _principal(RoleScope(IdentityRole.STORE_ASSOCIATE, uuid.uuid4()))

    assert not principal.has_permission(Permission.USERS_CREATE)
    assert not principal.has_permission(Permission.USERS_READ)
    assert not principal.has_permission(Permission.USERS_SUSPEND)
    assert not principal.has_permission(Permission.POLICY_MANAGE)
    assert not principal.has_permission(Permission.INVENTORY_ADJUST)
    assert principal.has_permission(Permission.REPLENISHMENT_EXECUTE)
