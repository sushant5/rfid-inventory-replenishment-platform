import uuid
from datetime import datetime

from pydantic import EmailStr, Field, SecretStr, field_validator, model_validator

from abacus.models.architecture import CanonicalIdentityRole
from abacus.models.identity import IdentityAuditAction, IdentityRole, UserStatus
from abacus.schemas.common import ApiModel

TENANT_CODE_PATTERN = r"^[a-z0-9][a-z0-9_-]{1,63}$"


class LoginRequest(ApiModel):
    tenant_code: str = Field(min_length=2, max_length=64, pattern=TENANT_CODE_PATTERN)
    email: EmailStr
    password: SecretStr = Field(min_length=1, max_length=128)

    @field_validator("tenant_code")
    @classmethod
    def normalize_tenant_code(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator("email")
    @classmethod
    def normalize_email(cls, value: EmailStr) -> str:
        return str(value).strip().lower()


class AccessTokenRead(ApiModel):
    access_token: str
    token_type: str = Field(default="bearer")
    expires_in: int


class RoleAssignmentCreate(ApiModel):
    role: IdentityRole
    store_id: uuid.UUID | None = None

    @model_validator(mode="after")
    def validate_role_scope_pair(self) -> "RoleAssignmentCreate":
        if self.role == IdentityRole.CORPORATE_ADMIN and self.store_id is not None:
            raise ValueError("CORPORATE_ADMIN must use tenant scope and cannot specify store_id")
        if self.role != IdentityRole.CORPORATE_ADMIN and self.store_id is None:
            raise ValueError("STORE_MANAGER and STORE_ASSOCIATE require store_id")
        return self


class RoleAssignmentRead(ApiModel):
    role: IdentityRole
    store_id: uuid.UUID | None


class UserRolesReplace(ApiModel):
    roles: list[CanonicalIdentityRole] = Field(min_length=1, max_length=4)

    @field_validator("roles")
    @classmethod
    def reject_duplicate_roles(
        cls,
        value: list[CanonicalIdentityRole],
    ) -> list[CanonicalIdentityRole]:
        if len(value) != len(set(value)):
            raise ValueError("roles cannot contain duplicates")
        return value


class UserRolesRead(ApiModel):
    user_id: uuid.UUID
    roles: list[CanonicalIdentityRole]


class UserStoreAssignmentsReplace(ApiModel):
    store_ids: list[uuid.UUID] = Field(max_length=500)

    @field_validator("store_ids")
    @classmethod
    def reject_duplicate_store_ids(cls, value: list[uuid.UUID]) -> list[uuid.UUID]:
        if len(value) != len(set(value)):
            raise ValueError("store_ids cannot contain duplicates")
        return value


class UserStoreAssignmentsRead(ApiModel):
    user_id: uuid.UUID
    store_ids: list[uuid.UUID]


class UserAccessReplace(ApiModel):
    """Atomically replace roles and store assignments as one valid access state."""

    roles: list[CanonicalIdentityRole] = Field(min_length=1, max_length=4)
    store_ids: list[uuid.UUID] = Field(default_factory=list, max_length=500)

    @model_validator(mode="after")
    def validate_access_pair(self) -> "UserAccessReplace":
        if len(self.roles) != len(set(self.roles)):
            raise ValueError("roles cannot contain duplicates")
        if len(self.store_ids) != len(set(self.store_ids)):
            raise ValueError("store_ids cannot contain duplicates")
        has_store_role = bool(
            set(self.roles).intersection(
                {
                    CanonicalIdentityRole.STORE_ASSOCIATE,
                    CanonicalIdentityRole.STORE_MANAGER,
                }
            )
        )
        if has_store_role and not self.store_ids:
            raise ValueError("store-scoped roles require at least one store_id")
        if not has_store_role and self.store_ids:
            raise ValueError("store_ids require a store-scoped role")
        return self


class UserAccessRead(ApiModel):
    user_id: uuid.UUID
    roles: list[CanonicalIdentityRole]
    store_ids: list[uuid.UUID]


class UserCreate(ApiModel):
    email: EmailStr
    display_name: str = Field(min_length=1, max_length=255)
    password: SecretStr = Field(min_length=12, max_length=128)
    role_assignments: list[RoleAssignmentCreate] = Field(min_length=1, max_length=500)

    @field_validator("email")
    @classmethod
    def normalize_email(cls, value: EmailStr) -> str:
        return str(value).strip().lower()

    @field_validator("display_name")
    @classmethod
    def normalize_display_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("display_name cannot be blank")
        return normalized

    @model_validator(mode="after")
    def reject_duplicate_assignments(self) -> "UserCreate":
        assignments = {
            (assignment.role, assignment.store_id) for assignment in self.role_assignments
        }
        if len(assignments) != len(self.role_assignments):
            raise ValueError("role_assignments cannot contain duplicates")
        return self


class UserCreateRequest(ApiModel):
    email: EmailStr
    display_name: str = Field(min_length=1, max_length=255)
    password: SecretStr = Field(min_length=12, max_length=128)
    roles: list[CanonicalIdentityRole] = Field(min_length=1, max_length=4)
    store_ids: list[uuid.UUID] = Field(default_factory=list, max_length=500)

    @field_validator("email")
    @classmethod
    def normalize_canonical_email(cls, value: EmailStr) -> str:
        return str(value).strip().lower()

    @field_validator("display_name")
    @classmethod
    def normalize_canonical_display_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("display_name cannot be blank")
        return normalized

    @model_validator(mode="after")
    def validate_canonical_access(self) -> "UserCreateRequest":
        if len(self.roles) != len(set(self.roles)):
            raise ValueError("roles cannot contain duplicates")
        if len(self.store_ids) != len(set(self.store_ids)):
            raise ValueError("store_ids cannot contain duplicates")
        store_roles = {
            CanonicalIdentityRole.STORE_ASSOCIATE,
            CanonicalIdentityRole.STORE_MANAGER,
        }
        has_store_role = bool(set(self.roles).intersection(store_roles))
        if has_store_role and not self.store_ids:
            raise ValueError("store-scoped roles require at least one store_id")
        if not has_store_role and self.store_ids:
            raise ValueError("store_ids require a store-scoped role")
        return self


class UserRead(ApiModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    email: EmailStr
    display_name: str
    status: UserStatus
    role_assignments: list[RoleAssignmentRead]
    last_login_at: datetime | None
    created_at: datetime
    updated_at: datetime


class UserResponse(ApiModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    email: EmailStr
    display_name: str
    status: UserStatus
    roles: list[CanonicalIdentityRole]
    store_ids: list[uuid.UUID]
    last_login_at: datetime | None
    created_at: datetime
    updated_at: datetime


class UserPage(ApiModel):
    items: list[UserRead]
    total: int
    limit: int
    offset: int


class UserListResponse(ApiModel):
    items: list[UserResponse]
    total: int
    limit: int
    offset: int


class CurrentPrincipalRead(ApiModel):
    user_id: uuid.UUID
    tenant_id: uuid.UUID
    email: EmailStr
    display_name: str
    role_assignments: list[RoleAssignmentRead]
    permissions: list[str]


class CurrentUserResponse(ApiModel):
    user_id: uuid.UUID
    tenant_id: uuid.UUID
    email: EmailStr
    display_name: str
    roles: list[CanonicalIdentityRole]
    store_ids: list[uuid.UUID]
    permissions: list[str]


CanonicalUserCreate = UserCreateRequest
CanonicalUserRead = UserResponse
CanonicalUserPage = UserListResponse
CanonicalPrincipalRead = CurrentUserResponse


class IdentityAuditRecordRead(ApiModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    actor_user_id: uuid.UUID | None
    action: IdentityAuditAction
    target_user_id: uuid.UUID | None
    details: dict[str, object]
    occurred_at: datetime


class IdentityAuditPage(ApiModel):
    items: list[IdentityAuditRecordRead]
    total: int
    limit: int
    offset: int
