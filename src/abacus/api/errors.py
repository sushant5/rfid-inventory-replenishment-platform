import re
from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute, RouteContext, iter_route_contexts
from pydantic import BaseModel

# These responses are operation-specific runtime behavior, not an assumption that
# every write can conflict. Keeping the registry keyed by stable operationId makes
# a renamed or removed operation fail OpenAPI generation instead of silently drifting.
OPERATION_PROBLEM_RESPONSES: dict[str, dict[str, str]] = {
    "createTenant": {
        "409": "The tenant code conflicts with existing tenant data",
    },
    "createStoreImport": {
        "400": "The idempotency key is blank",
        "409": "The idempotency key or requested store data conflicts with existing data",
    },
    "createStoreZone": {
        "409": "The zone code already exists in the store",
    },
    "registerStoreDevice": {
        "409": "The device serial or assignment conflicts with existing data",
    },
    "createCatalogImport": {
        "400": "The idempotency key is blank",
        "409": "The idempotency key conflicts with a previous catalog import",
        "413": "The uploaded catalog exceeds the hosted import limit",
    },
    "submitRfidObservationBatch": {
        "409": "An event identifier conflicts with a previously accepted observation",
    },
    "replayRfidQuarantine": {
        "409": "The quarantined event is incomplete, already in flight, or cannot be replayed",
    },
    "createUser": {
        "409": "The user email or requested access conflicts with existing data",
    },
    "replaceUserAccess": {
        "409": "The access update conflicts with protected or concurrently changed access",
    },
    "replaceUserRoles": {
        "409": "The role update conflicts with protected or concurrently changed access",
    },
    "replaceUserStoreAssignments": {
        "409": "The assignment update conflicts with concurrently changed access",
    },
    "suspendUser": {
        "409": "The user cannot be suspended in the current state",
    },
    "createReplenishmentPolicy": {
        "409": "The policy name or rules conflict with existing policy data",
    },
    "createReplenishmentPolicyVersion": {
        "409": "A policy version was created concurrently or has no source version",
    },
    "patchReplenishmentPolicyVersion": {
        "409": "The policy version is immutable or its rules changed concurrently",
    },
    "activateReplenishmentPolicyVersion": {
        "409": "The version cannot be activated or its rules overlap an active policy",
    },
    "evaluateReplenishment": {
        "409": "Policy selection is ambiguous or an active task was created concurrently",
    },
    "patchReplenishmentTask": {
        "409": "The task transition or optimistic-lock version conflicts with current state",
    },
    "login": {
        "429": "Too many login attempts",
    },
    "readiness": {
        "503": "A readiness dependency is unavailable",
    },
}


class ProblemDetail(BaseModel):
    type: str = "about:blank"
    title: str
    status: int
    detail: str
    instance: str | None = None
    code: str | None = None
    errors: list[dict[str, Any]] | None = None
    request_id: str | None = None


class ApiError(Exception):
    def __init__(
        self,
        status_code: int,
        title: str,
        detail: str,
        *,
        code: str | None = None,
        errors: list[dict[str, Any]] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.title = title
        self.detail = detail
        self.code = code
        self.errors = errors
        self.headers = dict(headers) if headers is not None else None


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def handle_api_error(request: Request, exc: ApiError) -> JSONResponse:
        problem = ProblemDetail(
            title=exc.title,
            status=exc.status_code,
            detail=exc.detail,
            instance=str(request.url.path),
            code=exc.code,
            errors=exc.errors,
            request_id=getattr(request.state, "request_id", None),
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=problem.model_dump(exclude_none=True),
            media_type="application/problem+json",
            headers=exc.headers,
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        errors = jsonable_encoder(exc.errors())
        # Pydantic includes the rejected raw value by default. Returning it is useful
        # for debugging ordinary fields, but it can echo passwords or future secrets
        # into clients, proxies, and response logs. Location and constraint metadata
        # are sufficient for callers to correct a request.
        for error in errors:
            error.pop("input", None)
        problem = ProblemDetail(
            title="Request validation failed",
            status=422,
            detail="One or more request fields are invalid.",
            instance=str(request.url.path),
            code="request_validation_failed",
            errors=errors,
            request_id=getattr(request.state, "request_id", None),
        )
        return JSONResponse(
            status_code=422,
            content=problem.model_dump(exclude_none=True),
            media_type="application/problem+json",
        )

    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, _exc: Exception) -> JSONResponse:
        request_id = getattr(request.state, "request_id", None)
        problem = ProblemDetail(
            title="Internal server error",
            status=500,
            detail="The service could not complete the request.",
            instance=str(request.url.path),
            code="internal_server_error",
            request_id=request_id,
        )
        headers = {"X-Request-ID": request_id} if request_id is not None else None
        return JSONResponse(
            status_code=500,
            content=problem.model_dump(exclude_none=True),
            media_type="application/problem+json",
            headers=headers,
        )


def install_openapi_error_contract(app: FastAPI) -> None:
    """Publish the same RFC 7807 failure envelope returned at runtime."""

    def problem_response(description: str) -> dict[str, Any]:
        return {
            "description": description,
            "content": {
                "application/problem+json": {
                    "schema": {"$ref": "#/components/schemas/ProblemDetail"}
                }
            },
        }

    def required_permissions(route: RouteContext | None) -> tuple[str, ...]:
        if route is None:
            return ()
        permissions: set[str] = set()
        pending = list(route.dependant.dependencies)
        while pending:
            dependency = pending.pop()
            permission = getattr(dependency.call, "__abacus_permission__", None)
            if isinstance(permission, str):
                permissions.add(permission)
            pending.extend(dependency.dependencies)
        return tuple(sorted(permissions))

    def custom_openapi() -> dict[str, Any]:
        if app.openapi_schema is not None:
            return app.openapi_schema
        schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
            tags=app.openapi_tags,
        )
        components = schema.setdefault("components", {}).setdefault("schemas", {})
        components["ProblemDetail"] = ProblemDetail.model_json_schema(
            ref_template="#/components/schemas/{model}"
        )
        validation_response = problem_response("Request validation failed")
        route_lookup = {
            (route.path_format, method.lower()): route
            for route in iter_route_contexts(app.routes)
            if isinstance(route.original_route, APIRoute)
            for method in (route.methods or set())
        }
        documented_operation_ids: set[str] = set()
        for path, path_item in schema.get("paths", {}).items():
            for method, operation in path_item.items():
                if not isinstance(operation, dict) or "responses" not in operation:
                    continue
                operation_id = operation.get("operationId")
                if isinstance(operation_id, str):
                    documented_operation_ids.add(operation_id)
                    words = re.sub(r"(?<!^)(?=[A-Z])", " ", operation_id)
                    words = (
                        words.replace("Rfid", "RFID").replace("Skus", "SKUs").replace("Sku", "SKU")
                    )
                    operation["summary"] = words[0].upper() + words[1:]
                responses = operation["responses"]
                current_validation = responses.get("422", {})
                current_schema = (
                    current_validation.get("content", {})
                    .get("application/json", {})
                    .get("schema", {})
                )
                if current_schema.get("$ref") == "#/components/schemas/HTTPValidationError":
                    responses["422"] = deepcopy(validation_response)
                route = route_lookup.get((path, method.lower()))
                security = operation.get("security")
                if (isinstance(security, list) and security) or path == "/v1/auth/login":
                    responses.setdefault(
                        "401",
                        problem_response("Authentication credentials are missing or invalid"),
                    )
                permissions = required_permissions(route)
                if permissions:
                    responses.setdefault(
                        "403",
                        problem_response(
                            "Permission required: " + ", ".join(f"`{item}`" for item in permissions)
                        ),
                    )
                elif security == [{"DeviceToken": []}]:
                    responses.setdefault(
                        "403",
                        problem_response("The device credential does not match the request"),
                    )
                if "{" in path:
                    responses.setdefault(
                        "404", problem_response("The requested resource was not found")
                    )
                if isinstance(operation_id, str):
                    for status_code, description in OPERATION_PROBLEM_RESPONSES.get(
                        operation_id, {}
                    ).items():
                        responses.setdefault(status_code, problem_response(description))
                responses.setdefault("500", problem_response("An unexpected server error occurred"))
        missing_operations = set(OPERATION_PROBLEM_RESPONSES).difference(documented_operation_ids)
        if missing_operations:
            missing = ", ".join(sorted(missing_operations))
            raise RuntimeError(f"OpenAPI error metadata references unknown operations: {missing}")
        app.openapi_schema = schema
        return schema

    app.openapi = custom_openapi  # type: ignore[method-assign]
