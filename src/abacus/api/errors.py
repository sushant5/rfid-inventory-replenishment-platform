from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from pydantic import BaseModel


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


def install_openapi_error_contract(app: FastAPI) -> None:
    """Make generated clients see the validation envelope returned at runtime."""

    def custom_openapi() -> dict[str, Any]:
        if app.openapi_schema is not None:
            return app.openapi_schema
        schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
        )
        components = schema.setdefault("components", {}).setdefault("schemas", {})
        components["ProblemDetail"] = ProblemDetail.model_json_schema(
            ref_template="#/components/schemas/{model}"
        )
        validation_response = {
            "description": "Request validation failed",
            "content": {
                "application/problem+json": {
                    "schema": {"$ref": "#/components/schemas/ProblemDetail"}
                }
            },
        }
        for path_item in schema.get("paths", {}).values():
            for operation in path_item.values():
                if not isinstance(operation, dict) or "responses" not in operation:
                    continue
                responses = operation["responses"]
                current_validation = responses.get("422", {})
                current_schema = (
                    current_validation.get("content", {})
                    .get("application/json", {})
                    .get("schema", {})
                )
                if current_schema.get("$ref") == "#/components/schemas/HTTPValidationError":
                    responses["422"] = deepcopy(validation_response)
        app.openapi_schema = schema
        return schema

    app.openapi = custom_openapi  # type: ignore[method-assign]
