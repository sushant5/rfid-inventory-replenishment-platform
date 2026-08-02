from typing import Any

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
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


class ApiError(Exception):
    def __init__(
        self,
        status_code: int,
        title: str,
        detail: str,
        *,
        code: str | None = None,
        errors: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.title = title
        self.detail = detail
        self.code = code
        self.errors = errors


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
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=problem.model_dump(exclude_none=True),
            media_type="application/problem+json",
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
        )
        return JSONResponse(
            status_code=422,
            content=problem.model_dump(exclude_none=True),
            media_type="application/problem+json",
        )
