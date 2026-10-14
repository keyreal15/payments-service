from typing import Literal

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException


class ErrorResponse(BaseModel):
    detail: str
    code: str


class ValidationErrorItem(BaseModel):
    loc: list[str | int]
    msg: str
    type: str


class ValidationErrorResponse(BaseModel):
    detail: list[ValidationErrorItem]
    code: Literal["validation_error"] = "validation_error"


class ApiError(Exception):
    """Ошибка с машиночитаемым кодом: {"detail": "...", "code": "..."}."""

    def __init__(self, status_code: int, code: str, detail: str, headers: dict[str, str] | None = None) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.code = code
        self.detail = detail
        self.headers = headers


def error_response(status_code: int, code: str, detail: str, headers: dict[str, str] | None = None) -> JSONResponse:
    return JSONResponse(ErrorResponse(detail=detail, code=code).model_dump(), status_code=status_code, headers=headers)


_HTTP_CODES = {
    status.HTTP_401_UNAUTHORIZED: "unauthorized",
    status.HTTP_404_NOT_FOUND: "not_found",
    status.HTTP_405_METHOD_NOT_ALLOWED: "method_not_allowed",
    status.HTTP_413_CONTENT_TOO_LARGE: "payload_too_large",
}


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def handle_api_error(_: Request, exc: ApiError) -> JSONResponse:
        return error_response(exc.status_code, exc.code, exc.detail, exc.headers)

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        # Без `input` и `ctx`: не эхо-им тело запроса (размер, секреты в URL), а NaN/Infinity
        # во входных данных не ломают сериализацию ответа.
        items = [
            ValidationErrorItem(loc=list(error["loc"]), msg=error["msg"], type=error["type"]) for error in exc.errors()
        ]
        return JSONResponse(
            ValidationErrorResponse(detail=items).model_dump(), status_code=status.HTTP_422_UNPROCESSABLE_CONTENT
        )

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        headers = dict(exc.headers) if exc.headers else None
        return error_response(exc.status_code, _HTTP_CODES.get(exc.status_code, "http_error"), str(exc.detail), headers)

    @app.exception_handler(Exception)
    async def handle_unexpected_error(_: Request, __: Exception) -> JSONResponse:
        # Трейсбек логирует uvicorn: Starlette пробрасывает исключение дальше после ответа.
        return error_response(status.HTTP_500_INTERNAL_SERVER_ERROR, "internal_error", "Internal server error")
