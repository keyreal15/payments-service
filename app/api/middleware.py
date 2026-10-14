import hashlib
import hmac

from starlette.datastructures import Headers
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.api.errors import error_response

API_KEY_HEADER = "X-API-Key"
PROTECTED_PATH_PREFIX = "/api/"


def api_key_matches(provided: str | None, expected: str) -> bool:
    if provided is None:
        return False
    # Сравнение хешей фиксированной длины за постоянное время: не выдаём ни содержимое, ни длину ключа.
    return hmac.compare_digest(hashlib.sha256(provided.encode()).digest(), hashlib.sha256(expected.encode()).digest())


class ApiGuardMiddleware:
    """Чистый ASGI-middleware для бизнес-эндпоинтов (/api/...).

    Срабатывает ДО того, как FastAPI прочитает и распарсит тело:
      * нет или неверный X-API-Key -> 401 (даже для битого или огромного тела);
      * тело больше max_body_size -> 413 (по Content-Length или по факту для chunked-запросов).
    """

    def __init__(self, app: ASGIApp, *, api_key: str, max_body_size: int) -> None:
        self._app = app
        self._api_key = api_key
        self._max_body_size = max_body_size

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not scope["path"].startswith(PROTECTED_PATH_PREFIX):
            await self._app(scope, receive, send)
            return

        headers = Headers(scope=scope)
        if not api_key_matches(headers.get(API_KEY_HEADER), self._api_key):
            response = error_response(
                401, "unauthorized", "Invalid or missing API key", headers={"WWW-Authenticate": "APIKey"}
            )
            await response(scope, receive, send)
            return

        content_length = headers.get("content-length")
        if content_length is not None and (not content_length.isdigit() or int(content_length) > self._max_body_size):
            await self._too_large(scope, receive, send)
            return

        # Тело (ограниченное лимитом) читаем сами и отдаём приложению из буфера:
        # так лимит соблюдается и для запросов без Content-Length (chunked).
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            body.extend(message.get("body", b""))
            if len(body) > self._max_body_size:
                await self._too_large(scope, receive, send)
                return
            if not message.get("more_body", False):
                break

        buffered: list[Message] = [{"type": "http.request", "body": bytes(body), "more_body": False}]

        async def replay() -> Message:
            if buffered:
                return buffered.pop()
            return await receive()

        await self._app(scope, replay, send)

    async def _too_large(self, scope: Scope, receive: Receive, send: Send) -> None:
        detail = f"Request body must not exceed {self._max_body_size} bytes"
        await error_response(413, "payload_too_large", detail)(scope, receive, send)
