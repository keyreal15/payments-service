"""ApiGuardMiddleware: ключ и размер тела проверяются до того, как приложение прочитает тело."""

from collections.abc import AsyncIterator

import httpx
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from app.api.middleware import ApiGuardMiddleware, api_key_matches

KEY = "secret-key-123"
LIMIT = 1024


async def echo(request: Request) -> JSONResponse:
    return JSONResponse({"size": len(await request.body())})


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    app = Starlette(routes=[Route("/api/echo", echo, methods=["POST"]), Route("/health", echo, methods=["POST"])])
    guarded = ApiGuardMiddleware(app, api_key=KEY, max_body_size=LIMIT)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=guarded), base_url="http://test") as client:
        yield client


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}, {"X-API-Key": ""}])
async def test_missing_or_invalid_key_is_rejected_before_body_parsing(
    client: httpx.AsyncClient, headers: dict[str, str]
) -> None:
    response = await client.post("/api/echo", content=b"{not json", headers=headers)
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid or missing API key", "code": "unauthorized"}
    assert response.headers["WWW-Authenticate"] == "APIKey"


async def test_valid_key_passes_body_through(client: httpx.AsyncClient) -> None:
    response = await client.post("/api/echo", content=b"x" * 100, headers={"X-API-Key": KEY})
    assert response.json() == {"size": 100}


async def test_body_over_limit_by_content_length(client: httpx.AsyncClient) -> None:
    response = await client.post("/api/echo", content=b"x" * (LIMIT + 1), headers={"X-API-Key": KEY})
    assert response.status_code == 413
    assert response.json()["code"] == "payload_too_large"


async def test_chunked_body_over_limit(client: httpx.AsyncClient) -> None:
    async def chunks() -> AsyncIterator[bytes]:
        for _ in range(10):
            yield b"x" * 200

    response = await client.post("/api/echo", content=chunks(), headers={"X-API-Key": KEY})
    assert response.status_code == 413


async def test_non_api_paths_are_not_guarded(client: httpx.AsyncClient) -> None:
    response = await client.post("/health", content=b"x" * (LIMIT * 2))
    assert response.status_code == 200


def test_api_key_comparison() -> None:
    assert api_key_matches(KEY, KEY)
    assert not api_key_matches(None, KEY)
    assert not api_key_matches(KEY + "x", KEY)
