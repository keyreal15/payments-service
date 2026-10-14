"""Демо-приёмник webhook'ов (compose-профиль `demo`, http://localhost:8090).

    POST /webhooks/ok                  -> 200
    POST /webhooks/fail                -> 500 (всегда: после всех попыток сообщение уйдёт в DLQ)
    POST /webhooks/flaky?failures=N    -> первые N доставок по платежу 503, затем 200
    GET  /webhooks                     -> все полученные запросы (с результатом проверки подписи)
    DELETE /webhooks                   -> очистить журнал

Если задан WEBHOOK_SECRET, подпись X-Webhook-Signature проверяется так же, как это сделал бы мерчант.
"""

import hashlib
import hmac
import logging
import os
from collections import Counter
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Request, Response, status

logger = logging.getLogger("uvicorn.error")  # в общий лог uvicorn — видно в docker compose logs

app = FastAPI(title="Webhook receiver (demo)")

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")

received: list[dict[str, Any]] = []
deliveries: Counter[str] = Counter()


def _signature_valid(request: Request, body: bytes) -> bool | None:
    if not WEBHOOK_SECRET:
        return None
    timestamp = request.headers.get("x-webhook-timestamp", "")
    expected = (
        "sha256=" + hmac.new(WEBHOOK_SECRET.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    )
    return hmac.compare_digest(expected, request.headers.get("x-webhook-signature", ""))


async def _record(request: Request, endpoint: str, response_status: int) -> None:
    raw = await request.body()
    body = await request.json()
    payment_id = str(body.get("payment", {}).get("payment_id"))
    deliveries[f"{endpoint}:{payment_id}"] += 1
    signature_valid = _signature_valid(request, raw)
    received.append(
        {
            "received_at": datetime.now(UTC).isoformat(),
            "endpoint": endpoint,
            "response_status": response_status,
            "signature_valid": signature_valid,
            "headers": {k: v for k, v in request.headers.items() if k.lower().startswith("x-webhook")},
            "body": body,
        }
    )
    logger.info(
        "webhook %s -> %d: %s %s (signature valid: %s)",
        endpoint,
        response_status,
        body.get("event"),
        payment_id,
        signature_valid,
    )


@app.get("/")
async def index() -> dict[str, str]:
    return {
        "received": "GET /webhooks",
        "ok": "POST /webhooks/ok",
        "fail": "POST /webhooks/fail",
        "flaky": "POST /webhooks/flaky?failures=N",
    }


@app.post("/webhooks/ok")
async def ok(request: Request) -> Response:
    await _record(request, "ok", status.HTTP_200_OK)
    return Response(status_code=status.HTTP_200_OK)


@app.post("/webhooks/fail")
async def fail(request: Request) -> Response:
    await _record(request, "fail", status.HTTP_500_INTERNAL_SERVER_ERROR)
    return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@app.post("/webhooks/flaky")
async def flaky(request: Request, failures: int = 2) -> Response:
    body = await request.json()
    payment_id = str(body.get("payment", {}).get("payment_id"))
    code = status.HTTP_503_SERVICE_UNAVAILABLE if deliveries[f"flaky:{payment_id}"] < failures else status.HTTP_200_OK
    await _record(request, "flaky", code)
    return Response(status_code=code)


@app.get("/webhooks")
async def list_received() -> list[dict[str, Any]]:
    return received


@app.delete("/webhooks", status_code=status.HTTP_204_NO_CONTENT)
async def clear() -> None:
    received.clear()
    deliveries.clear()
