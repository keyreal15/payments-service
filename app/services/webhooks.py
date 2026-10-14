import asyncio
import hashlib
import hmac
import ipaddress
import socket
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Literal

import httpx

from app.db.models import Payment
from app.enums import PaymentStatus
from app.schemas import PaymentRead, WebhookPayload
from app.services.errors import WebhookUrlNotAllowedError

SIGNATURE_HEADER = "X-Webhook-Signature"
TIMESTAMP_HEADER = "X-Webhook-Timestamp"
EVENT_HEADER = "X-Webhook-Event"
WEBHOOK_ID_HEADER = "X-Webhook-Id"

Resolver = Callable[[str, int], Awaitable[list[str]]]


class WebhookDeliveryError(Exception):
    """Webhook не доставлен (сеть, таймаут, DNS или ответ не 2xx) — сообщение уйдёт на retry."""


def sign(secret: str, timestamp: str, body: bytes) -> str:
    message = timestamp.encode() + b"." + body
    return "sha256=" + hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def webhook_id(payment: Payment) -> str:
    """Стабилен для пары (платёж, итог): получатель отсеивает повторные доставки (at-least-once)."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"payment:{payment.id}:{payment.status.value}"))


EVENTS: dict[PaymentStatus, Literal["payment.succeeded", "payment.failed"]] = {
    PaymentStatus.SUCCEEDED: "payment.succeeded",
    PaymentStatus.FAILED: "payment.failed",
}


def build_payload(payment: Payment) -> WebhookPayload:
    # webhook отправляется только для финального статуса; pending здесь — ошибка программиста (KeyError).
    return WebhookPayload(event=EVENTS[payment.status], payment=PaymentRead.model_validate(payment))


async def resolve_host(host: str, port: int) -> list[str]:
    infos = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return [str(info[4][0]) for info in infos]


def _is_public(address: str) -> bool:
    ip = ipaddress.ip_address(address.split("%", 1)[0])  # отбрасываем zone id IPv6
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    return ip.is_global and not ip.is_multicast


class WebhookSender:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        timeout: float,
        secret: str | None = None,
        allow_private_networks: bool = False,
        resolver: Resolver = resolve_host,
    ) -> None:
        self._client = client
        self._timeout = timeout
        self._secret = secret
        self._allow_private_networks = allow_private_networks
        self._resolver = resolver

    async def send(self, payment: Payment) -> None:
        payload = build_payload(payment)
        body = payload.model_dump_json().encode()
        headers = {
            "Content-Type": "application/json",
            EVENT_HEADER: payload.event,
            WEBHOOK_ID_HEADER: webhook_id(payment),
        }
        if self._secret:
            timestamp = str(int(time.time()))
            headers[TIMESTAMP_HEADER] = timestamp
            headers[SIGNATURE_HEADER] = sign(self._secret, timestamp, body)

        url = httpx.URL(payment.webhook_url)
        try:
            # Общий дедлайн на весь запрос (включая проверку адреса): таймаут httpx действует на каждую
            # операцию отдельно, и медленный получатель мог бы держать слот consumer'а бесконечно.
            async with asyncio.timeout(self._timeout):
                if not self._allow_private_networks:
                    await self._ensure_public_destination(url)
                async with self._client.stream(
                    "POST", url, content=body, headers=headers, follow_redirects=False
                ) as response:
                    status_code = response.status_code  # тело ответа не читаем — важен только статус
        except TimeoutError as exc:
            raise WebhookDeliveryError(f"Timed out after {self._timeout}s") from exc
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"{type(exc).__name__}: {exc}") from exc

        if not 200 <= status_code < 300:
            raise WebhookDeliveryError(f"Unexpected response status {status_code}")

    async def _ensure_public_destination(self, url: httpx.URL) -> None:
        """Защита от SSRF: все адреса хоста должны быть публичными.

        Проверка после резолва ловит и `http://127.1`, и DNS-имена, указывающие внутрь сети.
        От DNS rebinding между проверкой и подключением полностью защищает только egress-прокси —
        это вне рамок сервиса (см. README).
        """
        # raw_host — ASCII/punycode, именно к нему подключится httpx. url.host декодирован в Unicode,
        # а getaddrinfo перекодировал бы его по IDNA2003 (faß.de -> fass.de) — проверили бы не тот хост.
        host = url.raw_host.decode("ascii").strip("[]")
        port = url.port or (443 if url.scheme == "https" else 80)
        try:
            addresses = [str(ipaddress.ip_address(host))]
        except ValueError:
            try:
                addresses = await self._resolver(host, port)
            except OSError as exc:
                raise WebhookDeliveryError(f"Cannot resolve webhook host {host!r}: {exc}") from exc
        if not addresses or not all(_is_public(address) for address in addresses):
            raise WebhookUrlNotAllowedError(f"Webhook host {host!r} resolves to a non-public address")
