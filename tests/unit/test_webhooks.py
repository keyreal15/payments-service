import asyncio
import hashlib
import hmac
import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from app.db.models import Payment
from app.enums import Currency, PaymentStatus
from app.services.errors import WebhookUrlNotAllowedError
from app.services.webhooks import (
    EVENT_HEADER,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    WEBHOOK_ID_HEADER,
    Resolver,
    WebhookDeliveryError,
    WebhookSender,
)

PUBLIC_IP = "93.184.215.14"


def make_payment(
    status: PaymentStatus = PaymentStatus.SUCCEEDED, url: str = "https://merchant.example/hook"
) -> Payment:
    return Payment(
        id=uuid.uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.EUR,
        description="test",
        meta={"k": "v"},
        status=status,
        idempotency_key="key",
        request_hash="0" * 64,
        webhook_url=url,
        created_at=datetime.now(UTC),
        processed_at=datetime.now(UTC),
    )


def resolver_to(*addresses: str) -> Resolver:
    async def resolve(host: str, port: int) -> list[str]:
        return list(addresses)

    return resolve


def sender(
    handler: httpx.MockTransport, *, secret: str | None = None, timeout: float = 2.0, addresses: tuple[str, ...] = ()
) -> tuple[WebhookSender, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=handler)
    return (
        WebhookSender(
            client,
            timeout=timeout,
            secret=secret,
            allow_private_networks=not addresses,
            resolver=resolver_to(*addresses),
        ),
        client,
    )


async def test_posts_payment_with_signature_and_stable_id() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(204)

    payment = make_payment()
    webhooks, client = sender(httpx.MockTransport(handler), secret="s3cret")
    async with client:
        await webhooks.send(payment)
        await webhooks.send(payment)

    request = captured[0]
    body = json.loads(request.content)
    assert str(request.url) == payment.webhook_url
    assert body["event"] == "payment.succeeded"
    assert body["payment"]["payment_id"] == str(payment.id)
    assert body["payment"]["amount"] == "10.00"
    assert body["payment"]["metadata"] == {"k": "v"}
    assert request.headers[EVENT_HEADER] == "payment.succeeded"

    timestamp = request.headers[TIMESTAMP_HEADER]
    expected = hmac.new(b"s3cret", timestamp.encode() + b"." + request.content, hashlib.sha256).hexdigest()
    assert request.headers[SIGNATURE_HEADER] == f"sha256={expected}"
    # Повторная доставка того же события — тот же id: получатель может отсеять дубль.
    assert captured[0].headers[WEBHOOK_ID_HEADER] == captured[1].headers[WEBHOOK_ID_HEADER]


async def test_failed_payment_event_and_unsigned_without_secret() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200)

    webhooks, client = sender(httpx.MockTransport(handler))
    async with client:
        await webhooks.send(make_payment(PaymentStatus.FAILED))

    assert json.loads(captured[0].content)["event"] == "payment.failed"
    assert SIGNATURE_HEADER not in captured[0].headers


@pytest.mark.parametrize("status_code", [400, 404, 500, 503])
async def test_non_2xx_is_delivery_error(status_code: int) -> None:
    webhooks, client = sender(httpx.MockTransport(lambda _: httpx.Response(status_code)))
    async with client:
        with pytest.raises(WebhookDeliveryError, match=str(status_code)):
            await webhooks.send(make_payment())


async def test_redirect_is_not_followed() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(302, headers={"Location": "http://169.254.169.254/latest/meta-data"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True)
    async with client:
        with pytest.raises(WebhookDeliveryError, match="302"):
            await WebhookSender(client, timeout=2.0, allow_private_networks=True).send(make_payment())
    assert len(captured) == 1


async def test_transport_error_is_delivery_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    webhooks, client = sender(httpx.MockTransport(handler))
    async with client:
        with pytest.raises(WebhookDeliveryError, match="ConnectTimeout"):
            await webhooks.send(make_payment())


async def test_overall_deadline_covers_slow_receivers() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(5)
        return httpx.Response(200)

    webhooks, client = sender(httpx.MockTransport(handler), timeout=0.2)
    async with client:
        with pytest.raises(WebhookDeliveryError, match="Timed out"):
            await webhooks.send(make_payment())


@pytest.mark.parametrize(
    ("url", "addresses"),
    [
        ("http://127.0.0.1/hook", ()),
        ("http://[::1]/hook", ()),
        ("http://169.254.169.254/latest", ()),
        ("http://10.0.0.5/hook", ()),
        ("http://internal.example/hook", ("192.168.1.10",)),
        ("http://mixed.example/hook", (PUBLIC_IP, "10.0.0.1")),
        ("http://mapped.example/hook", ("::ffff:127.0.0.1",)),
    ],
)
async def test_private_destinations_are_blocked(url: str, addresses: tuple[str, ...]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("request must not be sent")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    webhooks = WebhookSender(client, timeout=2.0, resolver=resolver_to(*addresses))
    async with client:
        with pytest.raises(WebhookUrlNotAllowedError):
            await webhooks.send(make_payment(url=url))


async def test_public_destination_is_allowed() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200)

    webhooks, client = sender(httpx.MockTransport(handler), addresses=(PUBLIC_IP,))
    async with client:
        await webhooks.send(make_payment())
    assert len(captured) == 1


async def test_unresolvable_host_is_retryable_delivery_error() -> None:
    async def failing_resolver(host: str, port: int) -> list[str]:
        raise OSError("Name or service not known")

    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200)))
    async with client:
        with pytest.raises(WebhookDeliveryError, match="resolve"):
            await WebhookSender(client, timeout=2.0, resolver=failing_resolver).send(make_payment())


async def test_destination_check_uses_punycode_host_that_httpx_connects_to() -> None:
    resolved: list[str] = []

    async def recording_resolver(host: str, port: int) -> list[str]:
        resolved.append(host)
        return [PUBLIC_IP]

    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200)))
    async with client:
        await WebhookSender(client, timeout=2.0, resolver=recording_resolver).send(
            make_payment(url="http://xn--fa-hia.de/hook")  # faß.de; по IDNA2003 было бы fass.de
        )
    assert resolved == ["xn--fa-hia.de"]


async def test_destination_check_is_covered_by_overall_deadline() -> None:
    async def hanging_resolver(host: str, port: int) -> list[str]:
        await asyncio.sleep(5)
        return [PUBLIC_IP]

    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200)))
    async with client:
        with pytest.raises(WebhookDeliveryError, match="Timed out"):
            await WebhookSender(client, timeout=0.2, resolver=hanging_resolver).send(make_payment())
