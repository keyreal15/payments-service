import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import Channel
from faststream.rabbit.annotations import RabbitMessage

from app.config import Settings, get_settings
from app.consumer.handler import PaymentCreatedHandler
from app.db.session import create_db_engine, create_session_factory
from app.messaging.broker import create_broker
from app.messaging.retry import retry_policy_from_settings
from app.messaging.topology import build_topology, declare_topology
from app.schemas import PaymentCreatedEvent
from app.services.gateway import EmulatedPaymentGateway, PaymentGateway
from app.services.processing import PaymentProcessor
from app.services.webhooks import WebhookSender

USER_AGENT = "payments-service-webhooks/1.0"


def create_consumer_app(
    settings: Settings | None = None,
    *,
    gateway: PaymentGateway | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> FastStream:
    settings = settings or get_settings()

    retry_policy = retry_policy_from_settings(settings)
    topology = build_topology(settings, retry_policy)
    broker = create_broker(settings, graceful_timeout=settings.consumer_graceful_timeout)
    engine = create_db_engine(settings, application_name="payments-consumer")
    client = http_client or httpx.AsyncClient(
        timeout=settings.webhook_timeout,
        follow_redirects=False,
        headers={"User-Agent": USER_AGENT},
    )
    webhook_secret = settings.webhook_secret.get_secret_value() if settings.webhook_secret else None

    processor = PaymentProcessor(
        create_session_factory(engine),
        gateway
        or EmulatedPaymentGateway(
            min_delay=settings.gateway_min_delay,
            max_delay=settings.gateway_max_delay,
            success_rate=settings.gateway_success_rate,
            error_rate=settings.gateway_error_rate,
        ),
        WebhookSender(
            client,
            timeout=settings.webhook_timeout,
            secret=webhook_secret,
            allow_private_networks=settings.webhook_allow_private_networks,
        ),
        gateway_timeout=settings.gateway_timeout,
    )
    handler = PaymentCreatedHandler(
        processor,
        broker,
        topology,
        retry_policy,
        publish_timeout=settings.rabbitmq_publish_timeout,
    )

    # prefetch_count ограничивает число одновременно обрабатываемых сообщений:
    # aiormq запускает обработку каждой доставки отдельной задачей.
    subscriber_channel = Channel(prefetch_count=settings.consumer_prefetch_count)

    @broker.subscriber(
        topology.queue,
        topology.exchange,
        channel=subscriber_channel,
        ack_policy=AckPolicy.REJECT_ON_ERROR,
    )
    async def handle_payment_created(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        await handler.handle(event, message)

    async def declare() -> None:
        """Вся топология объявляется до старта подписчика: ошибка (например, PRECONDITION_FAILED
        из-за несовпадения аргументов очереди) роняет старт процесса, а не оставляет его «живым»
        без подписки.

        payments.new объявляется на канале подписчика (с QoS): FastStream кеширует объявленные
        очереди без учёта канала, и подписчик возьмёт из кеша именно этот объект. Объявление через
        broker.declare_queue() привязало бы очередь к каналу по умолчанию — consume без prefetch.
        """
        await broker.connect()
        await declare_topology(broker, topology, include_main_queue=False)
        queue = await broker.config.declarer.declare_queue(topology.queue, channel=subscriber_channel)
        await queue.bind(await broker.declare_exchange(topology.exchange), routing_key=topology.routing_key)

    async def close_resources() -> None:
        await client.aclose()
        await engine.dispose()

    return FastStream(broker, on_startup=[declare], after_shutdown=[close_resources])
