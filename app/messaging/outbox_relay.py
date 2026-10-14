import asyncio
import contextlib
import logging
from dataclasses import dataclass
from datetime import timedelta

from aiormq.exceptions import DeliveryError
from sqlalchemy import func

from app.db.session import SessionFactory
from app.messaging.publisher import EventPublisher
from app.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)

MAX_ERROR_LENGTH = 1000


@dataclass(frozen=True, slots=True)
class BatchResult:
    published: int
    failed: int
    broker_unavailable: bool

    @property
    def attempted(self) -> int:
        return self.published + self.failed


class OutboxRelay:
    """Публикует события из таблицы outbox в RabbitMQ.

    Гарантия — at-least-once: событие помечается опубликованным только после publisher confirm.
    Падение между confirm и коммитом приведёт к повторной публикации — consumer идемпотентен.

    Ошибки делятся на два вида:
      * отказ по конкретному сообщению (nack / unroutable): сообщение откладывается со своим
        экспоненциальным backoff, остальные продолжают публиковаться;
      * недоступность брокера: весь цикл уходит в экспоненциальный backoff (прерывается только
        остановкой), сами сообщения не откладываются — после восстановления уходят сразу.
    Подключение к брокеру выполняется здесь же, поэтому API стартует и принимает платежи,
    даже если RabbitMQ недоступен.
    """

    def __init__(
        self,
        session_factory: SessionFactory,
        publisher: EventPublisher,
        *,
        batch_size: int,
        poll_interval: float,
        retry_base_delay: float,
        retry_max_delay: float,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._retry_base_delay = retry_base_delay
        self._retry_max_delay = retry_max_delay
        self._wakeup = asyncio.Event()
        self._stopped = asyncio.Event()

    def notify(self) -> None:
        """Будит relay сразу после коммита нового события, не дожидаясь poll_interval."""
        self._wakeup.set()

    def stop(self) -> None:
        """Мягкая остановка: текущая пачка дообрабатывается, новая не начинается."""
        self._stopped.set()
        self._wakeup.set()

    def _backoff(self, attempt: int) -> float:
        exponent = min(max(attempt, 1) - 1, 30)  # без ограничения 2**n переполнил бы float
        return float(min(self._retry_base_delay * 2**exponent, self._retry_max_delay))

    async def publish_batch(self) -> BatchResult:
        try:
            await self._publisher.ensure_ready()
        except Exception as exc:
            logger.warning("Message broker is not available yet: %r", exc)
            return BatchResult(published=0, failed=0, broker_unavailable=True)

        async with self._session_factory() as session, session.begin():
            repo = OutboxRepository(session)
            messages = await repo.lock_pending_batch(self._batch_size)
            if not messages:
                return BatchResult(published=0, failed=0, broker_unavailable=False)

            # Confirms приходят асинхронно: публикуем пачку конкурентно, а не по одному на RTT.
            # Порядок между разными платежами не важен.
            results = await asyncio.gather(
                *(self._publisher.publish(message) for message in messages), return_exceptions=True
            )

            published_ids = []
            errors: list[Exception] = []
            for message, result in zip(messages, results, strict=True):
                if result is None:
                    published_ids.append(message.id)
                    continue
                if not isinstance(result, Exception):
                    raise result  # CancelledError и прочие BaseException не глотаем
                errors.append(result)
                message.attempts += 1
                message.last_error = repr(result)[:MAX_ERROR_LENGTH]
                if isinstance(result, DeliveryError):
                    # Брокер отказал именно этому сообщению — откладываем только его.
                    # При недоступности брокера сообщения не откладываются: паузу держит цикл relay,
                    # и после восстановления брокера backlog уходит сразу.
                    message.next_attempt_at = func.statement_timestamp() + timedelta(
                        seconds=self._backoff(message.attempts)
                    )

            if published_ids:
                await repo.mark_published(published_ids)

        broker_unavailable = any(not isinstance(error, DeliveryError) for error in errors)
        if published_ids:
            logger.info("Published %d outbox message(s)", len(published_ids))
        if errors:
            logger.warning(
                "Failed to publish %d outbox message(s) (broker unavailable: %s): %r",
                len(errors),
                broker_unavailable,
                errors[0],
            )
        return BatchResult(published=len(published_ids), failed=len(errors), broker_unavailable=broker_unavailable)

    async def run(self) -> None:
        logger.info("Outbox relay started")
        failure_streak = 0
        while not self._stopped.is_set():
            # Сбрасываем флаг до чтения пачки: notify(), пришедший во время публикации,
            # не потеряется и сразу запустит следующую итерацию.
            self._wakeup.clear()
            try:
                result = await self.publish_batch()
            except Exception:
                logger.exception("Outbox relay iteration failed (database unavailable?)")
                result = None

            if result is None or result.broker_unavailable:
                failure_streak = min(failure_streak + 1, 64)
                # Пауза не прерывается новыми событиями: иначе при недоступном брокере каждый
                # POST /payments порождал бы попытку подключения.
                await self._wait(self._stopped, self._backoff(failure_streak))
                continue

            failure_streak = 0
            if result.attempted == self._batch_size:
                continue  # вероятно, есть ещё — разбираем без паузы
            await self._wait(self._wakeup, self._poll_interval)
        logger.info("Outbox relay stopped")

    @staticmethod
    async def _wait(event: asyncio.Event, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(event.wait(), timeout=seconds)
