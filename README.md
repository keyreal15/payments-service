# Payments Service

Сервис асинхронной обработки платежей: принимает платёж по HTTP, проводит его через платёжный шлюз
(в репозитории — эмулятор) и сообщает клиенту результат webhook'ом.

**Стек:** Python 3.12 · FastAPI · Pydantic v2 · SQLAlchemy 2.0 (async) · PostgreSQL 17 · RabbitMQ 4.1 / FastStream 0.7 · Alembic · Docker Compose · uv

- **Transactional outbox** — событие о платеже пишется в одной транзакции с платежом и гарантированно доходит до брокера.
- **Идемпотентность** — `Idempotency-Key` на входе; на обработке — аренда платежа и idempotency key шлюза, двойного списания нет.
- **Retry и DLQ** — экспоненциальные повторы на отложенных quorum-очередях, DLQ с текстом ошибки, утилита возврата из DLQ.
- **Сверка** — платежи, зависшие в `pending`, автоматически возвращаются в обработку.
- **Webhooks** — HMAC-подпись, стабильный `X-Webhook-Id` для дедупликации, защита от SSRF.

## Быстрый старт

Нужен Docker с Compose v2.20+. Порты `8000`, `15672`, `8090` на `127.0.0.1` (меняются в `.env`).

```bash
make demo        # = docker compose --profile demo up -d --build --wait
```

Поднимаются `postgres`, `rabbitmq`, `api`, `consumer`, job `migrations` и демо-приёмник webhook'ов. Без приёмника — `make up`.

| | |
|---|---|
| Swagger UI | http://localhost:8000/docs — **Authorize**, ключ `dev-api-key` |
| RabbitMQ UI | http://localhost:15672 — `payments` / `payments` |
| Полученные webhook'и | http://localhost:8090/webhooks |
| Логи / остановка | `make logs` · `make down` · `make clean` (с удалением данных) |

Без `make`: каждая цель — одна команда `docker compose`, см. `Makefile`.

### Проверка за минуту

```bash
# три платежа: webhook принимается сразу / с 3-й доставки / после 3 неудач уходит в DLQ
for kind in ok flaky replay; do
  curl -s -X POST localhost:8000/api/v1/payments -H 'X-API-Key: dev-api-key' \
    -H "Idempotency-Key: demo-$kind-1" -H 'Content-Type: application/json' \
    --data-binary @examples/payment-$kind.json; echo
done

docker compose logs -f consumer                     # обработка 2–5 с, retry через 2 и 4 с, "moving message to DLQ"
docker compose exec -T rabbitmq rabbitmqctl list_queues name messages
make replay-dlq                                     # вернуть сообщение из DLQ — 4-я доставка пройдёт
curl -s localhost:8090/webhooks                     # что получил приёмник (и валидна ли подпись)
```

Демо-приёмник (адреса внутри сети compose): `/webhooks/ok` — 200, `/webhooks/flaky?failures=N` — первые N доставок 503,
`/webhooks/fail` — всегда 500. Технические сбои шлюза включаются `GATEWAY_ERROR_RATE` в `.env`.

## API

Все `/api/*` требуют `X-API-Key`; ключ и размер тела (≤ 64 КБ) проверяются до разбора тела.

**`POST /api/v1/payments`** — заголовок `Idempotency-Key` обязателен (1–255 печатных ASCII-символов).

```json
{"amount": "1500.00", "currency": "RUB", "description": "Оплата заказа #42",
 "metadata": {"order_id": 42}, "webhook_url": "http://webhook-receiver:9000/webhooks/ok"}
```

```http
HTTP/1.1 202 Accepted
Location: /api/v1/payments/e694b0aa-8c6c-4fe8-81da-a06e4dc85451
Idempotent-Replayed: false

{"payment_id": "e694b0aa-...", "status": "pending", "created_at": "2026-10-14T13:47:18.916371Z"}
```

| Поле | Правила |
|---|---|
| `amount` | > 0, `NUMERIC(18,2)`. Лучше строкой; JSON-число принимается, только если не теряет точность (≤ 15 значащих цифр) |
| `currency` | `RUB`, `USD`, `EUR` |
| `description` | необязательное, ≤ 512 символов |
| `metadata` | JSON-объект ≤ 16 КБ, вложенность ≤ 10, без `NaN`/`Infinity`/NUL |
| `webhook_url` | `http(s)`, ≤ 2048 символов после нормализации |

Повтор с тем же ключом и тем же телом (`"100"`, `100` и `"100.00"` равны, порядок ключей `metadata` не важен) возвращает
тот же платёж с текущим статусом и `Idempotent-Replayed: true`. Тот же ключ с другим телом — `422 idempotency_key_reused`.

**`GET /api/v1/payments/{payment_id}`** — платёж целиком: `payment_id`, `amount` (строкой), `currency`, `description`, `metadata`,
`status` (`pending` / `succeeded` / `failed`), `idempotency_key`, `webhook_url`, `created_at`, `processed_at`.

**Ошибки** — `{"detail": "...", "code": "..."}`: `401 unauthorized`, `404 payment_not_found`, `413 payload_too_large`,
`422 validation_error` (`detail` — список `{loc, msg, type}`), `422 idempotency_key_reused`, `500 internal_error`.

**Health** (без ключа): `/health/live`; `/health/ready` — 503, если недоступна PostgreSQL или упала фоновая задача.
Брокер и размер outbox — информационные поля: без RabbitMQ API продолжает принимать платежи.

```json
{"status": "ok", "database": "ok", "rabbitmq": "ok", "outbox_backlog": 0,
 "background_tasks": {"outbox_relay": "running", "reconciler": "running"}}
```

**Webhook** — `POST webhook_url` с телом `{"event": "payment.succeeded" | "payment.failed", "payment": {...как в GET...}}` и заголовками
`X-Webhook-Event`, `X-Webhook-Id` (одинаков при повторных доставках — по нему получатель отсеивает дубли), `X-Webhook-Timestamp`,
`X-Webhook-Signature: sha256=HMAC(secret, "<timestamp>.<тело>")`. Успех — любой `2xx` за `WEBHOOK_TIMEOUT`; редиректы не выполняются;
адрес получателя после DNS-резолва должен быть публичным.

```python
expected = "sha256=" + hmac.new(secret.encode(), timestamp.encode() + b"." + raw_body, hashlib.sha256).hexdigest()
valid = hmac.compare_digest(expected, signature) and abs(time.time() - int(timestamp)) < 300
```

## Как это работает

```
 POST /payments ─▶ API ─┬─ payments (pending) ┐ одна транзакция
                        └─ outbox             ┘
                              │ outbox relay (фон в API): SKIP LOCKED, publisher confirms
                              ▼
   exchange payments ─▶ payments.new ─▶ consumer: аренда → шлюз → статус → webhook
          ▲                                  │ ошибка, попытки остались   │ попытки исчерпаны
          │                                  ▼                            ▼
          └── TTL ── payments.new.retry.{2000,4000}ms          payments.dlx ─▶ payments.new.dlq
```

**Outbox.** Relay забирает пачку событий `SELECT … FOR UPDATE SKIP LOCKED` (безопасно для нескольких реплик API), публикует
конкурентно с publisher confirms и `mandatory` и только после подтверждения ставит `published_at`. Новый платёж будит relay сразу.
Отказ брокера по конкретному сообщению откладывает только его; недоступность брокера уводит в backoff весь цикл — API при этом
стартует и принимает платежи, события ждут в outbox. Гарантия at-least-once, поэтому consumer идемпотентен.

**Обработка платежа** — три коротких шага без транзакции на время вызова шлюза:
1. *аренда*: `UPDATE … SET lease_id, lease_expires_at WHERE status = 'pending' AND аренда свободна или истекла`;
2. *вызов шлюза* с `idempotency_key = payment_id` и таймаутом `GATEWAY_TIMEOUT`;
3. *фиксация*: compare-and-set `pending → succeeded | failed`, `processed_at`, аренда снимается.

Параллельный дубль видит живую аренду и откладывается без расхода попытки. Повтор того же сообщения (например, после сбоя БД
между шагами) несёт свой `lease_id` в заголовке `x-lease-id` и забирает аренду сразу. Если consumer умер посреди вызова, аренда
истекает (2 × `GATEWAY_TIMEOUT`) и платёж подхватывает следующая доставка; повторный вызов шлюза с тем же ключом вернёт прежний результат.
Для финализированного платежа повторяется только webhook. Соединение с БД занято миллисекунды, поэтому `CONSUMER_PREFETCH_COUNT = 50`.

**Retry и DLQ.**

| Ситуация | Что происходит |
|---|---|
| Ошибка webhook, сбой шлюза, непредвиденная ошибка | копия с `x-attempt + 1` в retry-очередь ступени (2 с, 4 с), затем снова `payments.new` |
| Третья неудача | копия в DLQ с `x-attempt`, `x-last-error`, `x-dead-letter-reason: max_attempts_exceeded` |
| Платёж не найден, `webhook_url` на приватный адрес | сразу в DLQ (`permanent_error`) |
| Невалидное тело сообщения | `reject` → нативный dead-lettering RabbitMQ |
| Платёж в аренде у другого обработчика; PostgreSQL недоступна | отсрочка без расхода попытки |
| Consumer упал без ack | повторная доставка; после 10 (`x-delivery-limit`) — DLQ |

Отдельная очередь на каждую ступень задержки — без head-of-line blocking у per-message TTL. Все очереди quorum: dead-lettering
at-least-once, у DLQ лимит передоставок отключён. `make replay-dlq` возвращает сообщения со сброшенным счётчиком попыток
(берёт не больше, чем было в DLQ на старте). Consumer объявляет топологию до старта подписки: при несовместимых аргументах
очереди процесс падает на старте, а не работает вхолостую.

**Сверка.** Раз в `RECONCILE_INTERVAL` фоновая задача API находит платежи, которые дольше `RECONCILE_STALE_AFTER` в `pending`
без живой аренды и без свежих событий (например, сообщение ушло в DLQ после серии сбоев шлюза), и публикует через outbox
событие `payment.reconciliation`. Повтор безопасен благодаря idempotency key шлюза.

**Решения, которые стоит знать.** Неуспешный ответ шлюза — бизнес-результат (`failed` + webhook), а не повод для retry;
технические сбои шлюза — отдельная история (`GATEWAY_ERROR_RATE`). Попыток 3 всего (первая + 2 повтора). Повтор ключа с другим
телом — `422` по [draft-ietf-httpapi-idempotency-key-header](https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/).
Колонка `metadata` в ORM называется `meta` — имя зарезервировано в SQLAlchemy.

## Устойчивость к сбоям

Проверено на поднятом стеке:

| Сценарий | Результат |
|---|---|
| RabbitMQ остановлен, API перезапущен | API стартует, платежи принимаются, `outbox_backlog` растёт; после запуска брокера всё доставлено |
| `docker compose restart postgres` во время обработки | сообщения отложены без расхода попыток, DLQ не пополнился |
| SIGTERM consumer'у во время обработки | сообщения в работе дообработаны до выхода |
| Шлюз три раза не ответил | сообщение в DLQ, платёж в `pending` → сверка → платёж проведён (есть автотест) |
| Очередь создана с другими аргументами | consumer падает на старте и поднимается после исправления |
| 100 параллельных платежей | ≈ 90% `succeeded` / 10% `failed`, outbox опубликован полностью |

## Конфигурация

Переменные окружения (`app/config.py`); в compose пробрасываются из `.env` или окружения. Основные:

| Переменная | По умолчанию | |
|---|---|---|
| `API_KEY` | — (в compose `dev-api-key`) | ключ `X-API-Key`, ≥ 8 символов |
| `DATABASE_URL`, `RABBITMQ_URL` | — (в compose собираются из кредов) | подключения |
| `CONSUMER_PREFETCH_COUNT` | `50` | параллельно обрабатываемых сообщений |
| `RETRY_MAX_ATTEMPTS` / `RETRY_BASE_DELAY` / `RETRY_BACKOFF_MULTIPLIER` | `3` / `2.0` / `2.0` | попытки и экспоненциальная задержка |
| `RECONCILE_INTERVAL` / `RECONCILE_STALE_AFTER` | `60` / `300` | сверка зависших платежей, с |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` / `GATEWAY_SUCCESS_RATE` | `2.0` / `5.0` / `0.9` | эмулятор шлюза |
| `GATEWAY_ERROR_RATE` / `GATEWAY_TIMEOUT` | `0.0` / `15` | технические сбои шлюза и таймаут вызова |
| `WEBHOOK_SECRET` / `WEBHOOK_TIMEOUT` | не задан / `5.0` | подпись (пусто — без подписи) и дедлайн доставки |
| `WEBHOOK_ALLOW_PRIVATE_NETWORKS` | `false` (в compose `true`) | разрешить приватные адреса для демо-приёмника |

Полный список — `.env.example` и `app/config.py`. После смены кредов PostgreSQL/RabbitMQ — `make clean`.

## Тесты

```bash
make test         # 230 тестов против настоящих PostgreSQL и RabbitMQ, ~35 с
make lint         # ruff + mypy --strict
uv run pytest -m "not integration"     # только unit, без Docker
```

Интеграционные тесты используют отдельную БД `payments_test` (миграции `downgrade base → upgrade head`) и уникальные очереди
на каждый тест, поэтому их можно запускать при поднятом стеке.

- **API:** аутентификация до разбора тела, лимит тела, валидация (включая значения, которые иначе дали бы 500), идемпотентность
  (повтор, нормализация тела, конфликт, 10 параллельных запросов с одним ключом), readiness, приём платежей без RabbitMQ.
- **Outbox relay:** публикация и пометка, недоступный брокер и «ядовитое» сообщение, параллельные relay без двойной публикации,
  ожидание брокера, отсутствие шторма подключений, мягкая остановка.
- **Consumer:** retry с измерением задержек, DLQ с финальной ошибкой, сбой шлюза → одно успешное списание, последовательные
  и параллельные дубли, истечение чужой аренды и возврат своей, `prefetch_count`, битые и неизвестные сообщения.
- **Сверка:** выбор зависших платежей и сквозной сценарий «DLQ → сверка → платёж проведён»; **DLQ replay**; сквозной POST → webhook → GET.
- **Unit:** исходы обработчика сообщений, распознавание недоступности БД на реальных исключениях asyncpg, схемы, топология,
  эмулятор шлюза, webhook (подпись, дедлайн, редиректы, SSRF), OpenAPI, настройки.
- **Миграции:** модели совпадают со схемой, CHECK-ограничения.

## Структура

```
app/
  api/            FastAPI: lifespan, middleware, ошибки, роуты /api/v1/payments, health, фоновые задачи
  consumer/       FastStream-приложение, обработчик payments.new, ASGI-обёртка с /health
  messaging/      топология RabbitMQ, outbox relay, публикация, retry-политика, dlq_replay
  services/       создание платежа, обработка (аренда → шлюз → статус → webhook), сверка, эмулятор шлюза, webhooks
  repositories/   payments, outbox
  db/             модели, сессии, распознавание недоступности БД
migrations/       Alembic: 0001 таблицы · 0002 аренда · 0003 индексы сверки
examples/         тела запросов для curl
tools/            демо-приёмник webhook'ов
tests/            unit + integration
```

## Дальнейшее развитие

- Метрики (размер и возраст outbox, глубина DLQ, латентность шлюза и webhook), трейсинг и алерты — у FastStream есть интеграции с Prometheus и OpenTelemetry.
- Очистка или партиционирование опубликованных событий outbox.
- Egress-прокси для webhook'ов — полная защита от DNS rebinding.
- Ключи API per-merchant с ротацией, rate limiting, TTL ключей идемпотентности.
