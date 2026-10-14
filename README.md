# Payments Service — асинхронный процессинг платежей

Микросервис принимает платежи по HTTP, обрабатывает их асинхронно через эмулятор платёжного шлюза
и уведомляет клиента о результате webhook'ом.

**Стек:** FastAPI + Pydantic v2 · SQLAlchemy 2.0 (async) + asyncpg · PostgreSQL 17 · RabbitMQ 4.1 (FastStream 0.7) · Alembic · Docker Compose · uv.

**Ключевое:** Transactional Outbox · Idempotency-Key · retry с экспоненциальной задержкой на отложенных очередях · Dead Letter Queue ·
идемпотентный consumer без двойного списания · quorum-очереди с at-least-once dead-lettering.

---

## Требования

- Docker с Compose v2.20+ (`docker compose`, не `docker-compose` 1.x) и BuildKit (включён по умолчанию).
- Свободные порты на `127.0.0.1`: `8000` (API), `15672` (RabbitMQ UI), `8090` (демо-приёмник webhook'ов) — переопределяются в `.env`.
- Windows: команды ниже рассчитаны на bash (Git Bash / WSL). В PowerShell используйте `curl.exe` и тела из `examples/`
  (`--data-binary "@examples/payment-ok.json"`) или Swagger UI. Без `make` — рядом с каждой командой указан её эквивалент.

## Быстрый старт

```bash
make demo        # = docker compose --profile demo up -d --build --wait
```

Поднимаются `postgres`, `rabbitmq`, `api`, `consumer`, одноразовый job `migrations` (применяет миграции до старта сервисов)
и демо-приёмник webhook'ов `webhook-receiver`. `--wait` возвращает управление, когда все healthchecks зелёные
(`make ps` = `docker compose --profile demo --profile test ps`). Без демо-приёмника (только postgres, rabbitmq, api, consumer): `make up` (`docker compose up -d --build --wait`).

| Что | Где |
|---|---|
| Swagger UI | http://localhost:8000/docs — кнопка **Authorize**, ключ `dev-api-key` |
| Готовность | http://localhost:8000/health/ready |
| RabbitMQ UI | http://localhost:15672 — `payments` / `payments` |
| Полученные webhook'и | http://localhost:8090/webhooks |
| Логи | `make logs` (`docker compose logs -f api consumer`) |

Остановить: `make down` (`docker compose --profile demo --profile test down`), с удалением данных — `make clean` (`… down -v`).

## Проверка за минуту

```bash
# 1. Три платежа: webhook принимается сразу / с 3-й доставки / после 3 неудач уходит в DLQ
for kind in ok flaky replay; do
  curl -s -X POST http://localhost:8000/api/v1/payments \
    -H 'X-API-Key: dev-api-key' -H "Idempotency-Key: demo-$kind-1" \
    -H 'Content-Type: application/json' --data-binary @examples/payment-$kind.json; echo
done

# 2. Обработка (2–5 с), retry через 2 и 4 с, DLQ после 3-й попытки (~10 с)
docker compose logs -f consumer             # Ctrl+C, когда увидите "moving message to DLQ"
curl -s localhost:8090/webhooks             # что получил приёмник (+ результат проверки подписи)

# 3. DLQ: одно сообщение (платёж из payment-replay.json)
docker compose exec -T rabbitmq rabbitmqctl list_queues name type messages
#   UI: http://localhost:15672/#/queues/%2F/payments.new.dlq → Get messages (заголовки x-last-error, x-dead-letter-reason)

# 4. Вернуть его в обработку: 4-я доставка демо-приёмник уже примет
make replay-dlq                             # = docker compose exec -T consumer python -m app.messaging.dlq_replay --limit 100
curl -s localhost:8090/webhooks
```

Демо-приёмник (`webhook_url` — адрес внутри сети compose; `localhost:8090` из контейнера недоступен):

| `webhook_url` | Поведение |
|---|---|
| `http://webhook-receiver:9000/webhooks/ok` | всегда 200 (`examples/payment-ok.json`) |
| `http://webhook-receiver:9000/webhooks/flaky?failures=N` | первые N доставок по платежу 503, затем 200 (`payment-flaky.json`: N=2, `payment-replay.json`: N=3 → DLQ → replay) |
| `http://webhook-receiver:9000/webhooks/fail` | всегда 500 → DLQ после 3 попыток и снова DLQ после replay (`payment-fail.json`) |

Технические сбои шлюза (а не отказы) включаются переменной `GATEWAY_ERROR_RATE`, например `GATEWAY_ERROR_RATE=0.5 make demo` —
тогда видно retry ошибок обработки.

---

## API

Все эндпоинты `/api/*` требуют заголовок `X-API-Key`. Ключ проверяется до чтения тела запроса (битое или огромное тело без ключа → `401`),
тело ограничено 64 КБ (`413`).

### `POST /api/v1/payments` — создать платёж

Заголовки: `X-API-Key`, `Idempotency-Key` (обязательный, 1–255 печатных ASCII-символов без пробелов).

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' \
  -H 'Idempotency-Key: order-42-payment-1' \
  -H 'Content-Type: application/json' \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Оплата заказа #42",
        "metadata": {"order_id": 42},
        "webhook_url": "http://webhook-receiver:9000/webhooks/ok"
      }'
```

```http
HTTP/1.1 202 Accepted
Location: /api/v1/payments/e694b0aa-8c6c-4fe8-81da-a06e4dc85451
Idempotent-Replayed: false

{"payment_id":"e694b0aa-8c6c-4fe8-81da-a06e4dc85451","status":"pending","created_at":"2026-10-14T13:47:18.916371Z"}
```

- Повтор с тем же ключом и тем же телом не создаёт новый платёж: `202`, тот же `payment_id` с **текущим** статусом, `Idempotent-Replayed: true`.
  «То же тело» — семантически: `"100"`, `100` и `"100.00"` совпадают, порядок ключей `metadata` не важен.
- Тот же ключ с другим телом → `422` с кодом `idempotency_key_reused`.

| Поле | Правила |
|---|---|
| `amount` | > 0, до 16 цифр до точки и 2 после (`NUMERIC(18,2)`). Рекомендуется строкой `"1500.00"`. JSON-числом — только если оно восстанавливается точно (до 15 значащих цифр): иначе точность теряется ещё при разборе JSON, и запрос отклоняется |
| `currency` | `RUB`, `USD`, `EUR` |
| `description` | необязательное, до 512 символов, пробелы по краям обрезаются, пустое → `null` |
| `metadata` | необязательный JSON-объект: до 16 КБ, вложенность до 10, без `NaN`/`Infinity`, NUL-символов и одиночных суррогатов |
| `webhook_url` | `http(s)`, до 2048 символов после нормализации |

Лишние поля запрещены.

### `GET /api/v1/payments/{payment_id}` — получить платёж

```json
{
  "payment_id": "e694b0aa-8c6c-4fe8-81da-a06e4dc85451",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Оплата заказа #42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "idempotency_key": "order-42-payment-1",
  "webhook_url": "http://webhook-receiver:9000/webhooks/ok",
  "created_at": "2026-10-14T13:47:18.916371Z",
  "processed_at": "2026-10-14T13:47:23.527711Z"
}
```

Суммы — строкой (без потерь точности float), даты — ISO 8601 UTC.

### Ошибки

Единый формат: `{"detail": "...", "code": "..."}`; у ошибок валидации `detail` — список `{loc, msg, type}` (без эха входных данных).

| Код | `code` | Когда |
|---|---|---|
| `401` | `unauthorized` | нет или неверный `X-API-Key` |
| `404` | `payment_not_found` / `not_found` | платёж / маршрут не найден |
| `405` | `method_not_allowed` | метод не поддерживается |
| `413` | `payload_too_large` | тело больше 64 КБ |
| `422` | `validation_error` | невалидное тело, заголовок `Idempotency-Key` или `payment_id` в пути |
| `422` | `idempotency_key_reused` | ключ уже использован с другим телом |
| `500` | `internal_error` | непредвиденная ошибка (тоже JSON) |

### Health

`/health/live` и `/health/ready` — без ключа (пробы оркестратора). `/health/ready` отвечает `503`, если недоступна PostgreSQL или упала фоновая
задача outbox relay. Брокер и размер очереди outbox — информационные поля: без RabbitMQ API продолжает принимать платежи.

```json
{"status": "ok", "database": "ok", "rabbitmq": "ok", "outbox_relay": "running", "outbox_backlog": 0}
```

`rabbitmq` / `outbox_relay` = `disabled`, если relay выключен (`OUTBOX_RELAY_ENABLED=false`) — тогда API к брокеру не подключается.
У consumer свой `GET /health` (внутри контейнера, для healthcheck): 204, если соединение с RabbitMQ живо и подписчик запущен.

### Webhook

`POST webhook_url` после финализации платежа:

```
Content-Type: application/json
User-Agent: payments-service-webhooks/1.0
X-Webhook-Event: payment.succeeded | payment.failed
X-Webhook-Id: 2d41b52b-...                     — одинаков при повторных доставках одного события
X-Webhook-Timestamp: 1791983243                — если задан WEBHOOK_SECRET (в compose задан)
X-Webhook-Signature: sha256=<hex>              — HMAC-SHA256(secret, "<timestamp>.<тело>")

{"event": "payment.succeeded", "payment": { ...как в GET /api/v1/payments/{id}... }}
```

- Доставка **at-least-once**: возможны повторы (retry, редкие дубли событий) — получатель отсеивает их по `X-Webhook-Id`.
- Успех — любой `2xx` в пределах `WEBHOOK_TIMEOUT` (общий дедлайн на запрос, включая DNS); тело ответа не читается, редиректы не выполняются.
- Защита от SSRF: все адреса хоста после DNS-резолва должны быть публичными (проверяется punycode-имя — именно к нему подключается клиент),
  иначе сообщение сразу уходит в DLQ. В compose приватные адреса разрешены (`WEBHOOK_ALLOW_PRIVATE_NETWORKS=true`): демо-приёмник живёт во внутренней сети.

Проверка подписи на стороне получателя:

```python
expected = "sha256=" + hmac.new(secret.encode(), timestamp.encode() + b"." + raw_body, hashlib.sha256).hexdigest()
valid = hmac.compare_digest(expected, request.headers["X-Webhook-Signature"])
fresh = abs(time.time() - int(timestamp)) < 300  # защита от повторной отправки перехваченного запроса
```

---

## Архитектура

```
 Client ──POST /api/v1/payments──▶ API ──┐  одна транзакция
                                         ├──▶ payments (pending)
                                         └──▶ outbox   (payment.created)
                                                │
                     Outbox relay (фоновая задача API: FOR UPDATE SKIP LOCKED, publisher confirms, mandatory)
                                                ▼
   ┌───────────── exchange "payments" (direct) ──payments.new──▶ quorum queue payments.new ──────────┐
   │                                                                                                   ▼
   │                                                                  Consumer (один обработчик, prefetch 10)
   │                                                       1. SELECT … FOR NO KEY UPDATE SKIP LOCKED (pending)
   │                                                       2. шлюз: idempotency key = payment_id, таймаут
   │                                                          (эмуляция: 2–5 с, 90% succeeded / 10% failed)
   │                                                       3. status + processed_at → COMMIT
   │                                                       4. webhook → клиент
   │                                                                      │                      │
   │   ошибка, попытки остались: копия с x-attempt+1                       │                      │ попытки исчерпаны /
   │   ◀───────────────────────────────────────────────────────────────────┘                      │ неисправимая ошибка:
   │   exchange "payments.retry" ─▶ payments.new.retry.2000ms (TTL 2 с) ┐                         │ копия с x-last-error
   │                             └▶ payments.new.retry.4000ms (TTL 4 с) ┤                         ▼
   └──────────────── x-dead-letter-exchange = payments ◀────────────────┘          exchange "payments.dlx"
                                                                      reject / x-delivery-limit ─▶ quorum queue payments.new.dlq
```

### Жизненный цикл платежа

`pending` → `succeeded` | `failed`. Финальные статусы неизменяемы. В БД — CHECK-ограничения: `amount > 0`, допустимые валюта и статус,
`(status = 'pending') = (processed_at IS NULL)`. `processed_at` — момент фиксации результата (`statement_timestamp()`, а не начало транзакции).

### Outbox pattern

- Платёж и событие `payment.created` пишутся **в одной транзакции**.
- **Relay** — фоновая задача процесса API. Берёт пачку неопубликованных событий через `SELECT … FOR UPDATE SKIP LOCKED` (безопасно при нескольких
  репликах API), публикует конкурентно с **publisher confirms** и `mandatory=True` (неотмаршрутизированное сообщение — ошибка, а не тихая потеря),
  persistent-сообщениями; `published_at` ставится только после подтверждения брокера.
- Сразу после коммита нового платежа relay будится (`asyncio.Event`): задержка публикации — миллисекунды, `OUTBOX_POLL_INTERVAL` — страховка.
- Отказ брокера по конкретному сообщению (nack, unroutable) откладывает только это сообщение (`attempts`, `last_error`, экспоненциальный `next_attempt_at`),
  остальные публикуются.
- **Брокер недоступен** (в т. ч. на старте API): API принимает платежи, события копятся в outbox (`outbox_backlog` в `/health/ready`). Relay
  переподключается с экспоненциальной паузой до `OUTBOX_RETRY_MAX_DELAY`; новые платежи паузу не прерывают (нет шторма подключений), сами события
  не откладываются — после восстановления брокера backlog уходит сразу.
- Цикл relay не падает от ошибок; если задача всё же завершится, это видно в `/health/ready` (`outbox_relay: stopped`, 503) и в логе (CRITICAL).
- Гарантия **at-least-once**: падение между confirm и коммитом даст повторную публикацию, поэтому consumer идемпотентен.

### Идемпотентность

**В API:** `Idempotency-Key` уникален в БД, создание — `INSERT … ON CONFLICT (idempotency_key) DO NOTHING RETURNING`: параллельные запросы с одним
ключом не создают дублей (второй INSERT дожидается коммита первого). Вместе с ключом хранится SHA-256 канонического тела — повтор с другим телом отклоняется.

**В consumer:** событие может прийти повторно (outbox, retry, replay из DLQ) и даже параллельно.
- Pending-платёж блокируется `SELECT … FOR NO KEY UPDATE SKIP LOCKED`; шлюз вызывается, результат фиксируется в той же транзакции.
- Параллельный дубль видит блокировку и **откладывается без расхода попытки** (а не выбрасывается): если владелец блокировки умрёт, дубль
  обработает платёж позже. Дубль после фиксации видит финальный статус и шлюз не вызывает — повторяется только webhook.
- Шлюзу передаётся `idempotency_key = payment_id`: если результат получен, а коммит в нашу БД не прошёл, настоящий шлюз на повторный вызов вернёт
  тот же результат, а не спишет деньги снова. Эмулятор повторяет это поведение в пределах процесса (кеш в памяти).
- Транзакция держится на время вызова шлюза — осознанный компромисс ради отсутствия параллельных списаний. Её длительность ограничена
  `GATEWAY_TIMEOUT`, а сессии «умерших» процессов PostgreSQL обрывает сам (`idle_in_transaction_session_timeout` = 2 × `GATEWAY_TIMEOUT`).
  Пул соединений consumer'а ≥ `CONSUMER_PREFETCH_COUNT`.

### Retry и DLQ

| Ситуация | Что происходит |
|---|---|
| Ошибка доставки webhook (сеть, таймаут, не-2xx), технический сбой шлюза, любая непредвиденная ошибка | копия сообщения с `x-attempt+1` → retry-очередь текущей ступени; через TTL — обратно в `payments.new` |
| 3-я попытка неудачна | копия в DLX с `x-attempt`, финальной `x-last-error` и `x-dead-letter-reason: max_attempts_exceeded` |
| Платёж не найден, `webhook_url` указывает на приватный адрес | сразу в DLQ (`x-dead-letter-reason: permanent_error`) |
| Невалидное тело сообщения | `reject` → нативный dead-lettering RabbitMQ (`x-death`) → DLQ |
| Платёж занят другим обработчиком; PostgreSQL недоступна (рестарт, recovery, сеть, исчерпаны соединения) | отсрочка через retry-очередь **без расхода попытки** — сбой БД не отправляет сообщения в DLQ. Ошибки данных и запросов (constraint, синтаксис) так не классифицируются и тратят попытки |
| Не удалось переопубликовать копию | unroutable → `reject` в DLX; иначе пауза и `nack(requeue=true)` (без горячего цикла) |
| Consumer упал посреди обработки | сообщение не подтверждено и вернётся в очередь; после `x-delivery-limit` (10) передоставок — DLQ (проверено на RabbitMQ 4.1) |
| SIGTERM | graceful shutdown дожидается сообщений в работе (`CONSUMER_GRACEFUL_TIMEOUT` ≥ `GATEWAY_TIMEOUT` + `WEBHOOK_TIMEOUT`, < `stop_grace_period`) |

- Задержки: `RETRY_BASE_DELAY * RETRY_BACKOFF_MULTIPLIER^(n-1)` → по умолчанию **2 с, 4 с**. Отдельная очередь на ступень с TTL на уровне очереди
  (у одной очереди с per-message TTL — head-of-line blocking); TTL в имени очереди, поэтому смена настроек не вызывает `PRECONDITION_FAILED`.
- Все очереди — **quorum**: переживают рестарт брокера и перекладывают сообщения в DLX в режиме at-least-once (у classic-очередей — at-most-once).
  У DLQ лимит передоставок отключён (`x-delivery-limit: -1`): в RabbitMQ 4.x по умолчанию он 20, и сообщение, возвращённое в DLQ 20 раз, удалялось бы.
- Повтор не вызывает шлюз (статус уже финальный) — повторяется только доставка webhook, статус платежа не «перебрасывается».
- `make replay-dlq` возвращает сообщения из DLQ со сброшенным счётчиком попыток; берёт не больше, чем было в DLQ на момент запуска
  (сообщение, которое снова упадёт, не гоняется по кругу), прежние причина и ошибка сохраняются в `x-replayed-*`.
- Consumer объявляет всю топологию до старта подписчика: при несовместимых аргументах существующей очереди (`PRECONDITION_FAILED`) процесс
  падает на старте и перезапускается, а не висит «живым» без подписки.

### Проверенные сценарии отказов

Кроме автотестов, вручную на поднятом стеке:

| Сценарий | Результат |
|---|---|
| RabbitMQ остановлен, API перезапущен | API стартует, `/health/ready` = 200 (`rabbitmq: unavailable`), платежи принимаются, `outbox_backlog` растёт; через ~10 с после старта RabbitMQ всё опубликовано и обработано |
| `docker compose restart postgres` посреди обработки | сообщения отложены без расхода попыток, после подъёма БД все платежи финализированы, DLQ не пополнился |
| SIGTERM consumer'у во время обработки 5 платежей | все 5 дообработаны до выхода |
| `payments.new` заранее создана с другими аргументами | consumer падает на старте (`Application startup failed`), после исправления поднимается сам |
| 100 параллельных платежей | ≈ 90% succeeded / 10% failed, outbox полностью опубликован |

---

## Принятые решения

| Вопрос | Решение | Почему |
|---|---|---|
| Неуспешные платежи эмулятора (10%) | Отказ шлюза → статус `failed` + webhook `payment.failed`, не retry. Технические сбои шлюза (исключение → retry → DLQ) — отдельно, `GATEWAY_ERROR_RATE` | Отказ банка — бизнес-результат, а не сбой: повтор его не изменит, а клиент должен узнать о нём сразу |
| Число попыток | 3 попытки **всего** (первая + 2 повтора), затем DLQ. Настраивается `RETRY_MAX_ATTEMPTS` | Около 6 с на восстановление получателя по умолчанию; дальше — DLQ и ручной replay |
| Что ретраится | Доставка webhook, технические сбои шлюза, непредвиденные ошибки | Статус платежа не зависит от доставки webhook |
| Где живёт outbox relay | Фоновая задача процесса API | Relay рядом с продюсером событий, отдельный сервис не нужен; `SKIP LOCKED` делает его безопасным при нескольких репликах API. Consumer остаётся одним обработчиком |
| Повтор ключа с другим телом | `422` + `code: idempotency_key_reused` | [draft-ietf-httpapi-idempotency-key-header](https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/); `code` отличает от ошибки валидации |
| Ответ на повтор с тем же ключом | `202` с тем же `payment_id` и **текущим** статусом + `Idempotent-Replayed: true` | Клиент получает актуальное состояние |
| Миграции | Одноразовый сервис `migrations` в compose | Применяются ровно один раз, без гонки между репликами; `api` и `consumer` ждут его завершения |
| Где проверяется `X-API-Key` | Все бизнес-эндпоинты `/api/*` (проверка в middleware — новые роуты под `/api/` защищены автоматически). `/health/*` открыты для проб; `/docs` и `/openapi.json` отключаются `DOCS_ENABLED=false` | Пробы оркестратора не должны знать секрет; бизнес-данных там нет |
| `depends_on: rabbitmq` у `api` в compose | Оставлено, хотя API стартует и без брокера | Только чтобы при первом запуске в логах не было предупреждений «broker is not available yet» |
| Поле `metadata` | Колонка `metadata` в БД, атрибут `meta` в ORM | `metadata` зарезервирован в SQLAlchemy Declarative API |
| Деньги | `NUMERIC(18,2)` в БД, `Decimal` в коде, строка в JSON | Без потерь точности float |
| `description` | Необязательное | Не все платежи имеют осмысленное описание; пустое → `null` |

---

## Конфигурация

Параметры — переменные окружения (`app/config.py`). В compose они пробрасываются из `.env` или окружения шелла
(`GATEWAY_SUCCESS_RATE=0 make demo`); незаданные берут значения по умолчанию. Шаблон — `.env.example`.

| Переменная | По умолчанию | Описание |
|---|---|---|
| `API_KEY` | — (для API обязательна; в compose `dev-api-key`) | статический ключ `X-API-Key`, ≥ 8 символов |
| `DATABASE_URL` / `RABBITMQ_URL` | — (обязательны; в compose собираются из кредов) | подключения |
| `DOCS_ENABLED` | `true` | Swagger UI / OpenAPI |
| `API_MAX_BODY_SIZE` | `65536` | лимит тела запроса, байт |
| `RABBITMQ_EXCHANGE` / `RABBITMQ_QUEUE` | `payments` / `payments.new` | имена; retry / DLX / DLQ выводятся из них |
| `RABBITMQ_PUBLISH_TIMEOUT` | `10` | ожидание publisher confirm, с |
| `OUTBOX_RELAY_ENABLED` | `true` | фоновая публикация outbox в процессе API |
| `OUTBOX_POLL_INTERVAL` / `OUTBOX_BATCH_SIZE` | `1.0` / `100` | страховочный интервал опроса и размер пачки |
| `OUTBOX_RETRY_BASE_DELAY` / `OUTBOX_RETRY_MAX_DELAY` | `1.0` / `30` | backoff публикации и переподключения к брокеру, с |
| `CONSUMER_PREFETCH_COUNT` | `10` | сколько сообщений consumer обрабатывает параллельно |
| `CONSUMER_GRACEFUL_TIMEOUT` | `30` | ожидание сообщений в работе при остановке, с; ≥ `GATEWAY_TIMEOUT` + `WEBHOOK_TIMEOUT` и меньше `stop_grace_period` (40 с) в compose |
| `RETRY_MAX_ATTEMPTS` | `3` | всего попыток до DLQ |
| `RETRY_BASE_DELAY` / `RETRY_BACKOFF_MULTIPLIER` | `2.0` / `2.0` | экспоненциальная задержка, с |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` | `2.0` / `5.0` | длительность эмуляции, с |
| `GATEWAY_SUCCESS_RATE` | `0.9` | доля `succeeded` (остальное — `failed`) |
| `GATEWAY_ERROR_RATE` | `0.0` | доля технических сбоев шлюза (retry) |
| `GATEWAY_TIMEOUT` | `15` | таймаут вызова шлюза, с |
| `WEBHOOK_TIMEOUT` | `5.0` | общий дедлайн доставки webhook, с |
| `WEBHOOK_SECRET` | не задан (в compose `dev-webhook-secret`) | секрет HMAC-подписи; пустое значение в `.env` отключает подпись |
| `WEBHOOK_ALLOW_PRIVATE_NETWORKS` | `false` (в compose `true`) | разрешить webhook на приватные адреса |
| `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` / `DB_ECHO` | `10` / `10` / `false` | пул соединений, SQL-логи |
| `LOG_LEVEL` | `INFO` | `DEBUG`…`CRITICAL` |

После смены кредов PostgreSQL / RabbitMQ пересоздайте volumes: `make clean`.

---

## Тесты и качество

```bash
make test     # = docker compose --profile test run --rm --build tests            (~35 с без учёта первой сборки)
make lint     # = docker compose --profile test run --rm --build --no-deps tests sh -c "ruff check . && ruff format --check . && mypy app tools tests"
```

223 теста. Интеграционные идут против **настоящих** PostgreSQL и RabbitMQ: отдельная БД `payments_test`, уникальные exchange/очереди на каждый тест
(можно запускать при поднятом стеке; два прогона одновременно — нельзя, БД общая). Миграции прогоняются `downgrade base → upgrade head`.
`postgres` и `rabbitmq` после `make test` остаются запущенными (`make down`). Без Docker — только unit-тесты и линтеры:
`uv run pytest -m "not integration"`, `make lint-local`.

- **API:** ключ проверяется до разбора тела (битый JSON / 100 КБ без ключа → 401), лимит тела, валидация (включая NUL, NaN, Infinity, одиночные суррогаты,
  URL после нормализации — 422, а не 500), 202 + outbox, идемпотентность (повтор, нормализация тела, другое тело, 10 параллельных запросов с одним ключом,
  параллельные с разными телами), GET/404/422, readiness (503 без БД и при упавшем relay; 200 без RabbitMQ), **старт и приём платежей без RabbitMQ**.
- **Outbox relay:** публикация и пометка, размер пачки, недоступный брокер не откладывает события, «ядовитое» сообщение откладывается само и не блокирует
  остальные (backoff по часам БД), 4 параллельных relay без двойной публикации, ожидание брокера на старте, пробуждение по `notify()`, мягкая остановка,
  отсутствие переполнения backoff и шторма подключений при потоке `notify()`, реальный RabbitMQ.
- **Consumer** (реальный RabbitMQ, webhook через `httpx.MockTransport`): обработка и `processed_at`, retry с измерением задержек, сбой шлюза → retry → одно
  успешное списание, DLQ с финальной ошибкой, сразу в DLQ (неизвестный платёж, битое сообщение), последовательный и параллельный дубли (шлюз — один раз;
  отсрочка дубля подтверждается логом), `prefetch_count` реально ограничивает параллелизм, `RETRY_MAX_ATTEMPTS=1`.
- **Unit:** все исходы обработчика сообщений на фейках (retry, DLQ, отсрочка, `nack`/`reject` при сбое публикации), распознавание недоступности БД на реальных
  исключениях asyncpg/SQLAlchemy, middleware, схемы и fingerprint, топология, эмулятор шлюза (2–5 с, ≈ 90%, идемпотентность по ключу),
  webhook (подпись, `X-Webhook-Id`, дедлайн, редиректы, SSRF, punycode), OpenAPI, валидация настроек.
- **Сквозной сценарий** POST → outbox → relay → RabbitMQ → consumer → БД → webhook → GET и **DLQ replay** (возврат, лимит, отсутствие зацикливания).
- **Миграции:** модели совпадают со схемой (autogenerate-diff с типами и server default пуст), CHECK-ограничения (проверяется, какое именно сработало).

---

## Структура проекта

```
app/
  main.py         точка входа API (uvicorn app.main:app)
  api/            create_app + lifespan, middleware (X-API-Key, лимит тела), ошибки, зависимости, /api/v1/payments, health
  consumer/       FastStream-приложение и обработчик payments.new (retry / DLQ / отсрочка); main.py — ASGI-обёртка с /health
  messaging/      топология RabbitMQ, брокер, outbox relay, publisher, retry-политика, dlq_replay
  services/       PaymentService (создание + outbox), PaymentProcessor (шлюз → статус → webhook), эмулятор шлюза, webhooks
  repositories/   доступ к payments и outbox
  db/             модели, Base с naming convention, engine/session, распознавание недоступности БД
  config.py, schemas.py, enums.py, logging_config.py
migrations/       Alembic (async env)
examples/         тела запросов для curl
tools/            webhook_receiver — демо-приёмник webhook'ов (профиль demo)
tests/            unit + integration (PostgreSQL + RabbitMQ)
```

---

## Дальнейшее развитие

- **Сверка «зависших» pending:** периодическая задача, которая находит pending-платежи старше N минут (например, ушедшие в DLQ) и переотправляет
  событие или сверяет статус со шлюзом; алерт на непустую DLQ и на события outbox с большим `attempts`.
- **Наблюдаемость:** метрики (размер и возраст outbox, глубина DLQ, латентность шлюза и webhook) и трейсинг — у FastStream есть интеграции с Prometheus и OpenTelemetry.
- **Очистка outbox:** удаление опубликованных событий старше N дней (или партиционирование по времени).
- **SSRF:** полная защита от DNS rebinding — egress-прокси для webhook'ов (сейчас адрес проверяется непосредственно перед подключением).
- **Безопасность API:** ключи per-merchant с ротацией и хранением хеша, rate limiting, привязка `Idempotency-Key` к клиенту и TTL ключей.
- **Webhook:** jitter в задержках, ретраи на горизонте часов (как у платёжных провайдеров), история доставок.
- **Шлюз:** при реальной интеграции — двухфазная обработка (статус `processing` с lease, вызов шлюза вне транзакции) вместо удержания блокировки.
