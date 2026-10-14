import uuid
from typing import Any

from fastapi import APIRouter, Request, Response, status

from app.api.dependencies import IdempotencyKey, PaymentServiceDep
from app.api.errors import ApiError, ErrorResponse, ValidationErrorResponse
from app.schemas import PaymentAccepted, PaymentCreate, PaymentRead
from app.services.errors import IdempotencyKeyReusedError, PaymentNotFoundError

router = APIRouter(prefix="/payments", tags=["payments"])

IDEMPOTENT_REPLAY_HEADER = "Idempotent-Replayed"

_COMMON_ERRORS: dict[int | str, dict[str, Any]] = {
    status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse, "description": "Нет или неверный X-API-Key"},
    status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ValidationErrorResponse, "description": "Невалидный запрос"},
}


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    operation_id="createPayment",
    summary="Создать платёж",
    responses={
        status.HTTP_202_ACCEPTED: {
            "description": "Платёж принят в обработку (или возвращён ранее созданный по тому же ключу)",
            "headers": {
                IDEMPOTENT_REPLAY_HEADER: {
                    "description": "`true`, если ответ — повтор ранее созданного платежа",
                    "schema": {"type": "string", "enum": ["true", "false"]},
                },
                "Location": {"description": "URL платежа", "schema": {"type": "string"}},
            },
        },
        **_COMMON_ERRORS,
        status.HTTP_413_CONTENT_TOO_LARGE: {"model": ErrorResponse, "description": "Слишком большое тело запроса"},
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "model": ValidationErrorResponse | ErrorResponse,
            "description": "Невалидный запрос (`validation_error`) или Idempotency-Key уже использован "
            "с другим телом (`idempotency_key_reused`)",
        },
    },
)
async def create_payment(
    payload: PaymentCreate,
    idempotency_key: IdempotencyKey,
    service: PaymentServiceDep,
    request: Request,
    response: Response,
) -> PaymentAccepted:
    """Принимает платёж в обработку. Результат придёт на `webhook_url`.

    Повторный запрос с тем же `Idempotency-Key` и тем же телом не создаёт новый платёж,
    а возвращает существующий (с текущим статусом) и заголовком `Idempotent-Replayed: true`.
    """
    try:
        result = await service.create(payload, idempotency_key)
    except IdempotencyKeyReusedError as exc:
        # draft-ietf-httpapi-idempotency-key-header: повтор ключа с другим телом -> 422.
        raise ApiError(status.HTTP_422_UNPROCESSABLE_CONTENT, "idempotency_key_reused", str(exc)) from exc

    payment = result.payment
    response.headers[IDEMPOTENT_REPLAY_HEADER] = "false" if result.created else "true"
    response.headers["Location"] = str(request.url_for("getPayment", payment_id=payment.id).path)
    return PaymentAccepted(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


@router.get(
    "/{payment_id}",
    response_model=PaymentRead,
    operation_id="getPayment",
    name="getPayment",
    summary="Получить платёж",
    responses={**_COMMON_ERRORS, status.HTTP_404_NOT_FOUND: {"model": ErrorResponse, "description": "Не найден"}},
)
async def get_payment(payment_id: uuid.UUID, service: PaymentServiceDep) -> PaymentRead:
    try:
        payment = await service.get(payment_id)
    except PaymentNotFoundError as exc:
        raise ApiError(status.HTTP_404_NOT_FOUND, "payment_not_found", "Payment not found") from exc
    return PaymentRead.model_validate(payment)
