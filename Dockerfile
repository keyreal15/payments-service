# syntax=docker/dockerfile:1

FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./


# Зависимости ставятся отдельным слоем: пересборка при изменении кода не трогает venv.
# --locked: сборка упадёт, если uv.lock не соответствует pyproject.toml.
FROM base AS deps
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-dev


FROM base AS dev-deps
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked


FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"
RUN groupadd --system app && useradd --system --gid app --no-create-home app
WORKDIR /app
COPY --from=deps /opt/venv /opt/venv
COPY alembic.ini ./
COPY migrations ./migrations
COPY app ./app
COPY tools ./tools
USER app
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-server-header"]


FROM runtime AS test
COPY --from=dev-deps /opt/venv /opt/venv
COPY pyproject.toml ./
COPY tests ./tests
# /app принадлежит root, процесс работает от app: кеши инструментов — во /tmp.
ENV PYTEST_ADDOPTS="-p no:cacheprovider" \
    RUFF_CACHE_DIR=/tmp/ruff-cache \
    MYPY_CACHE_DIR=/tmp/mypy-cache
CMD ["pytest"]
