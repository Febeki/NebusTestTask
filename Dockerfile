FROM python:3.12-slim AS builder
WORKDIR /app
RUN pip install --no-cache-dir uv==0.12.23
ENV UV_PYTHON_DOWNLOADS=never UV_LINK_MODE=copy
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable --compile-bytecode

FROM python:3.12-slim AS runtime
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
RUN groupadd --gid 10001 payments && useradd --uid 10001 --gid payments --no-create-home payments
COPY --from=builder /app/.venv /app/.venv
COPY alembic.ini ./
COPY migrations ./migrations
USER payments
CMD ["uvicorn", "payments.api.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
