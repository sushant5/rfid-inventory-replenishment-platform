FROM python:3.12.13-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN groupadd --system abacus && useradd --system --gid abacus --create-home abacus

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY alembic.ini ./
COPY alembic ./alembic
COPY scripts ./scripts
COPY examples ./examples
RUN pip install --no-cache-dir .

USER abacus
EXPOSE 8000

CMD ["uvicorn", "abacus.main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM base AS test

USER root
COPY tests ./tests
RUN pip install --no-cache-dir ".[dev]"
ENV COVERAGE_FILE=/tmp/.coverage
USER abacus

CMD ["python", "-m", "pytest", "-o", "cache_dir=/tmp/pytest-cache"]

FROM base AS runtime
