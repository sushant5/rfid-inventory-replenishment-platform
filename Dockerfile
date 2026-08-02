FROM python:3.13.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN groupadd --system abacus && useradd --system --gid abacus --create-home abacus

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY alembic.ini ./
COPY alembic ./alembic
RUN pip install --no-cache-dir .

USER abacus
EXPOSE 8000

CMD ["uvicorn", "abacus.main:app", "--host", "0.0.0.0", "--port", "8000"]
