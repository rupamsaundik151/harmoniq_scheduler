# Single image used by three services (api, worker, beat) — only the
# entrypoint command differs. Keeps image build cache warm and prevents
# code / dependency drift between processes.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# System deps: libpq for asyncpg (via psycopg2 fallback) and build tools
# only if pure-wheel install fails; keep the layer thin.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --upgrade pip \
    && pip install .

EXPOSE 8080

# Default command runs the API. Overridden in docker-compose / k8s for
# the worker and beat services.
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
