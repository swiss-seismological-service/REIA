FROM python:3.12-slim-bookworm AS builder

LABEL maintainer="Nicolas Schmid <nicolas.schmid@sed.ethz.ch>"

WORKDIR /web

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl libpq-dev git\
    && rm -rf /var/lib/apt/lists/* /usr/share/doc /usr/share/man \
    && apt-get clean

# Install build backend requirements
RUN pip install --no-cache-dir setuptools wheel

# Copy requirements first for better caching
COPY requirements-web.txt pyproject.toml ./

# Install dependencies in single layer
RUN pip install --no-cache-dir --upgrade pip wheel setuptools && \
    pip install --no-cache-dir . && \
    pip install --no-cache-dir -r requirements-web.txt

# Copy source code after dependencies
COPY . .
RUN pip freeze > requirements.txt

RUN pip wheel --no-cache-dir --no-deps --wheel-dir /web/wheels -r requirements.txt

FROM python:3.12-slim-bookworm

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/* /usr/share/doc /usr/share/man \
    && apt-get clean

RUN useradd --create-home python \
    && chown python:python -R /app

COPY --from=builder /web/wheels /wheels

RUN pip install --no-cache /wheels/* && rm -rf /wheels

USER python

# Copy the entire project and install it
COPY --chown=python:python . .

# Install only the package code (dependencies already installed via wheels)
RUN pip install --no-cache-dir --no-deps -e .

ENV PYTHONUNBUFFERED="true" \
    PYTHONPATH="/app" \
    PATH="${PATH}:/home/python/.local/bin" \
    USER="python"

EXPOSE 8000

CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-c", "python:reia.config.gunicorn", "reia.webservice.main:app"]