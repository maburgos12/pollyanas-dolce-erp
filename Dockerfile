FROM python:3.12-slim-bookworm AS builder

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV VIRTUAL_ENV=/opt/venv
ENV PATH=${VIRTUAL_ENV}/bin:$PATH
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv ${VIRTUAL_ENV}
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements.txt

FROM python:3.12-slim-bookworm

ARG INSTALL_PLAYWRIGHT_BROWSER=0

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV VIRTUAL_ENV=/opt/venv
ENV PATH=${VIRTUAL_ENV}/bin:$PATH
ENV HOME=/home/appuser
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --home-dir /home/appuser --shell /bin/bash appuser

COPY --from=builder /opt/venv /opt/venv

RUN mkdir -p /ms-playwright \
    && if [ "${INSTALL_PLAYWRIGHT_BROWSER}" = "1" ]; then \
         python -m playwright install --with-deps chromium; \
       else \
         echo "Skipping Playwright browser install in this image build"; \
       fi

COPY . .
RUN chmod +x start.sh scripts/auto_sync_pos_bridge.sh \
    && chown -R appuser:appuser /app /home/appuser /opt/venv /ms-playwright

EXPOSE 8080
USER appuser
CMD ["./start.sh"]
