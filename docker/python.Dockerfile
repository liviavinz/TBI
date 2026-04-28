FROM python:3.12-slim AS base

ARG USE_PROXY
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HTTP_PROXY=${HTTP_PROXY} \
    HTTPS_PROXY=${HTTPS_PROXY} \
    NO_PROXY=${NO_PROXY}

WORKDIR /app

# Base packages + ODBC driver dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        tini ca-certificates curl gnupg2 \
        unixodbc unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["/usr/bin/tini","-g","--"]

# USZ certificate
COPY certs/usz-bundle.crt /usr/local/share/ca-certificates/usz-bundle.crt
RUN update-ca-certificates

# Microsoft ODBC driver for SQL Server
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Pip config (with optional proxy)
RUN set -eux; \
    printf "[global]\ntrusted-host = pypi.org\n    files.pythonhosted.org\n" > /etc/pip.conf; \
    if [ "$USE_PROXY" = "true" ]; then \
        printf "proxy = %s\n" "$HTTP_PROXY" >> /etc/pip.conf; \
    fi

COPY requirements.txt ./
RUN python -m pip install --upgrade pip && pip install -r requirements.txt

RUN mkdir -p /data /logs

# Copy all Python files
COPY *.py ./



# ---------- Service 1: dashboard ----------
FROM base AS dashboard

EXPOSE 8051
CMD ["python", "dashboard_tbi.py"]

# ---------- Service 2: sync (with cron) ----------
FROM base AS sync

RUN apt-get update && apt-get install -y --no-install-recommends cron \
    && rm -rf /var/lib/apt/lists/*

# create cron job: every 15 minutes
RUN echo "*/15 * * * * cd /app && /usr/local/bin/python sync_tbi.py >> /logs/sync.log 2>&1" > /etc/cron.d/tbi-sync \
    && chmod 0644 /etc/cron.d/tbi-sync \
    && crontab /etc/cron.d/tbi-sync

CMD ["sh", "-c", "printenv | grep -E '^(DB_|SQLITE_|HTTP_|HTTPS_|NO_)' >> /etc/environment && cron && touch /logs/sync.log && tail -f /logs/sync.log"]


# ───────── Service 3: setup (one-shot) ─────────
FROM base AS setup
CMD ["python", "sqlite_masterdata_setup_tbi.py"]