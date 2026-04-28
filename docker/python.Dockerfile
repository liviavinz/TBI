FROM python:3.12-slim AS base

ARG USE_PROXY
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# export proxy vars so pip and others can see them
ENV HTTP_PROXY=${HTTP_PROXY}
ENV HTTPS_PROXY=${HTTPS_PROXY}
ENV NO_PROXY=${NO_PROXY}

WORKDIR /app

# tini, ca-certificates, curl, ODBC driver dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        tini ca-certificates curl gnupg2 \
        unixodbc unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["/usr/bin/tini","-g","--"]

# copy CA bundle and register it
COPY certs/usz-bundle.crt /usr/local/share/ca-certificates/usz-bundle.crt
RUN update-ca-certificates

# install Microsoft ODBC driver for SQL Server (needed by pyodbc)
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list | \
       sed 's|https://|https://|' > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# pip config, add proxy only if USE_PROXY=true
RUN set -eux; \
    printf "[global]\ntrusted-host = pypi.org\n    files.pythonhosted.org\n" > /etc/pip.conf; \
    if [ "$USE_PROXY" = "true" ]; then \
        printf "proxy = %s\n" "$HTTP_PROXY" >> /etc/pip.conf; \
        echo "pip will use proxy"; \
    else \
        echo "pip without proxy"; \
    fi

# install Python dependencies (shared between all targets)
COPY requirements.txt ./
RUN python -m pip install --upgrade pip && pip install -r requirements.txt

# create common dirs
RUN mkdir -p /data /logs

# copy application code
COPY *.py ./

# ---------- Service: dashboard ----------
FROM base AS dashboard

EXPOSE 8051
CMD ["python", "main_tbi.py"]

# ---------- Service: sync (with cron) ----------
FROM base AS sync

# install cron
RUN apt-get update && apt-get install -y --no-install-recommends cron \
    && rm -rf /var/lib/apt/lists/*

# create cron job: every 5 minutes
RUN echo "*/5 * * * * cd /app && /usr/local/bin/python sync_tbi.py >> /logs/sync.log 2>&1" > /etc/cron.d/tbi-sync \
    && chmod 0644 /etc/cron.d/tbi-sync \
    && crontab /etc/cron.d/tbi-sync

# run cron in foreground
CMD ["sh", "-c", "cron && tail -f /logs/sync.log"]

# ---------- Service: master-setup (one-shot) ----------
FROM base AS setup
CMD ["python", "sqlite_masterdata_setup_tbi.py"]