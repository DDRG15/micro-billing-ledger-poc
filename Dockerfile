# ── Build stage ───────────────────────────────────────────────────────────────
FROM python:3.12-alpine AS builder

WORKDIR /build

# gcc + musl-dev needed to compile any C-extension wheels (e.g. uvloop).
# Installed only in the builder stage — not copied to the final image.
RUN apk add --no-cache gcc musl-dev

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.12-alpine AS runtime

# Non-root user — defence in depth.
RUN addgroup -S billing && adduser -S billing -G billing

WORKDIR /app

# Copy only the installed packages from the builder — no compiler in runtime.
COPY --from=builder /install /usr/local
COPY ledger.py .

# SQLite WAL files land in /data so we can mount a volume there.
RUN mkdir -p /data && chown billing:billing /data
ENV BILLING_DB_PATH=/data/billing_ledger.db

USER billing

EXPOSE 8000

# Granian is faster than uvicorn for sync-heavy workloads (PostHog uses it too).
# Falls back to uvicorn if granian is not in requirements.
CMD ["python", "-m", "uvicorn", "ledger:app", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "1", "--no-access-log"]
