# Dockerfile — Micro-Billing-Ledger PoC
# =======================================
# Multi-stage build. Stage 1 (builder) compiles C-extension wheels.
#     Stage 2 (runtime) copies only the installed packages — no compiler in prod.
#     Result: a lean Alpine image with no build tools exposed in production.
#     Non-root user (billing) for defence-in-depth — process can't write outside /app and /data.
#

# ── Stage 1: Builder ────────────────────────────────────────────────────────
# Uses Python 3.12 Alpine as the base. Alpine keeps the image small (~50MB base).
#     AS builder names this stage so the runtime stage can COPY --from=builder.
FROM python:3.12-alpine AS builder

WORKDIR /build

# gcc and musl-dev are C compiler toolchain dependencies required to build
#     C-extension wheels (e.g., uvloop, which uvicorn[standard] pulls in).
#     --no-cache skips the Alpine package cache — keeps layer size minimal.
#     These are only in the builder stage — they never ship in the final image.
RUN apk add --no-cache gcc musl-dev

# Copy requirements first (before source code) so Docker can cache the pip
#     install layer. If only ledger.py changes, Docker reuses this cached layer
#     and skips the pip install — faster rebuilds.
COPY requirements.txt .

# --prefix=/install puts packages in /install instead of the system Python dirs.
#     The runtime stage then COPY --from=builder /install /usr/local to get just
#     the packages without the compiler. --no-cache-dir keeps the layer lean.
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: Runtime ────────────────────────────────────────────────────────
# Starts fresh from the same Alpine base — no builder artifacts (compiler, headers)
#     are present. This is the image that actually runs in production.
FROM python:3.12-alpine AS runtime

# Creates a dedicated non-root user and group named "billing".
#     Running as root in a container is a security risk — if the container escapes,
#     the attacker has root on the host. The billing user can only write to /app and /data.
RUN addgroup -S billing && adduser -S billing -G billing

WORKDIR /app

# Copy only the compiled packages from the builder stage.
#     This is the multi-stage magic: we get uvloop (C-extension) without the C compiler.
COPY --from=builder /install /usr/local

# Copy only the application source — not requirements.txt, not .git, not tests.
#     In production, ledger.py is the only file that needs to be in the container.
COPY ledger.py .

# Grant ownership of /app to the billing user BEFORE switching to it.
#     WORKDIR /app was created by root, so without this chown the billing user cannot
#     write billing_ledger.log — the app crashes on startup with a PermissionError.
RUN chown -R billing:billing /app

# DATABASE_URL tells ledger.py where the PostgreSQL instance lives.
#     Override at runtime: docker run -e DATABASE_URL=postgresql://user:pass@host:5432/db ...
#     In production, point this at a managed PostgreSQL service (RDS, Cloud SQL, Supabase, etc.).
#     The default here connects to a postgres container in the same Docker network.
#     Never hardcode credentials — pass DATABASE_URL as an environment variable at deploy time.
ENV DATABASE_URL=postgresql://postgres:postgres@postgres:5432/billing

# Switch to the non-root billing user. All subsequent commands (including CMD)
#     run as billing, not root. Defence in depth.
USER billing

# Expose port 8000 — documentation only. Docker doesn't actually bind this port
#     without -p 8000:8000 in the docker run command or a ports: mapping in compose.
EXPOSE 8000

# Start the FastAPI application with uvicorn.
#     --workers 1: single process shares the ThreadedConnectionPool (maxconn=20).
#       For higher concurrency, switch to Gunicorn + uvicorn workers — each worker
#       gets its own pool, so connections = workers × maxconn.
#     --no-access-log: reduces log noise; structured application logs handle observability.
CMD ["python", "-m", "uvicorn", "ledger:app", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "1", "--no-access-log"]
