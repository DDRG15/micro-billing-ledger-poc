# Dockerfile — Micro-Billing-Ledger PoC
# =======================================
# EN: Multi-stage build. Stage 1 (builder) compiles C-extension wheels.
#     Stage 2 (runtime) copies only the installed packages — no compiler in prod.
#     Result: a lean Alpine image with no build tools exposed in production.
#     Non-root user (billing) for defence-in-depth — process can't write outside /app and /data.
#
# ES: Build de múltiples etapas. Etapa 1 (builder) compila wheels de extensión C.
#     Etapa 2 (runtime) copia solo los paquetes instalados — sin compilador en prod.
#     Resultado: una imagen Alpine liviana sin herramientas de build expuestas en producción.
#     Usuario no-root (billing) para defensa en profundidad — el proceso no puede escribir fuera de /app y /data.

# ── Stage 1: Builder ────────────────────────────────────────────────────────
# EN: Uses Python 3.12 Alpine as the base. Alpine keeps the image small (~50MB base).
#     AS builder names this stage so the runtime stage can COPY --from=builder.
# ES: Usa Python 3.12 Alpine como base. Alpine mantiene la imagen pequeña (~50MB base).
#     AS builder nombra esta etapa para que la etapa runtime pueda hacer COPY --from=builder.
FROM python:3.12-alpine AS builder

WORKDIR /build

# EN: gcc and musl-dev are C compiler toolchain dependencies required to build
#     C-extension wheels (e.g., uvloop, which uvicorn[standard] pulls in).
#     --no-cache skips the Alpine package cache — keeps layer size minimal.
#     These are only in the builder stage — they never ship in the final image.
# ES: gcc y musl-dev son dependencias del toolchain del compilador C requeridas para
#     construir wheels de extensión C (ej., uvloop, que uvicorn[standard] trae).
#     --no-cache omite el caché de paquetes Alpine — mantiene el tamaño de capa mínimo.
#     Estos solo están en la etapa builder — nunca se envían en la imagen final.
RUN apk add --no-cache gcc musl-dev

# EN: Copy requirements first (before source code) so Docker can cache the pip
#     install layer. If only ledger.py changes, Docker reuses this cached layer
#     and skips the pip install — faster rebuilds.
# ES: Copiar requirements primero (antes del código fuente) para que Docker pueda
#     cachear la capa de instalación pip. Si solo cambia ledger.py, Docker reutiliza
#     esta capa cacheada y omite el pip install — rebuilds más rápidos.
COPY requirements.txt .

# EN: --prefix=/install puts packages in /install instead of the system Python dirs.
#     The runtime stage then COPY --from=builder /install /usr/local to get just
#     the packages without the compiler. --no-cache-dir keeps the layer lean.
# ES: --prefix=/install pone los paquetes en /install en lugar de los directorios
#     Python del sistema. La etapa runtime luego hace COPY --from=builder /install /usr/local
#     para obtener solo los paquetes sin el compilador. --no-cache-dir mantiene la capa liviana.
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: Runtime ────────────────────────────────────────────────────────
# EN: Starts fresh from the same Alpine base — no builder artifacts (compiler, headers)
#     are present. This is the image that actually runs in production.
# ES: Empieza de cero desde la misma base Alpine — sin artefactos del builder (compilador, headers)
#     presentes. Esta es la imagen que realmente corre en producción.
FROM python:3.12-alpine AS runtime

# EN: Creates a dedicated non-root user and group named "billing".
#     Running as root in a container is a security risk — if the container escapes,
#     the attacker has root on the host. The billing user can only write to /app and /data.
# ES: Crea un usuario y grupo dedicado no-root llamado "billing".
#     Ejecutar como root en un contenedor es un riesgo de seguridad — si el contenedor
#     escapa, el atacante tiene root en el host. El usuario billing solo puede escribir en /app y /data.
RUN addgroup -S billing && adduser -S billing -G billing

WORKDIR /app

# EN: Copy only the compiled packages from the builder stage.
#     This is the multi-stage magic: we get uvloop (C-extension) without the C compiler.
# ES: Copiar solo los paquetes compilados de la etapa builder.
#     Esta es la magia del multi-stage: obtenemos uvloop (extensión C) sin el compilador C.
COPY --from=builder /install /usr/local

# EN: Copy only the application source — not requirements.txt, not .git, not tests.
#     In production, ledger.py is the only file that needs to be in the container.
# ES: Copiar solo el código fuente de la aplicación — no requirements.txt, no .git, no tests.
#     En producción, ledger.py es el único archivo que necesita estar en el contenedor.
COPY ledger.py .

# EN: DATABASE_URL tells ledger.py where the PostgreSQL instance lives.
#     Override at runtime: docker run -e DATABASE_URL=postgresql://user:pass@host:5432/db ...
#     In production, point this at a managed PostgreSQL service (RDS, Cloud SQL, Supabase, etc.).
#     The default here connects to a postgres container in the same Docker network.
#     Never hardcode credentials — pass DATABASE_URL as an environment variable at deploy time.
# ES: DATABASE_URL le dice a ledger.py dónde vive la instancia PostgreSQL.
#     Sobreescribir en tiempo de ejecución: docker run -e DATABASE_URL=postgresql://user:pass@host:5432/db ...
#     En producción, apuntar esto a un servicio PostgreSQL administrado (RDS, Cloud SQL, Supabase, etc.).
#     El default aquí se conecta a un contenedor postgres en la misma red Docker.
#     Nunca hardcodear credenciales — pasar DATABASE_URL como variable de entorno al desplegar.
ENV DATABASE_URL=postgresql://postgres:postgres@postgres:5432/billing

# EN: Switch to the non-root billing user. All subsequent commands (including CMD)
#     run as billing, not root. Defence in depth.
# ES: Cambiar al usuario billing no-root. Todos los comandos subsiguientes (incluyendo CMD)
#     corren como billing, no root. Defensa en profundidad.
USER billing

# EN: Expose port 8000 — documentation only. Docker doesn't actually bind this port
#     without -p 8000:8000 in the docker run command or a ports: mapping in compose.
# ES: Exponer el puerto 8000 — solo documentación. Docker no vincula realmente este puerto
#     sin -p 8000:8000 en el comando docker run o un mapeo ports: en compose.
EXPOSE 8000

# EN: Start the FastAPI application with uvicorn.
#     --workers 1: single worker because SQLite is a single-writer engine.
#       With PostgreSQL, increase workers to match CPU core count.
#     --no-access-log: reduces log noise; structured application logs handle observability.
#     See BLUEPRINT_ANALYSIS.md §1 for the PostgreSQL migration path (enables multi-worker).
# ES: Iniciar la aplicación FastAPI con uvicorn.
#     --workers 1: worker único porque SQLite es un motor de escritura única.
#       Con PostgreSQL, aumentar workers para coincidir con el conteo de núcleos CPU.
#     --no-access-log: reduce el ruido de logs; los logs estructurados de la aplicación manejan la observabilidad.
#     Ver BLUEPRINT_ANALYSIS.md §1 para la ruta de migración a PostgreSQL (habilita multi-worker).
CMD ["python", "-m", "uvicorn", "ledger:app", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "1", "--no-access-log"]
