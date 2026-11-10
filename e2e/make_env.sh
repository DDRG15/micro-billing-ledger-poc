#!/bin/sh
# Create e2e/e2e.env with fresh random secrets (never the published demo values).
#     The file is git-ignored. Re-running it overwrites the previous secrets; recreate the
#     stack afterwards (./e2e/dc.sh down -v && ./e2e/dc.sh up -d --build --wait).
set -eu
cd "$(dirname "$0")"
rand() { od -An -N24 -tx1 /dev/urandom | tr -d ' \n'; }
PGPW=$(rand); APPPW=$(rand)
umask 077
cat > e2e.env <<ENV
POSTGRES_USER=postgres
POSTGRES_PASSWORD=$PGPW
POSTGRES_DB=billing
DATABASE_URL=postgresql://postgres:$PGPW@postgres:5432/billing
APP_DATABASE_URL=postgresql://billing_app:$APPPW@postgres:5432/billing
STRIPE_WEBHOOK_SECRET=whsec_e2e$(rand)
BILLING_API_KEY=op_$(rand)
DOWNSTREAM_URL=http://downstream:9000/ingest
WORKER_LOG_ONLY=false
WORKER_POLL_INTERVAL=1
BILLING_ENV=production
INGRESS_BIND_ADDR=127.0.0.1
CADDY_DOMAIN=localhost
LOG_FORMAT=json
ENV
mkdir -p recv && chmod 777 recv && echo ok > recv/mode
echo "wrote $(pwd)/e2e.env"
