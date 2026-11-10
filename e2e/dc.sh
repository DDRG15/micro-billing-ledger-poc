#!/bin/sh
# docker compose for the harness: separate project name, so it never touches a normal
#     `docker compose up` of this repo, plus the downstream receiver and e2e/e2e.env.
cd "$(dirname "$0")/.." && exec docker compose -p billing-e2e \
    -f docker-compose.yml -f e2e/compose.override.yml --env-file e2e/e2e.env "$@"
