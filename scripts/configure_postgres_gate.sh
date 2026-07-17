#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <postgres-container>" >&2
  exit 2
fi

container=$1

# The dispatch outbox intentionally performs durable per-recipient state
# transitions. PostgreSQL's 1GB default WAL window checkpoints continuously
# under the 100-recipient G7 workload and turns normal row updates into a
# full-page-image feedback loop. These are deployment settings, not relaxed
# load-test assertions.
docker exec -i "$container" psql -v ON_ERROR_STOP=1 -U privchat -d postgres <<'SQL'
ALTER SYSTEM SET max_wal_size = '16GB';
ALTER SYSTEM SET min_wal_size = '2GB';
ALTER SYSTEM SET checkpoint_timeout = '15min';
ALTER SYSTEM SET checkpoint_completion_target = '0.9';
ALTER SYSTEM SET wal_compression = 'lz4';
ALTER SYSTEM SET wal_buffers = '64MB';
ALTER SYSTEM SET shared_buffers = '512MB';
SELECT pg_reload_conf();
SQL

docker exec "$container" psql -v ON_ERROR_STOP=1 -U privchat -d postgres -Atc \
  "SELECT name || '=' || setting || COALESCE(unit, '')
   FROM pg_settings
   WHERE name IN ('max_wal_size', 'min_wal_size', 'checkpoint_timeout',
                  'checkpoint_completion_target', 'wal_compression',
                  'wal_buffers', 'shared_buffers')
   ORDER BY name;"
