#!/bin/sh
# Applies every migrations/NNN_*.sql not yet recorded in schema_migrations,
# in filename order, each inside its own transaction.
set -eu

psql -v ON_ERROR_STOP=1 -q -c \
  "CREATE TABLE IF NOT EXISTS schema_migrations (
     version    text PRIMARY KEY,
     applied_at timestamptz NOT NULL DEFAULT now())"

for file in /migrations/[0-9]*.sql; do
  version=$(basename "$file" .sql)
  done=$(psql -tA -c "SELECT 1 FROM schema_migrations WHERE version = '$version'")
  if [ -z "$done" ]; then
    echo "applying $version"
    psql -v ON_ERROR_STOP=1 -q --single-transaction \
      -f "$file" \
      -c "INSERT INTO schema_migrations (version) VALUES ('$version')"
  else
    echo "skipping $version (already applied)"
  fi
done
