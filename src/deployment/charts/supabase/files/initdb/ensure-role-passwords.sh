#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${POSTGRES_PASSWORD:-}" ]]; then
  echo "POSTGRES_PASSWORD is not set; skipping role password synchronization." >&2
  exit 0
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "psql not found; cannot update role passwords." >&2
  exit 1
fi

# Use base64 so we can safely round-trip any character without relying on shell quoting.
password_b64=$(printf '%s' "$POSTGRES_PASSWORD" | base64 | tr -d '\n')

psql --username postgres --dbname postgres <<SQL
DO \$vela$
DECLARE
  roles text[] := ARRAY['postgres', 'supabase_admin', 'authenticator', 'supabase_storage_admin'];
  role_name text;
  target_password text := convert_from(decode('${password_b64}', 'base64'), 'utf8');
BEGIN
  FOREACH role_name IN ARRAY roles LOOP
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = role_name) THEN
      EXECUTE format('ALTER ROLE %I WITH PASSWORD %L', role_name, target_password);
    END IF;
  END LOOP;
END;
\$vela$;
SQL
