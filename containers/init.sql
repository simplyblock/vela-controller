CREATE USER keycloak WITH PASSWORD 'keycloak';
CREATE DATABASE keycloak OWNER keycloak;
GRANT ALL PRIVILEGES ON DATABASE keycloak TO keycloak;
ALTER ROLE keycloak SET search_path TO keycloak,public;

CREATE USER logflare WITH PASSWORD 'logflare';
CREATE DATABASE logflare OWNER logflare;
GRANT ALL PRIVILEGES ON DATABASE logflare TO logflare;
ALTER ROLE logflare SET search_path TO logflare,public;

\c logflare
CREATE SCHEMA IF NOT EXISTS _analytics;
ALTER SCHEMA _analytics OWNER TO :logflare;
