CREATE USER vela_controller WITH PASSWORD 'controller';
CREATE DATABASE controller OWNER vela_controller;
GRANT ALL PRIVILEGES ON DATABASE controller TO vela_controller;

CREATE USER vela_celery WITH PASSWORD 'celery';
GRANT CONNECT ON DATABASE controller TO vela_celery;

\c controller
CREATE SCHEMA IF NOT EXISTS celery AUTHORIZATION vela_celery;
ALTER ROLE vela_celery IN DATABASE controller SET search_path TO celery;

\c postgres

CREATE USER keycloak WITH PASSWORD 'keycloak';
CREATE DATABASE keycloak OWNER keycloak;
GRANT ALL PRIVILEGES ON DATABASE keycloak TO keycloak;
ALTER ROLE keycloak SET search_path TO keycloak,public;

CREATE USER logflare WITH PASSWORD 'logflare';
CREATE DATABASE logflare OWNER logflare;
GRANT ALL PRIVILEGES ON DATABASE logflare TO logflare;
ALTER ROLE logflare SET search_path TO logflare,public;

\c logflare
CREATE SCHEMA IF NOT EXISTS _analytics AUTHORIZATION logflare;
