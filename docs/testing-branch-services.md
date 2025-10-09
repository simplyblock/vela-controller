### Intro

When a new branch is created, we provision PGMeta, PostgREST, PGRealtime, PGStorage, and PostgreSQL. This document outlines how each service has been validated and doubles as a quick-start reference for everyday use.

### Authentication

This section summarises the authentication model for each service. Kong-level authentication is still pending.

Every branch service requires an API token. Retrieve tokens for the Rest, Storage, and Realtime services from `/branch/<branch-id>/apikeys`, then supply the token as a bearer token in the `Authorization` header.

The API keys endpoint returns two keys: `anonKey` (read-only access) and `serviceRole` (elevated access with write permissions). Select the least-privileged key that satisfies your needs.

PGMeta does not require an `Authorization` header, but it does require the `x-connection-encrypted` header. The encrypted connection string is available at `/branch/<branch-id>` under the `encrypted_connection_string` field.

### PostgREST

This environment hosts https://github.com/PostgREST/postgrest. The official documentation is available at https://docs.postgrest.org/en/v13/tutorials/tut0.html.

PostgREST connects to the `postgres` database and exposes the `public`, `storage`, and `graphql_public` schemas.

Read data from the `todos` table (ensure the table exists):

```sh
curl -X GET "https://01k612e965yy5dy4vh27dtjt8r.staging.vela.run/pgrest/todos" \
  -H "Authorization: Bearer $TOKEN"
```

Insert a row into the `todos` table:

```sh
export TOKEN=anonKey
curl -X POST "https://01k612e965yy5dy4vh27dtjt8r.staging.vela.run/pgrest/todos" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Prefer: return=representation" \
  -d '{"task": "Write docs"}'
```

##### Switching schemas
TODO

### PGMeta

This deployment hosts https://github.com/supabase/postgres-meta. The OpenAPI specification is available at https://supabase.github.io/postgres-meta/.

PGMeta connects to the `postgres` database and the `public` schema.

List all schemas:

```sh
export ENCRYPTED_CONNECTION_STRING='U2FsdGVkX19oQDMfZ/1CLHjEU1T4T4p34tUYlIAgLkZ0KoHrW7c23FFygmZ/XjQL5FFT/1k/UaVl0rQjl09X5wt1Q9E/+Vt29p8J7Y1lHKY='
curl -X GET https://01k6mpdwnay4jf91j9pd916ngf.staging.vela.run/meta/schemas \
  -H "x-connection-encrypted: $ENCRYPTED_CONNECTION_STRING"
```

List all tables across schemas:

```sh
curl -X GET https://01k6mpdwnay4jf91j9pd916ngf.staging.vela.run/meta/tables \
  -H "x-connection-encrypted: $ENCRYPTED_CONNECTION_STRING"
```

### PGRealtime

This service hosts https://github.com/supabase/realtime and streams Postgres changes over WebSockets.

Opening a WebSocket session currently returns HTTP 200, indicating the upgrade path is not yet configured:

```sh
wscat -c wss://01k6mpdwnay4jf91j9pd916ngf.staging.vela.run/realtime
# error: Unexpected server response: 200
```

##### Listening for table events
TODO

### Storage API

This service hosts https://github.com/supabase/storage. Export the token fetched from the `/apikeys` endpoint into the `TOKEN` environment variable before running the commands below.

Create a bucket:

```sh
curl -X POST 'https://01k6mpdwnay4jf91j9pd916ngf.staging.vela.run/storage/bucket' \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "name": "my_bucket",
        "public": false
      }'
```

Upload a file to a bucket:

```sh
curl -X POST "https://01k6mpdwnay4jf91j9pd916ngf.staging.vela.run/storage/object/my_bucket/asdf.txt" \
  -H "Authorization: Bearer $TOKEN" \
  --data-binary @asdf.txt
```

List objects:

```sh
curl -X POST \
  "https://01k6mpdwnay4jf91j9pd916ngf.staging.vela.run/storage/object/list/my_bucket" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"prefix": ""}' | jq .
```

Retrieve the file and store it locally:

```sh
curl -X GET "https://01k6mpdwnay4jf91j9pd916ngf.staging.vela.run/storage/object/my_bucket/asdf.txt" \
  -H "Authorization: Bearer $TOKEN" -o asdf-out.txt
```

Verify the checksum of the original and downloaded files:

```sh
sha256sum asdf.txt asdf-out.txt
```

### Debugging

We use Alpine as the base image. The build recipe is available at http://github.com/simplyblock/image-tools/.

To access the KubeVirt VM console:

```
kubectl virt -n <namespace> console supabase-supabase-db
```
