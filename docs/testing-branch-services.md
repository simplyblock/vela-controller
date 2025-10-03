### Intro

When a branch is created, we setup PGMeta, PGRest, PGRealtime, PGStorage and PostgreSQL.
This guide talks about how each of the above service has been tested and could also act as a guide about how these can be used. 

### PG Rest

This is a hosted version of https://github.com/PostgREST/postgrest and the offical documentation can be found here https://docs.postgrest.org/en/v13/tutorials/tut0.html

The PG rest is connected to the `postgres` database and `public`,`storage`,`graphql_public` schemas.
At the moment there is no authentication. But we'll it very soon. 


To get information from `todos` table (make sure that this table exists). The below request can be used.
```sh
curl -X POST "https://01k612e965yy5dy4vh27dtjt8r.staging.vela.run/pgrest/todos"
```

To write info from `todos` table (make sure that this table exists). The below request can be used. 
```sh
curl -X POST "https://01k612e965yy5dy4vh27dtjt8r.staging.vela.run/pgrest/todos" \
  -H "Content-Type: application/json" \
  -H "Prefer: return=representation" \
  -d '{"task": "Write docs"}'
```

##### how to switch between schemas
TODO

### PG Meta

This is a hosted version of https://github.com/supabase/postgres-meta and the OpenAPI spec can be found at: https://supabase.github.io/postgres-meta/


The PG rest is connected to the `postgres` database and `public` schema. 
At the moment there is no authentication. But we'll it very soon. 

To list all the schemas
```sh
curl -X GET https://01k6mpdwnay4jf91j9pd916ngf.staging.vela.run/meta/schemas
```

To list all the tables across schemas
```sh
curl -X GET https://01k6mpdwnay4jf91j9pd916ngf.staging.vela.run/meta/tables
```

### PG Realtime

This is a hosted version of https://github.com/supabase/realtime that broadcast Postgres Changes via WebSockets

at the moment socket connect is establed
```sh
wscat -c wss://01k6mpdwnay4jf91j9pd916ngf.staging.vela.run/realtime
error: Unexpected server response: 200
>
```
#### how to listen for events changes on a particular table
TODO

### Storage API

This is a hosted version of https://github.com/supabase/storage

We don't generate tokens yet. So try out this service, a hardcoded token can be used. 

```
export TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyAgCiAgICAicm9sZSI6ICJzZXJ2aWNlX3JvbGUiLAogICAgImlzcyI6ICJzdXBhYmFzZS1kZW1vIiwKICAgICJpYXQiOiAxNjQxNzY5MjAwLAogICAgImV4cCI6IDE3OTk1MzU2MDAKfQ.DaYlNEoUrrEn2Ig7tqibS-PHK5vgusbcbo7X36XVt4Q
```

* Create a bucket
```sh
ccurl -X POST 'https://01k6mpdwnay4jf91j9pd916ngf.staging.vela.run/storage/bucket' \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "name": "my_bucket",
        "public": false
      }'
```

* upload a file to a bucket
```sh
curl -X POST "https://01k6mpdwnay4jf91j9pd916ngf.staging.vela.run/storage/object/my_bucket/asdf.txt" \
  -H "Authorization: Bearer $TOKEN" \
  --data-binary @asdf.txt
```

* list objects
```sh
ccurl -X POST \
  "https://01k6mpdwnay4jf91j9pd916ngf.staging.vela.run/storage/object/list/my_bucket" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"prefix": ""}' | jq .
```

* retrive the file and store it. 
```sh
curl -X GET "https://01k6mpdwnay4jf91j9pd916ngf.staging.vela.run/storage/object/my_bucket/asdf.txt" \
  -H "Authorization: Bearer $TOKEN" -o asdf-out.txt
```

* checksum between the 2 files
```sh
sha256sum asdf.txt asdf-out.txt 
```


### debugging

we use alpine as the base image and recipe to create the image can be found [here](http://github.com/simplyblock/image-tools/)


To Kubevirt VM to connect to the server.
```
kubectl virt -n <namespace> console supabase-supabase-db
```
