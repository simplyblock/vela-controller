### Introduction

explain me how Roles, Users, are organised in Supabase ecosystem. When supabase db is created, which roles, users, tables, schemas are created and how each of the services (PGMeta, PGRest, PGStorage, PGRealtime) connect to DB. What credentials (username, password) does it use? Do a deep research share a report. Take your time and produce quality. 


PostgREST/Storage/Realtime switch the active DB role based on a role claim in the JWT you send.

PostgREST uses a special login role (authenticator) to connect,

Typical roles you’ll see:
`anon`, `authenticated`, `service_role`, 
plus platform/service roles like:
`authenticator`, --> PGREST
`supabase_auth_admin`,
`supabase_storage_admin`,
`supabase_realtime_admin`,
`dashboard_user`,
`supabase_admin`


## Roles (database roles)
In Postgres, roles and users are the same underlying object. Supabase provisions a set of DB roles.

which of these are LOGIN roles and which are non-LOGIN roles?

# Postgres Roles

| Role name                  | Attributes                                                 | Member of                                                                                                                                                                         |
| -------------------------- | ---------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| anon                       | Cannot login                                               | {}                                                                                                                                                                                |
| authenticated              | Cannot login                                               | {}                                                                                                                                                                                |
| authenticator              | No inheritance                                             | {authenticated, anon, service_role}                                                                                                                                               |
| dashboard_user             | Create role, Create DB, Cannot login, Replication          | {}                                                                                                                                                                                |
| manohar_user               | Superuser, Create role, Create DB                          | {}                                                                                                                                                                                |
| pgbouncer                  |                                                            | {}                                                                                                                                                                                |
| pgsodium_keyholder         | Cannot login                                               | {pgsodium_keyiduser}                                                                                                                                                              |
| pgsodium_keyiduser         | Cannot login                                               | {}                                                                                                                                                                                |
| pgsodium_keymaker          | Cannot login                                               | {pgsodium_keyiduser, pgsodium_keyholder}                                                                                                                                          |
| postgres                   | Create role, Create DB, Replication, Bypass RLS            | {pg_monitor, supabase_auth_admin, supabase_functions_admin, supabase_storage_admin, pgsodium_keyiduser, pgsodium_keyholder, pgsodium_keymaker, authenticated, anon, service_role} |
| service_role               | Cannot login, Bypass RLS                                   | {pgsodium_keyholder}                                                                                                                                                              |
| supabase_admin             | Superuser, Create role, Create DB, Replication, Bypass RLS | {}                                                                                                                                                                                |
| supabase_auth_admin        | No inheritance, Create role                                | {}                                                                                                                                                                                |
| supabase_functions_admin   | No inheritance, Create role                                | {}                                                                                                                                                                                |
| supabase_read_only_user    | Bypass RLS                                                 | {pg_read_all_data}                                                                                                                                                                |
| supabase_replication_admin | Replication                                                | {}                                                                                                                                                                                |
| supabase_storage_admin     | No inheritance, Create role                                | {authenticator}                                                                                                                                                                   |


1. `postgres`: admin role.
2. `authenticator`: PostgREST’s connection role; very limited, used only to verify JWTs then SET ROLE to another.
3. `anon`: unauthenticated web access.
4. `authenticated`: logged-in users.
5. `service_role`: elevated/bypass RLS via server-side tokens—do not expose to browsers.
6. `supabase_auth_admin`: Auth service role (owns/works inside auth schema).
7. `supabase_storage_admin`: Storage service role (works inside storage schema).
8. `supabase_realtime_admin`: Realtime service role with replication-level privileges
9. `dashboard_user`: used by Studio
10. `supabase_admin`:  Internal management/migrations.


# Postgres Login Roles (Users)

| Role name    | Attributes                                      | Member of                                                                                                                                                                         |
| ------------ | ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| manohar_user | Superuser, Create role, Create DB               | {}                                                                                                                                                                                |
| postgres     | Create role, Create DB, Replication, Bypass RLS | {pg_monitor, supabase_auth_admin, supabase_functions_admin, supabase_storage_admin, pgsodium_keyiduser, pgsodium_keyholder, pgsodium_keymaker, authenticated, anon, service_role} |

### How each Supabase service connects to Postgres (and with what)

### PostgREST
Connects as: `authenticator` (LOGIN), using a DB URI and its password stored server-side.
Then: validates the JWT and SET ROLE to anon/authenticated or service_role for the request. That’s why your RLS runs under those roles. 

Relevant config: PGRST_DB_ANON_ROLE (maps anonymous requests), DB URI credentials, and JWT secret if you self-host. (That secret is for verifying/signing depending on setup.) 

### Storage API
Connects as: supabase_storage_admin for migrations/maintenance on storage.*. Request-time authorization still boils down to JWT → role (anon/authenticated/service_role) and RLS on storage.objects.
Tables: at minimum storage.buckets, storage.objects, and s3-multipart helper tables. Supabase explicitly advises treating storage schema as read-only outside the API. 

### Realtime
Connects as: a role with REPLICATION privileges (Supabase uses supabase_realtime_admin) to read WAL via logical replication/publications. Clients subscribe over websockets; authorization again uses JWT → role for what they’re allowed to listen to. 
Schema: uses realtime schema objects (e.g., realtime.messages) for broadcast/presence plumbing; you don’t remodel this schema. 

# PGMeta (a.k.a. Postgres Meta; used by Studio)

Connects as: a DB user via connection vars; defaults show postgres/postgres in the open-source repo. In Supabase Cloud, it runs behind the Studio and other platform roles like dashboard_user/supabase_admin. It’s for introspection/admin, not for your app path.

# Supabase roles & service identities (clean reference)

> **TL;DR**
>
> * App users **never** connect as the database superuser `postgres`. Frontend/API calls are mapped to **Postgres roles** via JWT claims and PostgREST: unauthenticated → `anon`, authenticated → `authenticated`, privileged server calls → `service_role`.
> * Supabase services use **dedicated login roles** to connect to Postgres (e.g., `authenticator` for PostgREST, `supabase_storage_admin` for Storage, `supabase_realtime_admin` for Realtime). These roles then rely on RLS/policies and `SET ROLE` behavior.
> * Changing the **`postgres` password** only affects connections that actually use the `postgres` role (your direct psql/ORM sessions, or PGMeta if you configured it that way). It does **not** break PostgREST/Realtime/Storage, which don’t authenticate as `postgres` by default.

---

## Mental model: JWT → role → RLS

1. Clients authenticate with Supabase **Auth**. The token they get includes a `role` claim.
2. **PostgREST** connects to the DB as a fixed login role `authenticator` (LOGIN, NOINHERIT), validates the JWT, and then **`SET LOCAL ROLE`** into the role named in the token (commonly `anon` or `authenticated`; server-side calls can use `service_role`).
3. Your **Row Level Security (RLS)** policies enforce data access for the current role.

This is standard PostgREST behavior and is the core reason you should think in terms of RLS policies rather than trusting SDKs.

---

## Default roles you’ll encounter

> Roles and users are the same underlying object in Postgres. Some roles can LOGIN; others are grouping roles (NOLOGIN) that PostgREST can switch into.

| Role | LOGIN? | Purpose (high level) |
|---|---|---|
| `postgres` | **LOGIN** (superuser) | Cluster owner/superuser. Not used by the data APIs. Avoid using it from apps.
| `authenticator` | **LOGIN NOINHERIT** | The single connection role used by PostgREST; becomes other roles per request.
| `anon` | **NOLOGIN** | Anonymous/unauthenticated API access; limited by RLS.
| `authenticated` | **NOLOGIN** | Authenticated API access; limited by RLS.
| `service_role` | **NOLOGIN**, bypass-RLS | Elevated role used by server-side code (via service key) to bypass RLS where intended.
| `supabase_auth_admin` | **LOGIN** | Used by the Auth service; scoped to the `auth` schema.
| `supabase_storage_admin` | **LOGIN** | Used by the Storage service; manages the `storage` schema.
| `supabase_realtime_admin` | **LOGIN REPLICATION** | Used by Realtime to read WAL (logical replication).
| `dashboard_user` | **NOLOGIN** | Used by Studio/management flows (read-only queries, etc.).
| `supabase_read_only_user` | **LOGIN** | Backing role for Dashboard’s Read‑Only access; member of `pg_read_all_data`.
| (others) |  | You may also see extension- or platform-related roles (e.g., `supabase_admin`, `pgsodium_*`).

> Exact role names can vary slightly between self‑hosted and Cloud, but the **contract** above remains the same: one login role connects for each service; PostgREST delegates per-request privileges by switching into NOLOGIN roles.

---

## Actors → which DB role do they use?

| Actor | DB login role used to connect | How requests are authorized | Primary schemas touched |
|---|---|---|---|
| **PostgREST** ("REST") | `authenticator` (LOGIN, NOINHERIT) | Validates JWT, then `SET LOCAL ROLE` to the role in `jwt.claims.role` (typically `anon` or `authenticated`; server uses `service_role`). `PGRST_DB_ANON_ROLE` defines the fallback for truly anonymous requests. | Whatever schemas you expose (often `public`).
| **Storage API** | `supabase_storage_admin` (LOGIN) | Uses caller JWT → `anon`/`authenticated`/`service_role` for row‑level checks on `storage.objects`. Admin role handles migrations/maintenance. | `storage` schema (`buckets`, `objects`, multipart tables). Treat as API‑only.
| **Realtime** | `supabase_realtime_admin` (LOGIN, REPLICATION, NOINHERIT) | Reads WAL via logical replication; client authorization still uses JWT/role mapping for channels and filters. | Internal `realtime` schema for presence/broadcast; publications for changefeed.
| **PGMeta** (Postgres Meta) | **Configurable** – whatever you put in its `DATABASE_URL` (often an admin role in self‑host). On Cloud it runs behind platform roles, not your app roles. | Not request‑time role switching; it’s an admin/introspection API. | All (admin/introspection), depending on grants of the connection role.

> **Important:** App users do **not** “use the `postgres` role” when they log in. They hit the REST/GraphQL APIs, which run as `authenticator` and then switch into `anon`/`authenticated` based on the JWT. The only time `postgres` is used is when **you** (or a service you configured) connect directly to the database with that user.

---

## Does rotating the `postgres` password break services?

Short answer: **No**, unless you’ve explicitly configured a service to connect as `postgres`.

* **Unaffected** by a `postgres` password change: **PostgREST**, **Storage**, **Realtime** – they do not authenticate as `postgres`; they each have their own login roles.
* **Potentially affected**: **PGMeta** (only if its `DATABASE_URL` uses `postgres`), any admin tools/ORMS/CLI (psql, Prisma, etc.) that connect as `postgres`, and any custom services you configured to use that role.
* **Cloud projects**: Rotating the DB password from **Database → Settings** updates the connection secret for you; third‑party clients must update their connection strings.
* **Self‑hosted**: If you change `POSTGRES_PASSWORD` in `.env`, be sure every service that uses a Postgres connection string referencing `postgres` is updated and the containers are restarted. (For existing volumes, follow the self‑hosting notes about secret rotation and restarts.)

> Best practice: **Don’t use `postgres`** for app/service connectivity. Create dedicated users per service and grant the minimum privileges required. It makes audit and password rotation clean and predictable.

---

## How to verify your instance

Run this to see which roles are LOGIN and what special attributes they have:

```sql
SELECT rolname,
       CASE WHEN rolcanlogin THEN 'LOGIN' ELSE 'NOLOGIN' END AS login,
       rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolinherit, rolbypassrls
FROM pg_roles
WHERE rolname IN (
  'postgres','authenticator','anon','authenticated','service_role',
  'supabase_auth_admin','supabase_storage_admin','supabase_realtime_admin',
  'dashboard_user','supabase_read_only_user'
)
ORDER BY rolname;
```

To confirm what PostgREST will switch into:

```sql
-- Which role is the anonymous fallback?
SHOW pgrst.db_anon_role;  -- or check your container env PGRST_DB_ANON_ROLE

-- Example of role switching in a session (for illustration only)
SET LOCAL ROLE authenticated;  -- should succeed under PostgREST after a valid JWT
```

---

## References

* PostgREST role model & authentication sequence (authenticator → `SET ROLE` to user/anon)  
  https://docs.postgrest.org/en/v12/references/auth.html
* PostgREST configuration (`PGRST_DB_ANON_ROLE`, in-DB config, etc.)  
  https://docs.postgrest.org/en/v12/references/configuration.html
* Supabase roles for API access (`anon`, `authenticated`, `service_role`)  
  https://supabase.com/docs/guides/database/postgres/roles
* RLS mapping of unauthenticated vs authenticated requests  
  https://supabase.com/docs/guides/database/postgres/row-level-security
* Storage schema and “use API, treat tables as read‑only” guidance  
  https://supabase.com/docs/guides/storage/schema/design
* Realtime “bring your own database” and replication role (`supabase_realtime_admin`)  
  https://supabase.com/docs/guides/realtime/bring-your-own-database
* API keys & when to use the service key (server only)  
  https://supabase.com/docs/guides/api/api-keys
* PGMeta (Postgres Meta) repo (connects with whatever `DATABASE_URL` you set)  
  https://github.com/supabase/postgres-meta
