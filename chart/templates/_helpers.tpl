{{/*
Reusable snippets shared across Vela Helm templates.
*/}}

{{/*
Returns the plaintext password for a given DB credential key, preserving it across upgrades
via lookup of the existing vela-controller-secret. On fresh installs the password is derived
deterministically from the release name, namespace, and key so that all templates in the same
render produce the same value.

Usage: {{ include "vela.dbPassword" (list "controller-db-password" .) }}
*/}}
{{- define "vela.dbPassword" -}}
{{- $key := index . 0 -}}
{{- $ctx := index . 1 -}}
{{- $existingSecret := lookup "v1" "Secret" $ctx.Release.Namespace "vela-controller-secret" -}}
{{- if and $existingSecret (index $existingSecret.data $key) -}}
{{- index $existingSecret.data $key | b64dec -}}
{{- else -}}
{{- printf "%s-%s-%s" $ctx.Release.Name $ctx.Release.Namespace $key | sha256sum | trunc 32 -}}
{{- end -}}
{{- end -}}

{{/*
Renders a Postgres init container that waits for the server to be ready.
When `database` is provided it also waits until that specific database accepts connections.
The helper accepts a dictionary with the following optional keys:
  - name               : init container name (default: wait-for-database)
  - image              : container image (default: postgres:17-alpine)
  - imagePullPolicy    : pull policy (default: IfNotPresent)
  - host               : database hostname (default: database)
  - port               : database port (default: 5432)
  - database           : if set, block until this database accepts connections
  - secretName         : secret containing credentials for the psql check (default: database)
  - usernameKey        : key for the DB username in secretName (default: superuser-username)
  - passwordKey        : key for the DB password in secretName (default: superuser-password)
  - securityContext    : optional security context applied to the init container
*/}}
{{- define "vela.waitForPostgresInitContainer" -}}
{{- $name := default "wait-for-database" .name -}}
{{- $image := default "postgres:17-alpine" .image -}}
{{- $imagePullPolicy := default "IfNotPresent" .imagePullPolicy -}}
{{- $host := default "database" .host -}}
{{- $port := default "5432" .port -}}
{{- $database := .database -}}
{{- $secretName := default "database" .secretName -}}
{{- $usernameKey := default "superuser-username" .usernameKey -}}
{{- $passwordKey := default "superuser-password" .passwordKey -}}
- name: {{ $name }}
  image: {{ $image }}
  imagePullPolicy: {{ $imagePullPolicy }}
  env:
    - name: DB_HOST
      value: {{ $host | quote }}
    - name: DB_PORT
      value: {{ $port | quote }}
{{- if $database }}
    - name: DB_USER
      valueFrom:
        secretKeyRef:
          name: {{ $secretName }}
          key: {{ $usernameKey }}
    - name: PGPASSWORD
      valueFrom:
        secretKeyRef:
          name: {{ $secretName }}
          key: {{ $passwordKey }}
{{- end }}
  command: ["/bin/sh", "-c"]
  args:
    - |
      echo "Waiting for database..."
      until pg_isready -h "$DB_HOST" -p "$DB_PORT"; do
        sleep 2
      done
      echo "Database is ready"
{{- if $database }}

      until psql -h "$DB_HOST" -U "$DB_USER" -d {{ $database | quote }} -c '\q' 2>/dev/null; do
        echo "Waiting for Postgres connection to {{ $database }}..."
        sleep 2
      done
{{- end }}
{{- with .securityContext }}
  securityContext:
{{ toYaml . | nindent 4 }}
{{- end }}
{{- end }}
