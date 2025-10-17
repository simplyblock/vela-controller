{{/*
Reusable snippets shared across Vela Helm templates.
*/}}

{{/*
Renders a Postgres init container that waits for the target database to accept connections.
The helper accepts a dictionary with the following optional keys:
  - name               : init container name (default: wait-for-database)
  - image              : container image (default: postgres:17-alpine)
  - imagePullPolicy    : pull policy (default: IfNotPresent)
  - host               : database hostname (default: database)
  - port               : database port (default: 5432)
  - secretName         : Kubernetes secret with credentials (default: database)
  - usernameKey        : Secret key used for DB username (default: superuser-username)
  - passwordKey        : Secret key used for DB password (default: superuser-password)
  - securityContext    : optional security context applied to the init container
*/}}
{{- define "vela.waitForPostgresInitContainer" -}}
{{- $name := default "wait-for-database" .name -}}
{{- $image := default "postgres:17-alpine" .image -}}
{{- $imagePullPolicy := default "IfNotPresent" .imagePullPolicy -}}
{{- $host := default "database" .host -}}
{{- $port := default "5432" .port -}}
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
  command: ["/bin/sh", "-c"]
  args:
    - |
      echo "Waiting for database..."
      until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER"; do
        sleep 2
      done
      echo "Database is ready"

      # Ensure postgres superuser can connect
      until psql -h "$DB_HOST" -U "$DB_USER" -d postgres -c '\q' 2>/dev/null; do
        echo "Waiting for Postgres superuser connection..."
        sleep 2
      done
{{- with .securityContext }}
  securityContext:
{{ toYaml . | nindent 4 }}
{{- end }}
{{- end }}
