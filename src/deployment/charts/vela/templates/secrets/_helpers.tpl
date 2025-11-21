{{/*
Expand the name of the JWT secret.
*/}}
{{- define "vela.secret.jwt" -}}
{{- printf "%s-jwt" (include "vela.fullname" .) }}
{{- end -}}

{{/*
Expand the name of the database secret.
*/}}
{{- define "vela.secret.db" -}}
{{- printf "%s-db" (include "vela.fullname" .) }}
{{- end -}}

{{/*
Expand the name of the analytics secret.
*/}}
{{- define "vela.secret.analytics" -}}
{{- printf "%s-analytics" (include "vela.fullname" .) }}
{{- end -}}
