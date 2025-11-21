{{/*
Expand the name of the JWT secret.
*/}}
{{- define "vela.secret.jwt" -}}
{{- printf "%s-jwt" (include "vela.fullname" .) }}
{{- end -}}

{{/*
Expand the name of the SMTP secret.
*/}}
{{- define "vela.secret.smtp" -}}
{{- printf "%s-smtp" (include "vela.fullname" .) }}
{{- end -}}

{{/*
Expand the name of the dashboard secret.
*/}}
{{- define "vela.secret.dashboard" -}}
{{- printf "%s-dashboard" (include "vela.fullname" .) }}
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

{{/*
Expand the name of the s3 secret.
*/}}
{{- define "vela.secret.s3" -}}
{{- printf "%s-s3" (include "vela.fullname" .) }}
{{- end -}}

{{/*
Check if both s3 keys are valid
*/}}
{{- define "vela.secret.s3.isValid" -}}
{{- $isValid := "false" -}}
{{- if .Values.secret.s3.keyId -}}
{{- if .Values.secret.s3.accessKey -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "false" -}}
{{- end -}}
{{- else -}}
{{- printf "false" -}}
{{- end -}}
{{- end -}}
