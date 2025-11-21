{{/*
Expand names for the Rest component.
*/}}
{{- define "vela.rest.name" -}}
{{- default (print (include "vela.name" .) "-rest") .Values.rest.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "vela.rest.fullname" -}}
{{- if .Values.rest.fullnameOverride -}}
{{- .Values.rest.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-rest" (include "vela.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end }}

{{- define "vela.rest.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vela.rest.name" . }}
app.kubernetes.io/component: rest
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "vela.rest.serviceAccountName" -}}
{{- if .Values.rest.serviceAccount.create -}}
{{- default (include "vela.rest.fullname" .) .Values.rest.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.rest.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{- define "vela.rest.secretName" -}}
{{- printf "%s-config" (include "vela.rest.fullname" .) -}}
{{- end }}
