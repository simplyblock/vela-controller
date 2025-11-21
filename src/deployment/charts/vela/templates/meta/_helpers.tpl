{{/*
Expand names for the Meta component.
*/}}
{{- define "vela.meta.name" -}}
{{- default (print (include "vela.name" .) "-meta") .Values.meta.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "vela.meta.fullname" -}}
{{- if .Values.meta.fullnameOverride -}}
{{- .Values.meta.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-meta" (include "vela.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end }}

{{- define "vela.meta.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vela.meta.name" . }}
app.kubernetes.io/component: meta
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "vela.meta.serviceAccountName" -}}
{{- if .Values.meta.serviceAccount.create -}}
{{- default (include "vela.meta.fullname" .) .Values.meta.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.meta.serviceAccount.name -}}
{{- end -}}
{{- end }}
