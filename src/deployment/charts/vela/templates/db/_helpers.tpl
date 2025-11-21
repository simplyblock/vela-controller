{{/*
Expand the name of the chart.
*/}}
{{- define "vela.db.name" -}}
{{- default (print .Chart.Name "-db") .Values.db.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "vela.db.fullname" -}}
{{- if .Values.db.fullnameOverride }}
{{- .Values.db.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else if .Values.db.nameOverride }}
{{- $name := .Values.db.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- else }}
{{- printf "%s-db" (include "vela.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "vela.db.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vela.db.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "vela.db.serviceAccountName" -}}
{{- if .Values.db.serviceAccount.create }}
{{- default (include "vela.db.fullname" .) .Values.db.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.db.serviceAccount.name }}
{{- end }}
{{- end }}
