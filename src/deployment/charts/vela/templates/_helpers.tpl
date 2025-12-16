{{/*
Expand the name of the chart.
*/}}
{{- define "vela.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "vela.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Return the autoscaler VM fullname.
*/}}
{{- define "vela.autoscaler.name" -}}
{{- printf "%s-autoscaler-vm" (include "vela.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Return the compose config map name associated with the autoscaler VM.
*/}}
{{- define "vela.autoscaler.composeConfigName" -}}
{{- printf "%s-compose" (include "vela.autoscaler.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Return the PgBouncer config map name that lives next to the autoscaler VM.
*/}}
{{- define "vela.autoscaler.pgbouncerConfigName" -}}
{{- printf "%s-pgbouncer" (include "vela.autoscaler.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "vela.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "vela.labels" -}}
helm.sh/chart: {{ include "vela.chart" . }}
{{ include "vela.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "vela.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vela.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "vela.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "vela.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
