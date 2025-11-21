{{/*
Expand names for the Storage component.
*/}}
{{- define "vela.storage.name" -}}
{{- default (print (include "vela.name" .) "-storage") .Values.storage.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "vela.storage.fullname" -}}
{{- if .Values.storage.fullnameOverride -}}
{{- .Values.storage.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-storage" (include "vela.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end }}

{{- define "vela.storage.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vela.storage.name" . }}
app.kubernetes.io/component: storage
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "vela.storage.serviceAccountName" -}}
{{- if .Values.storage.serviceAccount.create -}}
{{- default (include "vela.storage.fullname" .) .Values.storage.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.storage.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{- define "vela.storage.pvcName" -}}
{{- printf "%s-storage-pvc" (include "vela.db.fullname" .) -}}
{{- end }}

{{- define "vela.storage.secretName" -}}
{{- printf "%s-config" (include "vela.storage.fullname" .) -}}
{{- end }}
