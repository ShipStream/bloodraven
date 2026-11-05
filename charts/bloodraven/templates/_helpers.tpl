{{/*
Expand the name of the chart.
*/}}
{{- define "bloodraven.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this.
*/}}
{{- define "bloodraven.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "bloodraven.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "bloodraven.labels" -}}
helm.sh/chart: {{ include "bloodraven.chart" . }}
{{ include "bloodraven.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "bloodraven.selectorLabels" -}}
app.kubernetes.io/name: {{ include "bloodraven.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use.
*/}}
{{- define "bloodraven.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "bloodraven.fullname" .) .Values.serviceAccount.name }}
{{- else if .Values.serviceAccount.name }}
{{- .Values.serviceAccount.name }}
{{- else }}
{{- fail "serviceAccount.name is required when serviceAccount.create=false; the chart binds the operator ClusterRole to it, so it must not fall back to the namespace's default ServiceAccount" }}
{{- end }}
{{- end }}

{{/*
Auxiliary Service name used by sidecars for operator status checks.
*/}}
{{- define "bloodraven.auxiliaryServiceName" -}}
{{- default "bloodraven" .Values.auxiliary.service.name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Operator image reference.
*/}}
{{- define "bloodraven.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{/*
BLOODRAVEN_WS_MAX_CLIENTS value. Missing, null or blank keeps the default
100 and 0 disables the cap. Anything that is not a whole number from 0 to
2147483647 fails the render: the operator would otherwise fall back to 100
silently, and a lossy int64 coercion would turn junk into 0 (unlimited).
*/}}
{{- define "bloodraven.wsMaxClients" -}}
{{- $v := .Values.auxiliary.service.wsMaxClients -}}
{{- $msg := printf "auxiliary.service.wsMaxClients must be a whole number from 0 (unlimited) to 2147483647, got %v" $v -}}
{{- if kindIs "invalid" $v -}}
100
{{- else if kindIs "string" $v -}}
{{- $s := trim $v -}}
{{- if eq $s "" -}}
100
{{- else if regexMatch "^[0-9]{1,10}$" $s -}}
{{- if gt (atoi $s) 2147483647 }}{{ fail $msg }}{{ end -}}
{{- atoi $s -}}
{{- else -}}
{{- fail $msg -}}
{{- end -}}
{{- else if or (kindIs "float64" $v) (kindIs "int64" $v) (kindIs "int" $v) -}}
{{- $f := float64 $v -}}
{{- if or (lt $f 0.0) (gt $f 2147483647.0) (ne $f (float64 (int64 $f))) }}{{ fail $msg }}{{ end -}}
{{- int64 $f -}}
{{- else -}}
{{- fail $msg -}}
{{- end -}}
{{- end }}
