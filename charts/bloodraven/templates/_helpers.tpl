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
Default MySQL sidecar image (BLOODRAVEN_SIDECAR_IMAGE): the image a failover
group runs when spec.sidecar.image is empty.
*/}}
{{- define "bloodraven.sidecarImage" -}}
{{- $tag := .Values.sidecar.image.tag | default .Chart.AppVersion }}
{{- printf "%s:%s" .Values.sidecar.image.repository $tag }}
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

{{/*
bloodraven.userMountsTmp is non-empty when extraVolumeMounts already mounts
something at /tmp. The chart then leaves /tmp to that volume instead of
adding its own (two mounts at one path make the pod invalid).
*/}}
{{- define "bloodraven.userMountsTmp" -}}
{{- range (.Values.extraVolumeMounts | default list) -}}
{{- if eq (trimSuffix "/" (.mountPath | default "")) "/tmp" -}}true{{- end -}}
{{- end -}}
{{- end -}}

{{/*
"true" or "false": whether the usage report is on. A missing telemetry key
(`helm upgrade --reuse-values` from a release before it) is on;
`telemetry: false` is off. Anything that is not a map or a boolean fails.
*/}}
{{- define "bloodraven.telemetryEnabled" -}}
{{- $t := .Values.telemetry -}}
{{- if kindIs "invalid" $t -}}
true
{{- else if kindIs "bool" $t -}}
{{- $t -}}
{{- else if kindIs "map" $t -}}
{{- if not (hasKey $t "enabled") -}}
true
{{- else if kindIs "bool" $t.enabled -}}
{{- $t.enabled -}}
{{- else -}}
{{- fail (printf "telemetry.enabled must be true or false, got %v" $t.enabled) -}}
{{- end -}}
{{- else -}}
{{- fail (printf "telemetry must be a map (telemetry.enabled: true|false), got %v" $t) -}}
{{- end -}}
{{- end -}}

{{/*
The usage report endpoint override, empty for the default. It must be an
http(s) URL: the operator turns the report off for anything else.
*/}}
{{- define "bloodraven.telemetryEndpoint" -}}
{{- $t := .Values.telemetry -}}
{{- if kindIs "map" $t -}}
{{- with $t.endpoint -}}
{{- if not (regexMatch "^https?://[^/\\s]+" .) -}}
{{- fail (printf "telemetry.endpoint must be an http(s) URL, got %q" .) -}}
{{- end -}}
{{- . -}}
{{- end -}}
{{- end -}}
{{- end -}}
