{{/*
Expand the name of the chart.
*/}}
{{- define "xflow.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "xflow.fullname" -}}
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
{{- define "xflow.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "xflow.labels" -}}
helm.sh/chart: {{ include "xflow.chart" . }}
{{ include "xflow.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "xflow.selectorLabels" -}}
app.kubernetes.io/name: {{ include "xflow.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Build full image path from registry and repository
Usage: {{ include "xflow.image" (dict "Values" .Values "image" .Values.backend.image) }}
*/}}
{{- define "xflow.image" -}}
{{- $registry := .Values.global.imageRegistry -}}
{{- $repository := .image.repository -}}
{{- $tag := .image.tag | default "latest" -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry $repository $tag -}}
{{- else -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}
{{- end }}

{{/*
Build full image path for a specific component
Usage: {{ include "xflow.componentImage" (dict "Values" .Values "repository" "xflow-backend" "tag" "latest") }}
*/}}
{{- define "xflow.componentImage" -}}
{{- $registry := .Values.global.imageRegistry -}}
{{- $repository := .repository -}}
{{- $tag := .tag | default "latest" -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry $repository $tag -}}
{{- else -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}
{{- end }}

{{/*
Get the storage class based on cloud provider
*/}}
{{- define "xflow.storageClass" -}}
{{- if .Values.global.storageClass -}}
{{- .Values.global.storageClass -}}
{{- else if eq .Values.global.cloudProvider "aws" -}}
gp2
{{- else if eq .Values.global.cloudProvider "gcp" -}}
standard-rwo
{{- else if eq .Values.global.cloudProvider "azure" -}}
managed-premium
{{- else -}}
standard
{{- end -}}
{{- end }}

{{/*
Get the ingress class name based on cloud provider
*/}}
{{- define "xflow.ingressClassName" -}}
{{- if .Values.ingress.className -}}
{{- .Values.ingress.className -}}
{{- else if eq .Values.global.cloudProvider "aws" -}}
alb
{{- else if eq .Values.global.cloudProvider "gcp" -}}
gce
{{- else if eq .Values.global.cloudProvider "azure" -}}
azure-application-gateway
{{- else -}}
nginx
{{- end -}}
{{- end }}

{{/*
Generate AWS ALB ingress annotations
*/}}
{{- define "xflow.ingress.albAnnotations" -}}
{{- if and .Values.ingress.alb.enabled (eq .Values.global.cloudProvider "aws") -}}
alb.ingress.kubernetes.io/group.name: {{ .Values.ingress.alb.groupName | default "xflow-alb" | quote }}
alb.ingress.kubernetes.io/scheme: {{ .Values.ingress.alb.scheme | default "internet-facing" | quote }}
alb.ingress.kubernetes.io/target-type: {{ .Values.ingress.alb.targetType | default "ip" | quote }}
{{- if .Values.global.tls.acm.certificateArn }}
alb.ingress.kubernetes.io/certificate-arn: {{ .Values.global.tls.acm.certificateArn | quote }}
alb.ingress.kubernetes.io/listen-ports: '[{"HTTP": 80}, {"HTTPS": 443}]'
alb.ingress.kubernetes.io/ssl-redirect: "443"
{{- end }}
{{- end }}
{{- end }}

{{/*
Generate GCP ingress annotations
*/}}
{{- define "xflow.ingress.gceAnnotations" -}}
{{- if eq .Values.global.cloudProvider "gcp" -}}
kubernetes.io/ingress.class: "gce"
{{- if .Values.global.tls.enabled }}
networking.gke.io/managed-certificates: "xflow-cert"
{{- end }}
{{- end }}
{{- end }}

{{/*
Generate Azure ingress annotations
*/}}
{{- define "xflow.ingress.azureAnnotations" -}}
{{- if eq .Values.global.cloudProvider "azure" -}}
kubernetes.io/ingress.class: "azure/application-gateway"
{{- end }}
{{- end }}

{{/*
Generate ingress annotations based on cloud provider
*/}}
{{- define "xflow.ingress.annotations" -}}
{{- if eq .Values.global.cloudProvider "aws" }}
{{ include "xflow.ingress.albAnnotations" . }}
{{- else if eq .Values.global.cloudProvider "gcp" }}
{{ include "xflow.ingress.gceAnnotations" . }}
{{- else if eq .Values.global.cloudProvider "azure" }}
{{ include "xflow.ingress.azureAnnotations" . }}
{{- end }}
{{- with .Values.ingress.annotations }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Generate host name from subdomain and global domain
Usage: {{ include "xflow.host" (dict "Values" .Values "subdomain" "api") }}
*/}}
{{- define "xflow.host" -}}
{{- $subdomain := .subdomain -}}
{{- $domain := .Values.global.domain -}}
{{- if $domain -}}
{{- printf "%s.%s" $subdomain $domain -}}
{{- else -}}
{{- $subdomain -}}
{{- end -}}
{{- end }}

{{/*
Get namespace
*/}}
{{- define "xflow.namespace" -}}
{{- .Values.global.namespace | default .Release.Namespace -}}
{{- end }}

{{/*
Get MongoDB URL (auto-generate or use provided)
*/}}
{{- define "xflow.mongodbUrl" -}}
{{- if .Values.backend.secrets.mongodbUrl -}}
{{- .Values.backend.secrets.mongodbUrl -}}
{{- else -}}
{{- $username := .Values.mongodb.auth.rootUsername | default "root" -}}
{{- $password := .Values.mongodb.auth.rootPassword | default "changeme" -}}
{{- printf "mongodb://%s:%s@mongodb:27017" $username $password -}}
{{- end -}}
{{- end }}

{{/*
Get AWS region from global settings
*/}}
{{- define "xflow.awsRegion" -}}
{{- .Values.global.aws.region | default "ap-northeast-2" -}}
{{- end }}

{{/*
Generate ServiceAccount annotations for cloud-specific workload identity
Usage: {{ include "xflow.serviceAccount.annotations" (dict "Values" .Values "annotations" .Values.backend.serviceAccount.annotations) }}
*/}}
{{- define "xflow.serviceAccount.annotations" -}}
{{- with .annotations }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Generate component-specific labels
Usage: {{ include "xflow.componentLabels" (dict "component" "backend" "context" .) }}
*/}}
{{- define "xflow.componentLabels" -}}
{{ include "xflow.labels" .context }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Generate component-specific selector labels
Usage: {{ include "xflow.componentSelectorLabels" (dict "component" "backend" "context" .) }}
*/}}
{{- define "xflow.componentSelectorLabels" -}}
{{ include "xflow.selectorLabels" .context }}
app.kubernetes.io/component: {{ .component }}
{{- end }}
