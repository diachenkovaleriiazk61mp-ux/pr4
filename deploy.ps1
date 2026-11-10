$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
function Run($program) {
    $argv = $args
    & $program @argv
    if ($LASTEXITCODE -ne 0) { throw "$program failed ($LASTEXITCODE)" }
}
function K { Run kubectl --context kind-pr4 @args }
Run docker info --format '{{.ServerVersion}}'
if (!(Test-Path .env)) {
    $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
    $bytes = New-Object byte[] 24
    $rng.GetBytes($bytes)
    $rng.Dispose()
    $password = ($bytes | ForEach-Object { $_.ToString('x2') }) -join ''
    Set-Content -Encoding ascii .env "DB_PASSWORD=$password"
}
$entry = Get-Content .env | Where-Object { $_ -match '^DB_PASSWORD=[a-f0-9]{48}$' }
if (@($entry).Count -ne 1) { throw 'Expected one DB_PASSWORD with 48 hex characters in .env' }
$password = $entry.Substring(12)
Run kind create cluster --name pr4 --config kind.yaml --image kindest/node:v1.32.2 --wait 180s
K label node pr4-control-plane role=infra --overwrite
Run docker build -t vaccinations-pr4:local .
Run kind load docker-image vaccinations-pr4:local --name pr4
Run docker build -f worker.Dockerfile -t vaccinations-worker-pr4:local .
Run kind load docker-image vaccinations-worker-pr4:local --name pr4
$secret = @{apiVersion='v1';kind='Secret';metadata=@{name='db-secret'};stringData=@{password=$password}} | ConvertTo-Json -Compress
$secret | & kubectl --context kind-pr4 create -f -
if ($LASTEXITCODE -ne 0) { throw 'Secret creation failed' }
K create configmap schema --from-file=schema.sql
K create configmap prometheus-config --from-file=k8s/prometheus.yml
K apply -f k8s/db.yaml
K rollout status statefulset/db --timeout=240s
K wait --for=condition=complete job/migrate --timeout=240s
K apply -f k8s/web.yaml -f k8s/monitoring.yaml
K apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.7.2/components.yaml
$ms = K -n kube-system get deployment metrics-server --output=json | ConvertFrom-Json
$ms.spec.template.spec.containers[0].args += '--kubelet-insecure-tls'
$ms.spec.template.spec | Add-Member -NotePropertyName nodeSelector -NotePropertyValue @{role='infra';'kubernetes.io/os'='linux'} -Force
$tolerations = @($ms.spec.template.spec.tolerations | Where-Object { $_ }) + @(@{key='node-role.kubernetes.io/control-plane';operator='Exists';effect='NoSchedule'})
$ms.spec.template.spec | Add-Member -NotePropertyName tolerations -NotePropertyValue $tolerations -Force
$ms | ConvertTo-Json -Depth 50 -Compress | & kubectl --context kind-pr4 replace -f -
if ($LASTEXITCODE -ne 0) { throw 'metrics-server configuration failed' }
K -n kube-system rollout status deployment/metrics-server --timeout=240s
K rollout status deployment/web --timeout=240s
K apply -f k8s/worker.yaml
K rollout status deployment/worker --timeout=240s
K rollout status deployment/prometheus --timeout=240s
Write-Host 'API: http://localhost:8084 ; Prometheus: http://localhost:9094'
