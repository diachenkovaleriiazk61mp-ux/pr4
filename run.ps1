param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('warm','locks','freeze','capacity','little','overload','rollout','f1','d2','report')]
    [string]$Stage
)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
if (!(Test-Path .venv/Scripts/python.exe)) {
    python -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw 'Python environment setup failed' }
    & .venv/Scripts/python.exe -m pip install matplotlib==3.10.6
    if ($LASTEXITCODE -ne 0) { throw 'Dependency setup failed' }
}
if ($Stage -eq 'report') {
    & .venv/Scripts/python.exe summarize.py
} else {
    & .venv/Scripts/python.exe -u experiments.py $Stage
}
if ($LASTEXITCODE -ne 0) { throw "Stage $Stage failed" }
