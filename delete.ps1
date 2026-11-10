kind delete cluster --name pr4
if ($LASTEXITCODE -ne 0) { throw 'kind delete failed' }
