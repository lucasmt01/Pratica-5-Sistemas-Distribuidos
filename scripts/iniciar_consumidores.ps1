$ErrorActionPreference = 'Stop'
$env:PYTHONIOENCODING = 'utf-8'
$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonPath = Join-Path $projectRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $pythonPath)) {
    throw 'Crie a .venv e instale requirements.txt antes de iniciar os consumidores.'
}
$logDirectory = Join-Path $projectRoot 'data\logs'
New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
foreach ($consumer in @('jardim', 'semaforo', 'iluminacao', 'limpador')) {
    $process = Start-Process -FilePath $pythonPath -ArgumentList @('-u', '-m', 'cidade_inteligente', 'consumer', $consumer) -WorkingDirectory $projectRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $logDirectory "$consumer.log") -RedirectStandardError (Join-Path $logDirectory "$consumer.err.log") -PassThru
    Write-Output "$consumer iniciado: PID $($process.Id). Logs: $logDirectory"
}
Write-Output 'Para encerrar, execute Stop-Process -Id <PID> para cada PID exibido.'
