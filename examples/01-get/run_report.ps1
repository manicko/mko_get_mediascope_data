$ErrorActionPreference = 'Continue'

try {
    Set-Location -LiteralPath $PSScriptRoot

    $pythonCmd = $null
    $pythonArgs = @()

    if (Get-Command py -ErrorAction SilentlyContinue) {
        try {
            & py -3 -c "import sys" *> $null
            if ($LASTEXITCODE -eq 0) {
                $pythonCmd = 'py'
                $pythonArgs = @('-3')
            }
        } catch { }
    }

    if (-not $pythonCmd -and (Get-Command python -ErrorAction SilentlyContinue)) {
        try {
            & python -c "import sys" *> $null
            if ($LASTEXITCODE -eq 0) {
                $pythonCmd = 'python'
                $pythonArgs = @()
            }
        } catch { }
    }

    if (-not $pythonCmd -and (Get-Command uv -ErrorAction SilentlyContinue)) {
        $pythonCmd = 'uv'
        $pythonArgs = @('run', 'python')
    }

    $scriptPath = Join-Path $PSScriptRoot 'run_report.py'

    if (-not $pythonCmd) {
        Write-Host 'No suitable Python interpreter found (tried: py -3, python, uv run python).'
        Write-Host 'Ensure Python 3 and project dependencies are available in the active environment.'
        $global:LASTEXITCODE = 1
    } else {
        & $pythonCmd @pythonArgs $scriptPath
    }

    $exitCode = $LASTEXITCODE
    if ($null -eq $exitCode) { $exitCode = 0 }

    Write-Host ''
    Write-Host "Script finished with exit code $exitCode."
} finally {
    try {
        [void](Read-Host 'Press Enter to close this window')
    } catch { }
}
