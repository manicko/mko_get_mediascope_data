<#
.SYNOPSIS
    Makefile.ps1 - task runner for mko_get_mediascope_data.

.DESCRIPTION
    PowerShell-based replacement for Make. Wraps the uv-managed toolchains
    (ruff, mypy, pytest) and enforces the same quality gates used by CI.

    All commands run through `uv run` so the project-managed environment is
    used instead of any system-installed interpreter.

.EXAMPLE
    .\Makefile.ps1            # runs the default "all" target (lint, then test)
    .\Makefile.ps1 test       # runs the test suite
    .\Makefile.ps1 lint typecheck   # runs multiple targets in order
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0, ValueFromRemainingArguments = $true)]
    [ValidateSet('test', 'lint', 'typecheck', 'format', 'all')]
    [string[]] $Targets = @('all')
)

$ErrorActionPreference = 'Stop'

function Invoke-Step {
    param(
        [string] $Name,
        [scriptblock] $Command
    )
    Write-Host "==> $Name" -ForegroundColor Cyan
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Target '$Name' failed with exit code $LASTEXITCODE."
    }
}

function Run-Lint {
    Invoke-Step 'lint' { uv run ruff check src tests }
}

function Run-Test {
    Invoke-Step 'test' { uv run pytest }
}

function Run-TypeCheck {
    Invoke-Step 'typecheck' { uv run mypy src }
}

function Run-Format {
    Invoke-Step 'format' { uv run ruff check --fix src tests }
}

function Run-All {
    Run-Lint
    Run-Test
}

foreach ($target in $Targets) {
    switch ($target) {
        'lint'      { Run-Lint }
        'test'      { Run-Test }
        'typecheck' { Run-TypeCheck }
        'format'    { Run-Format }
        'all'       { Run-All }
    }
}
