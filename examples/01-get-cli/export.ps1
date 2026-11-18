<# 
  export.ps1 — Problem 04 (B2 scaffolding)
  Выбор и запуск отчёта (скелет, Шаг 1–3).
  Требования: Windows PowerShell 5.1/7+, без промежуточных .py, только CLI модуля.
#>

[CmdletBinding()]
param()

$ErrorActionPreference = 'Continue'

try {
    # Кодировка консоли (best-effort), не влияет на вывод дочерних CLI
    try {
        [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
        $OutputEncoding = [System.Text.Encoding]::UTF8
    } catch { }

    $script:FinalSuccess = $false

    function Write-StepStatus {
        param(
            [Parameter(Mandatory = $true)][string]$Step,
            [Parameter(Mandatory = $true)][ValidateSet('УСПЕХ','ОШИБКА','ИНФО')][string]$Status,
            [Parameter(Mandatory = $true)][string]$Message
        )
        Write-Host ("[{0}] {1}: {2}" -f $Status, $Step, $Message)
    }

    function Write-FinalStatus {
        param(
            [Parameter(Mandatory = $true)][bool]$Success,
            [Parameter(Mandatory = $true)][string]$Message
        )
        $label = if ($Success) { 'УСПЕХ' } else { 'ОШИБКА' }
        Write-Host ""
        Write-Host ("=== ИТОГ: {0} ===" -f $label)
        Write-Host $Message
    }

    function Resolve-TargetPython {
        <#
          R2: interpreter-first precedence py -3 > python > uv run python.
          Returns [pscustomobject]@{ Exe=string; Prefix=string[]; Display=string } or $null.
        #>
        $candidates = @()

        if (Get-Command py -ErrorAction SilentlyContinue) {
            $candidates += [pscustomobject]@{ Exe = 'py'; Prefix = @('-3'); Display = 'py -3' }
        }
        if (Get-Command python -ErrorAction SilentlyContinue) {
            $candidates += [pscustomobject]@{ Exe = 'python'; Prefix = @(); Display = 'python' }
        }
        if (Get-Command uv -ErrorAction SilentlyContinue) {
            $candidates += [pscustomobject]@{ Exe = 'uv'; Prefix = @('run','python'); Display = 'uv run python' }
        }

        foreach ($cand in $candidates) {
            try {
                $testArgs = @()
                if ($cand.Prefix -and $cand.Prefix.Count -gt 0) { $testArgs += $cand.Prefix }
                $testArgs += @('-c', 'import sys')
                & $cand.Exe @testArgs *> $null
                $code = $LASTEXITCODE
                if ($null -eq $code) { $code = 0 }
                if ($code -eq 0) {
                    return [pscustomobject]@{
                        Exe     = [string]$cand.Exe
                        Prefix  = @($cand.Prefix)
                        Display = [string]$cand.Display
                    }
                }
            } catch {
                # пробуем следующий кандидат
            }
        }
        return $null
    }

    function Invoke-TargetCommand {
        <#
          Запуск команды в выбранном окружении. Сохраняет вывод CLI как есть.
          Возвращает код завершения (int). $LASTEXITCODE проверяется сразу.
        #>
        param(
            [Parameter(Mandatory = $true)][string]$Exe,
            [string[]]$Prefix = @(),
            [Parameter(Mandatory = $true)][string[]]$Arguments
        )
        $allArgs = @()
        if ($Prefix -and $Prefix.Count -gt 0) { $allArgs += $Prefix }
        if ($Arguments -and $Arguments.Count -gt 0) { $allArgs += $Arguments }

        & $Exe @allArgs
        $code = $LASTEXITCODE
        if ($null -eq $code) { $code = 0 }
        return [int]$code
    }

    function Invoke-AppPathsProbe {
        <#
          R1: read-only probe через -c (без .py файлов). Возвращает объект путей или $null.
          При ошибке импорта/пробы/парсинга/валидации выводит ОШИБКА и возвращает $null.
        #>
        param(
            [Parameter(Mandatory = $true)]$Target
        )
        $probeArgs = @('-c', 'from mko_get_mediascope_data.core.paths import APP_PATHS; print(APP_PATHS.user_settings_dir); print(APP_PATHS.app_config); print(APP_PATHS.reports)')
        try {
            $output = & $Target.Exe @(@($Target.Prefix) + $probeArgs) 2>&1
            $code = $LASTEXITCODE
            if ($null -eq $code) { $code = 0 }
            if ($code -ne 0) {
                Write-StepStatus -Step 'R1 probe' -Status 'ОШИБКА' -Message "Не удалось получить пути настроек (код $code). Выполните инициализацию: init.ps1 или mko-get-mediascope init (в выбранном окружении) и повторите запуск."
                if ($output) { $output | ForEach-Object { Write-Host $_ } }
                return $null
            }
            $lines = @($output | ForEach-Object { "$_".TrimEnd("`r","`n") } | Where-Object { $_ -ne '' })
            if ($lines.Count -lt 3) {
                Write-StepStatus -Step 'R1 probe' -Status 'ОШИБКА' -Message "Некорректный формат результата probe (ожидалось 3 строки). Инициализируйте настройки и повторите."
                return $null
            }
            $userSettingsDir = [string]$lines[0]
            $appConfig = [string]$lines[1]
            $reports = [string]$lines[2]

            $valid = $true
            if (-not $appConfig.ToLowerInvariant().EndsWith('\app_config.json5') -and -not $appConfig.ToLowerInvariant().EndsWith('/app_config.json5')) { $valid = $false }
            if (-not $reports.ToLowerInvariant().EndsWith('\reports') -and -not $reports.ToLowerInvariant().EndsWith('/reports')) { $valid = $false }
            if (-not ($appConfig.StartsWith($userSettingsDir, [System.StringComparison]::OrdinalIgnoreCase))) { $valid = $false }
            if (-not $valid) {
                Write-StepStatus -Step 'R1 probe' -Status 'ОШИБКА' -Message "Пути настроек не прошли проверку формы. Уточните установку/инициализацию и повторите без угадывания путей."
                return $null
            }

            return [pscustomobject]@{
                UserSettingsDir = $userSettingsDir
                AppConfig       = $appConfig
                Reports         = $reports
            }
        } catch {
            Write-StepStatus -Step 'R1 probe' -Status 'ОШИБКА' -Message ("Ошибка probe: {0}. Выполните инициализацию (init) в выбранном окружении и повторите." -f $_.Exception.Message)
            return $null
        }
    }

    # --- Подготовка: выбор целевого интерпретатора (R2) ---
    $target = Resolve-TargetPython
    if ($null -eq $target) {
        Write-StepStatus -Step 'Подготовка' -Status 'ОШИБКА' -Message "Не найден подходящий интерпретатор Python (проверялись: py -3, python, uv run python). Установите Python/uv и повторите."
        $script:FinalSuccess = $false
        return
    }
    Write-StepStatus -Step 'Подготовка' -Status 'ИНФО' -Message ("Выбран интерпретатор: {0}" -f $target.Display)

    # --- Шаг 1: список/выбор отчёта (заглушка B2, реализация в B4) ---
    Write-StepStatus -Step 'Шаг 1 (список/выбор)' -Status 'ОШИБКА' -Message "Шаг не реализован в текущем этапе (ожидается в задаче B4). Выполнение остановлено."
    $script:FinalSuccess = $false
    return

    # --- Шаг 2: пользовательский путь/копирование (заглушка B2) ---
    Write-StepStatus -Step 'Шаг 2 (пользовательский путь)' -Status 'ОШИБКА' -Message "Шаг не реализован в текущем этапе (ожидается в задаче B4)."
    $script:FinalSuccess = $false
    return

    # --- Шаг 3: запуск отчёта (заглушка B2) ---
    Write-StepStatus -Step 'Шаг 3 (запуск отчёта)' -Status 'ОШИБКА' -Message "Шаг не реализован в текущем этапе (ожидается в задаче B4)."
    $script:FinalSuccess = $false
    return

} catch {
    Write-StepStatus -Step 'Выполнение' -Status 'ОШИБКА' -Message ("Непредвиденная ошибка: {0}" -f $_.Exception.Message)
    $script:FinalSuccess = $false
} finally {
    try {
        if ($script:FinalSuccess) {
            Write-FinalStatus -Success $true -Message "Скрипт завершён успешно."
        } else {
            Write-FinalStatus -Success $false -Message "Скрипт завершён с ошибкой. Проверьте сообщения выше."
        }
    } catch { }
    try {
        [void](Read-Host 'Нажмите Enter для выхода')
    } catch { }
}