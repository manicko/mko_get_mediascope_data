<# 
  export.ps1 — Problem 04 (B4)
  Выбор и запуск отчёта.
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

    function Get-NormalizedCustomReportPath {
        <#
          Нормализация пользовательского пути: поддержка ~, абсолютный путь.
          Возвращает строку полного пути или $null при ошибке/невалидном вводе.
        #>
        param([Parameter(Mandatory = $true)][string]$InputPath)

        try {
            $expanded = $InputPath
            $profile = [string]$env:USERPROFILE
            if ($InputPath -eq '~') {
                if ([string]::IsNullOrWhiteSpace($profile)) { return $null }
                $expanded = $profile
            } elseif ($InputPath.StartsWith('~/') -or $InputPath.StartsWith('~\')) {
                if ([string]::IsNullOrWhiteSpace($profile)) { return $null }
                $rest = $InputPath.Substring(2)
                if ($rest -eq '') {
                    $expanded = $profile
                } else {
                    $expanded = [System.IO.Path]::Combine($profile, $rest)
                }
            }
            $full = [System.IO.Path]::GetFullPath($expanded)
            return [string]$full
        } catch {
            return $null
        }
    }

    function Get-ReportCatalog {
        <#
          R5: детерминированная рекурсивная выдача *.json5 из reports root.
          Возвращает @([pscustomobject]@{ DisplayNumber; RelativePath; FullPath }) или $null при ошибке.
        #>
        param([Parameter(Mandatory = $true)][string]$ReportsRootFull)

        $items = @()
        try {
            $files = @(Get-ChildItem -LiteralPath $ReportsRootFull -Recurse -Filter '*.json5' -File -ErrorAction Stop)
        } catch {
            Write-StepStatus -Step 'Шаг 1 (список/выбор)' -Status 'ОШИБКА' -Message ("Не удалось прочитать каталог отчётов: {0}" -f $_.Exception.Message)
            return $null
        }

        $rootPrefix = $ReportsRootFull.TrimEnd('\','/') + [System.IO.Path]::DirectorySeparatorChar
        $idx = 0
        foreach ($f in $files) {
            $full = [System.IO.Path]::GetFullPath($f.FullName)
            $rel = $null
            if ($full.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
                $rel = $full.Substring($rootPrefix.Length)
            } else {
                $rel = [System.IO.Path]::GetFileName($full)
            }
            $relSortKey = ($rel -replace '/', '\').ToLowerInvariant()
            $items += [pscustomobject]@{
                Index        = $idx
                RelativePath = [string]$rel
                FullPath     = [string]$full
                RelSortKey   = [string]$relSortKey
            }
            $idx++
        }

        $sorted = @($items | Sort-Object -Property @{Expression={$_.RelSortKey}; Ascending=$true}, @{Expression={$_.Index}; Ascending=$true})

        $catalog = @()
        $displayNum = 1
        foreach ($s in $sorted) {
            $catalog += [pscustomobject]@{
                DisplayNumber = $displayNum
                RelativePath  = [string]$s.RelativePath
                FullPath      = [string]$s.FullPath
            }
            $displayNum++
        }

        return ,$catalog
    }

    # --- Подготовка: выбор целевого интерпретатора (R2) ---
    $target = Resolve-TargetPython
    if ($null -eq $target) {
        Write-StepStatus -Step 'Подготовка' -Status 'ОШИБКА' -Message "Не найден подходящий интерпретатор Python (проверялись: py -3, python, uv run python). Установите Python/uv и повторите."
        $script:FinalSuccess = $false
        return
    }
    Write-StepStatus -Step 'Подготовка' -Status 'ИНФО' -Message ("Выбран интерпретатор: {0}" -f $target.Display)

    # --- R1: probe путей и проверка каталога reports ---
    $paths = Invoke-AppPathsProbe -Target $target
    if ($null -eq $paths) {
        $script:FinalSuccess = $false
        return
    }

    $reportsRootFull = [System.IO.Path]::GetFullPath([string]$paths.Reports)
    if (-not (Test-Path -LiteralPath $reportsRootFull -PathType Container)) {
        Write-StepStatus -Step 'Подготовка' -Status 'ОШИБКА' -Message ("Каталог reports не найден: {0}. Выполните init.ps1 или mko-get-mediascope init в выбранном окружении." -f $reportsRootFull)
        $script:FinalSuccess = $false
        return
    }

    # --- Шаг 1: детерминированный каталог (R5) ---
    $catalog = Get-ReportCatalog -ReportsRootFull $reportsRootFull
    if ($null -eq $catalog) {
        $script:FinalSuccess = $false
        return
    }
    $catalog = @($catalog)

    if ($catalog.Count -eq 0) {
        Write-StepStatus -Step 'Шаг 1 (список/выбор)' -Status 'ИНФО' -Message ("В каталоге reports не найдено файлов *.json5: {0}. Доступен только выбор 0 (указать путь вручную)." -f $reportsRootFull)
    }

    # --- Шаг 1/2: цикл выбора и пользовательского пути (R5/Q5,Q6) ---
    $runTarget = $null
    while ($null -eq $runTarget) {
        Write-Host ""
        Write-Host "Шаг 1 (список/выбор): доступные отчёты"
        foreach ($item in $catalog) {
            Write-Host ("{0}) {1}" -f $item.DisplayNumber, $item.RelativePath)
        }
        Write-Host "0) Указать путь к файлу отчёта вручную"

        $rawChoice = Read-Host "Выберите номер отчёта (0..$($catalog.Count))"
        if ($null -eq $rawChoice) { $rawChoice = '' }
        $rawChoice = ([string]$rawChoice).Trim()

        $choice = 0
        if (-not [int]::TryParse($rawChoice, [ref]$choice)) {
            Write-StepStatus -Step 'Шаг 1 (список/выбор)' -Status 'ОШИБКА' -Message "Некорректный выбор. Введите целое число в диапазоне 0..$($catalog.Count)."
            continue
        }
        if ($choice -lt 0 -or $choice -gt $catalog.Count) {
            Write-StepStatus -Step 'Шаг 1 (список/выбор)' -Status 'ОШИБКА' -Message "Некорректный выбор. Введите целое число в диапазоне 0..$($catalog.Count)."
            continue
        }

        if ($choice -ge 1) {
            $selected = $null
            foreach ($it in $catalog) {
                if ($it.DisplayNumber -eq $choice) { $selected = $it; break }
            }
            if ($null -eq $selected) {
                Write-StepStatus -Step 'Шаг 1 (список/выбор)' -Status 'ОШИБКА' -Message "Не удалось определить выбранный отчёт. Повторите выбор."
                continue
            }
            $runTarget = [string]$selected.FullPath
            Write-StepStatus -Step 'Шаг 1 (список/выбор)' -Status 'ИНФО' -Message ("Выбран отчёт: {0}" -f $selected.RelativePath)
            continue
        }

        # choice == 0: Шаг 2 — пользовательский путь/копирование
        $customInput = Read-Host "Введите путь к файлу отчёта (.json5)"
        if ($null -eq $customInput) { $customInput = '' }
        $customInput = ([string]$customInput).Trim()
        if ($customInput -eq '') {
            Write-StepStatus -Step 'Шаг 2 (пользовательский путь)' -Status 'ОШИБКА' -Message "Путь не указан (пустой ввод). Выбор отменён, вернитесь к списку."
            continue
        }

        if ($customInput -eq '~' -or $customInput.StartsWith('~/') -or $customInput.StartsWith('~\')) {
            if ([string]::IsNullOrWhiteSpace([string]$env:USERPROFILE)) {
                Write-StepStatus -Step 'Шаг 2 (пользовательский путь)' -Status 'ОШИБКА' -Message "Не удалось раскрыть '~': переменная USERPROFILE недоступна."
                continue
            }
        }

        $srcFull = Get-NormalizedCustomReportPath -InputPath $customInput
        if ($null -eq $srcFull -or [string]::IsNullOrWhiteSpace($srcFull)) {
            Write-StepStatus -Step 'Шаг 2 (пользовательский путь)' -Status 'ОШИБКА' -Message "Не удалось нормализовать указанный путь. Проверьте корректность и повторите."
            continue
        }

        if (-not (Test-Path -LiteralPath $srcFull -PathType Leaf)) {
            Write-StepStatus -Step 'Шаг 2 (пользовательский путь)' -Status 'ОШИБКА' -Message ("Файл не найден: {0}" -f $srcFull)
            continue
        }

        $ext = [System.IO.Path]::GetExtension($srcFull)
        if (-not $ext.Equals('.json5', [System.StringComparison]::OrdinalIgnoreCase)) {
            Write-StepStatus -Step 'Шаг 2 (пользовательский путь)' -Status 'ОШИБКА' -Message "Указанный файл должен иметь расширение .json5."
            continue
        }

        $leaf = [System.IO.Path]::GetFileName($srcFull)
        $destFull = [System.IO.Path]::GetFullPath((Join-Path -Path $reportsRootFull -ChildPath $leaf))

        if ($srcFull.Equals($destFull, [System.StringComparison]::OrdinalIgnoreCase)) {
            Write-StepStatus -Step 'Шаг 2 (пользовательский путь)' -Status 'ИНФО' -Message ("Файл уже находится в каталоге reports и будет запущен без копирования: {0}" -f $destFull)
            $runTarget = [string]$destFull
            continue
        }

        $destExists = Test-Path -LiteralPath $destFull -PathType Leaf
        if ($destExists) {
            Write-Host ("[ПРЕДУПРЕЖДЕНИЕ] Файл с именем '{0}' уже существует в каталоге reports: {1}" -f $leaf, $destFull)
            $conflictAnswer = Read-Host "Заменить существующий файл? [R]eplace/[C]ancel (по умолчанию Cancel)"
            if ($null -eq $conflictAnswer) { $conflictAnswer = '' }
            $conflictNorm = ([string]$conflictAnswer).Trim()
            $isReplace = $conflictNorm.Equals('R', [System.StringComparison]::OrdinalIgnoreCase)
            if (-not $isReplace) {
                Write-StepStatus -Step 'Шаг 2 (пользовательский путь)' -Status 'ИНФО' -Message "Операция отменена. Копирование/замена/запуск не выполнены."
                continue
            }

            try {
                Copy-Item -LiteralPath $srcFull -Destination $destFull -Force -ErrorAction Stop
            } catch {
                Write-StepStatus -Step 'Шаг 2 (пользовательский путь)' -Status 'ОШИБКА' -Message ("Не удалось заменить файл в каталоге reports: {0}" -f $_.Exception.Message)
                continue
            }
            Write-StepStatus -Step 'Шаг 2 (пользовательский путь)' -Status 'УСПЕХ' -Message ("Файл заменён в каталоге reports: {0}" -f $destFull)
            $runTarget = [string]$destFull
            continue
        }

        try {
            Copy-Item -LiteralPath $srcFull -Destination $destFull -ErrorAction Stop
        } catch {
            Write-StepStatus -Step 'Шаг 2 (пользовательский путь)' -Status 'ОШИБКА' -Message ("Не удалось скопировать файл в каталог reports: {0}" -f $_.Exception.Message)
            continue
        }
        Write-StepStatus -Step 'Шаг 2 (пользовательский путь)' -Status 'УСПЕХ' -Message ("Файл скопирован в каталог reports: {0}" -f $destFull)
        $runTarget = [string]$destFull
    }

    # --- Шаг 3: запуск отчёта ---
    Write-StepStatus -Step 'Шаг 3 (запуск отчёта)' -Status 'ИНФО' -Message ("Запуск отчёта: {0}" -f $runTarget)
    $code = Invoke-TargetCommand -Exe $target.Exe -Prefix @($target.Prefix) -Arguments @('-m','mko_get_mediascope_data','run', $runTarget)
    if ($code -eq 0) {
        Write-StepStatus -Step 'Шаг 3 (запуск отчёта)' -Status 'УСПЕХ' -Message ("Отчёт успешно выполнен: {0}" -f $runTarget)
        $script:FinalSuccess = $true
        return
    } else {
        Write-StepStatus -Step 'Шаг 3 (запуск отчёта)' -Status 'ОШИБКА' -Message ("Команда запуска завершилась с кодом {0}. Отчёт: {1}" -f $code, $runTarget)
        $script:FinalSuccess = $false
        return
    }

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
