<# 
  init.ps1 — Problem 04 (B3)
  Инициализация окружения и настройка export_folder (Шаг 1–4).
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
                Write-StepStatus -Step 'R1 probe' -Status 'ОШИБКА' -Message "Не удалось получить пути настроек (код $code). Выполните инициализацию: mko-get-mediascope init (в выбранном окружении) и повторите запуск."
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

    function Update-SessionPathFromEnvironment {
        # Best-effort refresh PATH from Machine/User env vars (not trusted)
        try {
            $machine = [System.Environment]::GetEnvironmentVariable('Path', 'Machine')
            $user = [System.Environment]::GetEnvironmentVariable('Path', 'User')
            $parts = @()
            if ($null -ne $machine -and $machine -ne '') { $parts += $machine }
            if ($null -ne $user -and $user -ne '') { $parts += $user }
            if ($parts.Count -gt 0) {
                $env:Path = ($parts -join ';')
            }
        } catch { }
    }

    function Test-GitUsable {
        # Returns $true if git --version runs successfully (exit 0). Preserves output.
        try {
            & git --version
            $code = $LASTEXITCODE
            if ($null -eq $code) { $code = 0 }
            return ($code -eq 0)
        } catch {
            return $false
        }
    }

    function Test-WingetAvailable {
        try {
            return [bool](Get-Command winget -ErrorAction SilentlyContinue)
        } catch {
            return $false
        }
    }

    function Read-ExportFolderValue {
        <#
          R4: read-only single-match of "export_folder" JSON string value.
          Returns [pscustomobject]@{ Match=[System.Text.RegularExpressions.Match]; Value=[string] } or $null.
        #>
        param([Parameter(Mandatory = $true)][string]$Text)
        try {
            $pattern = '(?m)^(\s*"export_folder"\s*:\s*)"((?:\\.|[^"\\])*)"'
            $matches = [System.Text.RegularExpressions.Regex]::Matches($Text, $pattern)
            if ($null -eq $matches -or $matches.Count -ne 1) {
                return $null
            }
            $m = $matches[0]
            return [pscustomobject]@{
                Match = $m
                Value = [string]$m.Groups[2].Value
            }
        } catch {
            return $null
        }
    }

    function ConvertTo-JsonEscapedString {
        param([Parameter(Mandatory = $true)][AllowEmptyString()][string]$Value)
        $sb = New-Object System.Text.StringBuilder
        $chars = $Value.ToCharArray()
        foreach ($ch in $chars) {
            switch ($ch) {
                '\' { [void]$sb.Append('\\'); continue }
                '"' { [void]$sb.Append('\"'); continue }
                "`b" { [void]$sb.Append('\b'); continue }
                "`f" { [void]$sb.Append('\f'); continue }
                "`n" { [void]$sb.Append('\n'); continue }
                "`r" { [void]$sb.Append('\r'); continue }
                "`t" { [void]$sb.Append('\t'); continue }
                default {
                    $code = [int][char]$ch
                    if ($code -lt 0x20) {
                        [void]$sb.AppendFormat('\u{0:x4}', $code)
                    } else {
                        [void]$sb.Append($ch)
                    }
                }
            }
        }
        return $sb.ToString()
    }

    function Get-NormalizedExportFolderCandidate {
        <#
          Expands leading ~ (~, ~/ , ~\) to $env:USERPROFILE and returns absolute normalized path.
          Returns normalized path string or $null on failure.
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

    function Read-YesNoDecision {
        <#
          Prompts for y/n. Returns 'yes', 'no', or 'ambiguous-no' (safe no-change after re-prompt).
        #>
        param([Parameter(Mandatory = $true)][string]$Prompt)
        $answer = Read-Host $Prompt
        if ($null -eq $answer) { $answer = '' }
        $normalized = ([string]$answer).Trim().ToLowerInvariant()

        if ($normalized -eq 'y' -or $normalized -eq 'yes' -or $normalized -eq 'д' -or $normalized -eq 'да') {
            return 'yes'
        }
        if ($normalized -eq '' -or $normalized -eq 'n' -or $normalized -eq 'no' -or $normalized -eq 'н' -or $normalized -eq 'нет') {
            return 'no'
        }

        Write-Host "Не удалось однозначно распознать ответ. Введите 'y' (да) или 'n' (нет)."
        $answer2 = Read-Host $Prompt
        if ($null -eq $answer2) { $answer2 = '' }
        $normalized2 = ([string]$answer2).Trim().ToLowerInvariant()

        if ($normalized2 -eq 'y' -or $normalized2 -eq 'yes' -or $normalized2 -eq 'д' -or $normalized2 -eq 'да') {
            return 'yes'
        }
        if ($normalized2 -eq '' -or $normalized2 -eq 'n' -or $normalized2 -eq 'no' -or $normalized2 -eq 'н' -or $normalized2 -eq 'нет') {
            return 'no'
        }
        return 'ambiguous-no'
    }

    # --- Подготовка: выбор целевого интерпретатора (R2) ---
    $target = Resolve-TargetPython
    if ($null -eq $target) {
        Write-StepStatus -Step 'Подготовка' -Status 'ОШИБКА' -Message "Не найден подходящий интерпретатор Python (проверялись: py -3, python, uv run python). Установите Python/uv и повторите."
        $script:FinalSuccess = $false
        return
    }
    Write-StepStatus -Step 'Подготовка' -Status 'ИНФО' -Message ("Выбран интерпретатор: {0}" -f $target.Display)

    # --- Шаг 1 (Git): R3 detect/install/update/restart-stop ---
    $step1 = 'Шаг 1 (Git)'
    $gitUsableInitially = $false
    $gitMissingInitially = $true
    $gitInstallAttempted = $false

    $gitCommandPresent = [bool](Get-Command git -ErrorAction SilentlyContinue)
    if ($gitCommandPresent) {
        $gitUsableInitially = Test-GitUsable
    } else {
        $gitUsableInitially = $false
    }
    $gitMissingInitially = -not $gitUsableInitially

    if ($gitUsableInitially) {
        Write-StepStatus -Step $step1 -Status 'ИНФО' -Message "Git обнаружен и доступен в текущей сессии (проверка: git --version)."
        $wingetAvailable = Test-WingetAvailable
        if ($wingetAvailable) {
            Write-StepStatus -Step $step1 -Status 'ИНФО' -Message "Попытка best-effort обновления Git через winget (при ошибке шаг не считается проваленным)..."
            & winget upgrade --id Git.Git -e --source winget --accept-package-agreements --accept-source-agreements
            $wgCode = $LASTEXITCODE
            if ($null -eq $wgCode) { $wgCode = 0 }
            if ($wgCode -ne 0) {
                Write-StepStatus -Step $step1 -Status 'ИНФО' -Message ("Обновление Git через winget завершилось с кодом {0}. Текущая установленная версия будет использована дальше." -f $wgCode)
                [void](Test-GitUsable)
            } else {
                Write-StepStatus -Step $step1 -Status 'ИНФО' -Message "Проверка Git после попытки обновления:"
                [void](Test-GitUsable)
            }
        } else {
            Write-StepStatus -Step $step1 -Status 'ИНФО' -Message "winget недоступен, best-effort обновление Git пропущено. Текущая версия будет использована."
        }
        Write-StepStatus -Step $step1 -Status 'УСПЕХ' -Message "Git готов к использованию."
    } else {
        Write-StepStatus -Step $step1 -Status 'ИНФО' -Message "Git отсутствует или недоступен в текущей сессии. Пытаюсь определить возможность установки."
        $wingetAvailable = Test-WingetAvailable
        if (-not $wingetAvailable) {
            Write-StepStatus -Step $step1 -Status 'ОШИБКА' -Message "Git не найден, а winget недоступен. Установите Git вручную: https://git-scm.com/download/win затем перезапустите этот скрипт."
            $script:FinalSuccess = $false
            return
        }

        Write-StepStatus -Step $step1 -Status 'ИНФО' -Message "Запуск установки Git через winget..."
        & winget install --id Git.Git -e --source winget --accept-package-agreements --accept-source-agreements
        $gitInstallAttempted = $true
        $installCode = $LASTEXITCODE
        if ($null -eq $installCode) { $installCode = 0 }
        if ($installCode -ne 0) {
            Write-StepStatus -Step $step1 -Status 'ОШИБКА' -Message ("Установка Git через winget завершилась с кодом {0}. Установите Git вручную: https://git-scm.com/download/win и повторите запуск." -f $installCode)
            $script:FinalSuccess = $false
            return
        }

        Update-SessionPathFromEnvironment
        $gitUsableAfterInstall = Test-GitUsable
        if (-not $gitUsableAfterInstall) {
            Write-StepStatus -Step $step1 -Status 'ОШИБКА' -Message "Git был установлен (или попытка установки выполнена), но в текущей сессии Git всё ещё недоступен (PATH не обновлён). Перезапустите PowerShell/этот скрипт и запустите init.ps1 снова."
            $script:FinalSuccess = $false
            return
        }

        # Git стал доступен в этой сессии, но был установлен/впервые доступен в рамках текущего запуска => conservative restart-stop
        Write-StepStatus -Step $step1 -Status 'ОШИБКА' -Message "Git установлен/стал доступен в текущей сессии. Для корректного продолжения перезапустите PowerShell и запустите init.ps1 снова. Текущий запуск завершён без выполнения Шагов 2–4."
        $script:FinalSuccess = $false
        return
    }

    # --- Шаг 2 (mediascope-api-lib): R2 pip install/upgrade in chosen env ---
    $step2 = 'Шаг 2 (mediascope-api-lib)'
    Write-StepStatus -Step $step2 -Status 'ИНФО' -Message "Установка/обновление mediascope-api-lib в выбранном окружении..."
    $codeStep2 = Invoke-TargetCommand -Exe $target.Exe -Prefix @($target.Prefix) -Arguments @('-m', 'pip', 'install', '--upgrade', 'mediascope-api-lib')
    if ($codeStep2 -ne 0) {
        Write-StepStatus -Step $step2 -Status 'ОШИБКА' -Message ("Не удалось установить/обновить mediascope-api-lib (код {0})." -f $codeStep2)
        $script:FinalSuccess = $false
        return
    }
    Write-StepStatus -Step $step2 -Status 'УСПЕХ' -Message "mediascope-api-lib установлен/обновлён."

    # --- Шаг 3 (пакет + init): R2 git+ install, init, post-init R1 probe ---
    $step3 = 'Шаг 3 (пакет + init)'
    Write-StepStatus -Step $step3 -Status 'ИНФО' -Message "Установка/обновление mko_get_mediascope_data из git-репозитория..."
    $codeStep3a = Invoke-TargetCommand -Exe $target.Exe -Prefix @($target.Prefix) -Arguments @('-m', 'pip', 'install', '--upgrade', 'git+https://github.com/manicko/mko_get_mediascope_data.git')
    if ($codeStep3a -ne 0) {
        Write-StepStatus -Step $step3 -Status 'ОШИБКА' -Message ("Не удалось установить/обновить mko_get_mediascope_data (код {0}). Убедитесь, что Git доступен и есть доступ к репозиторию." -f $codeStep3a)
        $script:FinalSuccess = $false
        return
    }

    Write-StepStatus -Step $step3 -Status 'ИНФО' -Message "Запуск инициализации настроек: -m mko_get_mediascope_data init (без --force)..."
    $codeStep3b = Invoke-TargetCommand -Exe $target.Exe -Prefix @($target.Prefix) -Arguments @('-m', 'mko_get_mediascope_data', 'init')
    if ($codeStep3b -ne 0) {
        Write-StepStatus -Step $step3 -Status 'ОШИБКА' -Message ("Команда init завершилась с кодом {0}. Инициализация настроек не подтверждена." -f $codeStep3b)
        $script:FinalSuccess = $false
        return
    }

    Write-StepStatus -Step $step3 -Status 'ИНФО' -Message "Проверка путей настроек (R1 probe, readiness/proof)..."
    $paths = Invoke-AppPathsProbe -Target $target
    if ($null -eq $paths) {
        $script:FinalSuccess = $false
        return
    }
    Write-StepStatus -Step $step3 -Status 'УСПЕХ' -Message ("Пакет и настройки готовы. app_config: {0}" -f $paths.AppConfig)

    # --- Шаг 4 (export_folder): R1 path resolution + R4 read/prompt/validate/safe edit/confirm ---
    $step4 = 'Шаг 4 (export_folder)'
    $configPath = [string]$paths.AppConfig
    try {
        $configText = [System.IO.File]::ReadAllText($configPath, [System.Text.Encoding]::UTF8)
    } catch {
        Write-StepStatus -Step $step4 -Status 'ОШИБКА' -Message ("Не удалось прочитать app_config.json5: {0}. Файл не изменён." -f $_.Exception.Message)
        $script:FinalSuccess = $false
        return
    }

    $exportMatch = Read-ExportFolderValue -Text $configText
    if ($null -eq $exportMatch) {
        Write-StepStatus -Step $step4 -Status 'ОШИБКА' -Message "Не удалось однозначно определить поле export_folder в app_config.json5 (отсутствует, дублируется или небезопасно для точечного изменения). Файл не изменён."
        $script:FinalSuccess = $false
        return
    }

    $currentExportRaw = [string]$exportMatch.Value
    Write-StepStatus -Step $step4 -Status 'ИНФО' -Message ("Текущий export_folder: {0}" -f $currentExportRaw)

    $decision = Read-YesNoDecision -Prompt "Хотите изменить export_folder? (y/n)"
    if ($decision -ne 'yes') {
        if ($decision -eq 'ambiguous-no') {
            Write-StepStatus -Step $step4 -Status 'ИНФО' -Message "Ответ не распознан однозначно, безопасно принято решение не менять export_folder."
        }
        Write-StepStatus -Step $step4 -Status 'УСПЕХ' -Message "export_folder оставлен без изменений."
        $script:FinalSuccess = $true
        return
    }

    $newPathInput = Read-Host "Введите новый путь для export_folder"
    if ($null -eq $newPathInput) { $newPathInput = '' }
    $newPathInput = ([string]$newPathInput).Trim()
    if ($newPathInput -eq '') {
        Write-StepStatus -Step $step4 -Status 'ОШИБКА' -Message "Путь export_folder не указан (пустой ввод). Файл не изменён."
        $script:FinalSuccess = $false
        return
    }

    $candidatePath = Get-NormalizedExportFolderCandidate -InputPath $newPathInput
    if ($null -eq $candidatePath -or [string]::IsNullOrWhiteSpace($candidatePath)) {
        Write-StepStatus -Step $step4 -Status 'ОШИБКА' -Message "Не удалось вычислить абсолютный нормализованный путь export_folder. Файл не изменён."
        $script:FinalSuccess = $false
        return
    }

    if (-not (Test-Path -LiteralPath $candidatePath -PathType Container)) {
        Write-StepStatus -Step $step4 -Status 'ОШИБКА' -Message ("Указанная папка не существует: {0}. Создание папок не выполняется. Файл не изменён." -f $candidatePath)
        $script:FinalSuccess = $false
        return
    }

    $escapedCandidate = ConvertTo-JsonEscapedString -Value $candidatePath
    try {
        $m = $exportMatch.Match
        $valueGroup = $m.Groups[2]
        $before = $configText.Substring(0, $valueGroup.Index)
        $after = $configText.Substring($valueGroup.Index + $valueGroup.Length)
        $newConfigText = $before + $escapedCandidate + $after
    } catch {
        Write-StepStatus -Step $step4 -Status 'ОШИБКА' -Message ("Не удалось подготовить безопасное точечное изменение export_folder: {0}. Файл не изменён." -f $_.Exception.Message)
        $script:FinalSuccess = $false
        return
    }

    try {
        $utf8NoBom = $null
        try {
            $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
        } catch {
            $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
        }
        [System.IO.File]::WriteAllText($configPath, $newConfigText, $utf8NoBom)
    } catch {
        Write-StepStatus -Step $step4 -Status 'ОШИБКА' -Message ("Не удалось записать app_config.json5: {0}. Успех не подтверждён." -f $_.Exception.Message)
        $script:FinalSuccess = $false
        return
    }

    try {
        $confirmedText = [System.IO.File]::ReadAllText($configPath, [System.Text.Encoding]::UTF8)
        $confirmedMatch = Read-ExportFolderValue -Text $confirmedText
        if ($null -eq $confirmedMatch) {
            Write-StepStatus -Step $step4 -Status 'ОШИБКА' -Message "После записи не удалось однозначно подтвердить поле export_folder. Успех не подтверждён."
            $script:FinalSuccess = $false
            return
        }
        $confirmedRaw = [string]$confirmedMatch.Value
        if ($confirmedRaw -ne $escapedCandidate) {
            Write-StepStatus -Step $step4 -Status 'ОШИБКА' -Message "Подтверждение после записи не прошло: сохранённое значение export_folder отличается от ожидаемого."
            $script:FinalSuccess = $false
            return
        }
    } catch {
        Write-StepStatus -Step $step4 -Status 'ОШИБКА' -Message ("Не удалось перечитать/подтвердить app_config.json5 после записи: {0}. Успех не подтверждён." -f $_.Exception.Message)
        $script:FinalSuccess = $false
        return
    }

    Write-StepStatus -Step $step4 -Status 'УСПЕХ' -Message ("export_folder обновлён и подтверждён: {0}" -f $candidatePath)
    $script:FinalSuccess = $true
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