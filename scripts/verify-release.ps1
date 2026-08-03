[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $LinerStarterSource,

    [Parameter(Mandatory = $true)]
    [string] $LinerDtoSource
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$botRoot = [System.IO.Path]::GetFullPath(
    (Resolve-Path -LiteralPath (Split-Path -Parent $PSScriptRoot)).Path
)
$starterRoot = [System.IO.Path]::GetFullPath(
    (Resolve-Path -LiteralPath $LinerStarterSource).Path
)
$dtoRoot = [System.IO.Path]::GetFullPath(
    (Resolve-Path -LiteralPath $LinerDtoSource).Path
)
$runIdentity = (
    (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ") + "-" +
    [System.Guid]::NewGuid().ToString("N").Substring(0, 8)
)
$reportRoot = [System.IO.Path]::GetFullPath(
    (Join-Path (
        Join-Path $botRoot "build\reports\release-acceptance"
    ) $runIdentity)
)
[void] [System.IO.Directory]::CreateDirectory($reportRoot)
$reportPath = Join-Path $reportRoot "release-evidence.txt"

$ImageName = "binance-futures-level-breakout-bot:acceptance-$runIdentity"
$containerName = "breakout-bot-acceptance-" +
    [System.Guid]::NewGuid().ToString("N").Substring(0, 12)
$temporaryRoot = Join-Path (
    [System.IO.Path]::GetTempPath()
) ("breakout-bot-release-" + [System.Guid]::NewGuid().ToString("N"))
$contextPath = Join-Path $temporaryRoot "context"
$certificatePath = Join-Path $temporaryRoot "release-test.p12"
$auditPath = Join-Path $temporaryRoot "audit"
$keyStorePassword = "release-acceptance-keystore-password"

$evidence = [System.Collections.Generic.List[string]]::new()
$areaResults = [ordered]@{
    "31.1 Level management" = "NOT RUN"
    "31.2 Entry and protection" = "NOT RUN"
    "31.3 Strategy" = "NOT RUN"
    "31.4 Stops and take profits" = "NOT RUN"
    "31.5 Risk" = "NOT RUN"
    "31.6 Failure handling" = "NOT RUN"
    "31.7 Operations" = "NOT RUN"
}
$areaSuites = [ordered]@{
    "31.1 Level management" = @(
        "com.scalpsecta.breakoutbot.level.LevelServiceTest",
        "com.scalpsecta.breakoutbot.RuntimeLifecycleIntegrationTest"
    )
    "31.2 Entry and protection" = @(
        "com.scalpsecta.breakoutbot.execution.PreEntryExecutionServiceTest",
        "com.scalpsecta.breakoutbot.execution.BreakoutExecutionServiceTest",
        "com.scalpsecta.breakoutbot.execution.ExecutionServiceTest"
    )
    "31.3 Strategy" = @(
        "com.scalpsecta.breakoutbot.signal.SignalEngineTest",
        "com.scalpsecta.breakoutbot.level.BreakoutStateMachineTest"
    )
    "31.4 Stops and take profits" = @(
        "com.scalpsecta.breakoutbot.risk.AttemptRiskServiceTest",
        "com.scalpsecta.breakoutbot.execution.ExecutionServiceTest",
        "com.scalpsecta.breakoutbot.execution.BreakoutExecutionServiceTest"
    )
    "31.5 Risk" = @(
        "com.scalpsecta.breakoutbot.risk.AttemptRiskServiceTest",
        "com.scalpsecta.breakoutbot.risk.DailyRiskControlServiceTest",
        "com.scalpsecta.breakoutbot.level.SymbolCooldownsTest"
    )
    "31.6 Failure handling" = @(
        "com.scalpsecta.breakoutbot.execution.ExecutionServiceTest",
        "com.scalpsecta.breakoutbot.failure.SafeModeServiceTest",
        "com.scalpsecta.breakoutbot.marketdata.PublicMarketDataServiceTest"
    )
    "31.7 Operations" = @(
        "com.scalpsecta.breakoutbot.RuntimeLifecycleIntegrationTest",
        "com.scalpsecta.breakoutbot.PackagingStagingTest",
        "com.scalpsecta.breakoutbot.LocalDependencyResolutionTest",
        "com.scalpsecta.breakoutbot.OperatorSecurityIntegrationTest"
    )
}
$requiredLayers = [ordered]@{
    "unit and stable contracts" = @(
        "com.scalpsecta.breakoutbot.ReleaseAcceptanceContractTest",
        "com.scalpsecta.breakoutbot.signal.SignalEngineTest"
    )
    "deterministic state machines" = @(
        "com.scalpsecta.breakoutbot.level.BreakoutStateMachineTest"
    )
    "exchange adapters and transport guard" = @(
        "com.scalpsecta.breakoutbot.binance.LiveAuthenticatedBinanceClientTest",
        "com.scalpsecta.breakoutbot.binance.AutomatedVerificationBinanceTransportGuardTest"
    )
    "offline replay" = @(
        "com.scalpsecta.breakoutbot.replay.RecordedAttemptReplayTest",
        "com.scalpsecta.breakoutbot.replay.ScriptedFakeExchangeTest"
    )
    "security" = @(
        "com.scalpsecta.breakoutbot.OperatorSecurityIntegrationTest"
    )
    "packaging and lifecycle" = @(
        "com.scalpsecta.breakoutbot.PackagingStagingTest",
        "com.scalpsecta.breakoutbot.RuntimeLifecycleIntegrationTest"
    )
}
$acceptedRisks = @(
    "Live-only rollout: no Binance testnet exists; acceptance relies on fakes, replay, and an egress-isolated container.",
    "Restart resets the temporary daily-risk anchor before the next 03:00 UTC boundary.",
    "Startup does not reconcile or adopt old positions and orders.",
    "Pre-entry does not perform a general account exposure check; unmanaged One-way Mode exposure can merge.",
    "Levels are memory-only and disappear on restart or crash.",
    "Shutdown sends no Binance action; exchange-side orders and positions can outlive the process.",
    "Market gaps and slippage can exceed planned risk; STOP_MARKET cannot guarantee a 1% realized-loss ceiling.",
    "Self-signed TLS requires every operator device to trust and protect the private CA.",
    "Single-IP access requires firewall changes whenever the operator public IP changes.",
    "Strategy rules do not establish or guarantee positive expected value or profitability."
)

$containerCreated = $false
$imageBuilt = $false
$testsPassed = $false
$beforeRepositories = $null
$failure = $null

function Write-Evidence {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string] $Message
    )

    Write-Host $Message
    $evidence.Add($Message)
}

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)][string] $Description,
        [Parameter(Mandatory = $true)][string] $Executable,
        [Parameter(Mandatory = $true)][string[]] $Arguments
    )

    Write-Evidence "[RUN ] $Description"
    & $Executable @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE"
    }
    Write-Evidence "[PASS] $Description"
}

function Invoke-GitCapture {
    param(
        [Parameter(Mandatory = $true)][string] $Repository,
        [Parameter(Mandatory = $true)][string[]] $Arguments
    )

    $output = @(& git -C $Repository @Arguments 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "git failed for $Repository with arguments $($Arguments -join ' ')"
    }
    return ($output | ForEach-Object { $_.ToString() }) -join "`n"
}

function Get-RepositoryEvidence {
    param([Parameter(Mandatory = $true)][string] $Repository)

    $head = Invoke-GitCapture -Repository $Repository -Arguments @(
        "rev-parse", "HEAD"
    )
    $status = Invoke-GitCapture -Repository $Repository -Arguments @(
        "status", "--porcelain=v1", "--untracked-files=all"
    )
    $workingDiff = Invoke-GitCapture -Repository $Repository -Arguments @(
        "diff", "--no-ext-diff", "--binary"
    )
    $stagedDiff = Invoke-GitCapture -Repository $Repository -Arguments @(
        "diff", "--cached", "--no-ext-diff", "--binary"
    )
    $untracked = Invoke-GitCapture -Repository $Repository -Arguments @(
        "ls-files", "--others", "--exclude-standard"
    )
    $untrackedHashes = [System.Collections.Generic.List[string]]::new()
    foreach ($relativePath in ($untracked -split "`n")) {
        if ([string]::IsNullOrWhiteSpace($relativePath)) {
            continue
        }
        $file = Join-Path $Repository $relativePath
        if (Test-Path -LiteralPath $file -PathType Leaf) {
            $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $file).Hash
            $untrackedHashes.Add("$relativePath=$hash")
        }
    }
    $payload = @(
        "HEAD=$head",
        "STATUS=$status",
        "WORKING=$workingDiff",
        "STAGED=$stagedDiff",
        "UNTRACKED=$($untrackedHashes -join ';')"
    ) -join "`n"
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($payload)
    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    try {
        $digest = $sha256.ComputeHash($bytes)
    } finally {
        $sha256.Dispose()
    }
    $fingerprint = ($digest | ForEach-Object {
        $_.ToString("x2")
    }) -join ""
    return [pscustomobject]@{
        Root = $Repository
        Head = $head.Trim()
        Fingerprint = $fingerprint
        StatusEntries = @($status -split "`n" | Where-Object {
            -not [string]::IsNullOrWhiteSpace($_)
        }).Count
    }
}

function Get-ReferenceRepositoryEvidence {
    return @(
        Get-RepositoryEvidence -Repository $starterRoot
        Get-RepositoryEvidence -Repository $dtoRoot
    )
}

function Assert-ReferenceRepositoriesUnchanged {
    param(
        [Parameter(Mandatory = $true)][object[]] $Before,
        [Parameter(Mandatory = $true)][object[]] $After
    )

    foreach ($beforeRepository in $Before) {
        $afterRepository = $After | Where-Object {
            $_.Root -eq $beforeRepository.Root
        } | Select-Object -First 1
        if ($beforeRepository.Fingerprint -ne $afterRepository.Fingerprint) {
            throw (
                "Reference repository changed during verification: " +
                    $beforeRepository.Root
            )
        }
    }
}

function Assert-AutomatedTestEvidence {
    $resultRoot = Join-Path $botRoot "build\test-results\test"
    $resultFiles = @(Get-ChildItem -LiteralPath $resultRoot -Filter "TEST-*.xml")
    if ($resultFiles.Count -eq 0) {
        throw "Gradle produced no JUnit XML test evidence"
    }

    $suiteNames = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    $testCount = 0
    $skippedCount = 0
    foreach ($resultFile in $resultFiles) {
        [xml] $result = Get-Content -Raw -Encoding UTF8 -LiteralPath (
            $resultFile.FullName
        )
        $suite = $result.testsuite
        [void] $suiteNames.Add([string] $suite.name)
        $testCount += [int] $suite.tests
        $skippedCount += [int] $suite.skipped
        if ([int] $suite.failures -ne 0 -or [int] $suite.errors -ne 0) {
            throw "JUnit evidence contains failures for $($suite.name)"
        }
    }
    if ($skippedCount -ne 0) {
        throw "Release verification cannot accept $skippedCount skipped tests"
    }

    foreach ($layer in $requiredLayers.GetEnumerator()) {
        $missing = @($layer.Value | Where-Object {
            -not $suiteNames.Contains($_)
        })
        if ($missing.Count -ne 0) {
            throw "Missing $($layer.Key) evidence: $($missing -join ', ')"
        }
        Write-Evidence "[PASS] Automated $($layer.Key) evidence"
    }

    foreach ($area in $areaSuites.GetEnumerator()) {
        $missing = @($area.Value | Where-Object {
            -not $suiteNames.Contains($_)
        })
        if ($missing.Count -ne 0) {
            throw "Missing $($area.Key) evidence: $($missing -join ', ')"
        }
        if ($area.Key -ne "31.7 Operations") {
            $areaResults[$area.Key] = "PASS"
        }
    }
    Write-Evidence "[PASS] Complete automated suite: $testCount tests, no skips"
}

function Assert-SanitizedContext {
    param([Parameter(Mandatory = $true)][string] $Context)

    $forbiddenDirectoryNames = @(
        ".git", ".hg", ".svn", ".idea", ".vscode", ".vs", ".gradle",
        ".gradle-user-home", ".kotlin", ".m2", ".ssh", ".aws",
        ".gnupg", ".agents", ".codex", ".scratch", "build", "out",
        "target", "node_modules", "audit", "audits", "audit-data",
        "event-data", "certs", "certificates", "credentials", "secrets",
        "tls", "logs"
    )
    $forbiddenExtensions = @(
        ".p12", ".pfx", ".pem", ".key", ".crt", ".cer", ".jks",
        ".keystore", ".jsonl", ".log", ".db", ".sqlite", ".sqlite3",
        ".kdbx", ".gpg", ".secret", ".secrets"
    )
    $forbiddenFileNames = @(
        ".netrc", ".npmrc", ".pypirc", "credentials.json", "secrets.json",
        "settings.xml", "id_rsa", "id_dsa", "id_ecdsa", "id_ed25519",
        "local.properties", "application-local.yml", "application-local.yaml"
    )

    $contextPrefix = [System.IO.Path]::GetFullPath($Context).TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    ) + [System.IO.Path]::DirectorySeparatorChar
    foreach ($file in Get-ChildItem -Force -Recurse -File -LiteralPath $Context) {
        $relative = $file.FullName.Substring($contextPrefix.Length)
        $segments = $relative -split '[\\/]'
        if ($segments | Where-Object { $_ -in $forbiddenDirectoryNames }) {
            throw "Staged context contains forbidden directory material: $relative"
        }
        if ($file.Extension -in $forbiddenExtensions) {
            throw "Staged context contains forbidden secret file: $relative"
        }
        if ($file.Name -in $forbiddenFileNames) {
            throw "Staged context contains forbidden credential file: $relative"
        }
        $isEnvironmentFile =
            $file.Name -eq ".env" -or
            $file.Name.StartsWith(".env.") -or
            ($file.Name.EndsWith(".env") -and
                -not $file.Name.EndsWith(".env.example"))
        if ($isEnvironmentFile) {
            throw "Staged context contains populated environment file: $relative"
        }
        $content = [System.IO.File]::ReadAllText($file.FullName)
        if ($content -match '-----BEGIN [A-Z ]*PRIVATE KEY-----') {
            throw "Staged context contains PEM private-key material: $relative"
        }
    }

    $environmentExample = Join-Path (
        Join-Path $Context "bot"
    ) "deployment\runtime.env.example"
    $exampleValues = @{}
    foreach ($line in Get-Content -Encoding UTF8 -LiteralPath $environmentExample) {
        if ($line -match '^([^=]+)=(.*)$') {
            $exampleValues[$Matches[1]] = $Matches[2]
        }
    }
    foreach ($secretName in @(
        "BINANCE_API_KEY", "BINANCE_API_SECRET", "BOT_BASIC_USERNAME",
        "BOT_BASIC_PASSWORD", "TLS_KEYSTORE_PASSWORD"
    )) {
        if (-not $exampleValues.ContainsKey($secretName)) {
            throw "runtime.env.example omits $secretName"
        }
        if (-not [string]::IsNullOrEmpty($exampleValues[$secretName])) {
            throw "runtime.env.example contains a value for $secretName"
        }
    }
}

function New-TestCertificate {
    $keytool = (Get-Command keytool -ErrorAction Stop).Source
    Invoke-Checked `
        -Description "Test PKCS#12 certificate generation" `
        -Executable $keytool `
        -Arguments @(
            "-genkeypair", "-alias", "release-acceptance", "-keyalg", "RSA",
            "-keysize", "2048", "-storetype", "PKCS12", "-keystore",
            $certificatePath, "-storepass", $keyStorePassword, "-keypass",
            $keyStorePassword, "-dname", "CN=127.0.0.1", "-validity", "1",
            "-ext", "SAN=ip:127.0.0.1", "-noprompt"
        )
}

function Wait-ForHealthyContainer {
    for ($attempt = 1; $attempt -le 60; $attempt++) {
        $health = @(
            & docker inspect --format '{{.State.Status}} {{.State.Health.Status}}' `
                $containerName 2>&1
        ) -join ""
        if ($LASTEXITCODE -ne 0) {
            throw "Could not inspect release-acceptance container"
        }
        if ($health -eq "running healthy") {
            return
        }
        if ($health.StartsWith("exited ") -or $health.StartsWith("dead ")) {
            throw "Release-acceptance container stopped before HTTPS liveness"
        }
        Start-Sleep -Seconds 2
    }
    throw "Release-acceptance container did not reach HTTPS liveness in 120 seconds"
}

function Assert-ContainerIsolationAndMounts {
    $networkMode = @(
        & docker inspect --format '{{.HostConfig.NetworkMode}}' $containerName 2>&1
    ) -join ""
    if ($LASTEXITCODE -ne 0 -or $networkMode -ne "none") {
        throw "Release-acceptance container is not isolated with --network none"
    }

    $mountJson = @(
        & docker inspect --format '{{json .Mounts}}' $containerName 2>&1
    ) -join ""
    if ($LASTEXITCODE -ne 0) {
        throw "Could not inspect release-acceptance mounts"
    }
    $mounts = $mountJson | ConvertFrom-Json
    $certificateMount = $mounts | Where-Object {
        $_.Destination -eq "/run/tls/release-test.p12"
    } | Select-Object -First 1
    $auditMount = $mounts | Where-Object {
        $_.Destination -eq "/var/lib/breakout-bot/audit"
    } | Select-Object -First 1
    if ($null -eq $certificateMount -or $certificateMount.RW) {
        throw "Test certificate is not mounted read-only"
    }
    if ($null -eq $auditMount -or -not $auditMount.RW) {
        throw "Audit directory is not mounted read-write"
    }
}

function Remove-TemporaryRoot {
    if (-not (Test-Path -LiteralPath $temporaryRoot)) {
        return
    }
    $candidate = [System.IO.Path]::GetFullPath($temporaryRoot)
    $systemTemporary = [System.IO.Path]::GetFullPath(
        [System.IO.Path]::GetTempPath()
    ).TrimEnd([System.IO.Path]::DirectorySeparatorChar) +
        [System.IO.Path]::DirectorySeparatorChar
    $leaf = Split-Path -Leaf $candidate
    if (
        -not $candidate.StartsWith(
            $systemTemporary,
            [System.StringComparison]::OrdinalIgnoreCase
        ) -or
        -not $leaf.StartsWith("breakout-bot-release-")
    ) {
        throw "Refusing to remove unexpected temporary path: $candidate"
    }
    [System.IO.Directory]::Delete($candidate, $true)
}

Write-Evidence "Release acceptance run $runIdentity"
Write-Evidence "Evidence report: $reportPath"

try {
    $beforeRepositories = Get-ReferenceRepositoryEvidence
    foreach ($repository in $beforeRepositories) {
        Write-Evidence (
            "[BASE] $($repository.Root) HEAD=$($repository.Head) " +
                "fingerprint=$($repository.Fingerprint) " +
                "statusEntries=$($repository.StatusEntries)"
        )
    }

    $gradle = Join-Path $botRoot "gradlew.bat"
    Invoke-Checked `
        -Description "Complete Gradle automated suite with live-trading guard" `
        -Executable $gradle `
        -Arguments @(
            "cleanTest", "test", "--no-daemon",
            "-PlinerStarterJar=$starterRoot\build\libs\liner-spring-boot-starter-1.1.0.jar",
            "-PlinerDtoJar=$dtoRoot\build\libs\liner-dto-1.0.0.jar"
        )
    Assert-AutomatedTestEvidence
    $testsPassed = $true

    [void] [System.IO.Directory]::CreateDirectory($temporaryRoot)
    [void] [System.IO.Directory]::CreateDirectory($auditPath)
    Invoke-Checked `
        -Description "Immutable three-repository staging" `
        -Executable "powershell.exe" `
        -Arguments @(
            "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
            "-File", (Join-Path $PSScriptRoot "package-image.ps1"),
            "-BotSource", $botRoot,
            "-LinerStarterSource", $starterRoot,
            "-LinerDtoSource", $dtoRoot,
            "-ContextDirectory", $contextPath,
            "-StageOnly"
        )
    Assert-SanitizedContext -Context $contextPath
    Write-Evidence "[PASS] Staged context excludes repository metadata and secret material"

    Invoke-Checked `
        -Description "Docker daemon availability" `
        -Executable "docker" `
        -Arguments @("info", "--format", "{{.ServerVersion}}")
    Invoke-Checked `
        -Description "Staged single-image Docker build" `
        -Executable "docker" `
        -Arguments @(
            "build", "--file", (Join-Path $contextPath "Dockerfile"),
            "--tag", $ImageName, $contextPath
        )
    $imageBuilt = $true

    New-TestCertificate
    $containerId = @(
        & docker run --detach --name $containerName --network none `
            --env "BINANCE_API_KEY=release-acceptance-dummy-key" `
            --env "BINANCE_API_SECRET=release-acceptance-dummy-secret" `
            --env "BOT_BASIC_USERNAME=release-acceptance-operator" `
            --env "BOT_BASIC_PASSWORD=release-acceptance-basic-password" `
            --env "TLS_KEYSTORE_PATH=/run/tls/release-test.p12" `
            --env "TLS_KEYSTORE_PASSWORD=$keyStorePassword" `
            --env "AUDIT_DIRECTORY=/var/lib/breakout-bot/audit" `
            --mount "type=bind,src=$certificatePath,dst=/run/tls/release-test.p12,readonly" `
            --mount "type=bind,src=$auditPath,dst=/var/lib/breakout-bot/audit" `
            $ImageName 2>&1
    ) -join ""
    if ($LASTEXITCODE -ne 0) {
        throw "Egress-isolated container start failed"
    }
    $containerCreated = $true
    Write-Evidence "[PASS] Egress-isolated container started: $containerId"

    Wait-ForHealthyContainer
    Write-Evidence "[PASS] Container reached authenticated HTTPS liveness"
    Assert-ContainerIsolationAndMounts
    Write-Evidence "[PASS] Container uses no network, read-only TLS, and read-write audit mounts"
    Invoke-Checked `
        -Description "HTTPS readiness remains trading-blocked" `
        -Executable "docker" `
        -Arguments @(
            "exec", $containerName, "java", "-cp",
            "/app/liveness-probe.jar", "LivenessProbe",
            "--expect-trading-blocked"
        )

    $afterRepositories = Get-ReferenceRepositoryEvidence
    Assert-ReferenceRepositoriesUnchanged `
        -Before $beforeRepositories `
        -After $afterRepositories
    Write-Evidence "[PASS] liner-starter and liner-dto fingerprints are unchanged"
    $areaResults["31.7 Operations"] = "PASS"
} catch {
    $failure = $_
    Write-Evidence "[FAIL] $($_.Exception.Message)"
} finally {
    if ($null -ne $beforeRepositories) {
        try {
            $finalRepositories = Get-ReferenceRepositoryEvidence
            Assert-ReferenceRepositoriesUnchanged `
                -Before $beforeRepositories `
                -After $finalRepositories
        } catch {
            if ($null -eq $failure) {
                $failure = $_
            }
            Write-Evidence "[FAIL] $($_.Exception.Message)"
        }
    }

    if ($containerCreated) {
        & docker rm --force $containerName | Out-Null
    }
    if ($imageBuilt) {
        & docker image rm $ImageName | Out-Null
    }
    try {
        Remove-TemporaryRoot
    } catch {
        if ($null -eq $failure) {
            $failure = $_
        }
        Write-Evidence "[FAIL] $($_.Exception.Message)"
    }

    if ($null -ne $failure) {
        foreach ($area in @($areaResults.Keys)) {
            if ($areaResults[$area] -ne "PASS") {
                $areaResults[$area] = "FAIL"
            }
        }
        if (-not $testsPassed) {
            foreach ($area in @($areaResults.Keys)) {
                $areaResults[$area] = "FAIL"
            }
        }
    }

    Write-Evidence ""
    Write-Evidence "PRD section 31 results"
    foreach ($area in $areaResults.GetEnumerator()) {
        Write-Evidence "[$($area.Value)] $($area.Key)"
    }
    Write-Evidence ""
    Write-Evidence "Accepted operational risks from PRD section 32"
    for ($index = 0; $index -lt $acceptedRisks.Count; $index++) {
        Write-Evidence "$($index + 1). $($acceptedRisks[$index])"
    }
    Write-Evidence ""
    Write-Evidence (
        "This evidence demonstrates bounded engineering behavior only. It " +
            "does not guarantee profitability, gap-loss limits, or recovery " +
            "of exchange state outside the approved startup/shutdown scope."
    )
    [System.IO.File]::WriteAllLines(
        $reportPath,
        $evidence,
        [System.Text.UTF8Encoding]::new($false)
    )
}

if ($null -ne $failure) {
    throw "Release acceptance failed; see $reportPath"
}

Write-Host "Release acceptance passed; see $reportPath"
