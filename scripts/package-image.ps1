[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $LinerStarterSource,

    [Parameter(Mandatory = $true)]
    [string] $LinerDtoSource,

    [string] $ImageName = "binance-futures-level-breakout-bot:local",

    [string] $BotSource,

    [string] $ContextDirectory,

    [switch] $StageOnly,

    [switch] $NoCache
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($BotSource)) {
    $BotSource = Split-Path -Parent $PSScriptRoot
}

$excludedDirectoryNames = [System.Collections.Generic.HashSet[string]]::new(
    [System.StringComparer]::OrdinalIgnoreCase
)
@(
    ".git",
    ".hg",
    ".svn",
    ".idea",
    ".vscode",
    ".vs",
    ".gradle",
    ".gradle-user-home",
    ".kotlin",
    ".m2",
    ".ssh",
    ".aws",
    ".gnupg",
    "build",
    "out",
    "target",
    "node_modules",
    ".agents",
    ".codex",
    ".scratch"
) | ForEach-Object { [void] $excludedDirectoryNames.Add($_) }

$excludedRootDataDirectories = [System.Collections.Generic.HashSet[string]]::new(
    [System.StringComparer]::OrdinalIgnoreCase
)
@(
    "audit",
    "audits",
    "audit-data",
    "event-data",
    "certs",
    "certificates",
    "credentials",
    "secrets",
    "tls",
    "logs"
) | ForEach-Object { [void] $excludedRootDataDirectories.Add($_) }

$excludedFileNames = [System.Collections.Generic.HashSet[string]]::new(
    [System.StringComparer]::OrdinalIgnoreCase
)
@(
    ".netrc",
    ".npmrc",
    ".pypirc",
    "credentials.json",
    "secrets.json",
    "settings.xml",
    "id_rsa",
    "id_dsa",
    "id_ecdsa",
    "id_ed25519",
    "local.properties",
    "application-local.yml",
    "application-local.yaml",
    "Thumbs.db",
    ".DS_Store"
) | ForEach-Object { [void] $excludedFileNames.Add($_) }

$excludedFileExtensions = [System.Collections.Generic.HashSet[string]]::new(
    [System.StringComparer]::OrdinalIgnoreCase
)
@(
    ".iml",
    ".iws",
    ".ipr",
    ".p12",
    ".pfx",
    ".pem",
    ".key",
    ".crt",
    ".cer",
    ".jks",
    ".keystore",
    ".jsonl",
    ".log",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".kdbx",
    ".gpg",
    ".secret",
    ".secrets"
) | ForEach-Object { [void] $excludedFileExtensions.Add($_) }

function Resolve-SourceDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,

        [Parameter(Mandatory = $true)]
        [string] $Description
    )

    $resolved = Resolve-Path -LiteralPath $Path -ErrorAction Stop
    if (-not (Test-Path -LiteralPath $resolved.Path -PathType Container)) {
        throw "$Description is not a directory: $Path"
    }
    return [System.IO.Path]::GetFullPath($resolved.Path).TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )
}

function Test-PathInside {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Candidate,

        [Parameter(Mandatory = $true)]
        [string] $Root
    )

    if ($Candidate.Equals($Root, [System.StringComparison]::OrdinalIgnoreCase)) {
        return $true
    }
    $prefix = $Root + [System.IO.Path]::DirectorySeparatorChar
    return $Candidate.StartsWith(
        $prefix,
        [System.StringComparison]::OrdinalIgnoreCase
    )
}

function Test-ExcludedFile {
    param(
        [Parameter(Mandatory = $true)]
        [System.IO.FileInfo] $File
    )

    if ($excludedFileNames.Contains($File.Name)) {
        return $true
    }
    $isEnvironmentExample = $File.Name.EndsWith(
        ".env.example",
        [System.StringComparison]::OrdinalIgnoreCase
    )
    if (-not $isEnvironmentExample -and (
        $File.Name.Equals(".env", [System.StringComparison]::OrdinalIgnoreCase) -or
        $File.Name.StartsWith(".env.", [System.StringComparison]::OrdinalIgnoreCase) -or
        $File.Name.EndsWith(".env", [System.StringComparison]::OrdinalIgnoreCase) -or
        $File.Name.IndexOf(
            ".env.",
            [System.StringComparison]::OrdinalIgnoreCase
        ) -ge 0
    )) {
        return $true
    }
    if ($File.Name.EndsWith(".jsonl.gz", [System.StringComparison]::OrdinalIgnoreCase)) {
        return $true
    }
    return $excludedFileExtensions.Contains($File.Extension)
}

function Copy-SourceSnapshot {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Source,

        [Parameter(Mandatory = $true)]
        [string] $Destination
    )

    [void] [System.IO.Directory]::CreateDirectory($Destination)
    $pending = [System.Collections.Generic.Queue[object]]::new()
    $pending.Enqueue(@($Source, $Destination, 0))

    while ($pending.Count -gt 0) {
        $current = $pending.Dequeue()
        $currentSource = [string] $current[0]
        $currentDestination = [string] $current[1]
        $depth = [int] $current[2]

        foreach ($item in Get-ChildItem -Force -LiteralPath $currentSource) {
            if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                continue
            }
            if ($item.PSIsContainer) {
                if ($excludedDirectoryNames.Contains($item.Name)) {
                    continue
                }
                if ($depth -eq 0 -and $excludedRootDataDirectories.Contains($item.Name)) {
                    continue
                }
                $childDestination = Join-Path $currentDestination $item.Name
                [void] [System.IO.Directory]::CreateDirectory($childDestination)
                $pending.Enqueue(@($item.FullName, $childDestination, $depth + 1))
                continue
            }
            if (Test-ExcludedFile -File $item) {
                continue
            }
            [System.IO.File]::Copy(
                $item.FullName,
                (Join-Path $currentDestination $item.Name),
                $false
            )
        }
    }
}

$botSourcePath = Resolve-SourceDirectory -Path $BotSource -Description "Bot source"
$starterSourcePath = Resolve-SourceDirectory `
    -Path $LinerStarterSource `
    -Description "liner-starter source"
$dtoSourcePath = Resolve-SourceDirectory `
    -Path $LinerDtoSource `
    -Description "liner-dto source"

if ([string]::IsNullOrWhiteSpace($ContextDirectory)) {
    $contextPath = Join-Path (
        [System.IO.Path]::GetTempPath()
    ) ("breakout-bot-docker-" + [System.Guid]::NewGuid().ToString("N"))
    $removeContextWhenFinished = -not $StageOnly
} else {
    $contextPath = [System.IO.Path]::GetFullPath($ContextDirectory)
    $removeContextWhenFinished = $false
}

foreach ($sourcePath in @($botSourcePath, $starterSourcePath, $dtoSourcePath)) {
    if (Test-PathInside -Candidate $contextPath -Root $sourcePath) {
        throw "Docker context must be outside every source repository: $contextPath"
    }
}
if (Test-Path -LiteralPath $contextPath) {
    throw "Docker context already exists: $contextPath"
}

$contextCreated = $false
try {
    [void] [System.IO.Directory]::CreateDirectory($contextPath)
    $contextCreated = $true
    Copy-SourceSnapshot `
        -Source $botSourcePath `
        -Destination (Join-Path $contextPath "bot")
    Copy-SourceSnapshot `
        -Source $starterSourcePath `
        -Destination (Join-Path $contextPath "liner-starter")
    Copy-SourceSnapshot `
        -Source $dtoSourcePath `
        -Destination (Join-Path $contextPath "liner-dto")

    $stagedBotPath = Join-Path $contextPath "bot"
    $stagedDockerfile = Join-Path $stagedBotPath "Dockerfile"
    $stagedDockerignore = Join-Path $stagedBotPath ".dockerignore"
    if (-not (Test-Path -LiteralPath $stagedDockerfile -PathType Leaf)) {
        throw "Bot snapshot does not contain Dockerfile"
    }
    if (-not (Test-Path -LiteralPath $stagedDockerignore -PathType Leaf)) {
        throw "Bot snapshot does not contain .dockerignore"
    }
    [System.IO.File]::Copy(
        $stagedDockerfile,
        (Join-Path $contextPath "Dockerfile"),
        $false
    )
    [System.IO.File]::Copy(
        $stagedDockerignore,
        (Join-Path $contextPath ".dockerignore"),
        $false
    )

    if ($StageOnly) {
        Write-Output $contextPath
        return
    }

    $docker = Get-Command docker -ErrorAction Stop
    $dockerArguments = @(
        "build",
        "--file",
        (Join-Path $contextPath "Dockerfile"),
        "--tag",
        $ImageName
    )
    if ($NoCache) {
        $dockerArguments += "--no-cache"
    }
    $dockerArguments += $contextPath

    & $docker.Source @dockerArguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker build failed with exit code $LASTEXITCODE"
    }
    Write-Output "Built Docker image $ImageName"
} finally {
    if ($contextCreated -and $removeContextWhenFinished) {
        [System.IO.Directory]::Delete($contextPath, $true)
    }
}
