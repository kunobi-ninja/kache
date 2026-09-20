# Runs under Windows PowerShell 5.1, before actions that require pwsh.
$ErrorActionPreference = 'Stop'

function Add-ToolPath([string] $Directory) {
    $env:PATH = "$Directory;$env:PATH"
    # Windows PowerShell's default UTF-16 encoding is invalid for GITHUB_PATH.
    $Directory | Out-File -FilePath $env:GITHUB_PATH -Encoding utf8 -Append
}

if (-not (Get-Command nasm -ErrorAction SilentlyContinue)) {
    $nasm = Join-Path $env:ProgramFiles 'NASM'
    if (-not (Test-Path "$nasm\nasm.exe")) {
        throw "NASM is missing: $nasm\nasm.exe"
    }
    Add-ToolPath $nasm
}

if (-not (Get-Command clang -ErrorAction SilentlyContinue) -or
    -not (Get-Command clang-cl -ErrorAction SilentlyContinue)) {
    $vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio\Installer\vswhere.exe'
    if (Test-Path $vswhere) {
        $llvm = & $vswhere -latest -products '*' -find 'VC\Tools\Llvm\x64\bin\clang-cl.exe'
        if ($LASTEXITCODE -ne 0) { throw 'Visual Studio LLVM discovery failed' }
        if ($llvm) { Add-ToolPath (Split-Path ($llvm | Select-Object -First 1)) }
    }
}

if (-not (Get-Command pwsh -ErrorAction SilentlyContinue)) {
    $installed = Join-Path $env:ProgramFiles 'PowerShell\7\pwsh.exe'
    if (Test-Path $installed) {
        Add-ToolPath (Split-Path $installed)
    } else {
        # Job-local ZIP installation; no machine-wide PATH or service changes.
        $version = '7.6.6'
        $digest = '02fe458be20493fbdf43f61ea20610b811ee6c738ab1676c61b9cfcd1a33c860'
        $directory = Join-Path $env:RUNNER_TEMP "powershell-$version"
        $archive = "$directory.zip"
        & curl.exe --fail --location --retry 3 --output $archive "https://github.com/PowerShell/PowerShell/releases/download/v$version/PowerShell-$version-win-x64.zip"
        if ($LASTEXITCODE -ne 0) { throw 'PowerShell download failed' }
        if ((Get-FileHash -Algorithm SHA256 $archive).Hash -ne $digest) {
            throw 'PowerShell archive SHA256 mismatch'
        }
        Expand-Archive -Path $archive -DestinationPath $directory -Force
        Remove-Item $archive
        Add-ToolPath $directory
    }
}

foreach ($tool in @('cc', 'c++', 'make', 'nasm', 'clang', 'clang-cl', 'pwsh')) {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        throw "Missing build tool on PATH: $tool"
    }
}
& pwsh -NoLogo -NoProfile -Command '$PSVersionTable.PSVersion.ToString()'
if ($LASTEXITCODE -ne 0) { throw 'PowerShell 7 validation failed' }
