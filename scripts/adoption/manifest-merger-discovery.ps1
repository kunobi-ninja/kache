param([Parameter(Mandatory = $true)][string]$OutputDirectory)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
if (Test-Path -LiteralPath $OutputDirectory) {
    if (@(Get-ChildItem -LiteralPath $OutputDirectory -Force).Count -ne 0) {
        throw 'OutputDirectory must be empty to exclude stale probe evidence.'
    }
} else { [IO.Directory]::CreateDirectory($OutputDirectory) | Out-Null }
$utf8 = [Text.UTF8Encoding]::new($false)
$work = Join-Path ([IO.Path]::GetTempPath()) ('kache-merger-discovery-' + [guid]::NewGuid())
[IO.Directory]::CreateDirectory($work) | Out-Null
$previousDirectory = Get-Location
$originalPath = [Environment]::GetEnvironmentVariable('PATH', 'Process')
$originalProbeLog = [Environment]::GetEnvironmentVariable('KACHE_MT_PROBE_LOG', 'Process')
$implicitOptionNames = @('CL', '_CL_', 'LINK', '_LINK_')
$originalImplicitOptions = @{}
foreach ($name in $implicitOptionNames) { $originalImplicitOptions[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
$results = [Collections.Generic.List[object]]::new()
$metadata = [ordered]@{ originalPath = $originalPath; workDirectory = $work }

function Write-Json([string]$Name, [object]$Value) {
    [IO.File]::WriteAllText((Join-Path $OutputDirectory $Name), (ConvertTo-Json -InputObject $Value -Depth 10), $utf8)
}
function Path-Applications([string]$Name) {
    return @(Get-Command -Name $Name -CommandType Application -All -ErrorAction SilentlyContinue | ForEach-Object { $_.Source })
}
function Tool-Details([string]$Path) {
    if ([string]::IsNullOrEmpty($Path) -or -not (Test-Path -LiteralPath $Path -PathType Leaf)) { return $null }
    $version = [Diagnostics.FileVersionInfo]::GetVersionInfo($Path)
    return [pscustomobject]@{
        path = $Path; fileVersion = $version.FileVersion; productVersion = $version.ProductVersion
        productName = $version.ProductName; sha256 = (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash
    }
}
function Inventory([string]$Directory) {
    return @(Get-ChildItem -LiteralPath $Directory -File -Recurse | ForEach-Object {
        [pscustomobject]@{
            relativePath = [IO.Path]::GetRelativePath($Directory, $_.FullName)
            length = $_.Length; sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash
        }
    })
}
function Native-Link {
    foreach ($path in $metadata.originalPathLink) {
        $detail = Tool-Details $path
        if ($null -ne $detail -and $detail.productName -match 'Microsoft.*Visual') { return $path }
    }
    $programFiles = [Environment]::GetEnvironmentVariable('ProgramFiles(x86)', 'Process')
    if ([string]::IsNullOrEmpty($programFiles)) { throw 'ProgramFiles(x86) is unavailable.' }
    $vswhere = Join-Path $programFiles 'Microsoft Visual Studio/Installer/vswhere.exe'
    if (-not (Test-Path -LiteralPath $vswhere -PathType Leaf)) { throw 'The hosted VM has no vswhere.exe.' }
    $installation = (& $vswhere -latest -products '*' -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath 2>&1 | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrEmpty($installation)) { throw 'Cannot locate the native MSVC toolchain.' }
    $metadata.visualStudioInstallation = $installation
    $defaultFile = Join-Path $installation 'VC/Auxiliary/Build/Microsoft.VCToolsVersion.default.txt'
    $toolset = [IO.File]::ReadAllText($defaultFile).Trim()
    $metadata.defaultMsvcToolset = $toolset
    $link = Join-Path $installation ('VC/Tools/MSVC/' + $toolset + '/bin/Hostx64/x64/link.exe')
    if (-not (Test-Path -LiteralPath $link -PathType Leaf)) { throw 'The selected x64 MSVC linker is absent.' }
    return $link
}
function Sdk-Mergers {
    $roots = [Collections.Generic.List[string]]::new()
    foreach ($registry in @('HKLM:\SOFTWARE\Microsoft\Windows Kits\Installed Roots', 'HKLM:\SOFTWARE\WOW6432Node\Microsoft\Windows Kits\Installed Roots')) {
        $property = Get-ItemProperty -LiteralPath $registry -Name KitsRoot10 -ErrorAction SilentlyContinue
        if ($null -ne $property) { $roots.Add([string]$property.KitsRoot10) }
    }
    $programFiles = [Environment]::GetEnvironmentVariable('ProgramFiles(x86)', 'Process')
    if (-not [string]::IsNullOrEmpty($programFiles)) { $roots.Add((Join-Path $programFiles 'Windows Kits/10')) }
    $found = [Collections.Generic.List[string]]::new()
    foreach ($root in @($roots | Select-Object -Unique)) {
        $bin = Join-Path $root 'bin'
        if (Test-Path -LiteralPath $bin -PathType Container) {
            foreach ($directory in Get-ChildItem -LiteralPath $bin -Directory) {
                $candidate = Join-Path $directory.FullName 'x64/mt.exe'
                if (Test-Path -LiteralPath $candidate -PathType Leaf) { $found.Add($candidate) }
            }
        }
    }
    return @($found | Sort-Object -Unique | ForEach-Object { Tool-Details $_ })
}
function Compile-Case([string]$Name, [string]$Linker, [string]$Flavor, [bool]$InjectStub, [bool]$Direct = $false) {
    $case = Join-Path $OutputDirectory $Name
    [IO.Directory]::CreateDirectory($case) | Out-Null
    $env:PATH = if ($InjectStub) { $stubDirectory + [IO.Path]::PathSeparator + $originalPath } else { $originalPath }
    $stubLog = Join-Path $case 'mt-stub.log'
    $env:KACHE_MT_PROBE_LOG = $stubLog
    $exe = Join-Path $case 'application.exe'
    $program = if ($Direct) { $Linker } else { $rustc }
    $arguments = if ($Direct) {
        $prefix = @()
        if ($Flavor -eq 'lld-link') { $prefix = @('-flavor', 'link') }
        $prefix + @(
            '/NOLOGO', '/NODEFAULTLIB', '/ENTRY:mainCRTStartup', '/SUBSYSTEM:CONSOLE',
            '/MACHINE:X64', '/INCREMENTAL:NO', '/MANIFEST:EMBED',
            ('/MANIFESTINPUT:' + $manifest), ('/OUT:' + $exe), $object
        )
    } else {
        @(
            '--crate-name', 'manifest_merger_fixture', '--edition=2024', '--crate-type=bin',
            (Join-Path $work 'application.rs'), '--emit=link', '-Cdebuginfo=0',
            ('-Clinker=' + $Linker), ('-Clinker-flavor=' + $Flavor),
            '-Clink-arg=/MANIFEST:EMBED', ('-Clink-arg=/MANIFESTINPUT:' + $manifest), '-o', $exe
        )
    }
    $before = @(Path-Applications 'mt.exe' | ForEach-Object { Tool-Details $_ })
    & $program @arguments *> (Join-Path $case 'compile.log')
    $exitCode = $LASTEXITCODE
    $logged = (Test-Path -LiteralPath $stubLog -PathType Leaf) -and (Get-Item -LiteralPath $stubLog).Length -gt 0
    $finding = if ($logged -and $exitCode -ne 0) {
        'PATH search observed: the mt stub logged its invocation and linking failed.'
    } elseif ($logged) {
        'PATH search observed: the mt stub logged its invocation; inspect the linker result.'
    } else {
        'Inconclusive: no stub invocation was logged; this does not prove absence of PATH search.'
    }
    $result = [pscustomobject]@{
        name = $Name; linker = $Linker; linkerFlavor = $Flavor; injectedStub = $InjectStub
        program = $program; directLink = $Direct
        resolvedMtOnCasePath = $before; arguments = $arguments; exitCode = $exitCode
        stubLogPresent = (Test-Path -LiteralPath $stubLog -PathType Leaf); stubInvocationLogged = $logged
        applicationPresent = (Test-Path -LiteralPath $exe -PathType Leaf); finding = $finding
        files = @(Inventory $case)
    }
    $results.Add($result)
    [IO.File]::WriteAllText((Join-Path $case 'result.json'), (ConvertTo-Json -InputObject $result -Depth 10), $utf8)
}

try {
    foreach ($name in $implicitOptionNames) { [Environment]::SetEnvironmentVariable($name, $null, 'Process') }
    Set-Location $work
    $rustc = (Get-Command rustc -CommandType Application -ErrorAction Stop).Source
    $rustup = Get-Command rustup -CommandType Application -ErrorAction SilentlyContinue
    if ($null -ne $rustup) {
        $rustc = (& $rustup.Source which rustc 2>&1 | Out-String).Trim()
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $rustc -PathType Leaf)) { throw 'Cannot resolve native rustc.' }
    }
    $compiler = (& $rustc -vV 2>&1 | Out-String)
    [IO.File]::WriteAllText((Join-Path $OutputDirectory 'rustc-version.txt'), $compiler, $utf8)
    if ($LASTEXITCODE -ne 0 -or $compiler -notmatch 'host: x86_64-pc-windows-msvc') { throw 'This probe requires native x64 Windows MSVC rustc.' }
    $metadata.rustc = Tool-Details $rustc
    $metadata.originalPathLink = @(Path-Applications 'link.exe')
    $metadata.originalPathMt = @(Path-Applications 'mt.exe')
    $metadata.originalMtDetails = @($metadata.originalPathMt | ForEach-Object { Tool-Details $_ })
    $metadata.sdkMtCandidates = @(Sdk-Mergers)
    $nativeLink = Native-Link
    $metadata.selectedNativeLink = Tool-Details $nativeLink
    $cl = Join-Path (Split-Path -Parent $nativeLink) 'cl.exe'
    if (-not (Test-Path -LiteralPath $cl -PathType Leaf)) { throw 'The pinned linker has no sibling cl.exe.' }
    $metadata.selectedCl = Tool-Details $cl
    $sysroot = (& $rustc --print sysroot 2>&1 | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect the compiler sysroot.' }
    $lld = Join-Path $sysroot 'lib/rustlib/x86_64-pc-windows-msvc/bin/rust-lld.exe'
    if (-not (Test-Path -LiteralPath $lld -PathType Leaf)) { throw 'The native sysroot has no rust-lld.exe.' }
    $metadata.selectedRustLld = Tool-Details $lld
    & $lld -flavor link --version *> (Join-Path $OutputDirectory 'rust-lld-version.txt')
    $metadata.rustLldVersionExitCode = $LASTEXITCODE
    $stubDirectory = Join-Path $work 'stub'
    [IO.Directory]::CreateDirectory($stubDirectory) | Out-Null
    $stubSource = @'
use std::io::Write;
fn main() {
    if let Some(path) = std::env::var_os("KACHE_MT_PROBE_LOG") {
        if let Ok(mut log) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
            let _ = writeln!(log, "current_exe={:?}", std::env::current_exe());
            for (index, argument) in std::env::args_os().enumerate() {
                let _ = writeln!(log, "argv[{index}]={argument:?}");
            }
            let _ = log.flush();
        }
    }
    std::process::exit(42);
}
'@
    $stubSourcePath = Join-Path $work 'mt.rs'
    [IO.File]::WriteAllText($stubSourcePath, $stubSource, $utf8)
    [IO.File]::WriteAllText((Join-Path $OutputDirectory 'mt-stub.rs'), $stubSource, $utf8)
    & $rustc --crate-name mt_probe --edition=2024 --crate-type=bin --emit=link -Cdebuginfo=0 ('-Clinker=' + $nativeLink) ('-Clinker-flavor=msvc') $stubSourcePath -o (Join-Path $stubDirectory 'mt.exe') *> (Join-Path $OutputDirectory 'stub-build.log')
    $metadata.stubBuildExitCode = $LASTEXITCODE
    if ($LASTEXITCODE -ne 0) { throw 'Cannot build the maintainer mt.exe fixture.' }
    $metadata.stub = Tool-Details (Join-Path $stubDirectory 'mt.exe')
    [IO.File]::WriteAllText((Join-Path $work 'application.rs'), 'fn main() {}', $utf8)
    $manifest = Join-Path $work 'absolute-input.manifest'
    $xml = '<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="1.0"><assemblyIdentity name="Kache.MergerPathProbe" version="1.0.0.0" type="win32"/><description>KACHE_MT_PATH_PROBE</description></assembly>'
    [IO.File]::WriteAllText($manifest, $xml, $utf8)
    [IO.File]::WriteAllText((Join-Path $OutputDirectory 'input.manifest'), $xml, $utf8)
    # A header-free object removes rustc's SDK/PATH rewriting from the direct-link controls.
    $cSource = 'void mainCRTStartup(void) {}'
    $cPath = Join-Path $work 'entry.c'
    $object = Join-Path $work 'entry.obj'
    [IO.File]::WriteAllText($cPath, $cSource, $utf8)
    [IO.File]::WriteAllText((Join-Path $OutputDirectory 'entry.c'), $cSource, $utf8)
    & $cl /nologo /c /Zl /GS- ('/Fo' + $object) $cPath *> (Join-Path $OutputDirectory 'cl-build.log')
    $metadata.clBuildExitCode = $LASTEXITCODE
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $object -PathType Leaf)) { throw 'Cannot build the header-free direct-link fixture.' }
    $metadata.directObject = [pscustomobject]@{ path = $object; length = (Get-Item -LiteralPath $object).Length; sha256 = (Get-FileHash -LiteralPath $object -Algorithm SHA256).Hash }
    Copy-Item -LiteralPath $object -Destination (Join-Path $OutputDirectory 'entry.obj')
    Compile-Case 'direct-link-normal-path' $nativeLink 'msvc' $false $true
    Compile-Case 'direct-link-stub-first' $nativeLink 'msvc' $true $true
    Compile-Case 'direct-rust-lld-normal-path' $lld 'lld-link' $false $true
    Compile-Case 'direct-rust-lld-stub-first' $lld 'lld-link' $true $true
    Compile-Case 'link-normal-path' $nativeLink 'msvc' $false
    Compile-Case 'link-stub-first' $nativeLink 'msvc' $true
    Compile-Case 'rust-lld-normal-path' $lld 'lld-link' $false
    Compile-Case 'rust-lld-stub-first' $lld 'lld-link' $true
} finally {
    [Environment]::SetEnvironmentVariable('PATH', $originalPath, 'Process')
    [Environment]::SetEnvironmentVariable('KACHE_MT_PROBE_LOG', $originalProbeLog, 'Process')
    foreach ($name in $implicitOptionNames) { [Environment]::SetEnvironmentVariable($name, $originalImplicitOptions[$name], 'Process') }
    Write-Json 'metadata.json' $metadata
    Write-Json 'results.json' ($results.ToArray())
    Set-Location $previousDirectory
    # All removed paths belong to this invocation's fresh GUID directory.
    Remove-Item -LiteralPath $work -Recurse -Force
}
