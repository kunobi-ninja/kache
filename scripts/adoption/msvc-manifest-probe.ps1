param(
    [Parameter(Mandatory = $true)][string]$OutputDirectory,
    [string]$KachePath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

# This probe compiles only its own empty Rust program. An optional, already
# built Kache executable enables XML and merger-tool cache controls.
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
if ($KachePath) { $KachePath = (Get-Item -LiteralPath $KachePath).FullName }

# LOAD_LIBRARY_AS_DATAFILE | LOAD_LIBRARY_AS_IMAGE_RESOURCE maps resources
# without running an entry point or resolving imports. Inspect RT_MANIFEST ID 1,
# rather than finding a marker that might occur elsewhere in the executable.
Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
public static class KacheManifestResource {
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = true)]
    static extern IntPtr LoadLibraryExW(string path, IntPtr file, uint flags);
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = true)]
    static extern IntPtr FindResourceW(IntPtr module, IntPtr name, IntPtr type);
    [DllImport("kernel32.dll", SetLastError = true)]
    static extern uint SizeofResource(IntPtr module, IntPtr resource);
    [DllImport("kernel32.dll", SetLastError = true)]
    static extern IntPtr LoadResource(IntPtr module, IntPtr resource);
    [DllImport("kernel32.dll", SetLastError = true)]
    static extern IntPtr LockResource(IntPtr resource);
    [DllImport("kernel32.dll")]
    static extern bool FreeLibrary(IntPtr module);
    public static byte[] Read(string path) {
        IntPtr module = LoadLibraryExW(path, IntPtr.Zero, 0x22);
        if (module == IntPtr.Zero) throw new Win32Exception();
        try {
            IntPtr resource = FindResourceW(module, new IntPtr(1), new IntPtr(24));
            if (resource == IntPtr.Zero) throw new Win32Exception();
            uint size = SizeofResource(module, resource);
            if (size == 0 || size > 1048576) throw new Exception("Invalid RT_MANIFEST size");
            IntPtr data = LoadResource(module, resource);
            if (data == IntPtr.Zero) throw new Win32Exception();
            IntPtr bytes = LockResource(data);
            if (bytes == IntPtr.Zero) throw new Win32Exception();
            byte[] result = new byte[(int)size];
            Marshal.Copy(bytes, result, 0, result.Length);
            return result;
        } finally { FreeLibrary(module); }
    }
}
public static class KacheProbeEnvironment {
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = true)]
    static extern bool SetEnvironmentVariableW(string name, string value);
    public static void Set(string name, string value) {
        if (!SetEnvironmentVariableW(name, value)) throw new Win32Exception();
    }
    public static System.Collections.Generic.Dictionary<string, string> Capture() {
        var result = new System.Collections.Generic.Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (System.Collections.DictionaryEntry entry in Environment.GetEnvironmentVariables(EnvironmentVariableTarget.Process)) {
            result[(string)entry.Key] = (string)entry.Value;
        }
        return result;
    }
    public static void Restore(System.Collections.Generic.Dictionary<string, string> saved) {
        var current = Capture();
        foreach (var entry in current) {
            if (!saved.ContainsKey(entry.Key)) Set(entry.Key, null);
        }
        foreach (var entry in saved) {
            string value;
            if (!current.TryGetValue(entry.Key, out value) || value != entry.Value) Set(entry.Key, entry.Value);
        }
    }
}
'@

function Read-ManifestResource([string]$Exe, [string]$Name) {
    [byte[]]$bytes = [KacheManifestResource]::Read($Exe)
    [IO.File]::WriteAllBytes((Join-Path $OutputDirectory ($Name + '.RT_MANIFEST.xml')), $bytes)
    $stream = [IO.MemoryStream]::new($bytes, $false)
    $reader = [IO.StreamReader]::new($stream, [Text.UTF8Encoding]::new($false, $true), $true)
    try { return $reader.ReadToEnd() } finally { $reader.Dispose() }
}
$rustc = (Get-Command rustc -ErrorAction Stop).Source
$compiler = (& $rustc -vV 2>&1 | Out-String)
if ($LASTEXITCODE -ne 0 -or $compiler -notmatch 'host: .*windows-msvc') {
    throw 'The probe requires a native Windows MSVC Rust toolchain.'
}
# Resolve rustup's shim before changing directories, so every invocation uses
# the same toolchain whose version is recorded above.
$rustup = Get-Command rustup -ErrorAction SilentlyContinue
if ($null -ne $rustup) {
    $resolved = (& $rustup.Source which rustc 2>&1 | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $resolved)) {
        throw 'Could not resolve the active rustc executable.'
    }
    $rustc = $resolved
    $compiler = (& $rustc -vV 2>&1 | Out-String)
    if ($LASTEXITCODE -ne 0) { throw 'Could not inspect the resolved rustc.' }
}

function Publish-Notice([string]$Title, [string]$Message) {
    $escaped = $Message.Replace('%', '%25').Replace("`r", '%0D').Replace("`n", '%0A')
    Write-Host ('::notice title={0}::{1}' -f $Title, $escaped)
}
Publish-Notice 'Manifest probe compiler' ($rustc + "`n" + $compiler)

$root = Join-Path ([IO.Path]::GetTempPath()) ('kache-manifest-' + [guid]::NewGuid())
[IO.Directory]::CreateDirectory($root) | Out-Null
[IO.Directory]::CreateDirectory($OutputDirectory) | Out-Null
$utf8 = [Text.UTF8Encoding]::new($false)
$results = [Collections.Generic.List[object]]::new()
$previousDirectory = Get-Location

function Write-Utf8([string]$Name, [string]$Text) {
    [IO.File]::WriteAllText((Join-Path $root $Name), $Text, $utf8)
}

function Invoke-Case([string]$Name, [string]$Xml, [bool]$Unicode = $false, [bool]$ExplicitEmbed = $true, [bool]$BigEndian = $false) {
    $manifest = Join-Path $root 'input.manifest'
    $encoding = if ($BigEndian) { [Text.Encoding]::BigEndianUnicode } elseif ($Unicode) { [Text.Encoding]::Unicode } else { $utf8 }
    [IO.File]::WriteAllText($manifest, $Xml, $encoding)
    $exe = Join-Path $root ($Name + '.exe')
    $arguments = @(
        '--crate-name', 'manifest_probe', (Join-Path $root 'probe.rs'),
        '-C', 'debuginfo=0'
    )
    if ($ExplicitEmbed) { $arguments += @('-C', 'link-arg=/MANIFEST:EMBED') }
    $arguments += @('-C', ('link-arg=/MANIFESTINPUT:' + $manifest), '-o', $exe)
    $log = Join-Path $OutputDirectory ($Name + '.log')
    $before = @(Get-ChildItem -LiteralPath $root -File | ForEach-Object { $_.Name })
    & $rustc @arguments *> $log
    $exitCode = $LASTEXITCODE
    $text = ''
    $resourceError = $null
    if ($exitCode -eq 0) {
        try { $text = Read-ManifestResource $exe $Name }
        catch { $resourceError = $_.Exception.Message }
    }
    $created = @(Get-ChildItem -LiteralPath $root -File | Where-Object { $_.Name -notin $before } | ForEach-Object { $_.Name })
    $result = [pscustomobject]@{
        name = $Name; exitCode = $exitCode
        explicitEmbed = $ExplicitEmbed; resourceType = 24; resourceId = 1
        resourceError = $resourceError; createdFiles = $created
        markerV1 = $text.Contains('KACHE_MANIFEST_V1')
        markerV2 = $text.Contains('KACHE_MANIFEST_V2')
    }
    $results.Add($result)
    Publish-Notice $Name ($result | ConvertTo-Json -Compress)
    if ($exitCode -ne 0) {
        $diagnostic = [IO.File]::ReadAllText($log)
        Publish-Notice ($Name + ' diagnostic') ($diagnostic.Substring(0, [Math]::Min(4000, $diagnostic.Length)))
    }
    return $result
}

function Write-ProbeJson([string]$Name, [object]$Value) {
    [IO.File]::WriteAllText((Join-Path $OutputDirectory $Name), (ConvertTo-Json -InputObject $Value -Depth 10), $utf8)
}

function Probe-Tool([string]$Name) {
    $commands = @(Get-Command -Name $Name -CommandType Application -All -ErrorAction SilentlyContinue)
    if ($commands.Count -eq 0) { throw ('Developer environment has no ' + $Name) }
    $path = $commands[0].Source
    $version = [Diagnostics.FileVersionInfo]::GetVersionInfo($path)
    return [pscustomobject]@{
        path = $path; sha256 = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash
        fileVersion = $version.FileVersion; productVersion = $version.ProductVersion
    }
}

function Initialize-ProbeDeveloperEnvironment {
    $architecture = if ($compiler -match 'host: x86_64-pc-windows-msvc') { 'x64' }
        elseif ($compiler -match 'host: aarch64-pc-windows-msvc') { 'arm64' }
        else { throw 'Cache tool controls require native x64 or ARM64 MSVC Rust.' }
    $component = if ($architecture -eq 'arm64') { 'Microsoft.VisualStudio.Component.VC.Tools.ARM64' }
        else { 'Microsoft.VisualStudio.Component.VC.Tools.x86.x64' }
    $programFiles = [Environment]::GetEnvironmentVariable('ProgramFiles(x86)', 'Process')
    if ([string]::IsNullOrEmpty($programFiles)) { throw 'ProgramFiles(x86) is unavailable.' }
    $vswhere = Join-Path $programFiles 'Microsoft Visual Studio/Installer/vswhere.exe'
    if (-not (Test-Path -LiteralPath $vswhere -PathType Leaf)) { throw 'The installed Visual Studio discovery tool is absent.' }
    $installation = (& $vswhere -latest -products '*' -requires $component -property installationPath 2>&1 | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrEmpty($installation)) { throw ('Cannot locate installed Visual Studio for ' + $architecture) }
    $developerCommand = [IO.Path]::GetFullPath((Join-Path $installation 'Common7/Tools/VsDevCmd.bat'))
    if (-not (Test-Path -LiteralPath $developerCommand -PathType Leaf)) { throw 'Installed VsDevCmd.bat is absent.' }
    # Only the installed batch path enters cmd's command text. Reject shell
    # expansion characters; the arguments and && set suffix are constants.
    if ($developerCommand -match '["%!^&|<>\r\n\x00]') { throw 'VsDevCmd.bat path contains unsupported cmd expansion characters.' }
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = Join-Path ([Environment]::GetFolderPath([Environment+SpecialFolder]::System)) 'cmd.exe'
    # /s removes this outer quote pair, leaving the quoted batch path intact.
    $start.Arguments = '/d /u /s /c ""' + $developerCommand + '" -no_logo -arch=' + $architecture + ' -host_arch=' + $architecture + ' >nul && set"'
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.StandardOutputEncoding = [Text.Encoding]::Unicode
    $start.StandardErrorEncoding = [Text.Encoding]::Unicode
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $start
    try {
        if (-not $process.Start()) { throw 'Could not start the installed developer command prompt.' }
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit(120000)) {
            $process.Kill($true)
            $process.WaitForExit(10000) | Out-Null
            throw 'Developer command prompt exceeded two minutes.'
        }
        $environmentText = $stdout.GetAwaiter().GetResult()
        $diagnostic = $stderr.GetAwaiter().GetResult()
        [IO.File]::WriteAllText((Join-Path $OutputDirectory 'cache-vsdevcmd.stderr'), $diagnostic, $utf8)
        if ($process.ExitCode -ne 0) { throw ('VsDevCmd.bat failed with exit code ' + $process.ExitCode) }
        # SET output stays in memory: it can contain unrelated runner secrets.
        # Ignore cmd's hidden drive variables; only ordinary tool environment
        # variables are imported. Empty values are retained by the native API.
        foreach ($line in ($environmentText -split "`r?`n")) {
            $separator = $line.IndexOf('=')
            if ($separator -gt 0) {
                [KacheProbeEnvironment]::Set($line.Substring(0, $separator), $line.Substring($separator + 1))
            }
        }
    } finally { $process.Dispose() }
    if ([string]::IsNullOrEmpty($env:VCINSTALLDIR) -or $env:VSCMD_ARG_TGT_ARCH -ne $architecture) {
        throw 'VsDevCmd did not establish VCINSTALLDIR and the requested native target.'
    }
    return [pscustomobject]@{
        installation = $installation; command = $developerCommand
        commandSha256 = (Get-FileHash -LiteralPath $developerCommand -Algorithm SHA256).Hash
        hostArchitecture = $env:VSCMD_ARG_HOST_ARCH; targetArchitecture = $env:VSCMD_ARG_TGT_ARCH
        vcInstallDir = $env:VCINSTALLDIR
    }
}

function Invoke-CacheControls([string]$Body) {
    $savedEnvironment = [KacheProbeEnvironment]::Capture()
    $records = [Collections.Generic.List[object]]::new()
    $cache = Join-Path $root 'cache'
    $runtime = Join-Path $root 'runtime'
    $out = Join-Path $root 'target/debug/deps'
    $config = Join-Path $root 'config.toml'
    $overlay = Join-Path ([IO.Path]::GetTempPath()) ('kache-mt-overlay-' + [guid]::NewGuid())
    $overlayMt = Join-Path $overlay 'mt.exe'
    $daemonStartAttempted = $false
    try {
        $developer = Initialize-ProbeDeveloperEnvironment
        # Clear inherited Kache overrides before introducing fixture config.
        foreach ($entry in @([KacheProbeEnvironment]::Capture().GetEnumerator())) {
            if ($entry.Key -match '^(KACHE_|RUSTC_WRAPPER$|RUSTC_WORKSPACE_WRAPPER$|RUSTFLAGS$|RUSTDOCFLAGS$|CL$|_CL_$|LINK$|_LINK_$)') {
                [KacheProbeEnvironment]::Set($entry.Key, $null)
            }
        }
        foreach ($directory in @($cache, $runtime, $out, $overlay)) { [IO.Directory]::CreateDirectory($directory) | Out-Null }
        $cacheToml = $cache.Replace('\', '\\').Replace('"', '\"')
        $runtimeToml = $runtime.Replace('\', '\\').Replace('"', '\"')
        [IO.File]::WriteAllText($config, @"
[cache]
local_only = true
ignore_env = true
native_runtime_warmup = false
cache_executables = true
local_store = "$cacheToml"
runtime_dir = "$runtimeToml"
auto_gc = false
index_auto_compact = false
scheduler = false
daemon_publish = false
prefetch_enabled = false
min_store_compile_ms = 0
trust_daemon_execution = false
"@, $utf8)
        [KacheProbeEnvironment]::Set('KACHE_CONFIG', $config)
        [KacheProbeEnvironment]::Set('KACHE_HOST_CONFIG', '')
        $realMt = Probe-Tool 'mt.exe'
        $realLink = Probe-Tool 'link.exe'
        $realCl = Probe-Tool 'cl.exe'
        if ($overlay -eq $root -or $overlay -eq (Split-Path -Parent $realLink.path)) { throw 'Merger overlay is not isolated from cwd/linker.' }
        [byte[]]$originalMtBytes = [IO.File]::ReadAllBytes($realMt.path)
        [IO.File]::WriteAllBytes($overlayMt, $originalMtBytes)
        $originalOverlayHash = (Get-FileHash -LiteralPath $overlayMt -Algorithm SHA256).Hash
        if ($originalOverlayHash -ne $realMt.sha256) { throw 'Copied merger bytes differ from the installed SDK tool.' }
        $developerPath = [Environment]::GetEnvironmentVariable('PATH', 'Process')
        $sdkBin = Split-Path -Parent $realMt.path
        if ($sdkBin -notin @($developerPath -split ';')) { $developerPath = $developerPath + ';' + $sdkBin }
        [KacheProbeEnvironment]::Set('PATH', ($overlay + ';' + $developerPath))
        if ((Probe-Tool 'mt.exe').path -ne $overlayMt) { throw 'The owned overlay did not become the selected merger.' }
        $currentEnvironment = [KacheProbeEnvironment]::Capture()
        $changedNames = @($currentEnvironment.Keys | Where-Object { -not $savedEnvironment.ContainsKey($_) -or $savedEnvironment[$_] -cne $currentEnvironment[$_] } | Sort-Object)
        Write-ProbeJson 'cache-tool-setup.json' ([pscustomobject]@{
            developer = $developer; realMt = $realMt; realLink = $realLink; realCl = $realCl
            overlayDirectory = $overlay; overlayMt = $overlayMt; originalOverlaySha256 = $originalOverlayHash
            sdkBinRetained = $sdkBin; changedEnvironmentNames = $changedNames
        })
        $manifest = Join-Path $root 'cache-input.manifest'
        [IO.File]::WriteAllText($manifest, ($Body -f 'KACHE_MANIFEST_V1'), $utf8)
        $exe = Join-Path $out 'manifest_cache_probe.exe'
        $source = Join-Path $root 'probe.rs'
        $sourceHash = (Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash
        $v1ManifestHash = (Get-FileHash -LiteralPath $manifest -Algorithm SHA256).Hash
        $arguments = @(
            $rustc, '--crate-name', 'manifest_cache_probe', '--crate-type=bin',
            '--emit=link,dep-info', '--out-dir', $out, '-Cdebuginfo=0',
            '-Clink-arg=/MANIFEST:EMBED', ('-Clink-arg=/MANIFESTINPUT:' + $manifest), $source
        )
        # Starting the fixture daemon creates its runtime/index children now,
        # before the cold key's cwd/PATH absence guards are allowed to settle.
        $daemonStartAttempted = $true
        & $KachePath daemon start *> (Join-Path $OutputDirectory 'cache-daemon-start.log')
        if ($LASTEXITCODE -ne 0) { throw 'The fixture-owned daemon failed to start.' }
        $coldKey = $null
        $coldOutputs = $null
        foreach ($stage in @('cold', 'warm', 'edit', 'revert', 'tool-edit', 'tool-revert')) {
            if ($stage -in @('cold', 'edit', 'revert')) {
                $marker = if ($stage -eq 'edit') { 'KACHE_MANIFEST_V2' } else { 'KACHE_MANIFEST_V1' }
                [IO.File]::WriteAllText($manifest, ($Body -f $marker), $utf8)
                Start-Sleep -Milliseconds 2100
            } elseif ($stage -eq 'tool-edit') {
                [byte[]]$editedMt = [byte[]]::new($originalMtBytes.Length + 1)
                [Array]::Copy($originalMtBytes, $editedMt, $originalMtBytes.Length)
                $editedMt[$originalMtBytes.Length] = 0
                [IO.File]::WriteAllBytes($overlayMt, $editedMt)
                Start-Sleep -Milliseconds 2100
            } elseif ($stage -eq 'tool-revert') {
                [IO.File]::WriteAllBytes($overlayMt, $originalMtBytes)
                Start-Sleep -Milliseconds 2100
            }
            Get-ChildItem -LiteralPath $out -File | Remove-Item -Force
            $tools = [pscustomobject]@{ mt = (Probe-Tool 'mt.exe'); link = (Probe-Tool 'link.exe'); cl = (Probe-Tool 'cl.exe') }
            $overlayHash = (Get-FileHash -LiteralPath $overlayMt -Algorithm SHA256).Hash
            $manifestHash = (Get-FileHash -LiteralPath $manifest -Algorithm SHA256).Hash
            if ($tools.mt.path -ne $overlayMt -or $tools.mt.sha256 -ne $overlayHash) { throw ('Overlay selection changed: ' + $stage) }
            if ($stage -eq 'tool-edit') {
                if ($overlayHash -eq $originalOverlayHash) { throw 'The merger overlay edit did not change its hash.' }
            } elseif ($overlayHash -ne $originalOverlayHash) { throw ('Original merger bytes were not restored: ' + $stage) }
            if ($stage -ne 'edit' -and $manifestHash -ne $v1ManifestHash) { throw ('Original XML bytes changed: ' + $stage) }
            if ((Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash -ne $sourceHash) { throw ('Probe source changed: ' + $stage) }
            $stageInput = [pscustomobject]@{
                stage = $stage; cache = $cache; cwd = $root; arguments = $arguments
                effectivePath = [Environment]::GetEnvironmentVariable('PATH', 'Process')
                tools = $tools; overlaySha256 = $overlayHash; manifestSha256 = $manifestHash; sourceSha256 = $sourceHash
            }
            Write-ProbeJson ('cache-' + $stage + '-inputs.json') $stageInput
            & $KachePath @arguments *> (Join-Path $OutputDirectory ('cache-' + $stage + '.log'))
            $exitCode = $LASTEXITCODE
            $outputs = @(Get-ChildItem -LiteralPath $out -File | Sort-Object Name | ForEach-Object {
                [pscustomobject]@{ name = $_.Name; length = $_.Length; sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash }
            })
            Write-ProbeJson ('cache-' + $stage + '-outputs.json') $outputs
            if ($exitCode -ne 0) {
                $records.Add([pscustomobject]@{
                    stage = $stage; result = 'fixture_failure'; exitCode = $exitCode; key = $null
                    overlaySha256 = $overlayHash; tools = $tools; outputs = $outputs
                })
                throw ('Cache fixture compile failed: ' + $stage + '; inspect its log for merger load/dependency errors. Installed SDK/security settings are unchanged.')
            }
            $text = Read-ManifestResource $exe ('cache-' + $stage)
            $expectedMarker = if ($stage -eq 'edit') { 'KACHE_MANIFEST_V2' } else { 'KACHE_MANIFEST_V1' }
            if (-not $text.Contains($expectedMarker)) { throw ('Wrong embedded manifest: ' + $stage) }
            foreach ($output in $outputs) {
                if ([IO.Path]::GetExtension($output.name) -notin @('.exe', '.d', '.pdb')) { throw ('Uncaptured side output: ' + $output.name) }
            }
            $outputJson = $outputs | ConvertTo-Json -Compress
            if ($stage -eq 'cold') { $coldOutputs = $outputJson }
            elseif ($stage -in @('warm', 'revert', 'tool-revert') -and $outputJson -ne $coldOutputs) { throw ('Restored output bytes differ: ' + $stage) }
            $events = @(Get-Content -LiteralPath (Join-Path $runtime 'events.jsonl') | ForEach-Object { $_ | ConvertFrom-Json } | Where-Object { $_.PSObject.Properties.Name -contains 'crate_name' -and $_.crate_name -eq 'manifest_cache_probe' })
            if ($events.Count -ne $records.Count + 1) { throw ('Unexpected event count: ' + $stage) }
            $event = $events[-1]
            $record = [pscustomobject]@{
                stage = $stage; result = $event.result; exitCode = $exitCode; key = $event.cache_key
                marker = $expectedMarker; outputs = $outputs; overlaySha256 = $overlayHash; tools = $tools
            }
            $records.Add($record)
            $expectedResult = if ($stage -in @('cold', 'edit', 'tool-edit')) { 'miss' } else { 'local_hit' }
            if ($event.result -ne $expectedResult) { throw ('Expected ' + $expectedResult + ', got ' + $event.result + ': ' + $stage) }
            if ([string]::IsNullOrEmpty($event.cache_key)) { throw 'Missing cache key' }
            if ($stage -eq 'cold') { $coldKey = $event.cache_key }
            elseif ($stage -in @('edit', 'tool-edit')) {
                if ($event.cache_key -eq $coldKey) { throw ('Changed XML/merger bytes reused the original key: ' + $stage) }
            } elseif ($event.cache_key -ne $coldKey) { throw ('Original XML/merger did not recover its key: ' + $stage) }
        }
        if ((Get-FileHash -LiteralPath $realMt.path -Algorithm SHA256).Hash -ne $realMt.sha256) { throw 'The installed SDK merger changed during the fixture.' }
    } finally {
        try {
            try {
                Write-ProbeJson 'cache-results.json' ($records.ToArray())
                if (Test-Path -LiteralPath (Join-Path $runtime 'events.jsonl')) {
                    Copy-Item -LiteralPath (Join-Path $runtime 'events.jsonl') -Destination (Join-Path $OutputDirectory 'cache-events.jsonl')
                }
            } finally {
                if ($daemonStartAttempted) { & $KachePath daemon stop *> (Join-Path $OutputDirectory 'cache-daemon-stop.log') }
            }
        } finally {
            try { if (Test-Path -LiteralPath $overlay) { Remove-Item -LiteralPath $overlay -Recurse -Force } }
            finally { [KacheProbeEnvironment]::Restore($savedEnvironment) }
        }
    }
}

try {
    Set-Location $root
    Write-Utf8 'probe.rs' 'fn main() {}'
    $body = '<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="1.0"><assemblyIdentity name="Kache.ManifestProbe" version="1.0.0.0" type="win32"/><description>{0}</description></assembly>'
    foreach ($version in @('V1', 'V2')) {
        $plain = Invoke-Case ('plain-' + $version) ($body -f ('KACHE_MANIFEST_' + $version))
        $present = if ($version -eq 'V1') { $plain.markerV1 } else { $plain.markerV2 }
        if ($plain.exitCode -ne 0 -or -not $present) { throw 'The plain-XML positive control failed.' }
    }
    $unicode = Invoke-Case 'plain-utf16' ($body -f 'KACHE_MANIFEST_V1') $true
    if ($unicode.exitCode -ne 0 -or -not $unicode.markerV1) { throw 'The UTF-16 positive control failed.' }
    Invoke-Case 'plain-utf16be' ($body -f 'KACHE_MANIFEST_V1') $true $true $true | Out-Null
    # Record compiler/linker behavior without claiming rustc injects EMBED.
    Invoke-Case 'plain-without-explicit-embed' ($body -f 'KACHE_MANIFEST_V1') $false $false | Out-Null
    $internalXml = '<!DOCTYPE assembly [<!ENTITY probe "KACHE_MANIFEST_V1">]>' + ($body -f '&probe;')
    Invoke-Case 'internal-entity' $internalXml | Out-Null

    # Keep the manifest bytes and path fixed while changing only an external file.
    $entityXml = '<!DOCTYPE assembly [<!ENTITY probe SYSTEM "entity.txt">]>' + ($body -f '&probe;')
    foreach ($version in @('V1', 'V2')) {
        Write-Utf8 'entity.txt' ('KACHE_MANIFEST_' + $version)
        Invoke-Case ('external-entity-' + $version) $entityXml | Out-Null
    }
    $dtdXml = '<!DOCTYPE assembly SYSTEM "external.dtd">' + ($body -f '&probe;')
    foreach ($version in @('V1', 'V2')) {
        Write-Utf8 'external.dtd' ('<!ENTITY probe "KACHE_MANIFEST_' + $version + '">')
        Invoke-Case ('external-dtd-' + $version) $dtdXml | Out-Null
    }
    if ($KachePath) { Invoke-CacheControls $body }
} finally {
    [IO.File]::WriteAllText((Join-Path $OutputDirectory 'compiler.txt'), $compiler, $utf8)
    $results | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $OutputDirectory 'results.json') -Encoding utf8
    Set-Location $previousDirectory
    Remove-Item -LiteralPath $root -Recurse -Force
}
