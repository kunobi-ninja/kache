param(
    [Parameter(Mandatory = $true)][string]$Kache,
    [Parameter(Mandatory = $true)][string]$OutputDirectory
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$Kache = (Resolve-Path -LiteralPath $Kache).Path
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
[IO.Directory]::CreateDirectory($OutputDirectory) | Out-Null
$utf8 = [Text.UTF8Encoding]::new($false)
$rustc = (& rustup which rustc 2>&1 | Out-String).Trim()
if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $rustc)) { throw 'Cannot resolve rustc.' }
$version = (& $rustc -vV 2>&1 | Out-String)
if ($LASTEXITCODE -ne 0 -or $version -notmatch 'host: .*windows-msvc') { throw 'Requires native Windows MSVC.' }
[IO.File]::WriteAllText((Join-Path $OutputDirectory 'compiler.txt'), $version, $utf8)
& $Kache --version *> (Join-Path $OutputDirectory 'kache-version.txt')
if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect candidate version.' }
# This fixture contains only maintainer-authored standard-library Rust code.
# No Cargo project, third-party source, build script, service registration or trust opt-in.
$root = Join-Path $OutputDirectory ('fixture-' + [guid]::NewGuid())
[IO.Directory]::CreateDirectory($root) | Out-Null
[IO.File]::WriteAllText((Join-Path $root 'bin.rs'), 'fn main() { println!("kache-native-probe"); }', $utf8)
[IO.File]::WriteAllText((Join-Path $root 'proc.rs'), 'extern crate proc_macro; #[proc_macro] pub fn passthrough(input: proc_macro::TokenStream) -> proc_macro::TokenStream { input }', $utf8)
$settings = @('KACHE_CACHE_DIR', 'KACHE_RUNTIME_DIR', 'KACHE_PHASE_TRACE_DIR', 'KACHE_CACHE_EXECUTABLES', 'KACHE_LOG_FILE', 'KACHE_TRUST_DAEMON_EXECUTION', 'RUSTC_WRAPPER')
$previous = @{}
foreach ($name in $settings) { $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
$previousDirectory = Get-Location
$results = [Collections.Generic.List[object]]::new()
$spawned = [Collections.Generic.List[Diagnostics.Process]]::new()
try {
    Set-Location $root
    $env:RUSTC_WRAPPER = ''
    $env:KACHE_TRUST_DAEMON_EXECUTION = '0'
    $env:KACHE_LOG_FILE = 'kache=debug,kache_store=debug,kache_ipc=debug'
    foreach ($executables in @('false', 'true')) {
        $mode = Join-Path $root ('executables-' + $executables)
        [IO.Directory]::CreateDirectory($mode) | Out-Null
        $env:KACHE_CACHE_EXECUTABLES = $executables
        $env:KACHE_CACHE_DIR = Join-Path $mode 'cache'
        $env:KACHE_RUNTIME_DIR = Join-Path $mode 'runtime'
        $env:KACHE_PHASE_TRACE_DIR = Join-Path $mode 'phases'
        & $Kache daemon start *> (Join-Path $mode 'daemon-start.log')
        if ($LASTEXITCODE -ne 0) { throw 'Candidate daemon did not start.' }
        try {
            foreach ($pass in @('cold', 'warm')) {
                $processes = [Collections.Generic.List[object]]::new()
                foreach ($unit in 0..3) {
                    $type = if ($unit -lt 2) { 'bin' } else { 'proc-macro' }
                    $source = if ($unit -lt 2) { 'bin.rs' } else { 'proc.rs' }
                    $out = Join-Path $mode ('out-' + $unit)
                    if (Test-Path -LiteralPath $out) { Remove-Item -LiteralPath $out -Recurse -Force }
                    [IO.Directory]::CreateDirectory($out) | Out-Null
                    $start = [Diagnostics.ProcessStartInfo]::new()
                    $start.FileName = $Kache
                    $start.WorkingDirectory = $root
                    $start.UseShellExecute = $false
                    $start.RedirectStandardOutput = $true
                    $start.RedirectStandardError = $true
                    foreach ($argument in @($rustc, '--crate-name', ('fixture_' + $unit), '--crate-type', $type, '--edition=2024', '--emit=dep-info,link', '-Cdebuginfo=0', '--out-dir', $out, (Join-Path $root $source))) {
                        $start.ArgumentList.Add($argument)
                    }
                    $process = [Diagnostics.Process]::new()
                    $process.StartInfo = $start
                    $watch = [Diagnostics.Stopwatch]::StartNew()
                    if (-not $process.Start()) { throw 'Cannot start compiler wrapper.' }
                    $spawned.Add($process)
                    $processes.Add([pscustomobject]@{
                        process = $process; watch = $watch; unit = $unit; type = $type
                        stdout = $process.StandardOutput.ReadToEndAsync()
                        stderr = $process.StandardError.ReadToEndAsync()
                    })
                }
                foreach ($entry in $processes) {
                    if (-not $entry.process.WaitForExit(300000)) {
                        $entry.process.Kill($true)
                        throw 'Compiler probe exceeded five minutes.'
                    }
                    $entry.watch.Stop()
                    $name = $pass + '-' + $entry.unit
                    [IO.File]::WriteAllText((Join-Path $mode ($name + '.stdout')), $entry.stdout.GetAwaiter().GetResult(), $utf8)
                    [IO.File]::WriteAllText((Join-Path $mode ($name + '.stderr')), $entry.stderr.GetAwaiter().GetResult(), $utf8)
                    $results.Add([pscustomobject]@{ executables = $executables; pass = $pass; unit = $entry.unit; type = $entry.type; exitCode = $entry.process.ExitCode; observedWaitMs = $entry.watch.ElapsedMilliseconds })
                    if ($entry.process.ExitCode -ne 0) { throw ('Compiler failed: ' + $name) }
                }
                & $Kache daemon *> (Join-Path $mode ($pass + '-daemon-status.txt'))
                & $Kache stats *> (Join-Path $mode ($pass + '-stats.txt'))
            }
        } finally {
            & $Kache daemon stop *> (Join-Path $mode 'daemon-stop.log')
        }
    }
} finally {
    foreach ($process in $spawned) {
        try {
            if (-not $process.HasExited) { $process.Kill($true) }
            $process.WaitForExit(10000) | Out-Null
        } finally { $process.Dispose() }
    }
    $results | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $OutputDirectory 'results.json') -Encoding utf8
    $log = Join-Path $env:LOCALAPPDATA 'kache/kache.log'
    if (Test-Path -LiteralPath $log) { Copy-Item -LiteralPath $log -Destination (Join-Path $OutputDirectory 'kache.log') }
    Set-Location $previousDirectory
    foreach ($name in $settings) { [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process') }
}
