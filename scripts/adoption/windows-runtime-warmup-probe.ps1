#requires -Version 7.3
param(
    [Parameter(Mandatory = $true)][ValidateNotNullOrEmpty()][string]$Kache,
    [Parameter(Mandatory = $true)][ValidateNotNullOrEmpty()][string]$OutputDirectory
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
if (-not $IsWindows) { throw 'This fixture requires native Windows MSVC.' }

# SetEnvironmentVariableW distinguishes an empty value from an absent variable
# on PowerShell/.NET versions whose environment setters remove empty values.
if (-not ('KacheWarmupProbeEnvironment' -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
public static class KacheWarmupProbeEnvironment {
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool SetEnvironmentVariableW(string name, string value);
    public static void Set(string name, object value) {
        string text = value == null ? null : (string)value;
        if (!SetEnvironmentVariableW(name, text))
            throw new Win32Exception(Marshal.GetLastWin32Error());
    }
}
'@
}

$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
$runRoot = Join-Path $OutputDirectory ('runtime-warmup-' + [guid]::NewGuid().ToString('N'))
[void][IO.Directory]::CreateDirectory($runRoot)
$utf8 = [Text.UTF8Encoding]::new($false)
$originalEnvironment = [Environment]::GetEnvironmentVariables('Process')
$managed = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
$overrides = @(
    'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'RUSTFLAGS', 'RUSTDOCFLAGS',
    'CARGO_ENCODED_RUSTFLAGS', 'CARGO_ENCODED_RUSTDOCFLAGS', 'CARGO_MANIFEST_DIR',
    'CARGO_BUILD_TARGET', 'CARGO_TARGET_DIR', 'CARGO_INCREMENTAL', 'OUT_DIR',
    'CL', '_CL_', 'LINK', '_LINK_',
    'KACHE_CONFIG', 'KACHE_HOST_CONFIG', 'KACHE_CACHE_DIR', 'KACHE_RUNTIME_DIR',
    'KACHE_PHASE_TRACE_DIR', 'KACHE_NATIVE_RUNTIME_WARMUP',
    'KACHE_TRUST_DAEMON_EXECUTION', 'KACHE_LOG', 'KACHE_LOG_FILE', 'KACHE_LOG_FILE_PATH'
)
foreach ($name in $originalEnvironment.Keys) {
    if ($name.StartsWith('KACHE_', [StringComparison]::OrdinalIgnoreCase)) {
        [void]$managed.Add($name)
    }
}
foreach ($name in $overrides) { [void]$managed.Add($name) }
$states = [Collections.Generic.List[object]]::new()
$cases = [Collections.Generic.List[object]]::new()
$cleanupErrors = [Collections.Generic.List[string]]::new()
$summary = [ordered]@{
    status = 'running'; startedUtc = [DateTime]::UtcNow.ToString('o')
    evidenceDirectory = $runRoot; cases = $cases; cleanupErrors = $cleanupErrors
}
$failure = $null

function Set-ProbeEnvironment([string]$Name, [AllowNull()]$Value) {
    [KacheWarmupProbeEnvironment]::Set($Name, $Value)
}

function Write-ProbeJson([string]$Path, $Value) {
    [IO.File]::WriteAllText($Path, (ConvertTo-Json -InputObject $Value -Depth 30), $utf8)
}

function Start-Captured([string]$Program, [string[]]$Arguments, [string]$Directory, [string]$Prefix) {
    $info = [Diagnostics.ProcessStartInfo]::new()
    $info.FileName = $Program
    $info.WorkingDirectory = $Directory
    $info.UseShellExecute = $false
    $info.RedirectStandardOutput = $true
    $info.RedirectStandardError = $true
    foreach ($argument in $Arguments) { $info.ArgumentList.Add($argument) }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $info
    $watch = [Diagnostics.Stopwatch]::StartNew()
    if (-not $process.Start()) { throw 'Cannot start a fixture process.' }
    $state = [pscustomobject]@{
        Process = $process; Watch = $watch; Prefix = $Prefix; Saved = $false
        Stdout = $process.StandardOutput.ReadToEndAsync()
        Stderr = $process.StandardError.ReadToEndAsync()
    }
    $states.Add($state)
    return $state
}

function Save-Captured($State) {
    if ($State.Saved) { return }
    foreach ($stream in @(@('stdout', $State.Stdout), @('stderr', $State.Stderr))) {
        $task = $stream[1]
        $text = if ($task.Wait(10000)) { $task.GetAwaiter().GetResult() } else {
            $cleanupErrors.Add("Output capture did not close for $($State.Prefix).$($stream[0]).")
            '[capture did not close within ten seconds]'
        }
        [IO.File]::WriteAllText(($State.Prefix + '.' + $stream[0]), $text, $utf8)
    }
    $State.Saved = $true
}

function Complete-Captured($State, [int]$TimeoutMs = 120000) {
    $timedOut = -not $State.Process.WaitForExit($TimeoutMs)
    if ($timedOut) {
        if (-not $State.Process.HasExited) { $State.Process.Kill($true) }
        [void]$State.Process.WaitForExit(10000)
    }
    $State.Watch.Stop()
    Save-Captured $State
    $result = [pscustomobject]@{
        ExitCode = $(if ($State.Process.HasExited) { $State.Process.ExitCode } else { $null })
        ElapsedMs = $State.Watch.ElapsedMilliseconds; TimedOut = $timedOut
    }
    Write-ProbeJson ($State.Prefix + '.process.json') $result
    if ($timedOut) { throw 'A fixture process exceeded its bounded wait; logs were retained.' }
    return $result
}

function Invoke-Captured([string]$Program, [string[]]$Arguments, [string]$Directory, [string]$Prefix, [int]$TimeoutMs = 120000) {
    $state = Start-Captured $Program $Arguments $Directory $Prefix
    return (Complete-Captured $state $TimeoutMs)
}

function Read-FixtureEvents([string]$Path, [string]$Crate) {
    if (-not (Test-Path -LiteralPath $Path)) { return }
    foreach ($line in [IO.File]::ReadAllLines($Path)) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        $event = ConvertFrom-Json -InputObject $line -AsHashtable
        if ($event.ContainsKey('result') -and $event['crate_name'] -ceq $Crate) { $event }
    }
}

function Invoke-Compile($Case, [string]$Label, [int]$TimeoutMs = 120000) {
    $attemptWatch = [Diagnostics.Stopwatch]::StartNew()
    # This directory was created by this invocation under its GUID-owned root.
    # Remove only fixture outputs; never change SDK files or execute the DLL.
    foreach ($file in Get-ChildItem -LiteralPath $Case.Out -File) {
        Remove-Item -LiteralPath $file.FullName -Force
    }
    $before = @(Read-FixtureEvents $Case.Events $Case.Crate).Count
    $prefix = Join-Path $Case.Directory $Label
    $result = Invoke-Captured $Kache @(
        $rustc, '--crate-name', $Case.Crate, '--crate-type', 'proc-macro',
        '--edition=2024', '--emit=dep-info,link', '-Cdebuginfo=0',
        '--out-dir', $Case.Out, $Case.Source
    ) $Case.Work $prefix $TimeoutMs
    if ($result.ExitCode -ne 0) { throw "Compiler wrapper failed for $Label; inspect its stdout/stderr." }
    $dlls = @(Get-ChildItem -LiteralPath $Case.Out -File -Filter '*.dll')
    if ($dlls.Count -ne 1) { throw "Expected one proc-macro DLL after $Label." }
    $events = @(Read-FixtureEvents $Case.Events $Case.Crate)
    if ($events.Count -ne ($before + 1)) { throw "Expected exactly one completed compiler event for $Label." }
    $event = $events[-1]
    $expectedCompiles = if ($event['result'] -ceq 'local_hit') { 0 } else { 1 }
    if ($event['compiler_runs'] -ne $expectedCompiles) { throw "Unexpected compiler execution count for $Label." }

    # Explicit --format json uses the standalone report, not the --json envelope.
    # all_events omits passthrough/skipped rows. Compare its cacheable history
    # separately; raw events retain this feature's exact passthrough reason.
    $reportPath = $prefix + '.stats.json'
    $statsBudget = $TimeoutMs - [int]$attemptWatch.ElapsedMilliseconds
    if ($statsBudget -le 0) { throw "No admission budget remains for stats after $Label." }
    $stats = Invoke-Captured $Kache @('stats', '--format', 'json', '--since', '1h', '--output', $reportPath) $Case.Work ($prefix + '.stats') $statsBudget
    if ($stats.ExitCode -ne 0) { throw "Cannot read JSON stats after $Label." }
    $report = ConvertFrom-Json -AsHashtable -InputObject ([IO.File]::ReadAllText($reportPath))
    if (-not $report.ContainsKey('all_events')) { throw 'Stats did not return the standalone report schema.' }
    $reported = @($report['all_events'] | Where-Object { $_['crate_name'] -ceq $Case.Crate })
    $cacheable = @($events | Where-Object { $_['result'] -cnotin @('passthrough', 'skipped') })
    if ($reported.Count -ne $cacheable.Count) { throw "Cacheable event counts disagree after $Label." }
    for ($index = 0; $index -lt $cacheable.Count; $index++) {
        if ($reported[$index]['result'] -cne $cacheable[$index]['result'] -or $reported[$index]['cache_key'] -cne $cacheable[$index]['cache_key']) {
            throw "Cacheable event rows or keys disagree after $Label."
        }
    }
    $passthroughs = @($events | Where-Object { $_['result'] -ceq 'passthrough' }).Count
    if (-not $report.ContainsKey('summary') -or -not $report['summary'].ContainsKey('passthroughs') -or
        $report['summary']['passthroughs'] -ne $passthroughs) {
        throw "Passthrough summary disagrees with the isolated fixture events after $Label."
    }
    $skipped = @($events | Where-Object { $_['result'] -ceq 'skipped' }).Count
    if (-not $report['summary'].ContainsKey('skipped') -or $report['summary']['skipped'] -ne $skipped) {
        throw "Skipped summary disagrees with the isolated fixture events after $Label."
    }
    $record = [ordered]@{
        label = $Label; elapsedMs = $result.ElapsedMs; result = $event['result']
        reason = $(if ($event['result'] -ceq 'skipped') { [string]$event['skip_reason'] } else { [string]$event['passthrough_reason'] })
        cacheKey = [string]$event['cache_key']
        compilerRuns = $event['compiler_runs']; depInfoRuns = $event['dep_info_runs']; probeRuns = $event['probe_runs']
        dll = $dlls[0].FullName; dllSha256 = (Get-FileHash -LiteralPath $dlls[0].FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        stats = $reportPath; rawEvents = $Case.Events
    }
    $Case.Record.attempts.Add($record)
    Write-ProbeJson ($prefix + '.result.json') $record
    return $record
}

function Invoke-WarmupCase([bool]$Enabled) {
    $name = if ($Enabled) { 'warmup-enabled' } else { 'warmup-disabled' }
    $directory = Join-Path $runRoot $name
    foreach ($child in @('cache', 'runtime', 'source', 'out', 'phases')) {
        [void][IO.Directory]::CreateDirectory((Join-Path $directory $child))
    }
    $config = Join-Path $directory 'config.toml'
    # Synchronous publication removes an unrelated store-handoff race from the
    # next-build hit assertion. Runtime warmup still uses the owned daemon.
    # cache_executables is omitted and its inherited override was cleared.
    [IO.File]::WriteAllText($config, "[cache]`nlocal_only = true`ntrust_daemon_execution = false`nnative_runtime_warmup = true`ndaemon_publish = false`nauto_gc = false`nindex_auto_compact = false`nprefetch_enabled = false`nmin_store_compile_ms = 0`n", $utf8)
    $source = Join-Path $directory 'source/proc.rs'
    [IO.File]::WriteAllText($source, "extern crate proc_macro;`n#[proc_macro]`npub fn passthrough(input: proc_macro::TokenStream) -> proc_macro::TokenStream { input }`n", $utf8)
    $record = [ordered]@{
        mode = $name; status = 'running'; warmupEnabled = $Enabled
        sourceSha256 = (Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash.ToLowerInvariant()
        attempts = [Collections.Generic.List[object]]::new()
    }
    $cases.Add($record)
    $case = [pscustomobject]@{
        Directory = $directory; Work = (Join-Path $directory 'source'); Source = $source
        Out = (Join-Path $directory 'out'); Events = (Join-Path $directory 'runtime/events.jsonl')
        Crate = 'kache_runtime_warmup_fixture'; Record = $record
    }
    $settings = @{
        KACHE_CONFIG = $config; KACHE_HOST_CONFIG = ''
        KACHE_CACHE_DIR = (Join-Path $directory 'cache'); KACHE_RUNTIME_DIR = (Join-Path $directory 'runtime')
        KACHE_PHASE_TRACE_DIR = (Join-Path $directory 'phases')
        KACHE_NATIVE_RUNTIME_WARMUP = $(if ($Enabled) { '1' } else { '0' })
        KACHE_TRUST_DAEMON_EXECUTION = '0'; KACHE_LOG = 'off'
        KACHE_LOG_FILE = 'kache=debug,kache_store=debug,kache_ipc=debug'
        KACHE_LOG_FILE_PATH = (Join-Path $directory 'kache.log')
    }
    foreach ($setting in $settings.GetEnumerator()) { Set-ProbeEnvironment $setting.Key $setting.Value }
    $daemon = $null
    try {
        # Foreground mode gives this script ownership of the exact process tree.
        $daemon = Start-Captured $Kache @('daemon', 'run') $case.Work (Join-Path $directory 'daemon')
        $ready = [Diagnostics.Stopwatch]::StartNew()
        $readyAttempt = 0
        while ($true) {
            if ($daemon.Process.HasExited) { throw 'The fixture daemon exited before readiness.' }
            $statusPrefix = Join-Path $directory ('daemon-status-' + $readyAttempt++)
            $status = Invoke-Captured $Kache @('daemon', 'status', '--json') $case.Work $statusPrefix 10000
            if ($status.ExitCode -eq 0) {
                $json = ConvertFrom-Json -AsHashtable -InputObject ([IO.File]::ReadAllText($statusPrefix + '.stdout'))
                if ($json['daemon_running'] -eq $true) { break }
            }
            if ($ready.Elapsed.TotalSeconds -ge 30) { throw 'The fixture daemon did not become ready within thirty seconds.' }
            Start-Sleep -Milliseconds 200
        }
        $first = Invoke-Compile $case 'first'
        $record.firstWrapperElapsedMs = $first.elapsedMs
        if ($Enabled) {
            $warming = @(Read-FixtureEvents $case.Events $case.Crate | Where-Object { $_['passthrough_reason'] -ceq 'native runtime warmup' -or $_['skip_reason'] -ceq 'native runtime warmup' })
            if ($warming.Count -eq 0 -or $first.reason -cne 'native runtime warmup' -or $first.result -cnotin @('passthrough', 'skipped')) {
                throw 'A fresh enabled store must record a successful native-runtime-warmup compile without caching.'
            }
            $admission = [Diagnostics.Stopwatch]::StartNew()
            $attempt = 0
            $cachedMiss = $null
            while ($admission.Elapsed.TotalSeconds -lt 120) {
                if ((120000 - $admission.ElapsedMilliseconds) -le 2000) { break }
                Start-Sleep -Seconds 2
                $remaining = 120000 - [int]$admission.ElapsedMilliseconds
                if ($remaining -le 0) { break }
                $next = Invoke-Compile $case ('admission-' + $attempt++) $remaining
                if ($next.result -ceq 'miss') { $cachedMiss = $next; break }
                if ($next.result -ceq 'local_hit') { throw 'A fresh fixture hit without a recorded cacheable miss.' }
                if ($next.reason -cne 'native runtime warmup') { throw 'Admission polling encountered a different uncached reason.' }
            }
            if ($null -eq $cachedMiss) { throw 'Native runtime hashes did not admit a cacheable miss within 120 seconds.' }
        } else {
            if ($first.result -cne 'miss') { throw 'Warmup opt-out must cache its first build as a miss.' }
            $cachedMiss = $first
        }
        if ([string]::IsNullOrEmpty($cachedMiss.cacheKey)) { throw 'The admitted miss has no cache key.' }
        $hit = Invoke-Compile $case 'restored-hit'
        if ($hit.result -cne 'local_hit' -or $hit.cacheKey -cne $cachedMiss.cacheKey -or $hit.dllSha256 -cne $cachedMiss.dllSha256) {
            throw 'The deleted DLL was not restored as an identical local hit for the admitted miss.'
        }
        $events = @(Read-FixtureEvents $case.Events $case.Crate)
        $warmingCount = @($events | Where-Object { $_['passthrough_reason'] -ceq 'native runtime warmup' -or $_['skip_reason'] -ceq 'native runtime warmup' }).Count
        if (-not $Enabled -and $warmingCount -ne 0) { throw 'The opt-out store recorded a runtime warmup.' }
        $record.warmupEvents = $warmingCount
        $record.cachedMissSha256 = $cachedMiss.dllSha256
        $record.localHitSha256 = $hit.dllSha256
        $workerTraces = @(foreach ($file in Get-ChildItem -LiteralPath (Join-Path $directory 'phases') -File -Filter '*.trace.json') {
            $trace = ConvertFrom-Json -AsHashtable -InputObject ([IO.File]::ReadAllText($file.FullName))
            $spans = @($trace['traceEvents'] | Where-Object {
                $_['name'] -ceq 'native_msvc_runtime_warmup' -and $_['cat'] -ceq 'native-runtime-warmup'
            })
            if ($trace['kache_trace']['scope'] -ceq 'worker-thread' -and $spans.Count -gt 0) { $file.FullName }
        })
        if ($Enabled -and $workerTraces.Count -eq 0) { throw 'The daemon did not retain any native runtime worker trace.' }
        if (-not $Enabled -and $workerTraces.Count -ne 0) { throw 'The blocking opt-out unexpectedly ran a traced warmup worker.' }
        $record.runtimeWarmupTraceFiles = $workerTraces
        $record.status = 'passed'
    } catch {
        $record.status = 'failed'
        $record.failure = $_.Exception.Message
        throw
    } finally {
        if ($null -ne $daemon) {
            try {
                if (-not $daemon.Process.HasExited) {
                    $stop = Invoke-Captured $Kache @('daemon', 'stop') $case.Work (Join-Path $directory 'daemon-stop') 30000
                    if ($stop.ExitCode -ne 0) { $cleanupErrors.Add("Daemon stop failed for $name; terminating its owned process tree.") }
                }
                [void](Complete-Captured $daemon 10000)
            } catch { $cleanupErrors.Add("Daemon cleanup failed for ${name}: $($_.Exception.Message)") }
        }
        Write-ProbeJson (Join-Path $directory 'summary.json') $record
    }
}

try {
    foreach ($name in $managed) { Set-ProbeEnvironment $name $null }
    $Kache = (Resolve-Path -LiteralPath $Kache).Path
    $summary.kache = $Kache
    $summary.kacheSha256 = (Get-FileHash -LiteralPath $Kache -Algorithm SHA256).Hash.ToLowerInvariant()
    $resolve = Invoke-Captured 'rustup' @('which', 'rustc') $runRoot (Join-Path $runRoot 'rustc-path') 30000
    if ($resolve.ExitCode -ne 0) { throw 'Cannot resolve rustc.' }
    $rustc = [IO.File]::ReadAllText((Join-Path $runRoot 'rustc-path.stdout')).Trim()
    if (-not (Test-Path -LiteralPath $rustc -PathType Leaf)) { throw 'Resolved rustc does not exist.' }
    $version = Invoke-Captured $rustc @('-vV') $runRoot (Join-Path $runRoot 'rustc-version') 30000
    if ($version.ExitCode -ne 0) { throw 'Cannot inspect rustc.' }
    $versionText = [IO.File]::ReadAllText((Join-Path $runRoot 'rustc-version.stdout'))
    if ($versionText -notmatch 'host: .*windows-msvc') { throw 'Requires a native Windows MSVC compiler.' }
    $summary.rustc = $rustc
    $summary.rustcVersion = $versionText.Trim()
    $candidateVersion = Invoke-Captured $Kache @('--version') $runRoot (Join-Path $runRoot 'kache-version') 30000
    if ($candidateVersion.ExitCode -ne 0) { throw 'Cannot inspect candidate version.' }
    Invoke-WarmupCase $true
    Invoke-WarmupCase $false
    $summary.status = 'passed'
} catch {
    $failure = $_
    $summary.status = 'failed'
    $summary.failure = $_.Exception.Message
} finally {
    foreach ($state in $states) {
        try {
            if (-not $state.Process.HasExited) {
                $state.Process.Kill($true)
                [void]$state.Process.WaitForExit(10000)
            }
            Save-Captured $state
        } catch { $cleanupErrors.Add($_.Exception.Message) }
        finally { $state.Process.Dispose() }
    }
    foreach ($name in $managed) {
        try {
            $value = if ($originalEnvironment.Contains($name)) { [string]$originalEnvironment[$name] } else { $null }
            Set-ProbeEnvironment $name $value
        } catch { $cleanupErrors.Add("Cannot restore process variable ${name}: $($_.Exception.Message)") }
    }
    $restoredEnvironment = [Environment]::GetEnvironmentVariables('Process')
    foreach ($name in $managed) {
        if ($restoredEnvironment.Contains($name) -ne $originalEnvironment.Contains($name) -or
            ($originalEnvironment.Contains($name) -and [string]$restoredEnvironment[$name] -cne [string]$originalEnvironment[$name])) {
            $cleanupErrors.Add("Process variable restoration did not match for $name.")
        }
    }
    if ($cleanupErrors.Count -ne 0) { $summary.status = 'failed' }
    $summary.finishedUtc = [DateTime]::UtcNow.ToString('o')
    Write-ProbeJson (Join-Path $runRoot 'summary.json') $summary
    ConvertTo-Json -InputObject $summary -Depth 30
}
if ($null -ne $failure) { throw $failure }
if ($cleanupErrors.Count -ne 0) { throw 'Fixture cleanup failed; inspect summary.json.' }
