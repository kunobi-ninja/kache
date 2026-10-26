param([Parameter(Mandatory = $true)][string]$OutputDirectory)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

# This probe compiles only its own empty Rust program. It does not build Kache.
$rustc = (Get-Command rustc -ErrorAction Stop).Source
$compiler = (& $rustc -vV 2>&1 | Out-String)
if ($LASTEXITCODE -ne 0 -or $compiler -notmatch 'host: .*windows-msvc') {
    throw 'The probe requires a native Windows MSVC Rust toolchain.'
}

$root = Join-Path ([IO.Path]::GetTempPath()) ('kache-manifest-' + [guid]::NewGuid())
[IO.Directory]::CreateDirectory($root) | Out-Null
[IO.Directory]::CreateDirectory($OutputDirectory) | Out-Null
$utf8 = [Text.UTF8Encoding]::new($false)
$results = [Collections.Generic.List[object]]::new()
$previousDirectory = Get-Location

function Write-Utf8([string]$Name, [string]$Text) {
    [IO.File]::WriteAllText((Join-Path $root $Name), $Text, $utf8)
}

function Invoke-Case([string]$Name, [string]$Xml, [bool]$Unicode = $false) {
    $manifest = Join-Path $root 'input.manifest'
    $encoding = if ($Unicode) { [Text.Encoding]::Unicode } else { $utf8 }
    [IO.File]::WriteAllText($manifest, $Xml, $encoding)
    $exe = Join-Path $root ($Name + '.exe')
    $arguments = @(
        '--crate-name', 'manifest_probe', (Join-Path $root 'probe.rs'),
        '-C', 'debuginfo=0', '-C', 'link-arg=/MANIFEST:EMBED',
        '-C', ('link-arg=/MANIFESTINPUT:' + $manifest), '-o', $exe
    )
    & $rustc @arguments *> (Join-Path $OutputDirectory ($Name + '.log'))
    $exitCode = $LASTEXITCODE
    [byte[]]$bytes = @()
    if (Test-Path -LiteralPath $exe) { $bytes = [IO.File]::ReadAllBytes($exe) }
    $text = [Text.Encoding]::UTF8.GetString($bytes) + [Text.Encoding]::Unicode.GetString($bytes)
    $result = [pscustomobject]@{
        name = $Name; exitCode = $exitCode
        markerV1 = $text.Contains('KACHE_MANIFEST_V1')
        markerV2 = $text.Contains('KACHE_MANIFEST_V2')
    }
    $results.Add($result)
    return $result
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
} finally {
    [IO.File]::WriteAllText((Join-Path $OutputDirectory 'compiler.txt'), $compiler, $utf8)
    $results | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $OutputDirectory 'results.json') -Encoding utf8
    Set-Location $previousDirectory
    Remove-Item -LiteralPath $root -Recurse -Force
}
