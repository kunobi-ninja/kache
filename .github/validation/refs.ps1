$ErrorActionPreference = 'Stop'
$work = New-Item -Type Directory (Join-Path $env:RUNNER_TEMP ("kache-refs-" + [guid]::NewGuid()))
$image = Join-Path $work.FullName 'scratch.vhdx'
$attached = $false
try {
    $v = Get-Volume | Where-Object { $_.FileSystem -eq 'ReFS' -and $_.DriveLetter } | Select-Object -First 1
    if (-not $v) {
        $spec = Join-Path $work.FullName 'create.txt'
        @(
            "create vdisk file=`"$image`" maximum=65536 type=expandable",
            "select vdisk file=`"$image`"",
            "attach vdisk"
        ) | Set-Content -LiteralPath $spec -Encoding ascii
        & diskpart /s $spec
        if ($LASTEXITCODE -ne 0) { throw 'Could not attach the fresh scratch VHDX' }
        $attached = $true
        $disk = Get-DiskImage -ImagePath $image | Get-Disk
        if (!$disk -or $disk.IsSystem -or $disk.IsBoot -or $disk.PartitionStyle -ne 'RAW' -or $disk.Size -ne 64GB) {
            throw 'The attached image does not describe a new blank 64 GiB virtual disk'
        }
        $part = $disk | Initialize-Disk -PartitionStyle GPT -PassThru | New-Partition -AssignDriveLetter -UseMaximumSize
        $params = @{ Partition = $part; FileSystem = 'ReFS'; NewFileSystemLabel = 'KacheTest'; Confirm = $false }
        if ((Get-Command Format-Volume).Parameters.ContainsKey('DevDrive')) { $params.DevDrive = $true }
        $v = Format-Volume @params
    }
    if ($v.FileSystem -ne 'ReFS' -or !$v.DriveLetter) { throw 'The test needs a mounted ReFS volume' }
    $root = New-Item -Type Directory ("{0}:\kache-test-{1}" -f $v.DriveLetter, [guid]::NewGuid())
    $env:KACHE_TEST_REFS_DIR = $root.FullName
    $env:RUSTC_WRAPPER = ''
    Write-Output "Native volume $($v.FileSystem) $($v.DriveLetter): on $env:RUNNER_ARCH"
    $out = & cargo test --locked -p kache-store --lib windows_fingerprint_detects_restored_mtime_edits_and_replacement -- --nocapture 2>&1 | Tee-Object -Variable testOutput
    $status = $LASTEXITCODE
    $out | ForEach-Object { Write-Output $_ }
    if ($status -ne 0 -or "$testOutput" -notmatch '1 passed; 0 failed') { throw "ReFS regression test did not pass ($status)" }
} finally {
    if ($attached) { Dismount-DiskImage -ImagePath $image }
}
