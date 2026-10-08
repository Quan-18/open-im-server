param([switch]$Verify)
$ErrorActionPreference = 'Stop'
$repository = Split-Path -Parent $PSScriptRoot
$lock = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'stable-proto-tools.json') -Raw | ConvertFrom-Json
$toolRoot = Join-Path $repository '.tools/stable-proto'
New-Item -ItemType Directory -Force -Path $toolRoot | Out-Null
if ((go version) -notmatch ('go' + [regex]::Escape($lock.go) + ' ')) { throw 'Go toolchain does not match stable-proto-tools.json' }
$archive = Join-Path $toolRoot 'protoc.zip'
if (-not (Test-Path -LiteralPath $archive)) { Invoke-WebRequest -Uri $lock.protocWindowsArchive -OutFile $archive }
if ((Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant() -ne $lock.protocWindowsSha256) { throw 'protoc archive hash mismatch' }
$protoc = Join-Path $toolRoot 'protoc/bin/protoc.exe'
if (-not (Test-Path -LiteralPath $protoc)) { Expand-Archive -LiteralPath $archive -DestinationPath (Join-Path $toolRoot 'protoc') }
$oldBin = $env:GOBIN
$oldPath = $env:PATH
$oldToolchain = $env:GOTOOLCHAIN
try {
    $env:GOBIN = Join-Path $toolRoot 'bin'
    $env:GOTOOLCHAIN = 'local'
    $goPlugin = Join-Path $env:GOBIN 'protoc-gen-go.exe'
    if (-not (Test-Path -LiteralPath $goPlugin)) {
        go install ('google.golang.org/protobuf/cmd/protoc-gen-go@' + $lock.protocGenGo)
        if ($LASTEXITCODE -ne 0) { throw 'protoc-gen-go installation failed' }
    }
    if ((& $goPlugin --version) -notmatch ([regex]::Escape($lock.protocGenGo) + '$')) { throw 'protoc-gen-go version mismatch' }
    $grpcPlugin = Join-Path $env:GOBIN 'protoc-gen-go-grpc.exe'
    if (-not (Test-Path -LiteralPath $grpcPlugin)) {
        go install ('google.golang.org/grpc/cmd/protoc-gen-go-grpc@' + $lock.protocGenGoGrpc)
        if ($LASTEXITCODE -ne 0) { throw 'protoc-gen-go-grpc installation failed' }
    }
    if ((& $grpcPlugin --version) -notmatch ([regex]::Escape($lock.protocGenGoGrpc.TrimStart('v')) + '$')) { throw 'protoc-gen-go-grpc version mismatch' }
    $env:PATH = $env:GOBIN + [IO.Path]::PathSeparator + $oldPath
    $output = if ($Verify) { Join-Path $toolRoot 'verify' } else { $repository }
    New-Item -ItemType Directory -Force -Path $output | Out-Null
    Push-Location $toolRoot
    try {
        & $protoc ('--proto_path=' + $repository) ('--plugin=protoc-gen-go=' + $goPlugin) ('--plugin=protoc-gen-go-grpc=' + $grpcPlugin) ('--go_out=' + $output) --go_opt=paths=source_relative ('--go-grpc_out=' + $output) --go-grpc_opt=paths=source_relative (Join-Path $repository 'pkg/stablemessage/v1/stable_message.proto')
        if ($LASTEXITCODE -ne 0) { throw 'protobuf generation failed' }
    } finally { Pop-Location }
    if ($Verify) {
        foreach ($name in @('stable_message.pb.go', 'stable_message_grpc.pb.go')) {
            $relative = 'pkg/stablemessage/v1/' + $name
            if ((Get-FileHash (Join-Path $output $relative)).Hash -ne (Get-FileHash (Join-Path $repository $relative)).Hash) { throw ('Generated file differs: ' + $relative) }
        }
    }
} finally {
    $env:GOBIN = $oldBin
    $env:PATH = $oldPath
    $env:GOTOOLCHAIN = $oldToolchain
}
