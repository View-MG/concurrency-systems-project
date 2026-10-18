param([string]$Compiler = 'g++')
$ErrorActionPreference = 'Stop'
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    New-Item -ItemType Directory -Force build/obj | Out-Null
    $flags = @('-std=c++17', '-O2', '-g', '-Wall', '-Wextra', '-Wpedantic', '-Werror', '-D_WIN32_WINNT=0x0601', '-ICommon', '-pthread', '-static-libgcc', '-static-libstdc++')
    $commonObjects = @()
    foreach ($source in @('Common/analyzer.cpp', 'Common/chunks.cpp', 'Common/message.cpp', 'Common/ipc.cpp')) {
        $object = 'build/obj/' + [IO.Path]::GetFileNameWithoutExtension($source) + '.o'
        Write-Host "Compiling $source"
        & $Compiler @flags -c $source -o $object
        if ($LASTEXITCODE -ne 0) { throw "Compile failed: $source" }
        $commonObjects += $object
    }
    $targets = [ordered]@{
        client = 'Client/main.cpp'; broker = 'Broker/main.cpp'; worker = 'Worker/main.cpp'
        reference = 'Reference/main.cpp'; prepare = 'Prepare/main.cpp'; common_tests = 'tests/common_tests.cpp'
    }
    foreach ($target in $targets.Keys) {
        Write-Host "Building $target"
        & $Compiler @flags $targets[$target] @commonObjects -o "build/$target.exe" -lshell32
        if ($LASTEXITCODE -ne 0) { throw "Build failed: $target" }
    }
} finally { Pop-Location }
