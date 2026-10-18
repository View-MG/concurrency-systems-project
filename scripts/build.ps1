param([string]$Compiler = 'g++')
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    New-Item -ItemType Directory -Force build | Out-Null
    $commonSources = @('Common/analyzer.cpp', 'Common/chunks.cpp', 'Common/message.cpp', 'Common/ipc.cpp')
    $flags = @('-std=c++17', '-O2', '-g', '-Wall', '-Wextra', '-Wpedantic', '-Werror', '-D_WIN32_WINNT=0x0601', '-ICommon', '-pthread', '-static-libgcc', '-static-libstdc++')
    New-Item -ItemType Directory -Force build/obj | Out-Null
    $commonObjects = @()
    foreach ($commonSource in $commonSources) {
        $objectPath = 'build/obj/' + [IO.Path]::GetFileNameWithoutExtension($commonSource) + '.o'
        Write-Host "Compiling $commonSource"
        & $Compiler @flags -c $commonSource -o $objectPath
        if ($LASTEXITCODE -ne 0) { throw "Compile failed: $commonSource" }
        $commonObjects += $objectPath
    }
    foreach ($target in @('client','broker','worker','reference','prepare','common_tests')) {
        $source = switch ($target) {
            'client' { 'Client/main.cpp' }
            'broker' { 'Broker/main.cpp' }
            'worker' { 'Worker/main.cpp' }
            'reference' { 'Reference/main.cpp' }
            'prepare' { 'Prepare/main.cpp' }
            'common_tests' { 'tests/common_tests.cpp' }
        }
        Write-Host "Building $target"
        & $Compiler @flags $source @commonObjects -o "build/$target.exe" -lshell32
        if ($LASTEXITCODE -ne 0) { throw "Build failed: $target" }
    }
} finally { Pop-Location }
