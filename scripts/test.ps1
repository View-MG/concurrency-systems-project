$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$previousGuiTests = $env:LGA_GUI_PROCESS_TESTS
function Run-Python([string[]]$Arguments) {
    & python @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Test failed: python $Arguments" }
}
Push-Location $projectRoot
try {
    & "$PSScriptRoot/build.ps1"
    Run-Python @('tests/processes.py')
    $env:LGA_GUI_PROCESS_TESTS = '1'
    Run-Python @('-m', 'unittest', 'discover', '-s', 'tests', '-p', 'test_gui.py', '-v')
    Run-Python @('-m', 'unittest', 'discover', '-s', 'tests', '-p', 'test_experiments.py', '-v')
    Run-Python @('-m', 'unittest', 'discover', '-s', 'MOS/tests', '-p', 'test_mos.py', '-v')
    Run-Python @('MOS/riscv/build.py')
    Write-Host 'PASS: build, process, GUI, measurement, MOS and RISC-V checks'
} finally {
    $env:LGA_GUI_PROCESS_TESTS = $previousGuiTests
    Pop-Location
}
