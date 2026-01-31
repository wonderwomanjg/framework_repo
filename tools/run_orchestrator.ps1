# Run the orchestrator using the virtualenv Python 
# Usage: .\tools\run_orchestrator.ps1 --entity gels --source_system fpms --child_job contract_master --module_name landing

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPython = Join-Path $scriptDir "..\.venv311\Scripts\python.exe"

if (-not (Test-Path $venvPython)) {
    Write-Error "Virtualenv python not found at: $venvPython"
    exit 1
}
else
{
    Write-Host "Using virtualenv python at: $venvPython"
}

# Forward all args to the module
& $venvPython -m orchestrators.run_kudu @Args


