# FILE: scripts/Start-GraceHostContinuation.ps1
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Start the GRACE host-level continuation supervisor outside MCP tool-call lifetimes.
#   SCOPE: Resolve GRACE_MCP repository-local Python module path and invoke grace_orchestrator.host_continuation.
#   DEPENDS: M-ORCH-HOST-CONTINUATION
#   LINKS: M-ORCH-HOST-CONTINUATION, V-M-ORCH-HOST-CONTINUATION
#   ROLE: SCRIPT
#   MAP_MODE: LOCALS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   script body - validates data directory configuration and launches the Python host supervisor.
# END_MODULE_MAP

[CmdletBinding()]
param(
    [string]$DataDir = $env:GRACE_ORCHESTRATOR_DATA_DIR,
    [switch]$Once,
    [double]$PollIntervalSeconds = 5.0
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($DataDir)) {
    throw "Set GRACE_ORCHESTRATOR_DATA_DIR or pass -DataDir."
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$srcPath = Join-Path $repoRoot "src"
$python = $env:PYTHON
if ([string]::IsNullOrWhiteSpace($python)) {
    $python = "python"
}

if ([string]::IsNullOrWhiteSpace($env:PYTHONPATH)) {
    $env:PYTHONPATH = $srcPath
} else {
    $env:PYTHONPATH = "$srcPath;$env:PYTHONPATH"
}

$arguments = @(
    "-m",
    "grace_orchestrator.host_continuation",
    "--data-dir",
    $DataDir,
    "--poll-interval-seconds",
    [string]$PollIntervalSeconds
)

if ($Once) {
    $arguments += "--once"
}

& $python @arguments
exit $LASTEXITCODE
