"""Stable resource URI declarations for read-only workflow projections."""

# FILE: src/grace_orchestrator/resources.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Define read-only M-ORCH-MCP-SERVER resource URI and GRACE artifact mappings.
#   SCOPE: Stable constants only; no ledger read or mutation.
#   DEPENDS: M-ORCH-MCP-SERVER
#   LINKS: M-ORCH-MCP-SERVER, V-M-ORCH-MCP-SERVER
#   ROLE: CONFIG
#   MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   RESOURCE_URIS - MCP resource template set.
#   GRACE_ARTIFACT_TYPES - GRACE filename to ledger artifact mapping.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.3.0 - Added operational packet artifact needed by Codex final hook gate.
# END_CHANGE_SUMMARY

from __future__ import annotations


RESOURCE_URIS = {
    "orchestrator://project/{project_id}",
    "orchestrator://project/{project_id}/active",
    "orchestrator://task/{task_id}",
    "orchestrator://task/{task_id}/summary",
    "orchestrator://workpackage/{work_package_id}",
    "orchestrator://workpackage/{work_package_id}/summary",
    "orchestrator://submission/{submission_id}",
    "orchestrator://review/{review_id}",
    "orchestrator://mimo-session/{session_id}",
    "grace://project/{project_id}/requirements.xml",
    "grace://project/{project_id}/technology.xml",
    "grace://project/{project_id}/development-plan.xml",
    "grace://project/{project_id}/verification-plan.xml",
    "grace://project/{project_id}/knowledge-graph.xml",
    "grace://project/{project_id}/operational-packets.xml",
}


GRACE_ARTIFACT_TYPES = {
    "requirements.xml": "requirements",
    "technology.xml": "technology",
    "development-plan.xml": "development_plan",
    "verification-plan.xml": "verification_plan",
    "knowledge-graph.xml": "knowledge_graph",
    "operational-packets.xml": "operational_packets",
}
