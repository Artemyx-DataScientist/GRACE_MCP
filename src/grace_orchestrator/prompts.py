"""Reusable, compact role prompts exposed by the MCP server."""

# FILE: src/grace_orchestrator/prompts.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Provide read-only role prompts for M-ORCH-MCP-SERVER handoffs.
#   SCOPE: Prompt text only; no ledger mutation or actor authorization.
#   DEPENDS: M-ORCH-MCP-SERVER
#   LINKS: M-ORCH-MCP-SERVER, V-M-ORCH-MCP-SERVER
#   ROLE: CONFIG
#   MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   PROMPT_NAMES - canonical MCP prompt names.
#   codex_create_task_prompt - task-intent handoff instruction.
#   glm_decompose_task_prompt - GRACE planning instruction.
#   worker_implement_package_prompt - junior bounded implementation instruction.
#   pro_repair_package_prompt - Pro repair instruction.
#   mimo_connect_orchestrator_prompt - Mimo MCP identity-binding instruction.
#   glm_acceptance_review_prompt - intermediate acceptance instruction.
#   codex_final_acceptance_prompt - final acceptance instruction.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.2.0 - Added Mimo MCP connection instruction for role-bound worker sessions.
# END_CHANGE_SUMMARY

from __future__ import annotations


PROMPT_NAMES = {
    "codex_create_task",
    "glm_decompose_task",
    "worker_implement_package",
    "pro_repair_package",
    "mimo_connect_orchestrator",
    "glm_acceptance_review",
    "codex_final_acceptance",
}


def codex_create_task_prompt() -> str:
    return (
        "Create one immutable TaskSpec: objective, architecture intent, non-goals, "
        "allowed files, forbidden files, acceptance criteria, risks, and GLM handoff. "
        "Do not use selected/requested state as runtime truth."
    )


def glm_decompose_task_prompt() -> str:
    return (
        "Read the Codex TaskSpec. Create GRACE and verification revisions before production "
        "implementation, then create bounded packages whose scope is a subset of the parent. "
        "Raise ambiguity as blocked; do not rewrite architecture intent."
    )


def worker_implement_package_prompt() -> str:
    return (
        "Implement exactly the assigned work package. Use only its allowed files, report the "
        "base/head commit evidence and risks, and stop on a missing owner, verification, or dependency."
    )


def pro_repair_package_prompt() -> str:
    return (
        "Repair only the recorded rejected package and its GLM findings. Do not refactor unrelated "
        "code or broaden scope. Submit a new commit-derived diff for review."
    )


def mimo_connect_orchestrator_prompt() -> str:
    return (
        "Add the role-specific STDIO profile returned by mimo.connection_profile through Mimo's "
        "MCP-server setup. On every session call orchestrator.whoami first; stop if the process-bound "
        "identity is not the registered agent and role. The MCP connection grants workflow access, not "
        "automatic acceptance or permission to change files outside the assigned package."
    )


def glm_acceptance_review_prompt() -> str:
    return (
        "Review the original TaskSpec, package scope, server-derived diff, test records, and risks. "
        "Accept only if evidence matches the contract; otherwise issue specific repair requirements."
    )


def codex_final_acceptance_prompt() -> str:
    return (
        "Review the TaskSpec, all GRACE revisions, package outcomes, derived diffs, test evidence, "
        "GLM report, and residual risks. Final acceptance cannot precede GLM acceptance of every package."
    )
