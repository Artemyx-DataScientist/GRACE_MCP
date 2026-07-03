from __future__ import annotations

from typing import Any


WORKER_REPORT_FIELDS = [
    "operation id",
    "authority mode",
    "task id",
    "module id",
    "files read",
    "files changed",
    "contract delta",
    "commands run with exact results",
    "what is scaffolded",
    "what is wired",
    "what is verified",
    "unverified gaps",
    "stop conditions encountered",
    "protected-test and forbidden-scope statement",
    "graph delta proposal",
    "verification delta proposal",
    "next action",
]


def contract_discovery(
    *,
    module_id: str = "M-ORCH-LEDGER",
    verification_id: str = "V-M-ORCH-LEDGER",
) -> dict[str, Any]:
    return {
        "status": "pass",
        "contracts_read": ["docs/development-plan.xml", "docs/knowledge-graph.xml", "docs/verification-plan.xml"],
        "module_refs": [module_id],
        "verification_refs": [verification_id],
        "missing_contracts": [],
        "conflicts": [],
    }


def packet_kwargs(
    *,
    module_id: str = "M-ORCH-LEDGER",
    verification_id: str = "V-M-ORCH-LEDGER",
) -> dict[str, Any]:
    return {
        "contract_discovery": contract_discovery(module_id=module_id, verification_id=verification_id),
        "test_surface": ["unit"],
        "rollback_boundary": "Revert only package allowed files.",
        "compact_report_format": WORKER_REPORT_FIELDS,
        "module_id": module_id,
        "verification_id": verification_id,
        "commands_allowed": ["unit"],
        "session_routing": {
            "mode": "checkpoint_from_cache_anchor",
            "workstream": module_id,
            "reuse_allowed": "same workstream and cache anchor only",
            "new_session_when": ["scope changes"],
        },
        "cache_anchor": f"GRACE:{module_id}:{verification_id}",
        "retry_budget": 1,
        "stop_conditions": ["scope drift", "protected test changes"],
        "operation_id": "op-test",
        "authority_mode": "codex_led",
        "operation_root": "glm-5.2",
        "codex_required": True,
        "codex_instance_id": "codex-test",
        "glm_instance_id": "glm-5.2",
        "branch_worktree": "test-worktree",
        "glm_scan_plan_report": {"status": "provided", "architecture_map": "test"},
        "operation_isolation": {"status": "isolated"},
    }


def worker_report(
    *,
    task_id: int,
    package_id: int,
    files_changed: list[str],
    module_id: str = "M-ORCH-LEDGER",
) -> dict[str, Any]:
    return {
        "operation_id": "op-test",
        "authority_mode": "codex_led",
        "task_id": task_id,
        "work_package_id": package_id,
        "module_id": module_id,
        "files_read": files_changed,
        "files_changed": files_changed,
        "contract_delta": "no contract delta",
        "commands_run_with_exact_results": ["unit: exit 0"],
        "scaffolded": "none",
        "wired": files_changed,
        "verified": ["unit command evidence"],
        "unverified_gaps": ["no external smoke in unit test"],
        "stop_conditions_encountered": "none",
        "protected_test_and_forbidden_scope_statement": "No protected tests or forbidden files changed.",
        "graph_delta_proposal": "none",
        "verification_delta_proposal": "none",
        "next_action": "controller review",
    }
