"""FastMCP surface for the local GRACE orchestration ledger."""

# FILE: src/grace_orchestrator/server.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Expose M-ORCH-MCP-SERVER tools, resources, prompts, and Mimo dispatch over STDIO FastMCP.
#   SCOPE: Process-bound identity, tool registration, Mimo launch projection, and read-only resources.
#   DEPENDS: M-ORCH-DOMAIN, M-ORCH-LEDGER, M-ORCH-REPO-BOUNDARY, M-ORCH-MIMO-EXECUTOR
#   LINKS: M-ORCH-MCP-SERVER, V-M-ORCH-MCP-SERVER, M-ORCH-MIMO-EXECUTOR, fn-createServer
#   ROLE: RUNTIME
#   MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   create_server - builds the process-identity-bound FastMCP application.
#   REQUIRED_TOOLS - required callable orchestration surface.
#   REQUIRED_PROMPTS - required reusable handoff prompts.
#   REQUIRED_RESOURCES - required workflow resource URIs.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.3.6 - Expose mode-aware work-package reassignment.
# END_CHANGE_SUMMARY

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

from .models import ActorIdentity, MimoLaunchMode, OrchestratorError, OrchestratorRole, ProjectInitInput
from .prompts import (
    PROMPT_NAMES,
    codex_create_task_prompt,
    codex_final_acceptance_prompt,
    glm_acceptance_review_prompt,
    glm_decompose_task_prompt,
    mimo_connect_orchestrator_prompt,
    pro_repair_package_prompt,
    worker_implement_package_prompt,
)
from .policy import project_next_action
from .repo import RepositoryBoundary
from .resources import GRACE_ARTIFACT_TYPES, RESOURCE_URIS
from .service import OrchestratorService


ALL_REQUIRED_TOOLS = frozenset({
    "orchestrator.whoami",
    "inbox.next",
    "inbox.list",
    "project.init",
    "project.set_test_commands",
    "agent.register",
    "agent.set_availability",
    "task.create_codex_task",
    "task.plan",
    "task.get",
    "task.get_summary",
    "task.get_next_action",
    "task.recover_cancelled_packages",
    "task.request_final_review",
    "task.close",
    "task.force_transition",
    "role.delegate",
    "grace.upsert_artifact",
    "verification.register_plan",
    "gate.contract_discovery",
    "gate.validate_execution_packet",
    "gate.validate_worker_report",
    "gate.agent_infra_lint",
    "gate.acceptance_review",
    "workpackage.create",
    "workpackage.assign",
    "workpackage.reassign",
    "workpackage.reassign_by_controller",
    "workpackage.claim",
    "workpackage.cancel",
    "workpackage.force_reset",
    "workpackage.get_summary",
    "submission.create",
    "submission.controller_repair",
    "submission.controller_task_completion",
    "review.glm_submit",
    "review.codex_submit",
    "repo.status",
    "repo.diff",
    "repo.run_tests",
    "mimo.connection_profile",
    "mimo.launch_package",
    "mimo.get_session",
    "mimo.poll_session",
    "mimo.cancel_session",
    "mimo.recover_prepared_session",
    "mimo.recover_orphaned_running_session",
    "mimo.record_tui_closed",
    "handoff.list_events",
    "handoff.list_events_page",
    "handoff.wait_for_event",
    "handoff.report_worker_event",
    "audit.list",
    "audit.list_page",
    "continuation.ack",
    "continuation.resolve",
    "continuation.get",
    "continuation.requeue_dead_letter",
})

REQUIRED_TOOLS = ALL_REQUIRED_TOOLS

ADMINISTRATIVE_TOOLS = frozenset({
    "task.force_transition",
    "workpackage.force_reset",
    "continuation.ack",
    "continuation.resolve",
    "continuation.get",
    "continuation.requeue_dead_letter",
})

WORKER_TOOLS = frozenset({
    "orchestrator.whoami",
    "inbox.next",
    "inbox.list",
    "workpackage.claim",
    "workpackage.get_summary",
    "task.get_summary",
    "submission.create",
    "handoff.report_worker_event",
    "repo.run_tests",
    "gate.validate_worker_report",
})


REQUIRED_TOOLS_BY_ROLE: dict[OrchestratorRole, frozenset[str]] = {
    OrchestratorRole.USER: ALL_REQUIRED_TOOLS,
    OrchestratorRole.CODEX: ALL_REQUIRED_TOOLS,
    OrchestratorRole.GLM: ALL_REQUIRED_TOOLS - ADMINISTRATIVE_TOOLS,
    OrchestratorRole.TEST_OWNER: ALL_REQUIRED_TOOLS - ADMINISTRATIVE_TOOLS,
    OrchestratorRole.WORKER_PRO: WORKER_TOOLS,
    OrchestratorRole.WORKER_JUNIOR: WORKER_TOOLS,
}
REQUIRED_PROMPTS = PROMPT_NAMES
REQUIRED_RESOURCES = RESOURCE_URIS


def _plain(value: Any) -> Any:
    """Ensure FastMCP receives only JSON-compatible projections."""

    return json.loads(json.dumps(value, default=str))





def create_server(actor: ActorIdentity, data_dir: Path) -> FastMCP:
    # START_CONTRACT: create_server
    #   PURPOSE: Register all MCP surface handlers against one bound actor and local ledger.
    #   INPUTS: { actor: ActorIdentity - process-bound role, data_dir: Path - local ledger root }
    #   OUTPUTS: { FastMCP - configured STDIO-capable server }
    #   SIDE_EFFECTS: Creates local ledger directory/database; registers handlers in memory.
    #   LINKS: M-ORCH-MCP-SERVER, V-M-ORCH-MCP-SERVER
    # END_CONTRACT: create_server
    # START_BLOCK_BIND_IDENTITY_AND_REGISTER_MCP_SURFACE
    """Create a process-identity-bound MCP server; tool inputs cannot select a role."""

    data_dir.mkdir(parents=True, exist_ok=True)
    service = OrchestratorService(data_dir / "ledger.sqlite3")
    mcp = FastMCP(
        "grace-orchestrator-mcp",
        instructions=(
            "Local GRACE workflow ledger. Actor authority is bound at server startup; "
            "do not provide actor roles in tool arguments."
        ),
    )
    allowed_tools_for_role = REQUIRED_TOOLS_BY_ROLE.get(actor.primary_role, ALL_REQUIRED_TOOLS)

    def tool_decorator(name: str, description: str):
        if name in allowed_tools_for_role:
            return mcp.tool(name, description=description)
        def dummy_decorator(fn):
            return fn
        return dummy_decorator

    @tool_decorator("orchestrator.whoami", description="Return server-bound actor identity.")
    def whoami() -> dict[str, Any]:
        granted = actor.granted_role.value if actor.granted_role else actor.primary_role.value
        requested = actor.requested_role.value if actor.requested_role else granted
        return {
            "actor_id": actor.actor_id,
            "actor_name": actor.name,
            "primary_role": granted,
            "requested_role": requested,
            "granted_role": granted,
            "runtime": actor.runtime.value if actor.runtime else None,
            "provider": actor.provider,
            "model": actor.model,
            "reasoning_profile": actor.reasoning_profile,
        }

    @tool_decorator("project.init", description="Initialize local orchestration for one repository.")
    def project_init(
        name: str,
        repo_path: str,
        grace_path: str,
        main_branch: str,
        allowed_test_commands: dict[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        request = ProjectInitInput(
            name=name,
            repo_path=repo_path,
            grace_path=grace_path,
            main_branch=main_branch,
            allowed_test_commands=allowed_test_commands or {},
        )
        return _plain(
            service.init_project(
                actor,
                request.name,
                Path(request.repo_path),
                Path(request.grace_path),
                request.main_branch,
                request.allowed_test_commands,
            )
        )

    @tool_decorator("project.set_test_commands", description="Replace the fixed project test-command allowlist.")
    def set_project_test_commands(
        project_id: int,
        allowed_test_commands: dict[str, list[str]],
    ) -> dict[str, Any]:
        return _plain(service.set_allowed_test_commands(actor, project_id, allowed_test_commands))

    @tool_decorator("agent.register", description="Register an available model agent and its permitted role capabilities.")
    def register_agent(
        project_id: int,
        name: str,
        primary_role: str,
        capabilities: list[str],
        availability: str = "available",
        mimo_model: str | None = None,
        mimo_agent: str | None = None,
        runtime: str | None = None,
        provider: str | None = None,
        model: str | None = None,
        reasoning_profile: str | None = None,
    ) -> dict[str, Any]:
        try:
            parsed_primary_role = OrchestratorRole(primary_role)
            parsed_capabilities = [OrchestratorRole(value) for value in capabilities]
        except ValueError as error:
            raise OrchestratorError("Agent registration contains an unknown role capability") from error
        return _plain(
            service.register_agent(
                actor,
                project_id,
                name,
                parsed_primary_role,
                parsed_capabilities,
                availability,
                mimo_model,
                mimo_agent,
                runtime=runtime,
                provider=provider,
                model=model,
                reasoning_profile=reasoning_profile,
            )
        )

    @tool_decorator("agent.set_availability", description="Record whether a registered model agent is available for routing.")
    def set_agent_availability(project_id: int, name: str, availability: str) -> dict[str, Any]:
        return _plain(service.set_agent_availability(actor, project_id, name, availability))

    @tool_decorator("task.create_codex_task", description="Create an immutable top-level Codex task.")
    def create_codex_task(
        project_id: int,
        title: str,
        objective: str,
        architecture_intent: str,
        constraints: list[str],
        non_goals: list[str],
        acceptance_criteria: list[str],
        allowed_files: list[str],
        forbidden_files: list[str],
        parent_task_id: int | None = None,
    ) -> dict[str, Any]:
        return _plain(
            service.create_codex_task(
                actor,
                project_id,
                title,
                objective,
                architecture_intent,
                constraints,
                non_goals,
                acceptance_criteria,
                allowed_files,
                forbidden_files,
                parent_task_id,
            )
        )

    @tool_decorator("task.plan", description="Advance a Codex task into GLM GRACE planning.")
    def plan_task(task_id: int) -> dict[str, Any]:
        return _plain(service.plan_task(actor, task_id))

    @tool_decorator("task.get", description="Read task, work packages, GRACE revisions, and reviews.")
    def get_task(task_id: int) -> dict[str, Any]:
        return _plain(service.get_task(task_id))

    @tool_decorator("task.get_next_action", description="Project the next valid gate without changing state.")
    def get_next_action(task_id: int) -> dict[str, Any]:
        task = service.get_task(task_id)
        next_step = project_next_action(task["status"], task.get("work_packages"))
        blocked_reason = None
        if next_step["role"] not in {"none", "glm_or_codex", actor.primary_role.value}:
            try:
                service._authorize(actor, OrchestratorRole(next_step["role"]), task["project_id"], task_id)
            except OrchestratorError as error:
                blocked_reason = str(error)
        return _plain({"current_state": task["status"], "next": next_step, "blocked_reason": blocked_reason})


    @tool_decorator("task.request_final_review", description="Advance an all-GLM-accepted task to Codex final review.")
    def request_final_review(task_id: int) -> dict[str, Any]:
        return _plain(service.request_final_review(actor, task_id))

    @tool_decorator("task.close", description="Close a final Codex-accepted task.")
    def close_task(task_id: int) -> dict[str, Any]:
        return _plain(service.close_task(actor, task_id))

    @tool_decorator("role.delegate", description="Record explicit, expiring fallback authority for an unavailable role.")
    def delegate_role(
        project_id: int,
        task_id: int | None,
        unavailable_role: str,
        substitute_actor: str,
        reason: str,
        expires_at: str,
    ) -> dict[str, Any]:
        try:
            role = OrchestratorRole(unavailable_role)
            expiry = datetime.fromisoformat(expires_at)
        except ValueError as error:
            raise OrchestratorError("Role delegation requires a known role and ISO-8601 expiry") from error
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=UTC)
        return _plain(
            service.delegate_role(actor, project_id, task_id, role, substitute_actor, role, reason, expiry)
        )

    @tool_decorator("grace.upsert_artifact", description="Append a revisioned GLM-owned GRACE artifact.")
    def upsert_artifact(
        project_id: int,
        task_id: int,
        artifact_type: str,
        content: str,
        path: str,
    ) -> dict[str, Any]:
        return _plain(service.upsert_artifact(actor, project_id, task_id, artifact_type, content, path))

    @tool_decorator("verification.register_plan", description="Register GLM verification planning before production packages.")
    def register_verification_plan(
        task_id: int,
        test_strategy: str,
        test_commands: list[str],
        risk_coverage: list[str] | None = None,
        acceptance_mapping: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return _plain(
            service.register_verification_plan(
                actor,
                task_id,
                test_strategy,
                test_commands,
                risk_coverage,
                acceptance_mapping,
            )
        )

    @tool_decorator("gate.contract_discovery", description="Discover current contracts, graph refs, verification refs, and rule anchors for a task scope.")
    def contract_discovery_gate(
        project_id: int,
        affected_files: list[str],
        task_id: int | None = None,
    ) -> dict[str, Any]:
        return _plain(service.discover_contracts(actor, project_id, affected_files, task_id))

    @tool_decorator("gate.validate_execution_packet", description="Validate a worker packet before it can be dispatched.")
    def validate_execution_packet_gate(task_id: int, packet: dict[str, Any]) -> dict[str, Any]:
        return _plain(service.validate_execution_packet(actor, task_id, packet))

    @tool_decorator("gate.validate_worker_report", description="Validate a worker report before submission or acceptance.")
    def validate_worker_report_gate(
        work_package_id: int,
        worker_report: dict[str, Any],
        evidence_files: list[str] | None = None,
    ) -> dict[str, Any]:
        return _plain(service.validate_worker_report(actor, work_package_id, worker_report, evidence_files))

    @tool_decorator("gate.agent_infra_lint", description="Validate AGENTS/GRACE enforcement files without shell execution.")
    def agent_infra_lint_gate(project_id: int) -> dict[str, Any]:
        return _plain(service.lint_agent_infra(actor, project_id))

    @tool_decorator("gate.acceptance_review", description="Project whether the task currently satisfies GLM/Codex acceptance prerequisites.")
    def acceptance_review_gate(task_id: int) -> dict[str, Any]:
        return _plain(service.acceptance_review_gate(actor, task_id))

    @tool_decorator("workpackage.create", description="Create a GLM-scoped package under a Codex task.")
    def create_work_package(
        task_id: int,
        title: str,
        objective: str,
        allowed_files: list[str],
        forbidden_files: list[str],
        assigned_junior_agent: str,
        assigned_pro_agent: str,
        base_commit: str,
        contract_discovery: dict[str, Any] | None = None,
        test_surface: list[str] | None = None,
        rollback_boundary: str = "",
        compact_report_format: list[str] | None = None,
        module_id: str = "",
        verification_id: str = "",
        commands_allowed: list[str] | None = None,
        session_routing: dict[str, Any] | None = None,
        cache_anchor: str = "",
        retry_budget: int = 1,
        stop_conditions: list[str] | None = None,
        operation_id: str = "",
        authority_mode: str = "codex_led",
        operation_root: str = "",
        codex_required: bool | None = None,
        codex_instance_id: str = "",
        glm_instance_id: str = "",
        branch_worktree: str = "",
        glm_scan_plan_report: dict[str, Any] | None = None,
        operation_isolation: dict[str, Any] | None = None,
        pro_api_assignment: str = "",
    ) -> dict[str, Any]:
        return _plain(
            service.create_work_package(
                actor,
                task_id,
                title,
                objective,
                allowed_files,
                forbidden_files,
                assigned_junior_agent,
                assigned_pro_agent,
                base_commit,
                contract_discovery,
                test_surface,
                rollback_boundary,
                compact_report_format,
                module_id,
                verification_id,
                commands_allowed,
                session_routing,
                cache_anchor,
                retry_budget,
                stop_conditions,
                operation_id,
                authority_mode,
                operation_root,
                codex_required,
                codex_instance_id,
                glm_instance_id,
                branch_worktree,
                glm_scan_plan_report,
                operation_isolation,
                pro_api_assignment,
            )
        )

    @tool_decorator("workpackage.assign", description="Advance a created package to assigned state.")
    def assign_work_package(work_package_id: int) -> dict[str, Any]:
        return _plain(service.assign_work_package(actor, work_package_id))

    @tool_decorator("workpackage.reassign_by_controller", description="Audit and assign an unclaimed package to a registered junior worker under explicit Codex authority.")
    def reassign_work_package_by_controller(
        work_package_id: int,
        assigned_junior_agent: str,
        reason: str,
    ) -> dict[str, Any]:
        return _plain(
            service.reassign_work_package_by_controller(
                actor, work_package_id, assigned_junior_agent, reason
            )
        )

    @tool_decorator("workpackage.reassign", description="Reassign an unclaimed package using GLM authority for glm_direct and Codex authority otherwise.")
    def reassign_work_package(
        work_package_id: int,
        assigned_junior_agent: str,
        reason: str,
    ) -> dict[str, Any]:
        return _plain(
            service.reassign_work_package(actor, work_package_id, assigned_junior_agent, reason)
        )

    @tool_decorator("workpackage.claim", description="Claim an assigned junior or rejected Pro repair package.")
    def claim_work_package(work_package_id: int) -> dict[str, Any]:
        return _plain(service.claim_work_package(actor, work_package_id))

    @tool_decorator("workpackage.cancel", description="Cancel a stale or superseded package without treating it as accepted work.")
    def cancel_work_package(work_package_id: int, reason: str) -> dict[str, Any]:
        return _plain(service.cancel_work_package(actor, work_package_id, reason))

    @tool_decorator("task.recover_cancelled_packages", description="Repair a package-phase task only when every historical package is cancelled.")
    def recover_cancelled_packages(task_id: int, reason: str) -> dict[str, Any]:
        return _plain(service.recover_task_after_cancel_all(actor, task_id, reason))

    @tool_decorator("workpackage.force_reset", description="Perform an administrative force-reset of a stuck package back to CREATED state while preserving historical evidence.")
    def force_reset_work_package(
        work_package_id: int,
        reason: str,
        expected_current_status: str,
    ) -> dict[str, Any]:
        return _plain(
            service.force_reset_work_package(
                actor,
                work_package_id,
                reason=reason,
                expected_current_status=expected_current_status,
            )
        )

    @tool_decorator("task.force_transition", description="Perform an administrative recovery state transition on a task or workpackage with optimistic locking.")
    def force_transition(
        entity_type: str,
        entity_id: int,
        target_status: str,
        reason: str,
        expected_current_status: str,
        allow_terminal: bool = False,
    ) -> dict[str, Any]:
        return _plain(
            service.force_transition(
                actor,
                entity_type=entity_type,
                entity_id=entity_id,
                target_status=target_status,
                reason=reason,
                expected_current_status=expected_current_status,
                allow_terminal=allow_terminal,
            )
        )

    @tool_decorator("continuation.ack", description="Idempotently acknowledge adoption of a continuation by a revived controller.")
    def ack_continuation(
        continuation_id: str,
        source_event_id: str,
        attempt_id: str,
        controller_session_id: str | None = None,
    ) -> dict[str, Any]:
        return _plain(
            service.ack_continuation(
                actor,
                continuation_id,
                source_event_id,
                attempt_id,
                controller_session_id=controller_session_id,
            )
        )

    @tool_decorator("continuation.resolve", description="Resolve a continuation delivery after successful action or terminal outcome.")
    def resolve_continuation(
        continuation_id: str,
        source_event_id: str,
        attempt_id: str,
        resolution_notes: str = "",
        resolution_data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return _plain(
            service.resolve_continuation(
                actor,
                continuation_id,
                source_event_id,
                attempt_id,
                resolution_notes=resolution_notes,
                resolution_data=resolution_data,
            )
        )


    @tool_decorator("continuation.get", description="Fetch details of a continuation delivery record.")
    def get_continuation(continuation_id: str) -> dict[str, Any]:
        return _plain(service.get_continuation(continuation_id))

    @tool_decorator("continuation.requeue_dead_letter", description="Manually requeue a dead-lettered continuation back to PENDING state.")
    def requeue_dead_letter_continuation(continuation_id: str, reason: str) -> dict[str, Any]:
        return _plain(service.requeue_dead_letter_continuation(actor, continuation_id, reason=reason))

    @tool_decorator("mimo.connection_profile", description="Return a role-bound STDIO MCP profile to add in Mimo for one registered agent.")
    def mimo_connection_profile(project_id: int, agent_name: str) -> dict[str, Any]:
        return _plain(service.mimo_connection_profile(actor, project_id, agent_name))

    @tool_decorator("mimo.launch_package", description="Create an isolated Git worktree and launch the assigned Mimo model in TUI mode for one package.")
    def launch_mimo_package(work_package_id: int, mode: str = "tui") -> dict[str, Any]:
        try:
            launch_mode = MimoLaunchMode(mode)
        except ValueError as error:
            raise OrchestratorError("Mimo launch mode must be headless or tui") from error
        if launch_mode != MimoLaunchMode.TUI:
            raise OrchestratorError("Mimo dispatch is TUI-only; use mode='tui'")
        return _plain(service.launch_mimo_session(actor, work_package_id, launch_mode))

    @tool_decorator("mimo.get_session", description="Read immutable launch evidence and current recorded state for one Mimo session.")
    def get_mimo_session(session_id: int) -> dict[str, Any]:
        return _plain(service.get_mimo_session(session_id))

    @tool_decorator("mimo.poll_session", description="Record an observed exit code for a service-owned headless Mimo process.")
    def poll_mimo_session(session_id: int) -> dict[str, Any]:
        return _plain(service.poll_mimo_session(actor, session_id))

    @tool_decorator("mimo.cancel_session", description="Terminate only a service-owned active headless Mimo process.")
    def cancel_mimo_session(session_id: int) -> dict[str, Any]:
        return _plain(service.cancel_mimo_session(actor, session_id))

    @tool_decorator("mimo.recover_prepared_session", description="Record a controller-observed aborted pre-launch session, only when no workspace, briefing, or process evidence exists.")
    def recover_prepared_mimo_session(session_id: int, observation: str) -> dict[str, Any]:
        return _plain(service.recover_prepared_mimo_session(actor, session_id, observation))

    @tool_decorator("mimo.recover_orphaned_running_session", description="Record a controller-observed lost headless session only after its persisted PID is absent.")
    def recover_orphaned_running_mimo_session(session_id: int, observation: str) -> dict[str, Any]:
        return _plain(service.recover_orphaned_running_mimo_session(actor, session_id, observation))

    @tool_decorator("mimo.record_tui_closed", description="Record a controller-observed detached Mimo TUI closure without changing package acceptance.")
    def record_tui_closed(session_id: int, observation: str) -> dict[str, Any]:
        return _plain(service.record_detached_mimo_session_closed(actor, session_id, observation))

    @tool_decorator("handoff.list_events", description="Read machine-readable worker/controller handoff events for one package.")
    def list_handoff_events(work_package_id: int) -> list[dict[str, Any]]:
        return _plain(service.list_handoff_events(actor, work_package_id))

    @tool_decorator("handoff.wait_for_event", description="Wait for a new machine-readable handoff event for one work package.")
    def wait_for_handoff_event(work_package_id: int, after_event_count: int = 0, timeout_seconds: int = 600) -> dict[str, Any]:
        return _plain(service.wait_for_handoff_event(actor, work_package_id, after_event_count, timeout_seconds))

    @tool_decorator("handoff.report_worker_event", description="Report a closed blocked, needs-controller, or failed worker handoff event.")
    def report_worker_handoff_event(work_package_id: int, event_type: str, message: str) -> dict[str, Any]:
        return _plain(service.report_worker_handoff_event(actor, work_package_id, event_type, message))

    @tool_decorator("submission.create", description="Store a worker submission from server-derived Git commits.")
    def create_submission(
        work_package_id: int,
        summary: str,
        head_commit: str,
        tests_run: list[dict[str, Any]],
        risk_notes: str,
        worker_report: dict[str, Any],
    ) -> dict[str, Any]:
        package = service.get_work_package(work_package_id)
        task = service.get_task(package["task_id"])
        project = service.get_project(task["project_id"])
        evidence = RepositoryBoundary(Path(project["repo_path"])).derive_submission(package["base_commit"], head_commit)
        return _plain(service.submit_package(actor, work_package_id, summary, evidence, tests_run, risk_notes, worker_report))

    @tool_decorator("submission.controller_repair", description="Store an audited Codex controller repair submission for a rejected package when Pro repair is unavailable.")
    def create_controller_repair_submission(
        work_package_id: int,
        summary: str,
        head_commit: str,
        tests_run: list[dict[str, Any]],
        risk_notes: str,
        controller_report: dict[str, Any],
    ) -> dict[str, Any]:
        package = service.get_work_package(work_package_id)
        task = service.get_task(package["task_id"])
        project = service.get_project(task["project_id"])
        evidence = RepositoryBoundary(Path(project["repo_path"])).derive_submission(package["base_commit"], head_commit)
        return _plain(
            service.submit_controller_repair(
                actor,
                work_package_id,
                summary,
                evidence,
                tests_run,
                risk_notes,
                controller_report,
            )
        )

    @tool_decorator("submission.controller_task_completion", description="Store audited Codex controller-owned task completion evidence when no worker package is required.")
    def create_controller_task_completion(
        task_id: int,
        summary: str,
        base_commit: str,
        head_commit: str,
        tests_run: list[dict[str, Any]],
        risk_notes: str,
        controller_report: dict[str, Any],
    ) -> dict[str, Any]:
        task = service.get_task(task_id)
        project = service.get_project(task["project_id"])
        evidence = RepositoryBoundary(Path(project["repo_path"])).derive_submission(base_commit, head_commit)
        return _plain(
            service.submit_controller_task_completion(
                actor,
                task_id,
                summary,
                evidence,
                tests_run,
                risk_notes,
                controller_report,
            )
        )

    @tool_decorator("review.glm_submit", description="Submit GLM intermediate package review.")
    def glm_review(
        target_id: int,
        decision: str,
        findings: list[dict[str, Any] | str],
        required_fixes: list[dict[str, Any] | str],
    ) -> dict[str, Any]:
        return _plain(service.review_package(actor, target_id, decision, findings, required_fixes))

    @tool_decorator("review.codex_submit", description="Submit Codex final task review.")
    def codex_review(
        task_id: int,
        decision: str,
        findings: list[dict[str, Any] | str],
        required_fixes: list[dict[str, Any] | str],
    ) -> dict[str, Any]:
        return _plain(service.final_review(actor, task_id, decision, findings, required_fixes))

    @tool_decorator("inbox.next", description="Retrieve the single highest-priority inbox item for the bound actor identity.")
    def inbox_next(project_id: int | None = None) -> dict[str, Any]:
        return _plain(service.inbox_next(actor, project_id=project_id))

    @tool_decorator("inbox.list", description="Retrieve actor-bound inbox envelopes in deterministic priority order.")
    def inbox_list(project_id: int | None = None, limit: int = 20) -> dict[str, Any]:
        return _plain(service.inbox_list(actor, project_id=project_id, limit=limit))

    @tool_decorator("repo.status", description="Read project-local Git status without mutation.")
    def repo_status(project_id: int, work_package_id: int | None = None) -> dict[str, Any]:
        return _plain(service.repo_status(actor, project_id, work_package_id=work_package_id))

    @tool_decorator("repo.diff", description="Read a project-local Git diff for a fixed scope.")
    def repo_diff(project_id: int, work_package_id: int | None = None, scope: str = "all", file_list: list[str] | None = None) -> dict[str, str]:
        return _plain(service.repo_diff(actor, project_id, work_package_id=work_package_id, scope=scope, file_list=file_list))

    @tool_decorator("repo.run_tests", description="Run a registered allowlisted test command by key.")
    def repo_run_tests(
        project_id: int,
        task_id: int,
        work_package_id: int | None,
        command_key: str,
    ) -> dict[str, Any]:
        result = service.run_allowed_test(actor, project_id, task_id, work_package_id, command_key)
        return _plain(
            {
                "command_key": result.command_key,
                "exit_code": result.exit_code,
                "stdout_path": result.stdout_path,
                "stderr_path": result.stderr_path,
            }
        )

    @tool_decorator("task.get_summary", description="Retrieve a compact, structured summary of a task, its package counts, and recommended next_action.")
    def get_task_summary(task_id: int) -> dict[str, Any]:
        return _plain(service.get_task_summary(actor, task_id))

    @tool_decorator("workpackage.get_summary", description="Retrieve a compact summary of a work package.")
    def get_work_package_summary(work_package_id: int) -> dict[str, Any]:
        return _plain(service.get_work_package_summary(actor, work_package_id))

    @tool_decorator("handoff.list_events_page", description="Paginated event retrieval for work package handoffs.")
    def list_handoff_events_page(work_package_id: int, after_event_id: str | None = None, limit: int = 20) -> dict[str, Any]:
        return _plain(service.list_handoff_events_page(actor, work_package_id, after_event_id=after_event_id, limit=limit))

    @tool_decorator("audit.list", description="List all audit events for a task or globally.")
    def list_audit(task_id: int | None = None) -> list[dict[str, Any]]:
        return _plain(service.list_audit(actor, task_id=task_id))

    @tool_decorator("audit.list_page", description="Paginated audit event retrieval.")
    def list_audit_page(task_id: int | None = None, after_audit_id: int = 0, limit: int = 20) -> dict[str, Any]:
        return _plain(service.list_audit_page(actor, task_id=task_id, after_audit_id=after_audit_id, limit=limit))

    def artifact_resource_for(artifact_type: str):
        def artifact_resource(project_id: int) -> str:
            return service.get_latest_grace_artifact(actor, project_id, artifact_type)

        return artifact_resource

    role = actor.primary_role
    if role in {OrchestratorRole.USER, OrchestratorRole.CODEX, OrchestratorRole.GLM, OrchestratorRole.TEST_OWNER}:
        @mcp.resource("orchestrator://project/{project_id}", mime_type="application/json")
        def project_resource(project_id: int) -> str:
            return json.dumps(service.get_project(actor, project_id), default=str)

        @mcp.resource("orchestrator://project/{project_id}/active", mime_type="application/json")
        def project_active_resource(project_id: int) -> str:
            snapshot = service.get_orchestrator_status_snapshot(actor, project_id=project_id)
            return json.dumps(snapshot, default=str)

        @mcp.resource("orchestrator://task/{task_id}", mime_type="application/json")
        def task_resource(task_id: int) -> str:
            return json.dumps(service.get_task(actor, task_id), default=str)

        @mcp.resource("orchestrator://workpackage/{work_package_id}", mime_type="application/json")
        def work_package_resource(work_package_id: int) -> str:
            return json.dumps(service.get_work_package(actor, work_package_id), default=str)

        @mcp.resource("orchestrator://review/{review_id}", mime_type="application/json")
        def review_resource(review_id: int) -> str:
            return json.dumps(service.get_review(actor, review_id), default=str)

        @mcp.resource("orchestrator://mimo-session/{session_id}", mime_type="application/json")
        def mimo_session_resource(session_id: int) -> str:
            return json.dumps(service.get_mimo_session(actor, session_id), default=str)

        for filename, artifact_type in GRACE_ARTIFACT_TYPES.items():
            uri = f"grace://project/{{project_id}}/{filename}"
            mcp.resource(uri, mime_type="application/xml")(artifact_resource_for(artifact_type))

    @mcp.resource("orchestrator://task/{task_id}/summary", mime_type="application/json")
    def task_summary_resource(task_id: int) -> str:
        return json.dumps(service.get_task_summary(actor, task_id), default=str)

    @mcp.resource("orchestrator://workpackage/{work_package_id}/summary", mime_type="application/json")
    def work_package_summary_resource(work_package_id: int) -> str:
        return json.dumps(service.get_work_package_summary(actor, work_package_id), default=str)

    @mcp.resource("orchestrator://submission/{submission_id}", mime_type="application/json")
    def submission_resource(submission_id: int) -> str:
        return json.dumps(service.get_submission(actor, submission_id), default=str)


    @mcp.prompt("codex_create_task")
    def codex_create_task() -> str:
        return codex_create_task_prompt()

    @mcp.prompt("glm_decompose_task")
    def glm_decompose_task() -> str:
        return glm_decompose_task_prompt()

    @mcp.prompt("worker_implement_package")
    def worker_implement_package() -> str:
        return worker_implement_package_prompt()

    @mcp.prompt("pro_repair_package")
    def pro_repair_package() -> str:
        return pro_repair_package_prompt()

    @mcp.prompt("mimo_connect_orchestrator")
    def mimo_connect_orchestrator() -> str:
        return mimo_connect_orchestrator_prompt()

    @mcp.prompt("glm_acceptance_review")
    def glm_acceptance_review() -> str:
        return glm_acceptance_review_prompt()

    @mcp.prompt("codex_final_acceptance")
    def codex_final_acceptance() -> str:
        return codex_final_acceptance_prompt()

    return mcp
    # END_BLOCK_BIND_IDENTITY_AND_REGISTER_MCP_SURFACE
