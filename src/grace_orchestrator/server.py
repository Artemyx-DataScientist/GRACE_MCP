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
#   LAST_CHANGE: v0.3.2 - Expose Codex controller repair submission for unavailable Pro repair paths.
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
from .repo import RepositoryBoundary
from .resources import GRACE_ARTIFACT_TYPES, RESOURCE_URIS
from .service import OrchestratorService


REQUIRED_TOOLS = {
    "orchestrator.whoami",
    "project.init",
    "project.set_test_commands",
    "agent.register",
    "agent.set_availability",
    "task.create_codex_task",
    "task.plan",
    "task.get",
    "task.get_next_action",
    "task.request_final_review",
    "task.close",
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
    "workpackage.claim",
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
    "handoff.wait_for_event",
    "handoff.report_worker_event",
}
REQUIRED_PROMPTS = PROMPT_NAMES
REQUIRED_RESOURCES = RESOURCE_URIS


def _plain(value: Any) -> Any:
    """Ensure FastMCP receives only JSON-compatible projections."""

    return json.loads(json.dumps(value, default=str))


def _next_action(status: str) -> dict[str, str]:
    actions = {
        "CODEX_TASK_CREATED": {"role": "glm", "action": "task.plan"},
        "GLM_GRACE_PLANNED": {"role": "glm", "action": "verification.register_plan or submission.controller_task_completion"},
        "GLM_TESTS_PREPARED": {"role": "glm", "action": "workpackage.create or submission.controller_task_completion"},
        "WORK_PACKAGES_CREATED": {"role": "glm", "action": "workpackage.assign"},
        "WORK_PACKAGES_ASSIGNED": {"role": "worker_junior", "action": "workpackage.claim"},
        "GLM_REJECTED_REPAIR_REQUIRED": {"role": "glm", "action": "mimo.launch_package or submission.controller_repair"},
        "GLM_ACCEPTED": {"role": "codex", "action": "task.request_final_review"},
        "CODEX_FINAL_REVIEW": {"role": "codex", "action": "review.codex_submit"},
        "CODEX_ACCEPTED": {"role": "codex", "action": "task.close"},
        "TASK_CLOSED": {"role": "codex", "action": "task.next"},
    }
    return actions.get(status, {"role": "none", "action": "blocked"})


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

    @mcp.tool("orchestrator.whoami", description="Return server-bound actor identity.")
    def whoami() -> dict[str, str]:
        return {"actor_name": actor.name, "primary_role": actor.primary_role.value}

    @mcp.tool("project.init", description="Initialize local orchestration for one repository.")
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

    @mcp.tool("project.set_test_commands", description="Replace the fixed project test-command allowlist.")
    def set_project_test_commands(
        project_id: int,
        allowed_test_commands: dict[str, list[str]],
    ) -> dict[str, Any]:
        return _plain(service.set_allowed_test_commands(actor, project_id, allowed_test_commands))

    @mcp.tool("agent.register", description="Register an available model agent and its permitted role capabilities.")
    def register_agent(
        project_id: int,
        name: str,
        primary_role: str,
        capabilities: list[str],
        availability: str = "available",
        mimo_model: str | None = None,
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
            )
        )

    @mcp.tool("agent.set_availability", description="Record whether a registered model agent is available for routing.")
    def set_agent_availability(project_id: int, name: str, availability: str) -> dict[str, Any]:
        return _plain(service.set_agent_availability(actor, project_id, name, availability))

    @mcp.tool("task.create_codex_task", description="Create an immutable top-level Codex task.")
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

    @mcp.tool("task.plan", description="Advance a Codex task into GLM GRACE planning.")
    def plan_task(task_id: int) -> dict[str, Any]:
        return _plain(service.plan_task(actor, task_id))

    @mcp.tool("task.get", description="Read task, work packages, GRACE revisions, and reviews.")
    def get_task(task_id: int) -> dict[str, Any]:
        return _plain(service.get_task(task_id))

    @mcp.tool("task.get_next_action", description="Project the next valid gate without changing state.")
    def get_next_action(task_id: int) -> dict[str, Any]:
        task = service.get_task(task_id)
        next_step = _next_action(task["status"])
        blocked_reason = None
        if next_step["role"] not in {"none", actor.primary_role.value}:
            try:
                service._authorize(actor, OrchestratorRole(next_step["role"]), task["project_id"], task_id)
            except OrchestratorError as error:
                blocked_reason = str(error)
        return _plain({"current_state": task["status"], "next": next_step, "blocked_reason": blocked_reason})

    @mcp.tool("task.request_final_review", description="Advance an all-GLM-accepted task to Codex final review.")
    def request_final_review(task_id: int) -> dict[str, Any]:
        return _plain(service.request_final_review(actor, task_id))

    @mcp.tool("task.close", description="Close a final Codex-accepted task.")
    def close_task(task_id: int) -> dict[str, Any]:
        return _plain(service.close_task(actor, task_id))

    @mcp.tool("role.delegate", description="Record explicit, expiring fallback authority for an unavailable role.")
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

    @mcp.tool("grace.upsert_artifact", description="Append a revisioned GLM-owned GRACE artifact.")
    def upsert_artifact(
        project_id: int,
        task_id: int,
        artifact_type: str,
        content: str,
        path: str,
    ) -> dict[str, Any]:
        return _plain(service.upsert_artifact(actor, project_id, task_id, artifact_type, content, path))

    @mcp.tool("verification.register_plan", description="Register GLM verification planning before production packages.")
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

    @mcp.tool("gate.contract_discovery", description="Discover current contracts, graph refs, verification refs, and rule anchors for a task scope.")
    def contract_discovery_gate(
        project_id: int,
        affected_files: list[str],
        task_id: int | None = None,
    ) -> dict[str, Any]:
        return _plain(service.discover_contracts(actor, project_id, affected_files, task_id))

    @mcp.tool("gate.validate_execution_packet", description="Validate a worker packet before it can be dispatched.")
    def validate_execution_packet_gate(task_id: int, packet: dict[str, Any]) -> dict[str, Any]:
        return _plain(service.validate_execution_packet(actor, task_id, packet))

    @mcp.tool("gate.validate_worker_report", description="Validate a worker report before submission or acceptance.")
    def validate_worker_report_gate(
        work_package_id: int,
        worker_report: dict[str, Any],
        evidence_files: list[str] | None = None,
    ) -> dict[str, Any]:
        return _plain(service.validate_worker_report(actor, work_package_id, worker_report, evidence_files))

    @mcp.tool("gate.agent_infra_lint", description="Validate AGENTS/GRACE enforcement files without shell execution.")
    def agent_infra_lint_gate(project_id: int) -> dict[str, Any]:
        return _plain(service.lint_agent_infra(actor, project_id))

    @mcp.tool("gate.acceptance_review", description="Project whether the task currently satisfies GLM/Codex acceptance prerequisites.")
    def acceptance_review_gate(task_id: int) -> dict[str, Any]:
        return _plain(service.acceptance_review_gate(actor, task_id))

    @mcp.tool("workpackage.create", description="Create a GLM-scoped package under a Codex task.")
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
            )
        )

    @mcp.tool("workpackage.assign", description="Advance a created package to assigned state.")
    def assign_work_package(work_package_id: int) -> dict[str, Any]:
        return _plain(service.assign_work_package(actor, work_package_id))

    @mcp.tool("workpackage.claim", description="Claim an assigned junior or rejected Pro repair package.")
    def claim_work_package(work_package_id: int) -> dict[str, Any]:
        return _plain(service.claim_work_package(actor, work_package_id))

    @mcp.tool("mimo.connection_profile", description="Return a role-bound STDIO MCP profile to add in Mimo for one registered agent.")
    def mimo_connection_profile(project_id: int, agent_name: str) -> dict[str, Any]:
        return _plain(service.mimo_connection_profile(actor, project_id, agent_name))

    @mcp.tool("mimo.launch_package", description="Create an isolated Git worktree and launch the assigned Mimo model in TUI mode for one package.")
    def launch_mimo_package(work_package_id: int, mode: str = "tui") -> dict[str, Any]:
        try:
            launch_mode = MimoLaunchMode(mode)
        except ValueError as error:
            raise OrchestratorError("Mimo launch mode must be headless or tui") from error
        if launch_mode != MimoLaunchMode.TUI:
            raise OrchestratorError("Mimo dispatch is TUI-only; use mode='tui'")
        return _plain(service.launch_mimo_session(actor, work_package_id, launch_mode))

    @mcp.tool("mimo.get_session", description="Read immutable launch evidence and current recorded state for one Mimo session.")
    def get_mimo_session(session_id: int) -> dict[str, Any]:
        return _plain(service.get_mimo_session(session_id))

    @mcp.tool("mimo.poll_session", description="Record an observed exit code for a service-owned headless Mimo process.")
    def poll_mimo_session(session_id: int) -> dict[str, Any]:
        return _plain(service.poll_mimo_session(actor, session_id))

    @mcp.tool("mimo.cancel_session", description="Terminate only a service-owned active headless Mimo process.")
    def cancel_mimo_session(session_id: int) -> dict[str, Any]:
        return _plain(service.cancel_mimo_session(actor, session_id))

    @mcp.tool("mimo.recover_prepared_session", description="Record a controller-observed aborted pre-launch session, only when no workspace, briefing, or process evidence exists.")
    def recover_prepared_mimo_session(session_id: int, observation: str) -> dict[str, Any]:
        return _plain(service.recover_prepared_mimo_session(actor, session_id, observation))

    @mcp.tool("mimo.recover_orphaned_running_session", description="Record a controller-observed lost headless session only after its persisted PID is absent.")
    def recover_orphaned_running_mimo_session(session_id: int, observation: str) -> dict[str, Any]:
        return _plain(service.recover_orphaned_running_mimo_session(actor, session_id, observation))

    @mcp.tool("mimo.record_tui_closed", description="Record a controller-observed detached Mimo TUI closure without changing package acceptance.")
    def record_tui_closed(session_id: int, observation: str) -> dict[str, Any]:
        return _plain(service.record_detached_mimo_session_closed(actor, session_id, observation))

    @mcp.tool("handoff.list_events", description="Read machine-readable worker/controller handoff events for one package.")
    def list_handoff_events(work_package_id: int) -> list[dict[str, Any]]:
        return _plain(service.list_handoff_events(actor, work_package_id))

    @mcp.tool("handoff.wait_for_event", description="Wait for a new machine-readable handoff event for one work package.")
    def wait_for_handoff_event(work_package_id: int, after_event_count: int = 0, timeout_seconds: int = 600) -> dict[str, Any]:
        return _plain(service.wait_for_handoff_event(actor, work_package_id, after_event_count, timeout_seconds))

    @mcp.tool("handoff.report_worker_event", description="Report a closed blocked, needs-controller, or failed worker handoff event.")
    def report_worker_handoff_event(work_package_id: int, event_type: str, message: str) -> dict[str, Any]:
        return _plain(service.report_worker_handoff_event(actor, work_package_id, event_type, message))

    @mcp.tool("submission.create", description="Store a worker submission from server-derived Git commits.")
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

    @mcp.tool("submission.controller_repair", description="Store an audited Codex controller repair submission for a rejected package when Pro repair is unavailable.")
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

    @mcp.tool("submission.controller_task_completion", description="Store audited Codex controller-owned task completion evidence when no worker package is required.")
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

    @mcp.tool("review.glm_submit", description="Submit GLM intermediate package review.")
    def glm_review(
        target_id: int,
        decision: str,
        findings: list[dict[str, Any] | str],
        required_fixes: list[dict[str, Any] | str],
    ) -> dict[str, Any]:
        return _plain(service.review_package(actor, target_id, decision, findings, required_fixes))

    @mcp.tool("review.codex_submit", description="Submit Codex final task review.")
    def codex_review(
        task_id: int,
        decision: str,
        findings: list[dict[str, Any] | str],
        required_fixes: list[dict[str, Any] | str],
    ) -> dict[str, Any]:
        return _plain(service.final_review(actor, task_id, decision, findings, required_fixes))

    @mcp.tool("repo.status", description="Read project-local Git status without mutation.")
    def repo_status(project_id: int) -> dict[str, Any]:
        project = service.get_project(project_id)
        return _plain(RepositoryBoundary(Path(project["repo_path"])).status())

    @mcp.tool("repo.diff", description="Read a project-local Git diff for a fixed scope.")
    def repo_diff(project_id: int, scope: str = "all", file_list: list[str] | None = None) -> dict[str, str]:
        project = service.get_project(project_id)
        return {"diff": RepositoryBoundary(Path(project["repo_path"])).diff(scope, file_list)}

    @mcp.tool("repo.run_tests", description="Run a registered allowlisted test command by key.")
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

    @mcp.resource("orchestrator://project/{project_id}", mime_type="application/json")
    def project_resource(project_id: int) -> str:
        return json.dumps(service.get_project(project_id), default=str)

    @mcp.resource("orchestrator://task/{task_id}", mime_type="application/json")
    def task_resource(task_id: int) -> str:
        return json.dumps(service.get_task(task_id), default=str)

    @mcp.resource("orchestrator://workpackage/{work_package_id}", mime_type="application/json")
    def work_package_resource(work_package_id: int) -> str:
        return json.dumps(service.get_work_package(work_package_id), default=str)

    @mcp.resource("orchestrator://submission/{submission_id}", mime_type="application/json")
    def submission_resource(submission_id: int) -> str:
        row = service.store.connection.execute("SELECT * FROM submissions WHERE id = ?", (submission_id,)).fetchone()
        return json.dumps(dict(row) if row else {"error": "not_found"}, default=str)

    @mcp.resource("orchestrator://review/{review_id}", mime_type="application/json")
    def review_resource(review_id: int) -> str:
        row = service.store.connection.execute("SELECT * FROM reviews WHERE id = ?", (review_id,)).fetchone()
        return json.dumps(dict(row) if row else {"error": "not_found"}, default=str)

    @mcp.resource("orchestrator://mimo-session/{session_id}", mime_type="application/json")
    def mimo_session_resource(session_id: int) -> str:
        return json.dumps(service.get_mimo_session(session_id), default=str)

    def artifact_resource_for(artifact_type: str):
        def artifact_resource(project_id: int) -> str:
            row = service.store.connection.execute(
                """SELECT content FROM grace_artifacts WHERE project_id = ? AND artifact_type = ?
                   ORDER BY revision DESC LIMIT 1""",
                (project_id, artifact_type),
            ).fetchone()
            if row is None:
                raise OrchestratorError(f"No {artifact_type} artifact has been registered for project {project_id}")
            return str(row["content"])

        return artifact_resource

    for filename, artifact_type in GRACE_ARTIFACT_TYPES.items():
        uri = f"grace://project/{{project_id}}/{filename}"
        mcp.resource(uri, mime_type="application/xml")(artifact_resource_for(artifact_type))

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
