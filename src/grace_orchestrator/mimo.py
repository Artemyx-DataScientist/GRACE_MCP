"""Fixed-argv Mimo execution bridge with isolated package worktrees."""

# FILE: src/grace_orchestrator/mimo.py
# VERSION: 0.2.0
# START_MODULE_CONTRACT
#   PURPOSE: Launch a registered Mimo model in one isolated worktree without granting workflow authority.
#   SCOPE: Briefing file construction, fixed Mimo argv, log paths, detached TUI launch, and process observation.
#   DEPENDS: M-ORCH-DOMAIN, M-ORCH-REPO-BOUNDARY
#   LINKS: M-ORCH-MIMO-EXECUTOR, V-M-ORCH-MIMO-EXECUTOR, fn-launchMimoSession
#   ROLE: RUNTIME
#   MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   logger - stable Mimo launch telemetry sink.
#   MimoLaunchResult - externally observable process-launch evidence.
#   MimoRunner - owns service-managed headless process handles and fixed Mimo argv.
#   render_work_package_briefing - creates a non-authoritative scoped worker handoff projection.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.2.1 - Inline latest repair findings in rejected-package briefings.
# END_CHANGE_SUMMARY

from __future__ import annotations

from dataclasses import dataclass
import logging
from os import environ, name as os_name
from pathlib import Path
from shutil import which
import subprocess
from typing import Any, Mapping

from .models import MimoLaunchMode, OrchestratorError, OrchestratorRole

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MimoLaunchResult:
    """Process evidence returned after a fixed-argv Mimo launch."""

    argv: list[str]
    pid: int
    stdout_path: Path | None
    stderr_path: Path | None
    detached_tui: bool


class MimoRunner:
    """The MCP service owns headless children; interactive TUI windows remain user-owned."""

    def __init__(self, data_root: Path, command: str | None = None) -> None:
        self.data_root = data_root.resolve()
        requested_command = (command or environ.get("GRACE_ORCHESTRATOR_MIMO_COMMAND", "mimo")).strip()
        if not requested_command:
            raise OrchestratorError("GRACE_ORCHESTRATOR_MIMO_COMMAND must name one executable")
        self.command = self._resolve_command(requested_command)
        self._headless_processes: dict[int, subprocess.Popen[bytes]] = {}

    @staticmethod
    def _resolve_command(command: str) -> str:
        """Resolve a direct Mimo executable; never rely on a Windows shell shim."""

        direct = Path(command)
        if direct.is_file() and direct.suffix.lower() not in {".cmd", ".bat", ".ps1"}:
            return str(direct)
        if os_name == "nt":
            if direct.suffix.lower() == ".exe":
                return str(direct)
            direct_exe = which(f"{command}.exe") if direct.suffix == "" else None
            if direct_exe:
                return direct_exe
            windows_cmd = which(f"{command}.cmd") if direct.suffix == "" else str(direct)
            if windows_cmd:
                shim = Path(windows_cmd)
                binary_root = shim.parent / "node_modules" / "@mimo-ai" / "cli" / "node_modules"
                candidates = sorted(binary_root.glob("@mimo-ai/mimocode-windows-*/bin/mimo.exe"))
                preferred = [path for path in candidates if "baseline" not in path.as_posix().lower()]
                if preferred:
                    return str(preferred[0])
                if candidates:
                    return str(candidates[0])
                raise OrchestratorError(
                    "Mimo Windows shim found but no direct mimo.exe was found; set GRACE_ORCHESTRATOR_MIMO_COMMAND to mimo.exe"
                )
        return which(command) or command

    def build_command(
        self,
        *,
        mode: MimoLaunchMode,
        model: str,
        agent: str | None = None,
        workspace_path: Path,
        briefing_path: Path,
        session_id: int,
    ) -> list[str]:
        # START_CONTRACT: MimoRunner.build_command
        #   PURPOSE: Build a fixed argv vector for one registered provider/model backend and briefing.
        #   INPUTS: { mode, model, agent, workspace_path, briefing_path, session_id }
        #   OUTPUTS: { list[str] - shell-free executable argv }
        #   SIDE_EFFECTS: none.
        #   LINKS: M-ORCH-MIMO-EXECUTOR, V-M-ORCH-MIMO-EXECUTOR
        # END_CONTRACT: MimoRunner.build_command
        prompt = (
            f"Read the immutable GRACE work-package briefing at {briefing_path}. "
            "Use the configured GRACE Orchestrator MCP server, confirm its bound identity, "
            "and follow the briefing exactly. Do not claim acceptance in prose."
        )
        backend_model = normalized_explicit_backend_model(model)
        agent_binding = agent.strip() if isinstance(agent, str) and agent.strip() else None
        agent_args = ["--agent", agent_binding] if agent_binding else []
        if mode == MimoLaunchMode.HEADLESS:
            return [
                self.command,
                "run",
                *agent_args,
                "--model",
                backend_model,
                "--dir",
                str(workspace_path),
                "--file",
                str(briefing_path),
                "--title",
                f"grace-package-{session_id}",
                prompt,
            ]
        if mode == MimoLaunchMode.TUI:
            return [self.command, *agent_args, "--model", backend_model, "--trust", "--prompt", prompt]
        raise OrchestratorError(f"Unsupported Mimo launch mode: {mode}")

    def launch(
        self,
        *,
        session_id: int,
        mode: MimoLaunchMode,
        model: str,
        agent: str | None = None,
        workspace_path: Path,
        briefing_path: Path,
    ) -> MimoLaunchResult:
        # START_CONTRACT: MimoRunner.launch
        #   PURPOSE: Start a Mimo process with no shell and persistable output paths.
        #   INPUTS: { session_id, mode, model, workspace_path, briefing_path }
        #   OUTPUTS: { MimoLaunchResult - PID, argv, and log paths }
        #   SIDE_EFFECTS: Starts a local child process; TUI mode creates a user-owned console on Windows.
        #   LINKS: M-ORCH-MIMO-EXECUTOR, V-M-ORCH-MIMO-EXECUTOR
        # END_CONTRACT: MimoRunner.launch
        # START_BLOCK_LAUNCH_FIXED_ARGV_MIMO_PROCESS
        argv = self.build_command(
            mode=mode,
            model=model,
            agent=agent,
            workspace_path=workspace_path,
            briefing_path=briefing_path,
            session_id=session_id,
        )
        if mode == MimoLaunchMode.TUI:
            creationflags = getattr(subprocess, "CREATE_NEW_CONSOLE", 0) if os_name == "nt" else 0
            try:
                process = subprocess.Popen(argv, cwd=workspace_path, shell=False, creationflags=creationflags)
            except OSError as error:
                raise OrchestratorError(f"Mimo TUI launch failed: {error}") from error
            logger.info(
                "[GraceOrchestrator][mimo][TUI_SESSION_LAUNCH] started detached Mimo TUI",
                extra={"session_id": session_id, "pid": process.pid, "model": model},
            )
            return MimoLaunchResult(argv, process.pid, None, None, detached_tui=True)

        log_dir = self.data_root / "logs" / "mimo"
        log_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = log_dir / f"session-{session_id}.stdout.log"
        stderr_path = log_dir / f"session-{session_id}.stderr.log"
        with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
            try:
                process = subprocess.Popen(argv, cwd=workspace_path, shell=False, stdout=stdout, stderr=stderr)
            except OSError as error:
                raise OrchestratorError(f"Mimo headless launch failed: {error}") from error
        self._headless_processes[session_id] = process
        logger.info(
            "[GraceOrchestrator][mimo][HEADLESS_SESSION_LAUNCH] started Mimo headless session",
            extra={"session_id": session_id, "pid": process.pid, "model": model},
        )
        return MimoLaunchResult(argv, process.pid, stdout_path, stderr_path, detached_tui=False)
        # END_BLOCK_LAUNCH_FIXED_ARGV_MIMO_PROCESS

    def poll(self, session_id: int) -> int | None:
        """Return an exit code only for a service-owned current-process headless session."""

        process = self._headless_processes.get(session_id)
        if process is None:
            return None
        exit_code = process.poll()
        if exit_code is not None:
            self._headless_processes.pop(session_id, None)
        return exit_code

    def cancel(self, session_id: int) -> int:
        """Terminate only a service-owned headless child; detached TUI sessions are user-owned."""

        process = self._headless_processes.get(session_id)
        if process is None:
            raise OrchestratorError("Mimo session is not a service-owned active headless process")
        process.terminate()
        try:
            exit_code = process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            exit_code = process.wait(timeout=10)
        self._headless_processes.pop(session_id, None)
        return exit_code


LEGACY_IMPLICIT_MIMO_ALIASES = frozenset(
    {
        "auto",
        "auto-junior",
        "default",
        "mimo-auto-junior",
    }
)


def normalized_explicit_backend_model(model: str) -> str:
    normalized = model.strip()
    lowered = normalized.lower()
    if not normalized:
        raise OrchestratorError("Mimo backend model must be a non-empty explicit provider/model identifier")
    if lowered in LEGACY_IMPLICIT_MIMO_ALIASES or "/" not in normalized:
        raise OrchestratorError(
            "Mimo backend model must be explicit provider/model, for example "
            "xiaomi/mimo-v2.5 or zai-coding-plan/glm-5.2; legacy implicit aliases are blocked"
        )
    return normalized


def backend_family(model: str) -> str:
    normalized = normalized_explicit_backend_model(model).lower()
    if normalized.startswith("zai-coding-plan/") and "glm" in normalized:
        return "glm"
    if normalized.startswith("xiaomi/mimo-") or normalized.startswith("mimo/mimo-"):
        return "mimo"
    return "unknown"


def validate_backend_for_role(model: str, role: OrchestratorRole) -> None:
    family = backend_family(model)
    if role in {OrchestratorRole.WORKER_JUNIOR, OrchestratorRole.WORKER_PRO}:
        if family != "mimo":
            raise OrchestratorError(
                f"{role.value} agents must use a Xiaomi/MiMo worker backend; got {model!r}"
            )
        return
    if role in {OrchestratorRole.GLM, OrchestratorRole.TEST_OWNER}:
        if family != "glm":
            raise OrchestratorError(
                f"{role.value} agents must use the Z.ai Coding Plan GLM backend; got {model!r}"
            )
        return
    if family == "unknown":
        raise OrchestratorError(f"Unsupported Mimo backend model: {model!r}")


def default_mimocode_agent_for_role(role: str) -> str:
    if role in {OrchestratorRole.GLM.value, OrchestratorRole.TEST_OWNER.value}:
        return "build"
    return "build"


def render_work_package_briefing(
    *,
    session_id: int,
    agent: Mapping[str, Any],
    task: Mapping[str, Any],
    package: Mapping[str, Any],
    workspace_path: Path,
) -> str:
    """Render a non-authoritative handoff projection for one Mimo worker session."""

    operation_id = package.get("operation_id") or f"task-{task['id']}"
    mimo_agent = agent.get("mimo_agent") or default_mimocode_agent_for_role(str(agent["primary_role"]))
    backend_model = normalized_explicit_backend_model(str(agent["mimo_model"]))
    family = backend_family(backend_model)
    return "\n".join(
        [
            "# GRACE Mimo Work-Package Briefing",
            "",
            f"Session: {session_id}",
            f"Registered agent: {agent['name']}",
            f"Bound role required: {agent['primary_role']}",
            f"Selected provider/model backend: {backend_model}",
            f"Backend family: {family}",
            f"Selected MiMoCode TUI agent: {mimo_agent}",
            f"Isolated workspace: {workspace_path}",
            "",
            "## Operation authority",
            f"Operation id: {operation_id}",
            f"Authority mode: {package.get('authority_mode') or 'codex_led'}",
            f"Operation root: {package.get('operation_root') or 'unknown'}",
            f"Codex required: {package.get('codex_required')}",
            f"Codex instance id: {package.get('codex_instance_id') or 'not-recorded'}",
            f"GLM instance id: {package.get('glm_instance_id') or 'not-recorded'}",
            f"Branch/worktree: {package.get('branch_worktree') or workspace_path}",
            f"GLM scan/plan report: {package.get('glm_scan_plan_report') or {}}",
            f"Operation isolation: {package.get('operation_isolation') or {}}",
            "",
            "## Authority",
            "Use the Mimo MCP connection configured for this exact registered GRACE agent. First call `orchestrator.whoami`; if its identity or role differs from this briefing, stop and report a blocked session. MCP authority is process-bound and separate from the MiMoCode TUI agent/profile.",
            "",
            "## Parent task",
            f"Title: {task['title']}",
            f"Objective: {task['objective']}",
            f"Architecture intent: {task['architecture_intent']}",
            f"Constraints: {task['constraints']}",
            f"Non-goals: {task['non_goals']}",
            f"Acceptance criteria: {task['acceptance_criteria']}",
            "",
            "## Assigned package",
            f"Work-package id: {package['id']}",
            f"Title: {package['title']}",
            f"Objective: {package['objective']}",
            f"Base commit: {package['base_commit']}",
            *(
                [f"Repair source commit: {package['repair_source_commit']}"]
                if package.get("repair_source_commit")
                else []
            ),
            f"Allowed files: {package['allowed_files']}",
            f"Forbidden files: {package['forbidden_files']}",
            *(
                [
                    "",
                    "## Repair context",
                    f"Latest rejection findings: {package['repair_findings']}",
                    f"Required repair fixes: {package['repair_required_fixes']}",
                ]
                if package.get("repair_findings") or package.get("repair_required_fixes")
                else []
            ),
            "",
            "## Required sequence",
            "1. Read the registered GRACE context and call `workpackage.claim` for this exact package.",
            "2. Work only in the isolated workspace and only inside the allowed scope.",
            "3. Do not edit protected tests unless the active test-owner role explicitly owns that change.",
            "4. Commit the implementation in the workspace; run only registered tests through `repo.run_tests` when authorized.",
            "5. Use `submission.create` with the resulting head commit, evidence, and residual risks. It automatically emits WORKER_READY_FOR_REVIEW and writes the controller handoff report. A submission is not acceptance.",
            "6. On any scope, identity, dependency, or verification ambiguity, call `handoff.report_worker_event` with WORKER_BLOCKED or WORKER_NEEDS_CONTROLLER, then stop.",
        ]
    ) + "\n"
