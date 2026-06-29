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
#   LAST_CHANGE: v0.2.0 - Added the local Mimo CLI bridge with model selection and isolated worktrees.
# END_CHANGE_SUMMARY

from __future__ import annotations

from dataclasses import dataclass
import logging
from os import environ, name as os_name
from pathlib import Path
from shutil import which
import subprocess
from typing import Any, Mapping

from .models import MimoLaunchMode, OrchestratorError

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
        workspace_path: Path,
        briefing_path: Path,
        session_id: int,
    ) -> list[str]:
        # START_CONTRACT: MimoRunner.build_command
        #   PURPOSE: Build a fixed argv vector for one registered Mimo model and briefing.
        #   INPUTS: { mode, model, workspace_path, briefing_path, session_id }
        #   OUTPUTS: { list[str] - shell-free executable argv }
        #   SIDE_EFFECTS: none.
        #   LINKS: M-ORCH-MIMO-EXECUTOR, V-M-ORCH-MIMO-EXECUTOR
        # END_CONTRACT: MimoRunner.build_command
        prompt = (
            f"Read the immutable GRACE work-package briefing at {briefing_path}. "
            "Use the configured GRACE Orchestrator MCP server, confirm its bound identity, "
            "and follow the briefing exactly. Do not claim acceptance in prose."
        )
        use_cli_default_model = is_cli_default_mimo_model(model)
        if mode == MimoLaunchMode.HEADLESS:
            if use_cli_default_model:
                return [
                    self.command,
                    "run",
                    "--dir",
                    str(workspace_path),
                    "--file",
                    str(briefing_path),
                    "--title",
                    f"grace-package-{session_id}",
                    prompt,
                ]
            return [
                self.command,
                "run",
                "--model",
                model,
                "--dir",
                str(workspace_path),
                "--file",
                str(briefing_path),
                "--title",
                f"grace-package-{session_id}",
                prompt,
            ]
        if mode == MimoLaunchMode.TUI:
            if use_cli_default_model:
                return [self.command, "--trust", "--prompt", prompt]
            return [self.command, "--model", model, "--trust", "--prompt", prompt]
        raise OrchestratorError(f"Unsupported Mimo launch mode: {mode}")

    def launch(
        self,
        *,
        session_id: int,
        mode: MimoLaunchMode,
        model: str,
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


def is_cli_default_mimo_model(model: str) -> bool:
    return model.strip().lower() in {
        "auto",
        "auto-junior",
        "default",
        "mimo-auto-junior",
    }


def render_work_package_briefing(
    *,
    session_id: int,
    agent: Mapping[str, Any],
    task: Mapping[str, Any],
    package: Mapping[str, Any],
    workspace_path: Path,
) -> str:
    """Render a non-authoritative handoff projection for one Mimo worker session."""

    return "\n".join(
        [
            "# GRACE Mimo Work-Package Briefing",
            "",
            f"Session: {session_id}",
            f"Registered agent: {agent['name']}",
            f"Bound role required: {agent['primary_role']}",
            f"Selected Mimo model: {agent['mimo_model']}",
            f"Isolated workspace: {workspace_path}",
            "",
            "## Authority",
            "Use the Mimo MCP connection configured for this exact registered agent. First call `orchestrator.whoami`; if its identity or role differs from this briefing, stop and report a blocked session. MCP authority is process-bound, never selected in a tool argument.",
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
