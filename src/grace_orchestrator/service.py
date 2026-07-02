"""Application service owning orchestrator transitions and their audit records."""

# FILE: src/grace_orchestrator/service.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Own M-ORCH-LEDGER transitions, trusted post-mutation hooks, delegation, and external execution evidence.
#   SCOPE: Authorized workflow mutations, HookRegistry dispatch, Mimo records, and read projections over OrchestratorStore.
#   DEPENDS: M-ORCH-DOMAIN, M-ORCH-REPO-BOUNDARY, M-ORCH-MIMO-EXECUTOR, M-ORCH-HOOKS
#   LINKS: M-ORCH-LEDGER, V-M-ORCH-LEDGER, M-ORCH-DOMAIN, M-ORCH-MIMO-EXECUTOR, M-ORCH-HOOKS, fn-appendAudit
#   ROLE: RUNTIME
#   MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   logger - stable ledger audit telemetry sink.
#   REQUIRED_GRACE_ARTIFACT_TYPES - canonical artifact set required by the Codex final gate hook.
#   OrchestratorService - sole state-changing facade used by MCP tools.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.4.5 - Support iterative rejected-package repair with inline repair briefing context.
# END_CHANGE_SUMMARY

from __future__ import annotations

from datetime import UTC, datetime
import ctypes
from ctypes import wintypes
import json
import logging
import os
from pathlib import Path, PurePosixPath
import sqlite3
import sys
import threading
import time
from typing import Any, Iterable, Mapping, Sequence

from .db import OrchestratorStore
from .hooks import HookContext, HookEvent, HookRegistry, install_default_hooks
from .mimo import MimoRunner, render_work_package_briefing
from .models import (
    ActorIdentity,
    MimoLaunchMode,
    MimoSessionStatus,
    OrchestratorError,
    OrchestratorRole,
    SubmissionEvidence,
    TaskStatus,
    TestRunResult,
    WorkPackageStatus,
    stable_hash,
)
from .permissions import authorize_role
from .policy import (
    discover_contracts as policy_discover_contracts,
    lint_agent_infra as policy_lint_agent_infra,
    require_gate_pass,
    validate_contract_discovery as policy_validate_contract_discovery,
    validate_execution_packet as policy_validate_execution_packet,
    validate_worker_report as policy_validate_worker_report,
)
from .repo import RepositoryBoundary, resolve_within_root, validate_scoped_files
from .state_machine import assert_task_transition, assert_work_package_transition

logger = logging.getLogger(__name__)

REQUIRED_GRACE_ARTIFACT_TYPES = frozenset(
    {
        "requirements",
        "technology",
        "development_plan",
        "verification_plan",
        "knowledge_graph",
        "operational_packets",
    }
)

GRACE_ARTIFACT_PATHS = {
    "requirements": "docs/requirements.xml",
    "technology": "docs/technology.xml",
    "development_plan": "docs/development-plan.xml",
    "verification_plan": "docs/verification-plan.xml",
    "knowledge_graph": "docs/knowledge-graph.xml",
    "operational_packets": "docs/operational-packets.xml",
}

HANDOFF_WAIT_RETURN_GRACE_SECONDS = 5.0


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _process_exists(pid: int) -> bool:
    """Probe a persisted local PID without shelling out or inspecting any command text."""

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        # Windows reports an out-of-range or already-reaped PID as WinError 87.
        return False
    return True


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _loads(value: str) -> Any:
    return json.loads(value)


def _row(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        raise OrchestratorError("Requested record does not exist")
    return dict(row)


class _HandoffSignal:
    """Wake a controller waiting across separate local stdio MCP processes."""

    _WAIT_OBJECT_0 = 0

    def __init__(self, data_root: Path) -> None:
        self._condition = threading.Condition()
        self._handle: int | None = None
        if os.name != "nt":
            return
        event_name = "Local\\GraceOrchestratorHandoff_" + stable_hash(str(data_root).casefold())[:24]
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateEventW.argtypes = (wintypes.LPVOID, wintypes.BOOL, wintypes.BOOL, wintypes.LPCWSTR)
        kernel32.CreateEventW.restype = wintypes.HANDLE
        kernel32.SetEvent.argtypes = (wintypes.HANDLE,)
        kernel32.SetEvent.restype = wintypes.BOOL
        kernel32.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
        kernel32.WaitForSingleObject.restype = wintypes.DWORD
        self._kernel32 = kernel32
        handle = kernel32.CreateEventW(None, False, False, event_name)
        if handle:
            self._handle = int(handle)

    def notify(self) -> None:
        with self._condition:
            self._condition.notify_all()
        if self._handle is not None:
            self._kernel32.SetEvent(self._handle)

    def wait(self, timeout_seconds: float) -> bool:
        if self._handle is not None:
            wait_result = self._kernel32.WaitForSingleObject(
                self._handle,
                max(0, min(int(timeout_seconds * 1000), 600_000)),
            )
            return wait_result == self._WAIT_OBJECT_0
        with self._condition:
            return self._condition.wait(timeout_seconds)


class OrchestratorService:
    """The only state-changing facade used by MCP tools."""

    def __init__(self, database_path: Path, mimo_runner: MimoRunner | None = None) -> None:
        self.store = OrchestratorStore(database_path)
        self.data_root = database_path.parent.resolve()
        self.mimo_runner = mimo_runner or MimoRunner(self.data_root)
        self._handoff_signal = _HandoffSignal(self.data_root)
        self.hooks = HookRegistry()
        install_default_hooks(self.hooks)

    def _audit(
        self,
        conn: sqlite3.Connection,
        actor: ActorIdentity,
        effective_role: OrchestratorRole,
        event_type: str,
        target_type: str,
        target_id: int,
        payload: Mapping[str, object],
    ) -> None:
        conn.execute(
            """INSERT INTO audit_log (
                event_type, actor_role, effective_role, actor_agent, target_type,
                target_id, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                event_type,
                actor.primary_role.value,
                effective_role.value,
                actor.name,
                target_type,
                target_id,
                _json(dict(payload)),
                _now(),
            ),
        )
        logger.info("[GraceOrchestrator][ledger][ATOMIC_AUDIT_APPEND] audit event appended", extra={"event_type": event_type, "target_type": target_type, "target_id": target_id})

    def _project(self, project_id: int) -> dict[str, Any]:
        return _row(self.store.fetchone("SELECT * FROM projects WHERE id = ?", (project_id,)))

    def _task(self, task_id: int) -> dict[str, Any]:
        return _row(self.store.fetchone("SELECT * FROM tasks WHERE id = ?", (task_id,)))

    def _package(self, package_id: int) -> dict[str, Any]:
        return _row(self.store.fetchone("SELECT * FROM work_packages WHERE id = ?", (package_id,)))

    def _mimo_session(self, session_id: int) -> dict[str, Any]:
        return _row(self.store.fetchone("SELECT * FROM mimo_sessions WHERE id = ?", (session_id,)))

    def _handoff_paths(self, project_id: int, task_id: int, package_id: int) -> tuple[Path, Path, Path]:
        """Return the central, non-worktree run directory and its event/report projections."""

        run_root = self.data_root / "runs" / f"project-{project_id}" / f"task-{task_id}" / f"package-{package_id}"
        return run_root, run_root / "events.ndjson", run_root / "handoff"

    def _append_handoff_event(
        self,
        project_id: int,
        task_id: int,
        package_id: int,
        event_type: str,
        actor_name: str,
        payload: Mapping[str, object],
    ) -> dict[str, Any]:
        """Append one closed-schema handoff event without executing user-provided commands."""

        allowed = {
            "WORKER_STARTED",
            "WORKER_BLOCKED",
            "WORKER_NEEDS_CONTROLLER",
            "WORKER_READY_FOR_REVIEW",
            "WORKER_DONE",
            "WORKER_FAILED",
            "CONTROLLER_ACCEPTED",
            "CONTROLLER_REPAIR_SUBMITTED",
            "CONTROLLER_REWORK_REQUESTED",
            "CONTROLLER_ESCALATED_TO_USER",
        }
        if event_type not in allowed:
            raise OrchestratorError(f"Unsupported handoff event type: {event_type}")
        run_root, events_path, handoff_dir = self._handoff_paths(project_id, task_id, package_id)
        handoff_dir.mkdir(parents=True, exist_ok=True)
        event = {
            "type": event_type,
            "project_id": project_id,
            "task_id": task_id,
            "work_package_id": package_id,
            "worker": actor_name,
            "created_at": _now(),
            "payload": dict(payload),
        }
        with events_path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(event, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        self._handoff_signal.notify()
        event["events_path"] = str(events_path)
        event["run_root"] = str(run_root)
        return event

    def _write_worker_handoff_report(
        self,
        project_id: int,
        task_id: int,
        package: Mapping[str, Any],
        submission: Mapping[str, Any],
    ) -> str:
        """Write a controller-readable projection of immutable worker submission evidence."""

        _, _, handoff_dir = self._handoff_paths(project_id, task_id, int(package["id"]))
        handoff_dir.mkdir(parents=True, exist_ok=True)
        report_path = handoff_dir / f"WP-{int(package['id'])}.report.md"
        report_path.write_text(
            "\n".join(
                [
                    "# GRACE worker handoff",
                    "",
                    f"Work package: {package['title']} (id {package['id']})",
                    f"Worker: {submission['submitted_by_agent']}",
                    f"Base commit: {submission['base_commit']}",
                    f"Head commit: {submission['head_commit']}",
                    f"Files changed: {submission['files_changed_json']}",
                    f"Tests: {submission['tests_run_json']}",
                    "",
                    "## Summary",
                    str(submission["summary"]),
                    "",
                    "## Residual risks",
                    str(submission["risk_notes"]),
                    "",
                    "Controller action: inspect the scoped diff and make an explicit acceptance, rework, or escalation decision.",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return str(report_path)

    def _delegations(self, project_id: int, task_id: int | None) -> list[dict[str, Any]]:
        return [
            dict(item)
            for item in self.store.fetchall(
                """SELECT * FROM role_delegations
                   WHERE project_id = ? AND (task_id IS NULL OR task_id = ?)""",
                (project_id, task_id),
            )
        ]

    def _authorize(
        self,
        actor: ActorIdentity,
        required_role: OrchestratorRole,
        project_id: int,
        task_id: int | None = None,
    ) -> OrchestratorRole:
        return authorize_role(actor, required_role, self._delegations(project_id, task_id))

    def _validate_scope_patterns(self, patterns: Sequence[str], label: str) -> None:
        for pattern in patterns:
            path = PurePosixPath(pattern)
            if path.is_absolute() or ".." in path.parts:
                raise OrchestratorError(f"{label} scope pattern is not project-relative: {pattern}")

    def _validate_hook_scope(
        self,
        task: Mapping[str, Any],
        package: Mapping[str, Any] | None,
        payload: Mapping[str, Any],
    ) -> None:
        task_allowed = _loads(str(task["allowed_files_json"]))
        task_forbidden = _loads(str(task["forbidden_files_json"]))
        self._validate_scope_patterns(task_allowed, "Task allowed")
        self._validate_scope_patterns(task_forbidden, "Task forbidden")
        files_changed = payload.get("files_changed")
        if package is not None:
            package_allowed = _loads(str(package["allowed_files_json"]))
            package_forbidden = _loads(str(package["forbidden_files_json"]))
            self._validate_scope_patterns(package_allowed, "Work-package allowed")
            self._validate_scope_patterns(package_forbidden, "Work-package forbidden")
            if not package_allowed or not all(
                self._scope_is_subset(task_allowed, pattern) for pattern in package_allowed
            ):
                raise OrchestratorError("Hook rejected a work package outside the parent task scope")
            if files_changed is not None:
                if not isinstance(files_changed, Sequence) or isinstance(files_changed, (str, bytes)):
                    raise OrchestratorError("Hook submission files must be a sequence")
                validate_scoped_files(
                    [str(item) for item in files_changed],
                    allowed_files=package_allowed,
                    forbidden_files=package_forbidden,
                )
        elif files_changed is not None:
            if not isinstance(files_changed, Sequence) or isinstance(files_changed, (str, bytes)):
                raise OrchestratorError("Hook submission files must be a sequence")
            validate_scoped_files(
                [str(item) for item in files_changed],
                allowed_files=task_allowed,
                forbidden_files=task_forbidden,
            )
        artifact_path = payload.get("artifact_path")
        if artifact_path is not None:
            project = self._project(int(task["project_id"]))
            resolve_within_root(Path(project["repo_path"]), str(artifact_path))

    def _enable_worker_pro_for_hook(
        self,
        conn: sqlite3.Connection,
        package: Mapping[str, Any] | None,
    ) -> None:
        if package is None:
            raise OrchestratorError("GLM rejection hook requires a work package")
        conn.execute(
            "UPDATE work_packages SET worker_pro_available = 1, updated_at = ? WHERE id = ?",
            (_now(), package["id"]),
        )

    def _require_final_grace_artifacts(self, conn: sqlite3.Connection, task_id: int) -> dict[str, Any]:
        artifact_types = {
            str(row["artifact_type"])
            for row in conn.execute(
                "SELECT DISTINCT artifact_type FROM grace_artifacts WHERE task_id = ?", (task_id,)
            ).fetchall()
        }
        missing = sorted(REQUIRED_GRACE_ARTIFACT_TYPES - artifact_types)
        auto_imported: list[dict[str, object]] = []
        if missing:
            task = self._task(task_id)
            project = self._project(int(task["project_id"]))
            repo_root = Path(project["repo_path"])
            timestamp = _now()
            for artifact_type in tuple(missing):
                relative_path = GRACE_ARTIFACT_PATHS[artifact_type]
                artifact_path = repo_root / relative_path
                if not artifact_path.is_file():
                    continue
                content = artifact_path.read_text(encoding="utf-8")
                revision = int(
                    conn.execute(
                        "SELECT COALESCE(MAX(revision), 0) + 1 FROM grace_artifacts WHERE task_id = ? AND artifact_type = ?",
                        (task_id, artifact_type),
                    ).fetchone()[0]
                )
                cursor = conn.execute(
                    """INSERT INTO grace_artifacts (
                        project_id, task_id, artifact_type, path, content, content_hash,
                        revision, created_by_agent, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        project["id"],
                        task_id,
                        artifact_type,
                        relative_path,
                        content,
                        stable_hash(content),
                        revision,
                        "system:auto-import",
                        timestamp,
                    ),
                )
                artifact_types.add(artifact_type)
                auto_imported.append(
                    {
                        "artifact_id": int(cursor.lastrowid),
                        "artifact_type": artifact_type,
                        "path": relative_path,
                        "revision": revision,
                    }
                )
            missing = sorted(REQUIRED_GRACE_ARTIFACT_TYPES - artifact_types)
        if missing:
            raise OrchestratorError(
                "Codex final review requires GRACE artifacts: " + ", ".join(missing)
            )
        return {"status": "pass", "auto_imported": auto_imported, "artifact_types": sorted(artifact_types)}

    def _dispatch_hook(
        self,
        conn: sqlite3.Connection,
        actor: ActorIdentity,
        effective_role: OrchestratorRole,
        event: HookEvent,
        task: Mapping[str, Any],
        package: Mapping[str, Any] | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        hook_payload = dict(payload or {})
        target_type = "work_package" if package is not None else "task"
        target_id = int(package["id"] if package is not None else task["id"])

        def audit(hook_name: str, details: Mapping[str, Any]) -> None:
            self._audit(
                conn,
                actor,
                effective_role,
                f"hook.{hook_name}",
                target_type,
                target_id,
                dict(details),
            )

        def close_task() -> None:
            fresh_task = self._task(int(task["id"]))
            if TaskStatus(fresh_task["status"]) != TaskStatus.CODEX_ACCEPTED:
                raise OrchestratorError("Codex accepted hook may close only a CODEX_ACCEPTED task")
            self._advance_task(
                conn,
                actor,
                effective_role,
                fresh_task,
                TaskStatus.TASK_CLOSED,
                "task.closed_by_hook",
            )

        context = HookContext(
            event=event,
            project_id=int(task["project_id"]),
            task_id=int(task["id"]),
            work_package_id=int(package["id"]) if package is not None else None,
            payload=hook_payload,
            audit=audit,
            validate_scope=lambda: self._validate_hook_scope(task, package, hook_payload),
            enable_worker_pro=lambda: self._enable_worker_pro_for_hook(conn, package),
            require_grace_artifacts=lambda: self._require_final_grace_artifacts(conn, int(task["id"])),
            close_task=close_task,
        )
        self.hooks.dispatch(context)

    def _advance_task(
        self,
        conn: sqlite3.Connection,
        actor: ActorIdentity,
        effective_role: OrchestratorRole,
        task: dict[str, Any],
        target: TaskStatus,
        event_type: str,
    ) -> None:
        current = TaskStatus(task["status"])
        assert_task_transition(current, target)
        timestamp = _now()
        conn.execute("UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?", (target.value, timestamp, task["id"]))
        self._audit(
            conn,
            actor,
            effective_role,
            event_type,
            "task",
            task["id"],
            {"from_status": current.value, "to_status": target.value},
        )
        self._dispatch_hook(
            conn,
            actor,
            effective_role,
            HookEvent.GATE_PROMOTED,
            task,
            payload={"from_status": current.value, "to_status": target.value},
        )

    def _advance_package(
        self,
        conn: sqlite3.Connection,
        actor: ActorIdentity,
        effective_role: OrchestratorRole,
        package: dict[str, Any],
        target: WorkPackageStatus,
        event_type: str,
        claimed_by: str | None = None,
    ) -> None:
        current = WorkPackageStatus(package["status"])
        assert_work_package_transition(current, target)
        timestamp = _now()
        conn.execute(
            "UPDATE work_packages SET status = ?, claimed_by_agent = COALESCE(?, claimed_by_agent), updated_at = ? WHERE id = ?",
            (target.value, claimed_by, timestamp, package["id"]),
        )
        self._audit(
            conn,
            actor,
            effective_role,
            event_type,
            "work_package",
            package["id"],
            {"from_status": current.value, "to_status": target.value},
        )
        self._dispatch_hook(
            conn,
            actor,
            effective_role,
            HookEvent.GATE_PROMOTED,
            self._task(int(package["task_id"])),
            package,
            payload={"from_status": current.value, "to_status": target.value},
        )

    def init_project(
        self,
        actor: ActorIdentity,
        name: str,
        repo_path: Path,
        grace_path: Path,
        main_branch: str,
        allowed_test_commands: Mapping[str, Sequence[str]],
    ) -> dict[str, Any]:
        # START_CONTRACT: OrchestratorService.init_project
        #   PURPOSE: Register a root-confined project, fixed test commands, and initiating actor.
        #   INPUTS: { actor: ActorIdentity, repo_path: Path, grace_path: Path, command registry }
        #   OUTPUTS: { dict - registered project projection }
        #   SIDE_EFFECTS: Creates project/agent/audit ledger rows.
        #   LINKS: M-ORCH-LEDGER, M-ORCH-REPO-BOUNDARY
        # END_CONTRACT: OrchestratorService.init_project
        # START_BLOCK_REGISTER_PROJECT_AND_BOUND_ACTOR
        if actor.primary_role != OrchestratorRole.CODEX:
            raise OrchestratorError("Only codex may initialize a project")
        root = Path(repo_path).resolve()
        if not root.is_dir():
            raise OrchestratorError(f"Project repo_path does not exist: {root}")
        resolved_grace = resolve_within_root(root, grace_path)
        normalized_commands = {key: list(value) for key, value in allowed_test_commands.items()}
        if any(not key or not value for key, value in normalized_commands.items()):
            raise OrchestratorError("Registered test commands require a non-empty key and argv")
        timestamp = _now()
        with self.store.transaction() as conn:
            cursor = conn.execute(
                """INSERT INTO projects (name, repo_path, grace_path, main_branch,
                    allowed_test_commands_json, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (name, str(root), str(resolved_grace), main_branch, _json(normalized_commands), timestamp, timestamp),
            )
            project_id = int(cursor.lastrowid)
            conn.execute(
                """INSERT INTO agents (project_id, name, primary_role, capabilities_json, mimo_model, availability, updated_at)
                   VALUES (?, ?, ?, ?, NULL, 'available', ?)""",
                (project_id, actor.name, actor.primary_role.value, _json([actor.primary_role.value]), timestamp),
            )
            self._audit(conn, actor, OrchestratorRole.CODEX, "project.initialized", "project", project_id, {"name": name})
        return self.get_project(project_id)
        # END_BLOCK_REGISTER_PROJECT_AND_BOUND_ACTOR

    def register_agent(
        self,
        actor: ActorIdentity,
        project_id: int,
        name: str,
        primary_role: OrchestratorRole,
        capabilities: Sequence[OrchestratorRole],
        availability: str = "available",
        mimo_model: str | None = None,
    ) -> dict[str, Any]:
        # START_CONTRACT: OrchestratorService.register_agent
        #   PURPOSE: Register a model's availability and eligible role capabilities.
        #   INPUTS: { actor: Codex, project_id: int, name: str, capabilities: roles }
        #   OUTPUTS: { dict - registered agent projection }
        #   SIDE_EFFECTS: Upserts agent registry and appends audit evidence.
        #   LINKS: M-ORCH-DOMAIN, V-M-ORCH-DOMAIN
        # END_CONTRACT: OrchestratorService.register_agent
        self._project(project_id)
        effective = self._authorize(actor, OrchestratorRole.CODEX, project_id)
        if availability not in {"available", "unavailable"}:
            raise OrchestratorError("Agent availability must be available or unavailable")
        normalized_model = mimo_model.strip() if mimo_model is not None else None
        if mimo_model is not None and not normalized_model:
            raise OrchestratorError("Mimo model must be a non-empty provider/model identifier when supplied")
        capability_values = sorted({role.value for role in capabilities} | {primary_role.value})
        if primary_role == OrchestratorRole.WORKER_JUNIOR and capability_values != [OrchestratorRole.WORKER_JUNIOR.value]:
            raise OrchestratorError("Junior agents cannot be registered with fallback role capabilities")
        timestamp = _now()
        with self.store.transaction() as conn:
            conn.execute(
                """INSERT INTO agents (project_id, name, primary_role, capabilities_json, mimo_model, availability, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(project_id, name) DO UPDATE SET
                     primary_role = excluded.primary_role,
                     capabilities_json = excluded.capabilities_json,
                     mimo_model = excluded.mimo_model,
                     availability = excluded.availability,
                     updated_at = excluded.updated_at""",
                (project_id, name, primary_role.value, _json(capability_values), normalized_model, availability, timestamp),
            )
            agent_id = int(conn.execute("SELECT id FROM agents WHERE project_id = ? AND name = ?", (project_id, name)).fetchone()[0])
            self._audit(conn, actor, effective, "agent.registered", "agent", agent_id, {"name": name, "availability": availability, "capabilities": capability_values, "mimo_model": normalized_model})
        return self.get_agent(project_id, name)

    def set_allowed_test_commands(
        self,
        actor: ActorIdentity,
        project_id: int,
        allowed_test_commands: Mapping[str, Sequence[str]],
    ) -> dict[str, Any]:
        """Replace the project-owned allowlist used by verification plans and test evidence."""

        self._project(project_id)
        effective = self._authorize(actor, OrchestratorRole.CODEX, project_id)
        normalized_commands = {key: list(value) for key, value in allowed_test_commands.items()}
        if any(not key or not value for key, value in normalized_commands.items()):
            raise OrchestratorError("Registered test commands require a non-empty key and argv")
        with self.store.transaction() as conn:
            conn.execute(
                "UPDATE projects SET allowed_test_commands_json = ?, updated_at = ? WHERE id = ?",
                (_json(normalized_commands), _now(), project_id),
            )
            self._audit(
                conn,
                actor,
                effective,
                "project.test_commands_registered",
                "project",
                project_id,
                {"command_keys": sorted(normalized_commands)},
            )
        return self.get_project(project_id)

    def set_agent_availability(
        self,
        actor: ActorIdentity,
        project_id: int,
        name: str,
        availability: str,
    ) -> dict[str, Any]:
        self._project(project_id)
        effective = self._authorize(actor, OrchestratorRole.CODEX, project_id)
        if availability not in {"available", "unavailable"}:
            raise OrchestratorError("Agent availability must be available or unavailable")
        with self.store.transaction() as conn:
            cursor = conn.execute(
                "UPDATE agents SET availability = ?, updated_at = ? WHERE project_id = ? AND name = ?",
                (availability, _now(), project_id, name),
            )
            if cursor.rowcount != 1:
                raise OrchestratorError(f"Agent {name!r} is not registered for this project")
            agent_id = int(conn.execute("SELECT id FROM agents WHERE project_id = ? AND name = ?", (project_id, name)).fetchone()[0])
            self._audit(conn, actor, effective, "agent.availability_changed", "agent", agent_id, {"name": name, "availability": availability})
        return self.get_agent(project_id, name)

    def get_agent(self, project_id: int, name: str) -> dict[str, Any]:
        agent = _row(self.store.fetchone("SELECT * FROM agents WHERE project_id = ? AND name = ?", (project_id, name)))
        agent["capabilities"] = _loads(agent.pop("capabilities_json"))
        return agent

    def _require_available_capability(
        self,
        project_id: int,
        name: str,
        required_role: OrchestratorRole,
    ) -> dict[str, Any]:
        agent = self.get_agent(project_id, name)
        if agent["availability"] != "available":
            raise OrchestratorError(f"Assigned agent {name!r} is not available")
        if required_role.value not in agent["capabilities"]:
            raise OrchestratorError(f"Assigned agent {name!r} lacks capability {required_role.value}")
        return agent

    def _has_available_capability(
        self,
        project_id: int,
        name: str,
        required_role: OrchestratorRole,
    ) -> bool:
        try:
            agent = self.get_agent(project_id, name)
        except OrchestratorError:
            return False
        return agent["availability"] == "available" and required_role.value in agent["capabilities"]

    def _select_repair_mimo_assignment(
        self,
        project_id: int,
        package: Mapping[str, Any],
    ) -> tuple[str, OrchestratorRole, WorkPackageStatus, str]:
        if not bool(package["worker_pro_available"]):
            raise OrchestratorError("Repair dispatch requires a recorded GLM rejection hook")
        if self._has_available_capability(
            project_id,
            str(package["assigned_pro_agent"]),
            OrchestratorRole.WORKER_PRO,
        ):
            return (
                str(package["assigned_pro_agent"]),
                OrchestratorRole.WORKER_PRO,
                WorkPackageStatus.CLAIMED_PRO,
                "worker_pro",
            )
        self._require_available_capability(
            project_id,
            str(package["assigned_junior_agent"]),
            OrchestratorRole.WORKER_JUNIOR,
        )
        return (
            str(package["assigned_junior_agent"]),
            OrchestratorRole.WORKER_JUNIOR,
            WorkPackageStatus.CLAIMED_JUNIOR,
            "free_mimo_junior_repair",
        )

    def create_codex_task(
        self,
        actor: ActorIdentity,
        project_id: int,
        title: str,
        objective: str,
        architecture_intent: str,
        constraints: Sequence[str],
        non_goals: Sequence[str],
        acceptance_criteria: Sequence[str],
        allowed_files: Sequence[str],
        forbidden_files: Sequence[str],
        parent_task_id: int | None = None,
    ) -> dict[str, Any]:
        self._project(project_id)
        effective = self._authorize(actor, OrchestratorRole.CODEX, project_id)
        if not allowed_files:
            raise OrchestratorError("Top-level task requires at least one allowed file pattern")
        timestamp = _now()
        with self.store.transaction() as conn:
            cursor = conn.execute(
                """INSERT INTO tasks (
                   project_id, parent_task_id, created_by_role, title, objective,
                   architecture_intent, constraints_json, non_goals_json,
                   acceptance_criteria_json, allowed_files_json, forbidden_files_json,
                   status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    project_id,
                    parent_task_id,
                    effective.value,
                    title,
                    objective,
                    architecture_intent,
                    _json(list(constraints)),
                    _json(list(non_goals)),
                    _json(list(acceptance_criteria)),
                    _json(list(allowed_files)),
                    _json(list(forbidden_files)),
                    TaskStatus.CODEX_TASK_CREATED.value,
                    timestamp,
                    timestamp,
                ),
            )
            task_id = int(cursor.lastrowid)
            self._audit(conn, actor, effective, "task.codex_created", "task", task_id, {"title": title})
            self._dispatch_hook(
                conn,
                actor,
                effective,
                HookEvent.TASK_CREATED,
                self._task(task_id),
                payload={"title": title},
            )
        return self.get_task(task_id)

    def delegate_role(
        self,
        actor: ActorIdentity,
        project_id: int,
        task_id: int | None,
        unavailable_role: OrchestratorRole,
        substitute_actor: str,
        delegated_role: OrchestratorRole,
        reason: str,
        expires_at: datetime,
    ) -> dict[str, Any]:
        # START_CONTRACT: OrchestratorService.delegate_role
        #   PURPOSE: Delegate an unavailable non-Codex role only to an available capable substitute.
        #   INPUTS: { actor: Codex, unavailable_role: role, substitute_actor: registered agent }
        #   OUTPUTS: { dict - expiring delegation record }
        #   SIDE_EFFECTS: Appends delegation and audit ledger records.
        #   LINKS: M-ORCH-DOMAIN, V-M-ORCH-DOMAIN
        # END_CONTRACT: OrchestratorService.delegate_role
        # START_BLOCK_VALIDATE_FALLBACK_CAPABILITY_AND_AVAILABILITY
        self._project(project_id)
        effective = self._authorize(actor, OrchestratorRole.CODEX, project_id, task_id)
        if unavailable_role == OrchestratorRole.CODEX or unavailable_role != delegated_role:
            raise OrchestratorError("Only a non-codex unavailable role may be delegated to the same effective role")
        if expires_at.tzinfo is None:
            raise OrchestratorError("Role delegation expiry must be timezone-aware")
        try:
            substitute = self._require_available_capability(project_id, substitute_actor, delegated_role)
        except OrchestratorError as error:
            raise OrchestratorError(str(error).replace("Assigned agent", "Fallback substitute")) from error
        if substitute["primary_role"] == OrchestratorRole.WORKER_JUNIOR.value:
            raise OrchestratorError("Junior agents cannot receive fallback delegation")
        owner_primary_roles = (
            (OrchestratorRole.GLM.value, OrchestratorRole.TEST_OWNER.value)
            if unavailable_role == OrchestratorRole.TEST_OWNER
            else (unavailable_role.value,)
        )
        assigned_role_available = self.store.fetchone(
            "SELECT 1 FROM agents WHERE project_id = ? AND primary_role IN (?, ?) AND availability = 'available' LIMIT 1",
            (project_id, owner_primary_roles[0], owner_primary_roles[-1]),
        )
        if assigned_role_available is not None:
            raise OrchestratorError(
                f"Cannot delegate {unavailable_role.value}: an assigned available agent already owns that role"
            )
        timestamp = _now()
        with self.store.transaction() as conn:
            cursor = conn.execute(
                """INSERT INTO role_delegations (
                    project_id, task_id, unavailable_role, substitute_actor,
                    delegated_role, reason, expires_at, created_by_actor, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    project_id,
                    task_id,
                    unavailable_role.value,
                    substitute_actor,
                    delegated_role.value,
                    reason,
                    expires_at.astimezone(UTC).isoformat(),
                    actor.name,
                    timestamp,
                ),
            )
            delegation_id = int(cursor.lastrowid)
            self._audit(
                conn,
                actor,
                effective,
                "role.delegated",
                "role_delegation",
                delegation_id,
                {
                    "unavailable_role": unavailable_role.value,
                    "delegated_role": delegated_role.value,
                    "substitute_actor": substitute_actor,
                    "reason": reason,
                    "expires_at": expires_at.astimezone(UTC).isoformat(),
                },
            )
        return _row(self.store.fetchone("SELECT * FROM role_delegations WHERE id = ?", (delegation_id,)))
        # END_BLOCK_VALIDATE_FALLBACK_CAPABILITY_AND_AVAILABILITY

    def plan_task(self, actor: ActorIdentity, task_id: int) -> dict[str, Any]:
        # START_CONTRACT: OrchestratorService.plan_task
        #   PURPOSE: Advance a Codex task into GLM GRACE planning after effective-role check.
        #   INPUTS: { actor: ActorIdentity, task_id: int }
        #   OUTPUTS: { dict - updated task projection }
        #   SIDE_EFFECTS: Updates task status and appends audit event atomically.
        #   LINKS: M-ORCH-DOMAIN, M-ORCH-LEDGER
        # END_CONTRACT: OrchestratorService.plan_task
        task = self._task(task_id)
        effective = self._authorize(actor, OrchestratorRole.GLM, task["project_id"], task_id)
        with self.store.transaction() as conn:
            self._advance_task(conn, actor, effective, task, TaskStatus.GLM_GRACE_PLANNED, "task.grace_planned")
        return self.get_task(task_id)

    def register_verification_plan(
        self,
        actor: ActorIdentity,
        task_id: int,
        test_strategy: str,
        test_commands: Sequence[str],
        risk_coverage: Sequence[str] | None = None,
        acceptance_mapping: Mapping[str, object] | None = None,
    ) -> dict[str, Any]:
        # START_CONTRACT: OrchestratorService.register_verification_plan
        #   PURPOSE: Append an allowlisted GLM verification revision before work packaging.
        #   INPUTS: { actor: effective GLM, task_id: int, test command keys }
        #   OUTPUTS: { dict - revisioned verification plan }
        #   SIDE_EFFECTS: Inserts plan, advances task, appends audit events atomically.
        #   LINKS: M-ORCH-LEDGER, V-M-ORCH-LEDGER
        # END_CONTRACT: OrchestratorService.register_verification_plan
        task = self._task(task_id)
        effective = self._authorize(actor, OrchestratorRole.GLM, task["project_id"], task_id)
        project = self._project(task["project_id"])
        allowed = _loads(project["allowed_test_commands_json"])
        unknown = set(test_commands) - set(allowed)
        if unknown:
            raise OrchestratorError(f"Verification plan uses unregistered test commands: {sorted(unknown)}")
        if TaskStatus(task["status"]) != TaskStatus.GLM_GRACE_PLANNED:
            raise OrchestratorError("Verification plan requires GLM_GRACE_PLANNED task status")
        timestamp = _now()
        with self.store.transaction() as conn:
            revision = int(
                conn.execute("SELECT COALESCE(MAX(revision), 0) + 1 FROM verification_plans WHERE task_id = ?", (task_id,)).fetchone()[0]
            )
            cursor = conn.execute(
                """INSERT INTO verification_plans (
                   task_id, revision, test_strategy, test_commands_json, risk_coverage_json,
                   acceptance_mapping_json, created_by_agent, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    task_id,
                    revision,
                    test_strategy,
                    _json(list(test_commands)),
                    _json(list(risk_coverage or [])),
                    _json(dict(acceptance_mapping or {})),
                    actor.name,
                    timestamp,
                ),
            )
            plan_id = int(cursor.lastrowid)
            self._audit(conn, actor, effective, "verification.registered", "verification_plan", plan_id, {"revision": revision})
            self._advance_task(conn, actor, effective, task, TaskStatus.GLM_TESTS_PREPARED, "task.tests_prepared")
        return _row(self.store.fetchone("SELECT * FROM verification_plans WHERE id = ?", (plan_id,)))

    def upsert_artifact(
        self,
        actor: ActorIdentity,
        project_id: int,
        task_id: int,
        artifact_type: str,
        content: str,
        path: str,
    ) -> dict[str, Any]:
        task = self._task(task_id)
        if task["project_id"] != project_id:
            raise OrchestratorError("Artifact task does not belong to project")
        effective = self._authorize(actor, OrchestratorRole.GLM, project_id, task_id)
        project = self._project(project_id)
        resolve_within_root(Path(project["repo_path"]), path)
        timestamp = _now()
        with self.store.transaction() as conn:
            revision = int(
                conn.execute(
                    "SELECT COALESCE(MAX(revision), 0) + 1 FROM grace_artifacts WHERE task_id = ? AND artifact_type = ?",
                    (task_id, artifact_type),
                ).fetchone()[0]
            )
            cursor = conn.execute(
                """INSERT INTO grace_artifacts (
                    project_id, task_id, artifact_type, path, content, content_hash,
                    revision, created_by_agent, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (project_id, task_id, artifact_type, path, content, stable_hash(content), revision, actor.name, timestamp),
            )
            artifact_id = int(cursor.lastrowid)
            self._audit(conn, actor, effective, "grace.artifact_revision_created", "grace_artifact", artifact_id, {"artifact_type": artifact_type, "revision": revision})
            self._dispatch_hook(
                conn,
                actor,
                effective,
                HookEvent.GRACE_ARTIFACT_UPSERTED,
                task,
                payload={
                    "artifact_id": artifact_id,
                    "artifact_type": artifact_type,
                    "artifact_path": path,
                    "revision": revision,
                },
            )
        return _row(self.store.fetchone("SELECT * FROM grace_artifacts WHERE id = ?", (artifact_id,)))

    def _scope_is_subset(self, parent_patterns: Sequence[str], child_pattern: str) -> bool:
        for parent in parent_patterns:
            if parent == "**" or parent == child_pattern:
                return True
            if parent.endswith("/**"):
                prefix = parent[:-3]
                if child_pattern.startswith(prefix + "/"):
                    return True
        return False

    def discover_contracts(
        self,
        actor: ActorIdentity,
        project_id: int,
        affected_files: Sequence[str],
        task_id: int | None = None,
    ) -> dict[str, Any]:
        """Run the MCP contract discovery gate for a project scope without mutating workflow state."""

        project = self._project(project_id)
        if task_id is None:
            effective = self._authorize(actor, OrchestratorRole.CODEX, project_id)
            target_type = "project"
            target_id = project_id
        else:
            task = self._task(task_id)
            if task["project_id"] != project_id:
                raise OrchestratorError("Contract discovery task does not belong to project")
            effective = self._authorize(actor, OrchestratorRole.GLM, project_id, task_id)
            target_type = "task"
            target_id = task_id
        result = policy_discover_contracts(Path(project["repo_path"]), affected_files)
        with self.store.transaction() as conn:
            self._audit(
                conn,
                actor,
                effective,
                "gate.contract_discovery",
                target_type,
                target_id,
                {"status": result["status"], "issues": result["issues"], "affected_files": list(affected_files)},
            )
        return result

    def validate_execution_packet(
        self,
        actor: ActorIdentity,
        task_id: int,
        packet: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Run the operational packet validator and audit the decision."""

        task = self._task(task_id)
        effective = self._authorize(actor, OrchestratorRole.GLM, task["project_id"], task_id)
        project = self._project(task["project_id"])
        result = policy_validate_execution_packet(
            packet,
            repo_root=Path(project["repo_path"]),
            parent_allowed_files=_loads(task["allowed_files_json"]),
        )
        with self.store.transaction() as conn:
            self._audit(
                conn,
                actor,
                effective,
                "gate.validate_execution_packet",
                "task",
                task_id,
                {"status": result["status"], "issues": result["issues"], "warnings": result["warnings"]},
            )
        return result

    def lint_agent_infra(self, actor: ActorIdentity, project_id: int) -> dict[str, Any]:
        """Run the built-in agent-infra lint gate without invoking a shell."""

        project = self._project(project_id)
        effective = self._authorize(actor, OrchestratorRole.GLM, project_id)
        result = policy_lint_agent_infra(Path(project["repo_path"]))
        with self.store.transaction() as conn:
            self._audit(
                conn,
                actor,
                effective,
                "gate.agent_infra_lint",
                "project",
                project_id,
                {"status": result["status"], "issues": result["issues"], "warnings": result["warnings"]},
            )
        return result

    def _requires_agent_infra_lint(self, project: Mapping[str, Any]) -> bool:
        return (Path(project["repo_path"]) / ".agent-guards" / "agent-infra-policy.json").is_file()

    def create_work_package(
        self,
        actor: ActorIdentity,
        task_id: int,
        title: str,
        objective: str,
        allowed_files: Sequence[str],
        forbidden_files: Sequence[str],
        assigned_junior_agent: str,
        assigned_pro_agent: str,
        base_commit: str,
        contract_discovery: Mapping[str, Any] | None = None,
        test_surface: Sequence[str] | None = None,
        rollback_boundary: str = "",
        compact_report_format: Sequence[str] | None = None,
        module_id: str = "",
        verification_id: str = "",
        commands_allowed: Sequence[str] | None = None,
        session_routing: Mapping[str, Any] | None = None,
        cache_anchor: str = "",
        retry_budget: int = 1,
        stop_conditions: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        # START_CONTRACT: OrchestratorService.create_work_package
        #   PURPOSE: Create a GLM package whose scope remains inside parent task scope.
        #   INPUTS: { actor: effective GLM, task_id: int, scope and worker assignments }
        #   OUTPUTS: { dict - created package projection }
        #   SIDE_EFFECTS: Inserts package, advances task, appends audit events atomically.
        #   LINKS: M-ORCH-DOMAIN, V-M-ORCH-DOMAIN
        # END_CONTRACT: OrchestratorService.create_work_package
        task = self._task(task_id)
        effective = self._authorize(actor, OrchestratorRole.GLM, task["project_id"], task_id)
        if TaskStatus(task["status"]) not in {
            TaskStatus.GLM_TESTS_PREPARED,
            TaskStatus.WORK_PACKAGES_CREATED,
        }:
            raise OrchestratorError("Work package creation requires GLM_TESTS_PREPARED or WORK_PACKAGES_CREATED task status")
        parent_allowed = _loads(task["allowed_files_json"])
        if not allowed_files or not all(self._scope_is_subset(parent_allowed, pattern) for pattern in allowed_files):
            raise OrchestratorError("Work package scope must be a subset of its parent task scope")
        self._require_available_capability(task["project_id"], assigned_junior_agent, OrchestratorRole.WORKER_JUNIOR)
        pro_agent = self.get_agent(task["project_id"], assigned_pro_agent)
        if OrchestratorRole.WORKER_PRO.value not in pro_agent["capabilities"]:
            raise OrchestratorError(
                f"Assigned agent {assigned_pro_agent!r} lacks capability {OrchestratorRole.WORKER_PRO.value}"
            )
        project = self._project(task["project_id"])
        discovery = dict(contract_discovery or policy_discover_contracts(Path(project["repo_path"]), allowed_files))
        discovery_gate = policy_validate_contract_discovery(discovery)
        require_gate_pass(discovery_gate, "Contract discovery gate")
        if not module_id:
            module_refs = discovery.get("module_refs") or []
            module_id = str(module_refs[0]) if module_refs else ""
        if not verification_id:
            verification_refs = discovery.get("verification_refs") or []
            verification_id = str(verification_refs[0]) if verification_refs else ""
        if not cache_anchor:
            cache_anchor = f"GRACE:{module_id or 'unresolved'}:{verification_id or 'unresolved'}"
        session_route = dict(
            session_routing
            or {
                "mode": "checkpoint_from_cache_anchor",
                "workstream": module_id or "unresolved",
                "reuse_allowed": "only when the worker session already belongs to the same workstream and cache anchor",
                "new_session_when": [
                    "task crosses product or platform domain",
                    "cache anchor changed",
                    "session context contains conflicting stale decisions",
                ],
            }
        )
        inherited_forbidden = list(dict.fromkeys([*_loads(task["forbidden_files_json"]), *forbidden_files]))
        packet = {
            "task id": task_id,
            "module id": module_id,
            "verification id": verification_id,
            "goal": objective,
            "assigned role": OrchestratorRole.WORKER_JUNIOR.value,
            "allowed files": list(allowed_files),
            "forbidden files": inherited_forbidden,
            "worker runtime profile": assigned_junior_agent,
            "actual worker identity": assigned_junior_agent,
            "launch mode": MimoLaunchMode.TUI.value,
            "trust flag": "--trust required for generated worker worktrees",
            "forbidden model flags": "No --model argument and no paid/API MiMo model identifiers.",
            "claim identity": assigned_junior_agent,
            "required contracts read": discovery.get("contracts_read", []),
            "contract discovery report": discovery,
            "test surface": list(test_surface or []),
            "commands allowed": list(commands_allowed or []),
            "rollback boundary": rollback_boundary,
            "session routing": session_route,
            "cache anchor": cache_anchor,
            "retry budget": retry_budget,
            "stop conditions": list(stop_conditions or []),
            "compact worker report format": list(compact_report_format or []),
        }
        packet_gate = policy_validate_execution_packet(
            packet,
            repo_root=Path(project["repo_path"]),
            parent_allowed_files=parent_allowed,
        )
        require_gate_pass(packet_gate, "Operational packet validation")
        timestamp = _now()
        with self.store.transaction() as conn:
            cursor = conn.execute(
                """INSERT INTO work_packages (
                    task_id, title, objective, allowed_files_json, forbidden_files_json,
                    assigned_junior_agent, assigned_pro_agent, base_commit, contract_discovery_json,
                    test_surface_json, rollback_boundary, compact_report_format_json,
                    session_routing_json, cache_anchor, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    task_id,
                    title,
                    objective,
                    _json(list(allowed_files)),
                    _json(inherited_forbidden),
                    assigned_junior_agent,
                    assigned_pro_agent,
                    base_commit,
                    _json(discovery),
                    _json(list(test_surface or [])),
                    rollback_boundary,
                    _json(list(compact_report_format or [])),
                    _json(session_route),
                    cache_anchor,
                    WorkPackageStatus.CREATED.value,
                    timestamp,
                    timestamp,
                ),
            )
            package_id = int(cursor.lastrowid)
            self._audit(
                conn,
                actor,
                effective,
                "gate.validate_execution_packet",
                "work_package",
                package_id,
                {"status": packet_gate["status"], "issues": packet_gate["issues"], "warnings": packet_gate["warnings"]},
            )
            self._audit(
                conn,
                actor,
                effective,
                "work_package.created",
                "work_package",
                package_id,
                {"title": title, "contract_discovery_status": discovery.get("status")},
            )
            self._dispatch_hook(
                conn,
                actor,
                effective,
                HookEvent.WORKPACKAGE_CREATED,
                task,
                self._package(package_id),
                payload={"title": title},
            )
            if TaskStatus(task["status"]) == TaskStatus.GLM_TESTS_PREPARED:
                self._advance_task(conn, actor, effective, task, TaskStatus.WORK_PACKAGES_CREATED, "task.work_packages_created")
        return self.get_work_package(package_id)

    def assign_work_package(self, actor: ActorIdentity, package_id: int) -> dict[str, Any]:
        package = self._package(package_id)
        task = self._task(package["task_id"])
        effective = self._authorize(actor, OrchestratorRole.GLM, task["project_id"], task["id"])
        if TaskStatus(task["status"]) not in {
            TaskStatus.WORK_PACKAGES_CREATED,
            TaskStatus.WORK_PACKAGES_ASSIGNED,
        }:
            raise OrchestratorError("Work package assignment requires a packaged task")
        with self.store.transaction() as conn:
            self._advance_package(conn, actor, effective, package, WorkPackageStatus.ASSIGNED, "work_package.assigned")
            fresh_task = self._task(task["id"])
            if TaskStatus(fresh_task["status"]) == TaskStatus.WORK_PACKAGES_CREATED:
                self._advance_task(conn, actor, effective, fresh_task, TaskStatus.WORK_PACKAGES_ASSIGNED, "task.work_packages_assigned")
        return self.get_work_package(package_id)

    def claim_work_package(self, actor: ActorIdentity, package_id: int) -> dict[str, Any]:
        package = self._package(package_id)
        current = WorkPackageStatus(package["status"])
        task = self._task(package["task_id"])
        if current == WorkPackageStatus.ASSIGNED and actor.name == package["assigned_junior_agent"]:
            required = OrchestratorRole.WORKER_JUNIOR
            target = WorkPackageStatus.CLAIMED_JUNIOR
        elif current == WorkPackageStatus.REPAIR_REQUIRED and actor.name == package["assigned_pro_agent"]:
            if not bool(package["worker_pro_available"]):
                raise OrchestratorError("Assigned worker_pro is not yet enabled by a recorded GLM rejection hook")
            required = OrchestratorRole.WORKER_PRO
            target = WorkPackageStatus.CLAIMED_PRO
        elif current == WorkPackageStatus.REPAIR_REQUIRED and actor.name == package["assigned_junior_agent"]:
            assigned_agent, required, target, _repair_route = self._select_repair_mimo_assignment(
                task["project_id"],
                package,
            )
            if actor.name != assigned_agent:
                raise OrchestratorError("Only the assigned repair worker may claim the package")
        else:
            raise OrchestratorError("Only the assigned worker may claim the package in its current state")
        effective = self._authorize(actor, required, task["project_id"], task["id"])
        self._require_available_capability(task["project_id"], actor.name, required)
        with self.store.transaction() as conn:
            self._advance_package(conn, actor, effective, package, target, "work_package.claimed", actor.name)
        return self.get_work_package(package_id)

    def submit_package(
        self,
        actor: ActorIdentity,
        package_id: int,
        summary: str,
        evidence: SubmissionEvidence,
        tests_run: Sequence[Mapping[str, object]],
        risk_notes: str,
        worker_report: Mapping[str, Any],
    ) -> dict[str, Any]:
        # START_CONTRACT: OrchestratorService.submit_package
        #   PURPOSE: Persist only claimed-worker evidence that matches the approved package scope.
        #   INPUTS: { actor: worker, package_id: int, evidence: SubmissionEvidence }
        #   OUTPUTS: { dict - immutable submission record }
        #   SIDE_EFFECTS: Inserts submission, advances package, appends audit events atomically.
        #   LINKS: M-ORCH-REPO-BOUNDARY, V-M-ORCH-REPO-BOUNDARY
        # END_CONTRACT: OrchestratorService.submit_package
        # START_BLOCK_COMMIT_SCOPE_VALIDATED_SUBMISSION
        package = self._package(package_id)
        current = WorkPackageStatus(package["status"])
        required = OrchestratorRole.WORKER_JUNIOR if current == WorkPackageStatus.CLAIMED_JUNIOR else OrchestratorRole.WORKER_PRO
        if actor.name != package["claimed_by_agent"]:
            raise OrchestratorError("Only the worker that claimed a package may submit it")
        task = self._task(package["task_id"])
        effective = self._authorize(actor, required, task["project_id"], task["id"])
        validate_scoped_files(
            evidence.files_changed,
            allowed_files=_loads(package["allowed_files_json"]),
            forbidden_files=_loads(package["forbidden_files_json"]),
        )
        if evidence.base_commit != package["base_commit"]:
            raise OrchestratorError("Submission base commit does not match work package base commit")
        project = self._project(task["project_id"])
        report_gate = policy_validate_worker_report(
            worker_report,
            task_id=int(task["id"]),
            work_package_id=package_id,
            allowed_files=_loads(package["allowed_files_json"]),
            forbidden_files=_loads(package["forbidden_files_json"]),
            evidence_files=evidence.files_changed,
            repo_root=Path(project["repo_path"]),
        )
        require_gate_pass(report_gate, "Worker report validation")
        timestamp = _now()
        with self.store.transaction() as conn:
            cursor = conn.execute(
                """INSERT INTO submissions (
                    work_package_id, submitted_by_agent, base_commit, head_commit, diff,
                    diff_hash, summary, tests_run_json, files_changed_json, worker_report_json,
                    worker_report_validation_json, risk_notes, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    package_id,
                    actor.name,
                    evidence.base_commit,
                    evidence.head_commit,
                    evidence.diff,
                    evidence.diff_hash,
                    summary,
                    _json(list(tests_run)),
                    _json(evidence.files_changed),
                    _json(dict(worker_report)),
                    _json(report_gate),
                    risk_notes,
                    timestamp,
                ),
            )
            submission_id = int(cursor.lastrowid)
            self._audit(
                conn,
                actor,
                effective,
                "gate.validate_worker_report",
                "submission",
                submission_id,
                {"status": report_gate["status"], "issues": report_gate["issues"], "warnings": report_gate["warnings"]},
            )
            self._audit(conn, actor, effective, "submission.created", "submission", submission_id, {"work_package_id": package_id, "diff_hash": evidence.diff_hash})
            self._advance_package(conn, actor, effective, package, WorkPackageStatus.SUBMITTED, "work_package.submitted")
            self._dispatch_hook(
                conn,
                actor,
                effective,
                HookEvent.SUBMISSION_CREATED,
                task,
                self._package(package_id),
                payload={"submission_id": submission_id, "files_changed": evidence.files_changed},
            )
        submission = _row(self.store.fetchone("SELECT * FROM submissions WHERE id = ?", (submission_id,)))
        report_path = self._write_worker_handoff_report(task["project_id"], task["id"], package, submission)
        event = self._append_handoff_event(
            task["project_id"],
            task["id"],
            package_id,
            "WORKER_READY_FOR_REVIEW",
            actor.name,
            {
                "submission_id": submission_id,
                "head_commit": evidence.head_commit,
                "report": report_path,
            },
        )
        with self.store.transaction() as conn:
            self._audit(
                conn,
                actor,
                effective,
                "handoff.worker_ready_for_review",
                "work_package",
                package_id,
                {"submission_id": submission_id, "events_path": event["events_path"], "report": report_path},
            )
        submission["handoff_event"] = event
        submission["handoff_report_path"] = report_path
        return submission
        # END_BLOCK_COMMIT_SCOPE_VALIDATED_SUBMISSION

    def submit_controller_repair(
        self,
        actor: ActorIdentity,
        package_id: int,
        summary: str,
        evidence: SubmissionEvidence,
        tests_run: Sequence[Mapping[str, object]],
        risk_notes: str,
        controller_report: Mapping[str, Any],
    ) -> dict[str, Any]:
        # START_CONTRACT: OrchestratorService.submit_controller_repair
        #   PURPOSE: Persist a Codex-owned repair submission when a rejected package cannot use a Pro worker path.
        #   INPUTS: { actor: Codex, package_id: int, evidence: SubmissionEvidence, controller_report: mapping }
        #   OUTPUTS: { dict - immutable submission record with controller repair handoff event }
        #   SIDE_EFFECTS: Inserts submission, advances package back to SUBMITTED, appends audit/handoff events atomically.
        #   LINKS: M-ORCH-LEDGER, V-M-ORCH-LEDGER, M-ORCH-DOMAIN
        # END_CONTRACT: OrchestratorService.submit_controller_repair
        # START_BLOCK_COMMIT_SCOPE_VALIDATED_CONTROLLER_REPAIR
        package = self._package(package_id)
        task = self._task(package["task_id"])
        effective = self._authorize(actor, OrchestratorRole.CODEX, task["project_id"], task["id"])
        if WorkPackageStatus(package["status"]) != WorkPackageStatus.REPAIR_REQUIRED:
            raise OrchestratorError("Controller repair submission requires a REPAIR_REQUIRED package")
        if TaskStatus(task["status"]) != TaskStatus.GLM_REJECTED_REPAIR_REQUIRED:
            raise OrchestratorError("Controller repair submission requires a GLM_REJECTED_REPAIR_REQUIRED task")
        validate_scoped_files(
            evidence.files_changed,
            allowed_files=_loads(package["allowed_files_json"]),
            forbidden_files=_loads(package["forbidden_files_json"]),
        )
        if evidence.base_commit != package["base_commit"]:
            raise OrchestratorError("Submission base commit does not match work package base commit")
        project = self._project(task["project_id"])
        report_gate = policy_validate_worker_report(
            controller_report,
            task_id=int(task["id"]),
            work_package_id=package_id,
            allowed_files=_loads(package["allowed_files_json"]),
            forbidden_files=_loads(package["forbidden_files_json"]),
            evidence_files=evidence.files_changed,
            repo_root=Path(project["repo_path"]),
        )
        require_gate_pass(report_gate, "Controller repair report validation")
        timestamp = _now()
        with self.store.transaction() as conn:
            cursor = conn.execute(
                """INSERT INTO submissions (
                    work_package_id, submitted_by_agent, base_commit, head_commit, diff,
                    diff_hash, summary, tests_run_json, files_changed_json, worker_report_json,
                    worker_report_validation_json, risk_notes, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    package_id,
                    actor.name,
                    evidence.base_commit,
                    evidence.head_commit,
                    evidence.diff,
                    evidence.diff_hash,
                    summary,
                    _json(list(tests_run)),
                    _json(evidence.files_changed),
                    _json(dict(controller_report)),
                    _json(report_gate),
                    risk_notes,
                    timestamp,
                ),
            )
            submission_id = int(cursor.lastrowid)
            self._audit(
                conn,
                actor,
                effective,
                "gate.validate_controller_repair_report",
                "submission",
                submission_id,
                {"status": report_gate["status"], "issues": report_gate["issues"], "warnings": report_gate["warnings"]},
            )
            self._audit(
                conn,
                actor,
                effective,
                "submission.controller_repair_created",
                "submission",
                submission_id,
                {"work_package_id": package_id, "diff_hash": evidence.diff_hash},
            )
            self._advance_package(conn, actor, effective, package, WorkPackageStatus.SUBMITTED, "work_package.controller_repair_submitted")
            self._dispatch_hook(
                conn,
                actor,
                effective,
                HookEvent.SUBMISSION_CREATED,
                task,
                self._package(package_id),
                payload={"submission_id": submission_id, "files_changed": evidence.files_changed, "controller_repair": True},
            )
        submission = _row(self.store.fetchone("SELECT * FROM submissions WHERE id = ?", (submission_id,)))
        report_path = self._write_worker_handoff_report(task["project_id"], task["id"], package, submission)
        event = self._append_handoff_event(
            task["project_id"],
            task["id"],
            package_id,
            "CONTROLLER_REPAIR_SUBMITTED",
            actor.name,
            {
                "submission_id": submission_id,
                "head_commit": evidence.head_commit,
                "report": report_path,
            },
        )
        with self.store.transaction() as conn:
            self._audit(
                conn,
                actor,
                effective,
                "handoff.controller_repair_submitted",
                "work_package",
                package_id,
                {"submission_id": submission_id, "events_path": event["events_path"], "report": report_path},
            )
        submission["handoff_event"] = event
        submission["handoff_report_path"] = report_path
        return submission
        # END_BLOCK_COMMIT_SCOPE_VALIDATED_CONTROLLER_REPAIR

    def submit_controller_task_completion(
        self,
        actor: ActorIdentity,
        task_id: int,
        summary: str,
        evidence: SubmissionEvidence,
        tests_run: Sequence[Mapping[str, object]],
        risk_notes: str,
        controller_report: Mapping[str, Any],
    ) -> dict[str, Any]:
        # START_CONTRACT: OrchestratorService.submit_controller_task_completion
        #   PURPOSE: Persist audited Codex controller-owned completion evidence when no worker package is required.
        #   INPUTS: { actor: Codex, task_id: int, evidence: task-scope diff, controller_report: mapping }
        #   OUTPUTS: { dict - immutable task review record marking the GLM gate satisfied }
        #   SIDE_EFFECTS: Inserts task review, advances task to GLM_ACCEPTED, appends audit/hook events atomically.
        #   LINKS: M-ORCH-LEDGER, V-M-ORCH-LEDGER, M-ORCH-DOMAIN
        # END_CONTRACT: OrchestratorService.submit_controller_task_completion
        # START_BLOCK_COMMIT_CONTROLLER_OWNED_COMPLETION
        task = self._task(task_id)
        effective = self._authorize(actor, OrchestratorRole.CODEX, task["project_id"], task_id)
        package_count = self.store.fetchone("SELECT COUNT(*) AS count FROM work_packages WHERE task_id = ?", (task_id,))["count"]
        if package_count:
            raise OrchestratorError("Controller task completion is only allowed when the task has no work packages")
        if TaskStatus(task["status"]) not in {TaskStatus.GLM_GRACE_PLANNED, TaskStatus.GLM_TESTS_PREPARED}:
            raise OrchestratorError("Controller task completion requires a planned task with no worker packages")
        allowed_files = _loads(task["allowed_files_json"])
        forbidden_files = _loads(task["forbidden_files_json"])
        validate_scoped_files(evidence.files_changed, allowed_files=allowed_files, forbidden_files=forbidden_files)
        project = self._project(task["project_id"])
        report_gate = policy_validate_worker_report(
            controller_report,
            task_id=task_id,
            work_package_id=0,
            allowed_files=allowed_files,
            forbidden_files=forbidden_files,
            evidence_files=evidence.files_changed,
            repo_root=Path(project["repo_path"]),
        )
        require_gate_pass(report_gate, "Controller task completion report validation")
        timestamp = _now()
        completion_payload = {
            "summary": summary,
            "evidence": {
                "base_commit": evidence.base_commit,
                "head_commit": evidence.head_commit,
                "diff_hash": evidence.diff_hash,
                "files_changed": list(evidence.files_changed),
            },
            "tests_run": list(tests_run),
            "risk_notes": risk_notes,
            "controller_report": dict(controller_report),
            "controller_report_validation": report_gate,
        }
        with self.store.transaction() as conn:
            cursor = conn.execute(
                """INSERT INTO reviews (
                    target_type, target_id, reviewer_role, reviewer_agent, effective_role,
                    decision, findings_json, required_fixes_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    "task",
                    task_id,
                    actor.primary_role.value,
                    actor.name,
                    effective.value,
                    "controller_completed",
                    _json([completion_payload]),
                    _json([]),
                    timestamp,
                ),
            )
            review_id = int(cursor.lastrowid)
            self._audit(
                conn,
                actor,
                effective,
                "gate.validate_controller_task_completion_report",
                "review",
                review_id,
                {"status": report_gate["status"], "issues": report_gate["issues"], "warnings": report_gate["warnings"]},
            )
            self._audit(
                conn,
                actor,
                effective,
                "submission.controller_task_completion_created",
                "review",
                review_id,
                {"task_id": task_id, "diff_hash": evidence.diff_hash, "files_changed": list(evidence.files_changed)},
            )
            self._advance_task(conn, actor, effective, task, TaskStatus.GLM_ACCEPTED, "task.controller_completion_accepted")
            self._dispatch_hook(
                conn,
                actor,
                effective,
                HookEvent.GLM_ACCEPTED,
                self._task(task_id),
                payload={"review_id": review_id, "files_changed": evidence.files_changed, "controller_owned": True},
            )
        review = _row(self.store.fetchone("SELECT * FROM reviews WHERE id = ?", (review_id,)))
        review["controller_completion"] = completion_payload
        return review
        # END_BLOCK_COMMIT_CONTROLLER_OWNED_COMPLETION

    def review_package(
        self,
        actor: ActorIdentity,
        package_id: int,
        decision: str,
        findings: Sequence[Mapping[str, object] | str],
        required_fixes: Sequence[Mapping[str, object] | str],
    ) -> dict[str, Any]:
        # START_CONTRACT: OrchestratorService.review_package
        #   PURPOSE: Resolve package acceptance or repair and derive parent GLM readiness.
        #   INPUTS: { actor: effective GLM, package_id: int, decision and findings }
        #   OUTPUTS: { dict - immutable GLM review record }
        #   SIDE_EFFECTS: Updates package/task, inserts review, appends audit events atomically.
        #   LINKS: M-ORCH-DOMAIN, V-M-ORCH-DOMAIN
        # END_CONTRACT: OrchestratorService.review_package
        # START_BLOCK_RESOLVE_GLM_PACKAGE_ACCEPTANCE
        if decision not in {"accepted", "rejected_repair_required", "blocked"}:
            raise OrchestratorError(f"Unsupported GLM review decision: {decision}")
        package = self._package(package_id)
        task = self._task(package["task_id"])
        effective = self._authorize(actor, OrchestratorRole.GLM, task["project_id"], task["id"])
        if decision == "accepted":
            submission = self.store.fetchone(
                "SELECT * FROM submissions WHERE work_package_id = ? ORDER BY id DESC LIMIT 1",
                (package_id,),
            )
            if submission is None:
                raise OrchestratorError("GLM acceptance requires a worker submission")
            report_gate = _loads(str(submission["worker_report_validation_json"]))
            require_gate_pass(report_gate, "GLM acceptance worker report gate")
        timestamp = _now()
        with self.store.transaction() as conn:
            self._advance_package(conn, actor, effective, package, WorkPackageStatus.GLM_REVIEW_IN_PROGRESS, "work_package.review_started")
            reviewed = self._package(package_id)
            if decision == "accepted":
                target = WorkPackageStatus.GLM_ACCEPTED
            elif decision == "rejected_repair_required":
                target = WorkPackageStatus.REPAIR_REQUIRED
            else:
                target = WorkPackageStatus.REPAIR_REQUIRED
            self._advance_package(conn, actor, effective, reviewed, target, "work_package.review_resolved")
            cursor = conn.execute(
                """INSERT INTO reviews (
                    target_type, target_id, reviewer_role, reviewer_agent, effective_role,
                    decision, findings_json, required_fixes_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ("work_package", package_id, actor.primary_role.value, actor.name, effective.value, decision, _json(list(findings)), _json(list(required_fixes)), timestamp),
            )
            review_id = int(cursor.lastrowid)
            self._audit(conn, actor, effective, "review.glm_submitted", "review", review_id, {"decision": decision, "work_package_id": package_id})
            self._dispatch_hook(
                conn,
                actor,
                effective,
                HookEvent.GLM_ACCEPTED if decision == "accepted" else HookEvent.GLM_REJECTED,
                task,
                self._package(package_id),
                payload={"review_id": review_id, "decision": decision},
            )
            fresh_task = self._task(task["id"])
            accepted_count = conn.execute(
                "SELECT COUNT(*) FROM work_packages WHERE task_id = ? AND status = ?",
                (task["id"], WorkPackageStatus.GLM_ACCEPTED.value),
            ).fetchone()[0]
            package_count = conn.execute("SELECT COUNT(*) FROM work_packages WHERE task_id = ?", (task["id"],)).fetchone()[0]
            if decision == "accepted" and package_count > 0 and accepted_count == package_count:
                self._advance_task(conn, actor, effective, fresh_task, TaskStatus.GLM_ACCEPTED, "task.glm_accepted")
            elif decision != "accepted":
                if TaskStatus(fresh_task["status"]) == TaskStatus.GLM_REJECTED_REPAIR_REQUIRED:
                    self._audit(
                        conn,
                        actor,
                        effective,
                        "task.glm_repair_required_reaffirmed",
                        "task",
                        task["id"],
                        {"status": TaskStatus.GLM_REJECTED_REPAIR_REQUIRED.value, "review_id": review_id},
                    )
                else:
                    self._advance_task(conn, actor, effective, fresh_task, TaskStatus.GLM_REJECTED_REPAIR_REQUIRED, "task.glm_repair_required")
        review = _row(self.store.fetchone("SELECT * FROM reviews WHERE id = ?", (review_id,)))
        handoff_event_type = "CONTROLLER_ACCEPTED" if decision == "accepted" else "CONTROLLER_REWORK_REQUESTED"
        event = self._append_handoff_event(
            task["project_id"],
            task["id"],
            package_id,
            handoff_event_type,
            actor.name,
            {"review_id": review_id, "decision": decision, "findings": list(findings), "required_fixes": list(required_fixes)},
        )
        with self.store.transaction() as conn:
            self._audit(
                conn,
                actor,
                effective,
                "handoff.controller_review_resolved",
                "work_package",
                package_id,
                {"review_id": review_id, "event_type": handoff_event_type, "events_path": event["events_path"]},
            )
        review["handoff_event"] = event
        return review
        # END_BLOCK_RESOLVE_GLM_PACKAGE_ACCEPTANCE

    def request_final_review(self, actor: ActorIdentity, task_id: int) -> dict[str, Any]:
        task = self._task(task_id)
        effective = self._authorize(actor, OrchestratorRole.CODEX, task["project_id"], task_id)
        if TaskStatus(task["status"]) != TaskStatus.GLM_ACCEPTED:
            raise OrchestratorError("Codex final review requires GLM acceptance of every work package")
        project = self._project(task["project_id"])
        if self._requires_agent_infra_lint(project):
            lint_result = policy_lint_agent_infra(Path(project["repo_path"]))
            require_gate_pass(lint_result, "Agent-infra lint gate")
        with self.store.transaction() as conn:
            if self._requires_agent_infra_lint(project):
                self._audit(
                    conn,
                    actor,
                    effective,
                    "gate.agent_infra_lint",
                    "task",
                    task_id,
                    {"status": lint_result["status"], "issues": lint_result["issues"], "warnings": lint_result["warnings"]},
                )
            self._advance_task(conn, actor, effective, task, TaskStatus.CODEX_FINAL_REVIEW, "task.codex_final_review_requested")
        return self.get_task(task_id)

    def final_review(
        self,
        actor: ActorIdentity,
        task_id: int,
        decision: str,
        findings: Sequence[Mapping[str, object] | str],
        required_fixes: Sequence[Mapping[str, object] | str],
    ) -> dict[str, Any]:
        # START_CONTRACT: OrchestratorService.final_review
        #   PURPOSE: Resolve Codex final acceptance only after the GLM task gate.
        #   INPUTS: { actor: Codex, task_id: int, decision and findings }
        #   OUTPUTS: { dict - immutable Codex review record }
        #   SIDE_EFFECTS: Inserts review, advances task, appends audit events atomically.
        #   LINKS: M-ORCH-DOMAIN, V-M-ORCH-MCP-SERVER
        # END_CONTRACT: OrchestratorService.final_review
        # START_BLOCK_RESOLVE_CODEX_FINAL_ACCEPTANCE
        if decision not in {"accepted", "rejected_repair_required", "blocked"}:
            raise OrchestratorError(f"Unsupported Codex review decision: {decision}")
        task = self._task(task_id)
        effective = self._authorize(actor, OrchestratorRole.CODEX, task["project_id"], task_id)
        if TaskStatus(task["status"]) != TaskStatus.CODEX_FINAL_REVIEW:
            raise OrchestratorError("Codex review requires CODEX_FINAL_REVIEW task status")
        if decision == "accepted":
            project = self._project(task["project_id"])
            if self._requires_agent_infra_lint(project):
                lint_result = policy_lint_agent_infra(Path(project["repo_path"]))
                require_gate_pass(lint_result, "Codex acceptance agent-infra gate")
        target = TaskStatus.CODEX_ACCEPTED if decision == "accepted" else TaskStatus.CODEX_REJECTED_REPAIR_REQUIRED
        timestamp = _now()
        with self.store.transaction() as conn:
            if decision == "accepted" and self._requires_agent_infra_lint(project):
                self._audit(
                    conn,
                    actor,
                    effective,
                    "gate.acceptance_review",
                    "task",
                    task_id,
                    {"status": lint_result["status"], "issues": lint_result["issues"], "warnings": lint_result["warnings"]},
                )
            cursor = conn.execute(
                """INSERT INTO reviews (
                    target_type, target_id, reviewer_role, reviewer_agent, effective_role,
                    decision, findings_json, required_fixes_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ("task", task_id, actor.primary_role.value, actor.name, effective.value, decision, _json(list(findings)), _json(list(required_fixes)), timestamp),
            )
            review_id = int(cursor.lastrowid)
            self._audit(conn, actor, effective, "review.codex_submitted", "review", review_id, {"decision": decision, "task_id": task_id})
            self._advance_task(conn, actor, effective, task, target, "task.codex_review_resolved")
            self._dispatch_hook(
                conn,
                actor,
                effective,
                HookEvent.CODEX_ACCEPTED if decision == "accepted" else HookEvent.CODEX_REJECTED,
                self._task(task_id),
                payload={"review_id": review_id, "decision": decision},
            )
        return _row(self.store.fetchone("SELECT * FROM reviews WHERE id = ?", (review_id,)))
        # END_BLOCK_RESOLVE_CODEX_FINAL_ACCEPTANCE

    def close_task(self, actor: ActorIdentity, task_id: int) -> dict[str, Any]:
        task = self._task(task_id)
        effective = self._authorize(actor, OrchestratorRole.CODEX, task["project_id"], task_id)
        if TaskStatus(task["status"]) == TaskStatus.TASK_CLOSED:
            return self.get_task(task_id)
        if TaskStatus(task["status"]) != TaskStatus.CODEX_ACCEPTED:
            raise OrchestratorError("Only a Codex-accepted task may be closed")
        with self.store.transaction() as conn:
            self._advance_task(conn, actor, effective, task, TaskStatus.TASK_CLOSED, "task.closed")
        return self.get_task(task_id)

    def run_allowed_test(
        self,
        actor: ActorIdentity,
        project_id: int,
        task_id: int,
        work_package_id: int | None,
        command_key: str,
    ) -> TestRunResult:
        # START_CONTRACT: OrchestratorService.run_allowed_test
        #   PURPOSE: Execute and record a GLM-authorized registered test command.
        #   INPUTS: { actor: effective GLM, project/task/package ids, command_key }
        #   OUTPUTS: { TestRunResult - exit code and evidence log paths }
        #   SIDE_EFFECTS: Starts allowlisted process and inserts test/audit rows.
        #   LINKS: M-ORCH-REPO-BOUNDARY, V-M-ORCH-REPO-BOUNDARY
        # END_CONTRACT: OrchestratorService.run_allowed_test
        task = self._task(task_id)
        if task["project_id"] != project_id:
            raise OrchestratorError("Task does not belong to project")
        effective = self._authorize(actor, OrchestratorRole.GLM, project_id, task_id)
        project = self._project(project_id)
        allowed = _loads(project["allowed_test_commands_json"])
        boundary = RepositoryBoundary(Path(project["repo_path"]), self.store.database_path.parent / "logs")
        result = boundary.run_allowed_test(command_key, allowed)
        with self.store.transaction() as conn:
            cursor = conn.execute(
                """INSERT INTO test_runs (
                    task_id, work_package_id, command_key, command_json, exit_code,
                    stdout_path, stderr_path, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    task_id,
                    work_package_id,
                    command_key,
                    _json(allowed[command_key]),
                    result.exit_code,
                    str(result.stdout_path),
                    str(result.stderr_path),
                    _now(),
                ),
            )
            test_run_id = int(cursor.lastrowid)
            self._audit(conn, actor, effective, "repo.test_run_recorded", "test_run", test_run_id, {"command_key": command_key, "exit_code": result.exit_code})
        return result

    def mimo_connection_profile(
        self,
        actor: ActorIdentity,
        project_id: int,
        agent_name: str,
    ) -> dict[str, Any]:
        # START_CONTRACT: OrchestratorService.mimo_connection_profile
        #   PURPOSE: Project a role-bound STDIO MCP profile for one registered Mimo agent.
        #   INPUTS: { actor: Codex, project_id: int, agent_name: registered model agent }
        #   OUTPUTS: { dict - copyable Mimo stdio MCP configuration fields }
        #   SIDE_EFFECTS: none.
        #   LINKS: M-ORCH-MIMO-EXECUTOR, V-M-ORCH-MIMO-EXECUTOR
        # END_CONTRACT: OrchestratorService.mimo_connection_profile
        project = self._project(project_id)
        self._authorize(actor, OrchestratorRole.CODEX, project_id)
        agent = self.get_agent(project_id, agent_name)
        package_root = Path(__file__).resolve().parents[2]
        return {
            "name": f"grace-orchestrator-{agent['name']}",
            "transport": "stdio",
            "command": sys.executable,
            "args": ["-m", "grace_orchestrator"],
            "cwd": str(package_root),
            "env": {
                "GRACE_ORCHESTRATOR_ACTOR_NAME": agent["name"],
                "GRACE_ORCHESTRATOR_ACTOR_ROLE": agent["primary_role"],
                "GRACE_ORCHESTRATOR_DATA_DIR": str(self.data_root),
                "PYTHONUNBUFFERED": "1",
            },
            "note": (
                "Add these fields through Mimo's stdio MCP-server dialog for this exact agent. "
                "Each agent needs its own profile because actor identity is bound at server start."
            ),
            "project_root": project["repo_path"],
        }

    def launch_mimo_session(
        self,
        actor: ActorIdentity,
        work_package_id: int,
        mode: MimoLaunchMode,
    ) -> dict[str, Any]:
        # START_CONTRACT: OrchestratorService.launch_mimo_session
        #   PURPOSE: Dispatch an assigned package to its registered Mimo-backed worker in an isolated worktree.
        #   INPUTS: { actor: effective GLM, work_package_id: int, mode: closed Mimo launch mode }
        #   OUTPUTS: { dict - audited Mimo session projection }
        #   SIDE_EFFECTS: Creates a detached Git worktree, briefing file, and Mimo child process.
        #   LINKS: M-ORCH-MIMO-EXECUTOR, V-M-ORCH-MIMO-EXECUTOR
        # END_CONTRACT: OrchestratorService.launch_mimo_session
        # START_BLOCK_DISPATCH_MIMO_WORK_PACKAGE
        package = self._package(work_package_id)
        task = self._task(package["task_id"])
        project = self._project(task["project_id"])
        effective = self._authorize(actor, OrchestratorRole.GLM, project["id"], task["id"])
        package_status = WorkPackageStatus(package["status"])
        if package_status == WorkPackageStatus.ASSIGNED:
            assigned_agent = package["assigned_junior_agent"]
            required_role = OrchestratorRole.WORKER_JUNIOR
            workspace_base_commit = package["base_commit"]
        elif package_status == WorkPackageStatus.REPAIR_REQUIRED:
            assigned_agent, required_role, _claim_target, repair_route = self._select_repair_mimo_assignment(
                project["id"],
                package,
            )
            repair_submission = self.store.fetchone(
                "SELECT head_commit FROM submissions WHERE work_package_id = ? ORDER BY id DESC LIMIT 1",
                (package["id"],),
            )
            if repair_submission is None:
                raise OrchestratorError("Mimo repair dispatch requires a submitted worker commit to repair")
            workspace_base_commit = str(repair_submission["head_commit"])
        else:
            raise OrchestratorError(
                "Mimo dispatch requires an ASSIGNED package or a REPAIR_REQUIRED package"
            )
        agent = self._require_available_capability(project["id"], assigned_agent, required_role)
        model = agent.get("mimo_model")
        if not isinstance(model, str) or not model:
            raise OrchestratorError(
                f"Assigned agent {assigned_agent!r} has no configured Mimo model; register mimo_model first"
            )
        active_session = self.store.fetchone(
            """SELECT id FROM mimo_sessions WHERE work_package_id = ?
               AND lifecycle_state IN (?, ?, ?) ORDER BY id DESC LIMIT 1""",
            (
                package["id"],
                MimoSessionStatus.PREPARED.value,
                MimoSessionStatus.RUNNING.value,
                MimoSessionStatus.TUI_DETACHED.value,
            ),
        )
        if active_session is not None:
            raise OrchestratorError(
                f"Work package already has an active Mimo session: {int(active_session['id'])}"
            )
        with self.store.transaction() as conn:
            cursor = conn.execute(
                """INSERT INTO mimo_sessions (
                    project_id, task_id, work_package_id, requested_by_agent, assigned_agent,
                    assigned_role, mimo_model, mode, lifecycle_state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    project["id"],
                    task["id"],
                    package["id"],
                    actor.name,
                    assigned_agent,
                    required_role.value,
                    model,
                    mode.value,
                    MimoSessionStatus.PREPARED.value,
                    _now(),
                ),
            )
            session_id = int(cursor.lastrowid)
            self._audit(
                conn,
                actor,
                effective,
                "mimo.session_prepared",
                "mimo_session",
                session_id,
                {
                    "work_package_id": package["id"],
                    "assigned_agent": assigned_agent,
                    "assigned_role": required_role.value,
                    "mode": mode.value,
                    "repair_route": repair_route if package_status == WorkPackageStatus.REPAIR_REQUIRED else None,
                },
            )

        workspace_path = self.data_root / "worktrees" / f"project-{project['id']}" / f"package-{package['id']}" / f"session-{session_id}"
        briefing_path = self.data_root / "briefings" / f"mimo-session-{session_id}.md"
        try:
            boundary = RepositoryBoundary(Path(project["repo_path"]), self.data_root / "logs")
            created_workspace = boundary.create_detached_worktree(workspace_path, workspace_base_commit)
            briefing_path.parent.mkdir(parents=True, exist_ok=True)
            briefing_package = self.get_work_package(package["id"])
            if workspace_base_commit != package["base_commit"]:
                briefing_package["repair_source_commit"] = workspace_base_commit
                latest_repair_review = self.store.fetchone(
                    """SELECT findings_json, required_fixes_json FROM reviews
                       WHERE target_type = 'work_package'
                         AND target_id = ?
                         AND decision = 'rejected_repair_required'
                       ORDER BY id DESC LIMIT 1""",
                    (package["id"],),
                )
                if latest_repair_review is not None:
                    briefing_package["repair_findings"] = _loads(str(latest_repair_review["findings_json"]))
                    briefing_package["repair_required_fixes"] = _loads(str(latest_repair_review["required_fixes_json"]))
            briefing_path.write_text(
                render_work_package_briefing(
                    session_id=session_id,
                    agent=agent,
                    task=self.get_task(task["id"]),
                    package=briefing_package,
                    workspace_path=created_workspace,
                ),
                encoding="utf-8",
            )
            launch = self.mimo_runner.launch(
                session_id=session_id,
                mode=mode,
                model=model,
                workspace_path=created_workspace,
                briefing_path=briefing_path,
            )
        except (OSError, OrchestratorError) as error:
            with self.store.transaction() as conn:
                conn.execute(
                    """UPDATE mimo_sessions SET lifecycle_state = ?, workspace_path = ?, briefing_path = ?,
                       failure_reason = ?, ended_at = ? WHERE id = ?""",
                    (
                        MimoSessionStatus.FAILED.value,
                        str(workspace_path) if workspace_path.exists() else None,
                        str(briefing_path) if briefing_path.exists() else None,
                        str(error),
                        _now(),
                        session_id,
                    ),
                )
                self._audit(
                    conn,
                    actor,
                    effective,
                    "mimo.session_failed",
                    "mimo_session",
                    session_id,
                    {"reason": str(error)},
                )
            raise

        state = MimoSessionStatus.TUI_DETACHED if launch.detached_tui else MimoSessionStatus.RUNNING
        with self.store.transaction() as conn:
            conn.execute(
                """UPDATE mimo_sessions SET lifecycle_state = ?, workspace_path = ?, briefing_path = ?,
                   command_json = ?, pid = ?, stdout_path = ?, stderr_path = ?, started_at = ? WHERE id = ?""",
                (
                    state.value,
                    str(created_workspace),
                    str(briefing_path),
                    _json(launch.argv),
                    launch.pid,
                    str(launch.stdout_path) if launch.stdout_path else None,
                    str(launch.stderr_path) if launch.stderr_path else None,
                    _now(),
                    session_id,
                ),
            )
            self._audit(
                conn,
                actor,
                effective,
                "mimo.session_launched",
                "mimo_session",
                session_id,
                {
                    "pid": launch.pid,
                    "mode": mode.value,
                    "workspace_path": str(created_workspace),
                    "workspace_base_commit": workspace_base_commit,
                },
            )
        handoff_event = self._append_handoff_event(
            project["id"],
            task["id"],
            package["id"],
            "WORKER_STARTED",
            assigned_agent,
            {"session_id": session_id, "mode": mode.value, "workspace_path": str(created_workspace)},
        )
        session = self.get_mimo_session(session_id)
        session["handoff_event"] = handoff_event
        return session
        # END_BLOCK_DISPATCH_MIMO_WORK_PACKAGE

    def poll_mimo_session(self, actor: ActorIdentity, session_id: int) -> dict[str, Any]:
        """Record a terminal exit code for a service-owned headless Mimo process, if observed."""

        session = self._mimo_session(session_id)
        effective = self._authorize(actor, OrchestratorRole.GLM, session["project_id"], session["task_id"])
        if session["lifecycle_state"] != MimoSessionStatus.RUNNING.value:
            return self.get_mimo_session(session_id)
        exit_code = self.mimo_runner.poll(session_id)
        if exit_code is None:
            return self.get_mimo_session(session_id)
        state = MimoSessionStatus.EXITED if exit_code == 0 else MimoSessionStatus.FAILED
        with self.store.transaction() as conn:
            conn.execute(
                "UPDATE mimo_sessions SET lifecycle_state = ?, exit_code = ?, ended_at = ? WHERE id = ?",
                (state.value, exit_code, _now(), session_id),
            )
            self._audit(
                conn,
                actor,
                effective,
                "mimo.session_exited",
                "mimo_session",
                session_id,
                {"exit_code": exit_code, "state": state.value},
            )
        return self.get_mimo_session(session_id)

    def cancel_mimo_session(self, actor: ActorIdentity, session_id: int) -> dict[str, Any]:
        """Cancel only a current-service headless child, never an interactive detached TUI window."""

        session = self._mimo_session(session_id)
        effective = self._authorize(actor, OrchestratorRole.GLM, session["project_id"], session["task_id"])
        if session["lifecycle_state"] != MimoSessionStatus.RUNNING.value:
            raise OrchestratorError("Only a running headless Mimo session can be cancelled by the service")
        exit_code = self.mimo_runner.cancel(session_id)
        with self.store.transaction() as conn:
            conn.execute(
                "UPDATE mimo_sessions SET lifecycle_state = ?, exit_code = ?, ended_at = ? WHERE id = ?",
                (MimoSessionStatus.CANCELLED.value, exit_code, _now(), session_id),
            )
            self._audit(
                conn,
                actor,
                effective,
                "mimo.session_cancelled",
                "mimo_session",
                session_id,
                {"exit_code": exit_code},
            )
        return self.get_mimo_session(session_id)

    def recover_prepared_mimo_session(
        self,
        actor: ActorIdentity,
        session_id: int,
        observation: str,
    ) -> dict[str, Any]:
        """Close an orphaned pre-launch record without treating it as a worker result."""

        session = self._mimo_session(session_id)
        effective = self._authorize(actor, OrchestratorRole.GLM, session["project_id"], session["task_id"])
        if session["lifecycle_state"] != MimoSessionStatus.PREPARED.value:
            raise OrchestratorError("Only a prepared Mimo session may be recovered")
        if session["workspace_path"] is not None or session["briefing_path"] is not None or session["pid"] is not None:
            raise OrchestratorError("Prepared Mimo recovery requires a session with no launch evidence")
        if not observation.strip():
            raise OrchestratorError("Prepared Mimo recovery requires a non-empty controller observation")
        with self.store.transaction() as conn:
            conn.execute(
                "UPDATE mimo_sessions SET lifecycle_state = ?, failure_reason = ?, ended_at = ? WHERE id = ?",
                (MimoSessionStatus.FAILED.value, observation.strip(), _now(), session_id),
            )
            self._audit(
                conn,
                actor,
                effective,
                "mimo.prepared_session_recovered",
                "mimo_session",
                session_id,
                {"observation": observation.strip()},
            )
        return self.get_mimo_session(session_id)

    def recover_orphaned_running_mimo_session(
        self,
        actor: ActorIdentity,
        session_id: int,
        observation: str,
    ) -> dict[str, Any]:
        """Close a headless session only after its persisted process is observed absent."""

        session = self._mimo_session(session_id)
        effective = self._authorize(actor, OrchestratorRole.GLM, session["project_id"], session["task_id"])
        if session["lifecycle_state"] != MimoSessionStatus.RUNNING.value or session["mode"] != MimoLaunchMode.HEADLESS.value:
            raise OrchestratorError("Only a running headless Mimo session may be recovered")
        pid = session["pid"]
        if not isinstance(pid, int) or pid <= 0:
            raise OrchestratorError("Running Mimo recovery requires a persisted process ID")
        if _process_exists(pid):
            raise OrchestratorError("Running Mimo recovery requires an observed absent process")
        if not observation.strip():
            raise OrchestratorError("Running Mimo recovery requires a non-empty controller observation")
        with self.store.transaction() as conn:
            conn.execute(
                "UPDATE mimo_sessions SET lifecycle_state = ?, failure_reason = ?, ended_at = ? WHERE id = ?",
                (MimoSessionStatus.FAILED.value, observation.strip(), _now(), session_id),
            )
            self._audit(
                conn,
                actor,
                effective,
                "mimo.running_session_recovered",
                "mimo_session",
                session_id,
                {"pid": pid, "observation": observation.strip()},
            )
        return self.get_mimo_session(session_id)

    def record_detached_mimo_session_closed(
        self,
        actor: ActorIdentity,
        session_id: int,
        observation: str,
    ) -> dict[str, Any]:
        """Record a GLM-observed TUI closure; it ends a session lock but never accepts a package."""

        session = self._mimo_session(session_id)
        effective = self._authorize(actor, OrchestratorRole.GLM, session["project_id"], session["task_id"])
        if session["lifecycle_state"] != MimoSessionStatus.TUI_DETACHED.value:
            raise OrchestratorError("Only a detached TUI Mimo session may be marked closed")
        if not observation.strip():
            raise OrchestratorError("Detached TUI closure requires a non-empty controller observation")
        with self.store.transaction() as conn:
            conn.execute(
                "UPDATE mimo_sessions SET lifecycle_state = ?, ended_at = ? WHERE id = ?",
                (MimoSessionStatus.EXITED.value, _now(), session_id),
            )
            self._audit(
                conn,
                actor,
                effective,
                "mimo.tui_session_closed_observed",
                "mimo_session",
                session_id,
                {"observation": observation.strip()},
            )
        return self.get_mimo_session(session_id)

    def get_project(self, project_id: int) -> dict[str, Any]:
        project = self._project(project_id)
        project["allowed_test_commands"] = _loads(project.pop("allowed_test_commands_json"))
        return project

    def get_mimo_session(self, session_id: int) -> dict[str, Any]:
        session = self._mimo_session(session_id)
        raw_command = session.pop("command_json")
        session["command"] = _loads(raw_command) if raw_command else None
        return session

    def list_handoff_events(self, actor: ActorIdentity, work_package_id: int) -> list[dict[str, Any]]:
        """Read the ordered handoff event stream for a package; this never changes workflow state."""

        package = self._package(work_package_id)
        task = self._task(package["task_id"])
        self._authorize(actor, OrchestratorRole.CODEX, task["project_id"], task["id"])
        _, events_path, _ = self._handoff_paths(task["project_id"], task["id"], work_package_id)
        if not events_path.is_file():
            return []
        return [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def wait_for_handoff_event(
        self,
        actor: ActorIdentity,
        work_package_id: int,
        after_event_count: int,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        """Block a live Codex controller until this package gains a new closed-schema handoff event.

        The wait is backed by a named Windows event shared by the independent
        worker and controller MCP processes.  It deliberately has a bounded
        timeout: a caller may renew its wait, but no request may become an
        uninterruptible hidden supervisor.
        """

        package = self._package(work_package_id)
        task = self._task(package["task_id"])
        self._authorize(actor, OrchestratorRole.CODEX, task["project_id"], task["id"])
        if after_event_count < 0:
            raise OrchestratorError("after_event_count must be zero or greater")
        if not 1 <= timeout_seconds <= 600:
            raise OrchestratorError("timeout_seconds must be between 1 and 600")
        _, events_path, _ = self._handoff_paths(task["project_id"], task["id"], work_package_id)
        return_grace = min(HANDOFF_WAIT_RETURN_GRACE_SECONDS, max(0.1, timeout_seconds * 0.02))
        deadline = time.monotonic() + max(0.0, timeout_seconds - return_grace)
        while True:
            events = (
                [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines() if line.strip()]
                if events_path.is_file()
                else []
            )
            if len(events) > after_event_count:
                return {
                    "status": "event",
                    "event_count": len(events),
                    "events": events[after_event_count:],
                    "events_path": str(events_path),
                }
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return {
                    "status": "timeout",
                    "event_count": len(events),
                    "events": [],
                    "events_path": str(events_path),
                }
            self._handoff_signal.wait(remaining)

    def validate_worker_report(
        self,
        actor: ActorIdentity,
        work_package_id: int,
        worker_report: Mapping[str, Any],
        evidence_files: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Run the worker report validator as an explicit MCP preflight gate."""

        package = self._package(work_package_id)
        task = self._task(package["task_id"])
        if actor.name == package["claimed_by_agent"]:
            required = OrchestratorRole.WORKER_JUNIOR if package["status"] == WorkPackageStatus.CLAIMED_JUNIOR.value else OrchestratorRole.WORKER_PRO
            effective = self._authorize(actor, required, task["project_id"], task["id"])
        else:
            effective = self._authorize(actor, OrchestratorRole.GLM, task["project_id"], task["id"])
        project = self._project(task["project_id"])
        result = policy_validate_worker_report(
            worker_report,
            task_id=int(task["id"]),
            work_package_id=work_package_id,
            allowed_files=_loads(package["allowed_files_json"]),
            forbidden_files=_loads(package["forbidden_files_json"]),
            evidence_files=evidence_files,
            repo_root=Path(project["repo_path"]),
        )
        with self.store.transaction() as conn:
            self._audit(
                conn,
                actor,
                effective,
                "gate.validate_worker_report",
                "work_package",
                work_package_id,
                {"status": result["status"], "issues": result["issues"], "warnings": result["warnings"]},
            )
        return result

    def acceptance_review_gate(self, actor: ActorIdentity, task_id: int) -> dict[str, Any]:
        """Project whether GLM/Codex acceptance prerequisites are currently satisfied."""

        task = self._task(task_id)
        effective = self._authorize(actor, OrchestratorRole.CODEX, task["project_id"], task_id)
        project = self._project(task["project_id"])
        issues: list[str] = []
        warnings: list[str] = []
        packages = self.store.fetchall("SELECT * FROM work_packages WHERE task_id = ? ORDER BY id", (task_id,))
        if not packages:
            completion = self.store.fetchone(
                """SELECT * FROM reviews
                   WHERE target_type = 'task' AND target_id = ? AND decision = 'controller_completed'
                   ORDER BY id DESC LIMIT 1""",
                (task_id,),
            )
            if completion is None:
                issues.append("Acceptance gate requires at least one work package or audited controller task completion")
            elif task["status"] not in {
                TaskStatus.GLM_ACCEPTED.value,
                TaskStatus.CODEX_FINAL_REVIEW.value,
                TaskStatus.CODEX_ACCEPTED.value,
                TaskStatus.TASK_CLOSED.value,
            }:
                issues.append("Audited controller task completion has not advanced the task to GLM_ACCEPTED")
        for package in packages:
            if package["status"] != WorkPackageStatus.GLM_ACCEPTED.value:
                issues.append(f"Work package {package['id']} is not GLM_ACCEPTED")
            submission = self.store.fetchone(
                "SELECT * FROM submissions WHERE work_package_id = ? ORDER BY id DESC LIMIT 1",
                (package["id"],),
            )
            if submission is None:
                issues.append(f"Work package {package['id']} has no worker submission")
                continue
            report_gate = _loads(str(submission["worker_report_validation_json"]))
            if report_gate.get("status") not in {"pass", "verified"}:
                issues.append(f"Work package {package['id']} worker report gate is not pass")
        if self._requires_agent_infra_lint(project):
            lint_result = policy_lint_agent_infra(Path(project["repo_path"]))
            if lint_result["status"] != "pass":
                issues.extend(f"Agent infra: {issue}" for issue in lint_result["issues"])
            warnings.extend(f"Agent infra: {warning}" for warning in lint_result["warnings"])
        result = {
            "status": "pass" if not issues else "blocked",
            "issues": issues,
            "warnings": warnings,
            "task_status": task["status"],
        }
        with self.store.transaction() as conn:
            self._audit(
                conn,
                actor,
                effective,
                "gate.acceptance_review",
                "task",
                task_id,
                result,
            )
        return result

    def report_worker_handoff_event(
        self,
        actor: ActorIdentity,
        work_package_id: int,
        event_type: str,
        message: str,
    ) -> dict[str, Any]:
        """Let only the currently claimed worker report a closed blocked/needs-controller/failure event."""

        package = self._package(work_package_id)
        if actor.name != package["claimed_by_agent"]:
            raise OrchestratorError("Only the currently claimed worker may report a handoff event")
        task = self._task(package["task_id"])
        required = OrchestratorRole.WORKER_JUNIOR if package["status"] == WorkPackageStatus.CLAIMED_JUNIOR.value else OrchestratorRole.WORKER_PRO
        effective = self._authorize(actor, required, task["project_id"], task["id"])
        if event_type not in {"WORKER_BLOCKED", "WORKER_NEEDS_CONTROLLER", "WORKER_FAILED"}:
            raise OrchestratorError("Workers may report only WORKER_BLOCKED, WORKER_NEEDS_CONTROLLER, or WORKER_FAILED")
        if not message.strip():
            raise OrchestratorError("Worker handoff event requires a non-empty message")
        event = self._append_handoff_event(
            task["project_id"], task["id"], work_package_id, event_type, actor.name, {"message": message.strip()}
        )
        with self.store.transaction() as conn:
            self._audit(conn, actor, effective, "handoff.worker_reported", "work_package", work_package_id, event)
        return event

    def get_work_package(self, package_id: int) -> dict[str, Any]:
        package = self._package(package_id)
        package["allowed_files"] = _loads(package.pop("allowed_files_json"))
        package["forbidden_files"] = _loads(package.pop("forbidden_files_json"))
        package["contract_discovery"] = _loads(package.pop("contract_discovery_json"))
        package["test_surface"] = _loads(package.pop("test_surface_json"))
        package["compact_report_format"] = _loads(package.pop("compact_report_format_json"))
        package["session_routing"] = _loads(package.pop("session_routing_json"))
        package["worker_pro_available"] = bool(package["worker_pro_available"])
        return package

    def get_task(self, task_id: int) -> dict[str, Any]:
        task = self._task(task_id)
        for field in ("constraints", "non_goals", "acceptance_criteria", "allowed_files", "forbidden_files"):
            task[field] = _loads(task.pop(f"{field}_json"))
        packages = self.store.fetchall("SELECT * FROM work_packages WHERE task_id = ? ORDER BY id", (task_id,))
        task["work_packages"] = [self.get_work_package(item["id"]) for item in packages]
        artifacts = self.store.fetchall("SELECT * FROM grace_artifacts WHERE task_id = ? ORDER BY id", (task_id,))
        task["grace_artifacts"] = [dict(item) for item in artifacts]
        reviews = self.store.fetchall(
            "SELECT * FROM reviews WHERE (target_type = 'task' AND target_id = ?) OR target_id IN (SELECT id FROM work_packages WHERE task_id = ?) ORDER BY id DESC",
            (task_id, task_id),
        )
        task["reviews"] = [dict(item) for item in reviews]
        sessions = self.store.fetchall(
            "SELECT id FROM mimo_sessions WHERE task_id = ? ORDER BY id", (task_id,)
        )
        task["mimo_sessions"] = [self.get_mimo_session(int(item["id"])) for item in sessions]
        submissions = self.store.fetchall(
            "SELECT * FROM submissions WHERE work_package_id IN (SELECT id FROM work_packages WHERE task_id = ?) ORDER BY id",
            (task_id,),
        )
        task["submissions"] = [
            {
                **dict(item),
                "tests_run": _loads(str(item["tests_run_json"])),
                "files_changed": _loads(str(item["files_changed_json"])),
                "worker_report": _loads(str(item["worker_report_json"])),
                "worker_report_validation": _loads(str(item["worker_report_validation_json"])),
            }
            for item in submissions
        ]
        return task

    def list_audit(self, task_id: int | None = None) -> list[dict[str, Any]]:
        if task_id is None:
            rows = self.store.fetchall("SELECT * FROM audit_log ORDER BY id")
        else:
            rows = self.store.fetchall(
                """SELECT * FROM audit_log WHERE (target_type = 'task' AND target_id = ?)
                   OR target_id IN (SELECT id FROM work_packages WHERE task_id = ?)
                   OR target_id IN (SELECT id FROM submissions WHERE work_package_id IN (SELECT id FROM work_packages WHERE task_id = ?))
                   OR target_id IN (SELECT id FROM reviews WHERE target_type = 'task' AND target_id = ?)
                   OR target_id IN (SELECT id FROM mimo_sessions WHERE task_id = ?)
                   ORDER BY id""",
                (task_id, task_id, task_id, task_id, task_id),
            )
        return [dict(item) for item in rows]
