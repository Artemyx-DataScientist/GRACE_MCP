"""Host-level continuation watcher for durable GRACE handoff events."""

# FILE: src/grace_orchestrator/host_continuation.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Watch durable run events and start controller continuation outside MCP request lifetimes.
#   SCOPE: File-backed event cursor, per-run continuation lock, controller prompt rendering, and configured Codex process launch.
#   DEPENDS: M-ORCH-LEDGER, M-ORCH-DOMAIN
#   LINKS: M-ORCH-HOST-CONTINUATION, V-M-ORCH-HOST-CONTINUATION
#   ROLE: HOST
#   MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   HostContinuationConfig - environment-backed host supervisor configuration.
#   HostContinuationSupervisor - scans durable event streams and starts controller continuation.
#   main - CLI entrypoint used by scripts/Start-GraceHostContinuation.ps1.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.1.0 - Added durable host continuation for worker handoff events.
# END_CHANGE_SUMMARY

from __future__ import annotations

import argparse
import ctypes
from ctypes import wintypes
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import shlex
import sqlite3
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence
import uuid

from .models import stable_hash
from .db import OrchestratorStore
from .process_identity import ProcessIdentity, ProcessMatchState, capture_process_identity, verify_process_liveness


TRIGGER_EVENT_TYPES = frozenset(
    {
        "WORKER_READY_FOR_REVIEW",
        "WORKER_BLOCKED",
        "WORKER_FAILED",
        "WORKER_NEEDS_CONTROLLER",
    }
)

HOST_EVENT_TYPES = frozenset(
    {
        "HOST_CONTINUATION_DETECTED",
        "HOST_CONTROLLER_RESUME_ATTEMPTED",
        "HOST_CONTROLLER_RESUME_STARTED",
        "HOST_CONTROLLER_RESUME_FAILED",
        "HOST_CONTROLLER_LOGICAL_CONTINUATION_STARTED",
    }
)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _loads_json(raw: object, fallback: Any) -> Any:
    if raw is None:
        return fallback
    if not isinstance(raw, str):
        return raw
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return fallback


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _write_durable_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


def _event_identity(run_id: str, line_index: int, event: dict[str, Any]) -> str:
    raw_id = str(event.get("event_id") or "").strip()
    if raw_id:
        return raw_id
    canonical_payload = json.dumps(event, sort_keys=True)
    hash_sig = stable_hash(f"{run_id}_{line_index}_{canonical_payload}")[:16]
    return f"legacy_evt_{hash_sig}"


def _append_ndjson(path: Path, event: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    evt_dict = dict(event)
    if "event_id" not in evt_dict or not str(evt_dict["event_id"]).strip():
        evt_dict["event_id"] = str(uuid.uuid4())
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(evt_dict, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def _relative_key(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _sanitize_lock_name(run_id: str) -> str:
    return stable_hash(run_id)[:32]


def _split_configured_command(command_line: str) -> list[str]:
    if os.name != "nt":
        return shlex.split(command_line, posix=True)

    argc = ctypes.c_int()
    command_line_to_argv = ctypes.windll.shell32.CommandLineToArgvW
    command_line_to_argv.argtypes = [wintypes.LPCWSTR, ctypes.POINTER(ctypes.c_int)]
    command_line_to_argv.restype = ctypes.POINTER(wintypes.LPWSTR)
    local_free = ctypes.windll.kernel32.LocalFree
    local_free.argtypes = [ctypes.c_void_p]
    local_free.restype = ctypes.c_void_p

    argv_ptr = command_line_to_argv(command_line, ctypes.byref(argc))
    if not argv_ptr:
        raise ValueError("Windows command parsing failed")
    try:
        return [argv_ptr[index] for index in range(argc.value)]
    finally:
        local_free(argv_ptr)


@dataclass(frozen=True, slots=True)
class HostContinuationConfig:
    """Configuration for a host process that lives outside the MCP call lifecycle."""

    data_dir: Path
    poll_interval_seconds: float = 5.0
    resume_command: str | None = None
    start_command: str | None = None
    command_wait_seconds: float = 0.0

    @classmethod
    def from_environment(cls, data_dir: Path | None = None) -> "HostContinuationConfig":
        raw_wait = os.environ.get("GRACE_HOST_CONTINUATION_WAIT_SECONDS", "0").strip()
        try:
            command_wait_seconds = max(0.0, float(raw_wait))
        except ValueError:
            command_wait_seconds = 0.0
        return cls(
            data_dir=Path(data_dir or os.environ.get("GRACE_ORCHESTRATOR_DATA_DIR", ".grace-orchestrator-state")),
            resume_command=os.environ.get("GRACE_CODEX_RESUME_COMMAND") or None,
            start_command=os.environ.get("GRACE_CODEX_START_COMMAND") or None,
            command_wait_seconds=command_wait_seconds,
        )


@dataclass(frozen=True, slots=True)
class EventLocation:
    events_path: Path
    event_index: int
    run_root: Path
    run_id: str


class HostContinuationSupervisor:
    """Scan durable handoff events and launch controller continuation from host state."""

    def __init__(self, config: HostContinuationConfig) -> None:
        # START_CONTRACT: HostContinuationSupervisor.__init__
        #   PURPOSE: Bind one host supervisor to a durable GRACE data directory.
        #   INPUTS: { config: HostContinuationConfig }
        #   OUTPUTS: { HostContinuationSupervisor }
        #   SIDE_EFFECTS: none.
        #   LINKS: M-ORCH-HOST-CONTINUATION
        # END_CONTRACT: HostContinuationSupervisor.__init__
        self.config = config
        self.data_dir = config.data_dir.resolve()
        self.state_dir = self.data_dir / "host-continuation"
        self.cursor_path = self.state_dir / "cursor.json"
        self.lock_root = self.state_dir / "locks"
        self.prompt_root = self.state_dir / "prompts"
        self._last_spawned_pid: int | None = None
        self._last_spawned_argv: list[str] | None = None
        self._last_spawned_nonce: str | None = None

    def run_forever(self) -> None:
        """Poll durable run events until the host process is stopped."""

        while True:
            self.run_once()
            time.sleep(max(0.2, self.config.poll_interval_seconds))

    def run_once(self) -> dict[str, Any]:
        # START_CONTRACT: HostContinuationSupervisor.run_once
        #   PURPOSE: Process each unconsumed durable event at most once according to the cursor.
        #   INPUTS: none.
        #   OUTPUTS: { dict - one scan summary }
        #   SIDE_EFFECTS: Updates host cursor, writes host events, enqueues and processes continuation_deliveries.
        #   LINKS: M-ORCH-HOST-CONTINUATION, V-M-ORCH-HOST-CONTINUATION
        # END_CONTRACT: HostContinuationSupervisor.run_once
        # START_BLOCK_SCAN_DURABLE_RUN_EVENTS
        processed: list[dict[str, Any]] = []
        locked: list[dict[str, Any]] = []
        ignored = 0

        # Phase 1: Scan event stream & insert unconsumed trigger events into SQLite continuation_deliveries
        # Move event scan cursor immediately after inserting into continuation_deliveries
        for events_path in self._event_files():
            event_key = _relative_key(self.data_dir, events_path)
            start_index = self._cursor_count(event_key)
            for event_index, event in self._read_events(events_path, start_index):
                event_type = str(event.get("type", ""))
                if event_type not in TRIGGER_EVENT_TYPES:
                    self._advance_cursor(event_key, event_index)
                    ignored += 1
                    continue

                location = self._location_for(events_path, event_index)
                lock_path = self._run_lock_path(location.run_id)
                if lock_path.exists():
                    locked.append({"status": "locked", "run_id": location.run_id, "event_index": event_index})
                    break

                source_event_id = _event_identity(location.run_id, event_index, event)
                continuation_id = f"cont_{stable_hash(location.run_id + '_' + source_event_id)[:16]}"

                self._enqueue_continuation_delivery(
                    continuation_id=continuation_id,
                    run_id=location.run_id,
                    source_event_id=source_event_id,
                    event_location=location,
                    event=event,
                )
                self._advance_cursor(event_key, event_index)

        # Phase 2: Process pending deliveries from continuation_deliveries table
        delivery_results = self._process_pending_deliveries()
        processed = [res for res in delivery_results if res.get("status") in {"started", "retry_scheduled"}]

        return {
            "status": "ok",
            "processed": processed,
            "processed_count": len(processed),
            "locked": locked,
            "locked_count": len(locked),
            "delivery_results": delivery_results,
            "ignored_count": ignored,
        }
        # END_BLOCK_SCAN_DURABLE_RUN_EVENTS

    def build_run_context(self, location: EventLocation, trigger_event: Mapping[str, Any]) -> dict[str, Any]:
        # START_CONTRACT: HostContinuationSupervisor.build_run_context
        #   PURPOSE: Reconstruct controller review context from durable files and SQLite ledger rows.
        #   INPUTS: { location: event location, trigger_event: durable worker event }
        #   OUTPUTS: { dict - compact controller continuation context }
        #   SIDE_EFFECTS: Reads durable state only.
        #   LINKS: M-ORCH-HOST-CONTINUATION, M-ORCH-LEDGER
        # END_CONTRACT: HostContinuationSupervisor.build_run_context
        # START_BLOCK_RECONSTRUCT_DURABLE_CONTEXT
        project_id = int(trigger_event.get("project_id") or 0)
        task_id = int(trigger_event.get("task_id") or 0)
        package_id = int(trigger_event.get("work_package_id") or 0)
        ledger = self._ledger_snapshot(project_id, task_id, package_id)
        raw_pkg = ledger.get("package")
        package: dict[str, Any] = dict(raw_pkg) if isinstance(raw_pkg, dict) else {}
        raw_sess = ledger.get("latest_session")
        session: dict[str, Any] = dict(raw_sess) if isinstance(raw_sess, dict) else {}
        raw_pay = trigger_event.get("payload")
        payload: dict[str, Any] = dict(raw_pay) if isinstance(raw_pay, dict) else {}
        report_path = self._handoff_report_path(location.run_root, package_id, payload)
        controller_metadata = self._controller_metadata(location.run_root)
        module_id, verification_id = self._module_context(package)
        controller_session_id = self._controller_session_id(controller_metadata)
        worker_worktree = (
            str(session.get("workspace_path") or "")
            or str(payload.get("workspace_path") or "")
            or None
        )
        worker_id = str(trigger_event.get("worker") or package.get("claimed_by_agent") or package.get("assigned_junior_agent") or "")
        return {
            "run_id": location.run_id,
            "continuation_id": trigger_event.get("continuation_id"),
            "source_event_id": trigger_event.get("source_event_id"),
            "attempt_id": trigger_event.get("attempt_id"),
            "attempt_count": trigger_event.get("attempt_count", 1),
            "project_id": project_id,
            "task_id": task_id,
            "work_package_id": package_id,
            "event_index": location.event_index,
            "events_path": str(location.events_path),
            "host_events_path": str(location.run_root / "host-events.ndjson"),
            "run_root": str(location.run_root),
            "trigger_event": dict(trigger_event),
            "trigger_event_type": str(trigger_event.get("type", "")),
            "worker_id": worker_id,
            "worker_worktree": worker_worktree,
            "handoff_report_path": report_path,
            "controller_metadata": controller_metadata,
            "controller_session_id": controller_session_id,
            "controller_state": {
                "task_status": (ledger.get("task") or {}).get("status"),
                "package_status": package.get("status"),
                "latest_review_decision": (ledger.get("latest_review") or {}).get("decision"),
            },
            "project": ledger.get("project") or {},
            "task": ledger.get("task") or {},
            "package": package,
            "module_id": module_id,
            "verification_id": verification_id,
            "latest_session": session,
            "latest_submission": ledger.get("latest_submission") or {},
            "latest_review": ledger.get("latest_review") or {},
        }
        # END_BLOCK_RECONSTRUCT_DURABLE_CONTEXT

    def build_controller_prompt(self, context: Mapping[str, Any]) -> str:
        """Render the compact prompt passed to a resumed or logically restarted controller."""

        raw_package = context.get("package")
        package: dict[str, Any] = raw_package if isinstance(raw_package, dict) else {}
        raw_task = context.get("task")
        task: dict[str, Any] = raw_task if isinstance(raw_task, dict) else {}
        raw_project = context.get("project")
        project: dict[str, Any] = raw_project if isinstance(raw_project, dict) else {}
        raw_trigger = context.get("trigger_event")
        trigger: dict[str, Any] = raw_trigger if isinstance(raw_trigger, dict) else {}
        allowed = _loads_json(package.get("allowed_files_json"), [])
        forbidden = _loads_json(package.get("forbidden_files_json"), [])
        test_surface = _loads_json(package.get("test_surface_json"), [])
        prompt = [
            "# GRACE host controller continuation",
            "",
            "Continue GRACE controller review for the durable worker handoff below.",
            "This is host-level continuation outside `handoff.wait_for_event`; do not assume the previous controller session is alive.",
            "",
            "## Continuation ACK Keys",
            f"- continuation_id: {context.get('continuation_id')}",
            f"- source_event_id: {context.get('source_event_id')}",
            f"- attempt_id: {context.get('attempt_id')}",
            f"- attempt_count: {context.get('attempt_count')}",
            "",
            "## Durable run",
            f"- Run id: {context.get('run_id')}",
            f"- Project id: {context.get('project_id')}",
            f"- Task id: {context.get('task_id')}",
            f"- Work package id: {context.get('work_package_id')}",
            f"- Module id: {context.get('module_id') or 'unknown'}",
            f"- Verification id: {context.get('verification_id') or 'unknown'}",
            f"- Trigger event: {context.get('trigger_event_type')} at index {context.get('event_index')}",
            f"- Events path: {context.get('events_path')}",
            f"- Handoff report path: {context.get('handoff_report_path') or 'not found'}",
            f"- Worker id: {context.get('worker_id') or 'unknown'}",
            f"- Worker worktree: {context.get('worker_worktree') or 'not recorded'}",
            f"- Controller session id: {context.get('controller_session_id') or 'not recorded'}",
            "",
            "## Current controller state",
            f"- Task status: {context.get('controller_state', {}).get('task_status')}",
            f"- Package status: {context.get('controller_state', {}).get('package_status')}",
            f"- Latest review decision: {context.get('controller_state', {}).get('latest_review_decision')}",
            "",
            "## Task",
            f"- Title: {task.get('title', 'unknown')}",
            f"- Objective: {task.get('objective', 'unknown')}",
            f"- Architecture intent: {task.get('architecture_intent', 'unknown')}",
            "",
            "## Work package",
            f"- Title: {package.get('title', 'unknown')}",
            f"- Objective: {package.get('objective', 'unknown')}",
            f"- Allowed files: {json.dumps(allowed, sort_keys=True)}",
            f"- Forbidden files: {json.dumps(forbidden, sort_keys=True)}",
            f"- Test surface: {json.dumps(test_surface, sort_keys=True)}",
            f"- Rollback boundary: {package.get('rollback_boundary', '')}",
            f"- Cache anchor: {package.get('cache_anchor', '')}",
            "",
            "## Required controller actions",
            "1. Read the worker handoff report.",
            "2. Inspect the worker diff in the recorded worker worktree or repository scope.",
            "3. Verify the diff stays inside allowed files and avoids forbidden files.",
            "4. Run the required tests or record exactly why they cannot run.",
            "5. Decide exactly one outcome: ACCEPTED / REWORK_REQUIRED / BLOCKED_WAITING_USER.",
            "6. Emit the corresponding controller event through the GRACE MCP review path; for package review use `review.glm_submit` with `accepted`, `rejected_repair_required`, or `blocked` as appropriate.",
            "7. Report scaffolded, wired, verified, unverified gaps, commands, and exact results.",
            "",
            "## Trigger payload",
            "```json",
            json.dumps(trigger, indent=2, sort_keys=True),
            "```",
            "",
            "Do not treat host events, prompts, or reports as product runtime truth.",
        ]
        if project.get("repo_path"):
            prompt.insert(18, f"- Repository path: {project.get('repo_path')}")
        return "\n".join(prompt) + "\n"

    def _enqueue_continuation_delivery(
        self,
        continuation_id: str,
        run_id: str,
        source_event_id: str,
        event_location: EventLocation,
        event: Mapping[str, Any],
    ) -> bool:
        db_path = self.data_dir / "ledger.sqlite3"
        store = OrchestratorStore(db_path)

        with store.transaction() as conn:
            existing = conn.execute(
                "SELECT id FROM continuation_deliveries WHERE run_id = ? AND source_event_id = ?",
                (run_id, source_event_id),
            ).fetchone()
            if existing is not None:
                return False

            now_str = _now()
            conn.execute(
                """INSERT INTO continuation_deliveries (
                    continuation_id, run_id, source_event_id, state, attempt_count, next_attempt_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (continuation_id, run_id, source_event_id, "PENDING", 0, now_str, now_str),
            )
            return True

    def _recover_expired_leases(self, store: OrchestratorStore) -> list[dict[str, Any]]:
        recovered = []
        now_dt = datetime.now(UTC)
        now_str = _now()

        with store.transaction() as conn:
            # 1. Recover expired CLAIMED leases (supervisor crashed before Popen)
            claimed_expired = conn.execute(
                """SELECT * FROM continuation_deliveries 
                   WHERE state = 'CLAIMED' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?""",
                (now_str,),
            ).fetchall()

            for row in claimed_expired:
                deliv = dict(row)
                attempts = int(deliv["attempt_count"])
                if attempts >= 3:
                    conn.execute(
                        "UPDATE continuation_deliveries SET state = 'DEAD_LETTER', last_error = ? WHERE continuation_id = ?",
                        ("Exceeded maximum attempt limit (3) during CLAIMED lease recovery", deliv["continuation_id"]),
                    )
                    recovered.append({"continuation_id": deliv["continuation_id"], "status": "dead_lettered_claimed_lease"})
                else:
                    backoff_sec = 5 if attempts <= 1 else (30 if attempts == 2 else 120)
                    next_retry = datetime.fromtimestamp(now_dt.timestamp() + backoff_sec, UTC).isoformat()
                    conn.execute(
                        """UPDATE continuation_deliveries 
                           SET state = 'RETRY_WAIT', next_attempt_at = ?, last_error = ? 
                           WHERE continuation_id = ?""",
                        (next_retry, "Claim lease expired before controller launch", deliv["continuation_id"]),
                    )
                    recovered.append({"continuation_id": deliv["continuation_id"], "status": "retry_claimed_lease"})

            # 2. Recover expired CONTROLLER_STARTED leases (controller unacknowledged timeout)
            started_expired = conn.execute(
                """SELECT * FROM continuation_deliveries 
                   WHERE state = 'CONTROLLER_STARTED' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?""",
                (now_str,),
            ).fetchall()

            for row in started_expired:
                deliv = dict(row)
                pid = deliv.get("controller_pid")
                attempts = int(deliv["attempt_count"])

                # Tri-state process liveness check
                is_alive = False
                is_uncertain = False
                if pid:
                    ident = ProcessIdentity(
                        pid=int(pid),
                        process_started_at_os=str(deliv.get("controller_process_started_at_os") or "UNKNOWN"),
                        executable_path=str(deliv.get("controller_executable_path") or ""),
                        argv_hash=str(deliv.get("controller_argv_hash") or ""),
                        launch_nonce=str(deliv.get("controller_launch_nonce") or ""),
                    )
                    match_state = verify_process_liveness(ident)
                    if match_state == ProcessMatchState.MATCH:
                        is_alive = True
                    elif match_state == ProcessMatchState.UNKNOWN:
                        is_uncertain = True

                if is_alive:
                    new_lease = datetime.fromtimestamp(now_dt.timestamp() + 300, UTC).isoformat()
                    conn.execute(
                        "UPDATE continuation_deliveries SET lease_expires_at = ? WHERE continuation_id = ?",
                        (new_lease, deliv["continuation_id"]),
                    )
                    recovered.append({"continuation_id": deliv["continuation_id"], "status": "lease_extended"})
                elif is_uncertain:
                    # UNKNOWN status: do NOT spawn duplicate controller. Extend bounded uncertainty lease (60s).
                    new_lease = datetime.fromtimestamp(now_dt.timestamp() + 60, UTC).isoformat()
                    conn.execute(
                        "UPDATE continuation_deliveries SET lease_expires_at = ?, last_error = ? WHERE continuation_id = ?",
                        (new_lease, "Controller process liveness status UNKNOWN; uncertainty lease extended", deliv["continuation_id"]),
                    )
                    recovered.append({"continuation_id": deliv["continuation_id"], "status": "uncertainty_extended"})
                else:
                    if attempts >= 3:
                        conn.execute(
                            "UPDATE continuation_deliveries SET state = 'DEAD_LETTER', last_error = ? WHERE continuation_id = ?",
                            ("Exceeded maximum attempt limit (3) without controller ACK", deliv["continuation_id"]),
                        )
                        recovered.append({"continuation_id": deliv["continuation_id"], "status": "dead_lettered_unacked"})
                    else:
                        backoff_sec = 5 if attempts <= 1 else (30 if attempts == 2 else 120)
                        next_retry = datetime.fromtimestamp(now_dt.timestamp() + backoff_sec, UTC).isoformat()
                        conn.execute(
                            """UPDATE continuation_deliveries 
                               SET state = 'RETRY_WAIT', next_attempt_at = ?, last_error = ? 
                               WHERE continuation_id = ?""",
                            (next_retry, "Controller process confirmed dead (NOT_FOUND_OR_REUSED)", deliv["continuation_id"]),
                        )
                        recovered.append({"continuation_id": deliv["continuation_id"], "status": "retry_unacked"})


        return recovered

    def _process_pending_deliveries(self) -> list[dict[str, Any]]:
        db_path = self.data_dir / "ledger.sqlite3"
        store = OrchestratorStore(db_path)

        self._recover_expired_leases(store)

        results = []
        now_str = _now()
        with store.transaction() as conn:
            pending = conn.execute(
                """SELECT * FROM continuation_deliveries 
                   WHERE state IN ('PENDING', 'RETRY_WAIT') AND next_attempt_at <= ?
                   ORDER BY id ASC LIMIT 10""",
                (now_str,),
            ).fetchall()
            pending_deliveries = [dict(row) for row in pending]

        for deliv in pending_deliveries:
            res = self._dispatch_delivery(store, deliv)
            results.append(res)

        return results

    def _dispatch_delivery(self, store: OrchestratorStore, delivery: dict[str, Any]) -> dict[str, Any]:
        continuation_id = delivery["continuation_id"]
        run_id = delivery["run_id"]
        attempts = int(delivery["attempt_count"]) + 1

        if attempts > 3:
            with store.transaction() as conn:
                conn.execute(
                    "UPDATE continuation_deliveries SET state = ?, last_error = ? WHERE continuation_id = ?",
                    ("DEAD_LETTER", "Exceeded maximum attempt limit (3)", continuation_id),
                )
            return {"continuation_id": continuation_id, "status": "dead_lettered"}

        now_dt = datetime.now(UTC)
        now_str = now_dt.isoformat()
        claim_lease = datetime.fromtimestamp(now_dt.timestamp() + 60, UTC).isoformat()
        attempt_id = f"att_{uuid.uuid4().hex[:16]}"

        # STEP 1: Short claim transaction
        with store.transaction() as conn:
            cursor = conn.execute(
                """UPDATE continuation_deliveries 
                   SET state = 'CLAIMED', attempt_id = ?, claimed_by = ?, lease_expires_at = ?, attempt_count = ?
                   WHERE continuation_id = ? AND (state = 'PENDING' OR (state = 'RETRY_WAIT' AND next_attempt_at <= ?))""",
                (attempt_id, f"supervisor_{os.getpid()}", claim_lease, attempts, continuation_id, now_str),
            )
            if cursor.rowcount == 0:
                return {"continuation_id": continuation_id, "status": "already_claimed"}

        # STEP 2: File reading and prompt preparation OUTSIDE transaction
        events_path = self.data_dir / "runs" / run_id / "events.ndjson"
        if not events_path.is_file():
            events_path = self.data_dir / run_id / "events.ndjson"
        if not events_path.is_file():
            backoff_sec = 5 if attempts <= 1 else (30 if attempts == 2 else 120)
            next_retry = datetime.fromtimestamp(datetime.now(UTC).timestamp() + backoff_sec, UTC).isoformat()
            with store.transaction() as conn:
                conn.execute(
                    """UPDATE continuation_deliveries 
                       SET state = 'RETRY_WAIT', next_attempt_at = ?, last_error = ? 
                       WHERE continuation_id = ? AND attempt_id = ? AND state = 'CLAIMED'""",
                    (next_retry, "Events log file missing", continuation_id, attempt_id),
                )
            return {"continuation_id": continuation_id, "status": "events_path_missing"}

        all_events: list[tuple[int, dict[str, Any]]] = []
        for line_idx, line in enumerate(events_path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            try:
                evt = json.loads(line)
                if isinstance(evt, dict):
                    all_events.append((line_idx, evt))
            except json.JSONDecodeError:
                logger.warning("Corrupt JSON line %d in %s", line_idx, events_path)

        trigger_event = None
        event_idx = 0
        for idx, evt in all_events:
            evt_id = _event_identity(run_id, idx, evt)
            if evt_id == delivery["source_event_id"]:
                trigger_event = evt
                event_idx = idx
                break

        if trigger_event is None:
            backoff_sec = 5 if attempts <= 1 else (30 if attempts == 2 else 120)
            next_retry = datetime.fromtimestamp(datetime.now(UTC).timestamp() + backoff_sec, UTC).isoformat()
            with store.transaction() as conn:
                conn.execute(
                    """UPDATE continuation_deliveries 
                       SET state = 'RETRY_WAIT', next_attempt_at = ?, last_error = ? 
                       WHERE continuation_id = ? AND attempt_id = ? AND state = 'CLAIMED'""",
                    (next_retry, f"Source event {delivery['source_event_id']} not found in events log", continuation_id, attempt_id),
                )
            return {"continuation_id": continuation_id, "status": "no_trigger_event"}

        location = EventLocation(events_path, event_idx, events_path.parent, run_id)
        lock_path = self._run_lock_path(run_id)

        if not self._acquire_lock(lock_path, location, trigger_event):
            return {"continuation_id": continuation_id, "status": "locked"}

        try:
            enriched_trigger = dict(trigger_event)
            enriched_trigger["continuation_id"] = continuation_id
            enriched_trigger["source_event_id"] = delivery["source_event_id"]
            enriched_trigger["attempt_id"] = attempt_id
            enriched_trigger["attempt_count"] = attempts

            context = self.build_run_context(location, enriched_trigger)
            prompt_path = self._write_prompt(context)

            self._append_host_event(
                context,
                "HOST_CONTINUATION_DETECTED",
                {
                    "prompt_path": str(prompt_path),
                    "trigger_event_type": context["trigger_event_type"],
                    "event_index": location.event_index,
                    "attempt_id": attempt_id,
                },
            )

            resumed = self._attempt_resume(context, prompt_path)
            logical = False if resumed else self._attempt_logical_start(context, prompt_path)
            spawn_pid = self._last_spawned_pid
            spawn_argv = self._last_spawned_argv or []
            spawn_nonce = self._last_spawned_nonce or ""

            # STEP 3: Short update transaction after Popen with Process Identity capture
            with store.transaction() as conn:
                if (resumed or logical) and spawn_pid:
                    ident = capture_process_identity(
                        spawn_pid,
                        spawn_argv[0] if spawn_argv else "",
                        spawn_argv,
                        launch_nonce=spawn_nonce,
                    )
                    ack_lease = datetime.fromtimestamp(datetime.now(UTC).timestamp() + 300, UTC).isoformat()
                    cursor = conn.execute(
                        """UPDATE continuation_deliveries 
                           SET state = 'CONTROLLER_STARTED', lease_expires_at = ?, controller_pid = ?,
                               controller_process_started_at_os = ?, controller_executable_path = ?,
                               controller_argv_hash = ?, controller_launch_nonce = ? 
                           WHERE continuation_id = ? AND attempt_id = ? AND state = 'CLAIMED'""",
                        (
                            ack_lease,
                            spawn_pid,
                            str(ident.process_started_at_os),
                            str(ident.executable_path),
                            ident.argv_hash,
                            ident.launch_nonce,
                            continuation_id,
                            attempt_id,
                        ),
                    )
                    if cursor.rowcount == 0:
                        rec = conn.execute(
                            "SELECT state FROM continuation_deliveries WHERE continuation_id = ?",
                            (continuation_id,),
                        ).fetchone()
                        if rec and rec["state"] in ("ACKNOWLEDGED", "RESOLVED"):
                            return {
                                "continuation_id": continuation_id,
                                "attempt_id": attempt_id,
                                "status": "already_acknowledged",
                                "attempts": attempts,
                                "pid": spawn_pid,
                            }
                    return {"continuation_id": continuation_id, "attempt_id": attempt_id, "status": "started", "attempts": attempts, "pid": spawn_pid}
                else:
                    backoff_sec = 5 if attempts == 1 else (30 if attempts == 2 else 120)
                    next_retry = datetime.fromtimestamp(datetime.now(UTC).timestamp() + backoff_sec, UTC).isoformat()
                    conn.execute(
                        """UPDATE continuation_deliveries 
                           SET state = 'RETRY_WAIT', next_attempt_at = ?, last_error = ? 
                           WHERE continuation_id = ? AND attempt_id = ? AND state = 'CLAIMED'""",
                        (next_retry, "Controller command failed to start", continuation_id, attempt_id),
                    )
                    return {"continuation_id": continuation_id, "attempt_id": attempt_id, "status": "retry_scheduled", "attempts": attempts}

        finally:
            self._release_lock(lock_path)

    def _attempt_resume(self, context: Mapping[str, Any], prompt_path: Path) -> bool:
        session_id = str(context.get("controller_session_id") or "").strip()
        if not session_id:
            return False
        self._append_host_event(
            context,
            "HOST_CONTROLLER_RESUME_ATTEMPTED",
            {"controller_session_id": session_id, "configured": bool(self.config.resume_command)},
        )
        if not self.config.resume_command:
            self._append_host_event(
                context,
                "HOST_CONTROLLER_RESUME_FAILED",
                {
                    "attempted_mode": "resume",
                    "controller_session_id": session_id,
                    "reason": "GRACE_CODEX_RESUME_COMMAND is not configured",
                },
            )
            return False
        result = self._launch_configured_command("resume", self.config.resume_command, context, prompt_path)
        if result["ok"]:
            self._last_spawned_pid = result.get("pid")
            self._append_host_event(context, "HOST_CONTROLLER_RESUME_STARTED", result)
            return True
        self._append_host_event(context, "HOST_CONTROLLER_RESUME_FAILED", result)
        return False

    def _attempt_logical_start(self, context: Mapping[str, Any], prompt_path: Path) -> bool:
        if not self.config.start_command:
            self._append_host_event(
                context,
                "HOST_CONTROLLER_RESUME_FAILED",
                {
                    "attempted_mode": "logical",
                    "reason": "GRACE_CODEX_START_COMMAND is not configured",
                },
            )
            return False
        result = self._launch_configured_command("logical", self.config.start_command, context, prompt_path)
        if result["ok"]:
            self._last_spawned_pid = result.get("pid")
            self._append_host_event(context, "HOST_CONTROLLER_LOGICAL_CONTINUATION_STARTED", result)
            return True
        self._append_host_event(context, "HOST_CONTROLLER_RESUME_FAILED", result)
        return False

    def _launch_configured_command(
        self,
        mode: str,
        command_template: str,
        context: Mapping[str, Any],
        prompt_path: Path,
    ) -> dict[str, Any]:
        prompt_text = prompt_path.read_text(encoding="utf-8")
        placeholders = {
            "prompt_file": str(prompt_path),
            "prompt": prompt_text,
            "data_dir": str(self.data_dir),
            "run_id": str(context.get("run_id") or ""),
            "continuation_id": str(context.get("continuation_id") or ""),
            "source_event_id": str(context.get("source_event_id") or ""),
            "attempt_id": str(context.get("attempt_id") or ""),
            "task_id": str(context.get("task_id") or ""),
            "work_package_id": str(context.get("work_package_id") or ""),
            "report_path": str(context.get("handoff_report_path") or ""),
            "worktree_path": str(context.get("worker_worktree") or ""),
            "controller_session_id": str(context.get("controller_session_id") or ""),
        }
        rendered = command_template
        for key, value in placeholders.items():
            rendered = rendered.replace("{" + key + "}", value)
        try:
            argv = _split_configured_command(rendered)
        except ValueError as error:
            return {"ok": False, "attempted_mode": mode, "reason": f"Invalid command template: {error}"}
        if not argv:
            return {"ok": False, "attempted_mode": mode, "reason": "Configured command is empty"}
        if "{prompt_file}" not in command_template and "{prompt}" not in command_template:
            argv.append(str(prompt_path))
        launch_nonce = uuid.uuid4().hex[:16]
        env = os.environ.copy()
        env.update(
            {
                "GRACE_CONTROLLER_CONTINUATION_MODE": mode,
                "GRACE_CONTROLLER_CONTINUATION_PROMPT_FILE": str(prompt_path),
                "GRACE_CONTROLLER_CONTINUATION_RUN_ID": str(context.get("run_id") or ""),
                "GRACE_CONTINUATION_ID": str(context.get("continuation_id") or ""),
                "GRACE_CONTINUATION_SOURCE_EVENT_ID": str(context.get("source_event_id") or ""),
                "GRACE_CONTINUATION_ATTEMPT_ID": str(context.get("attempt_id") or ""),
                "GRACE_CONTINUATION_LAUNCH_NONCE": launch_nonce,
                "GRACE_CONTINUATION_RUN_ID": str(context.get("run_id") or ""),
                "GRACE_CONTINUATION_ATTEMPT": str(context.get("attempt_count") or "1"),
                "GRACE_CONTROLLER_CONTINUATION_TASK_ID": str(context.get("task_id") or ""),
                "GRACE_CONTROLLER_CONTINUATION_WORK_PACKAGE_ID": str(context.get("work_package_id") or ""),
                "GRACE_CONTROLLER_CONTINUATION_REPORT_PATH": str(context.get("handoff_report_path") or ""),
                "GRACE_CONTROLLER_CONTINUATION_WORKTREE": str(context.get("worker_worktree") or ""),
            }
        )
        cwd = self._command_cwd(context)
        try:
            process = subprocess.Popen(argv, cwd=cwd, env=env)
        except OSError as error:
            return {"ok": False, "attempted_mode": mode, "argv": argv, "cwd": str(cwd), "reason": str(error)}
        self._last_spawned_pid = process.pid
        self._last_spawned_argv = argv
        self._last_spawned_nonce = launch_nonce
        result: dict[str, Any] = {"ok": True, "attempted_mode": mode, "pid": process.pid, "argv": argv, "cwd": str(cwd), "launch_nonce": launch_nonce}

        if self.config.command_wait_seconds > 0:
            try:
                exit_code = process.wait(timeout=self.config.command_wait_seconds)
            except subprocess.TimeoutExpired:
                result["still_running"] = True
            else:
                result["exit_code"] = exit_code
                if exit_code != 0:
                    result["ok"] = False
                    result["reason"] = f"Configured command exited with {exit_code}"
        return result

    def _command_cwd(self, context: Mapping[str, Any]) -> Path:
        worktree = context.get("worker_worktree")
        if isinstance(worktree, str) and worktree and Path(worktree).is_dir():
            return Path(worktree)
        raw_proj = context.get("project")
        project: dict[str, Any] = dict(raw_proj) if isinstance(raw_proj, dict) else {}
        repo_path = project.get("repo_path")
        if isinstance(repo_path, str) and repo_path and Path(repo_path).is_dir():
            return Path(repo_path)
        return self.data_dir

    def _write_prompt(self, context: Mapping[str, Any]) -> Path:
        event_key = stable_hash(
            f"{context.get('run_id')}:{context.get('event_index')}:{context.get('trigger_event_type')}"
        )[:16]
        prompt_path = self.prompt_root / f"{event_key}.md"
        _write_durable_text(prompt_path, self.build_controller_prompt(context))
        return prompt_path

    def _append_host_event(self, context: Mapping[str, Any], event_type: str, payload: Mapping[str, Any]) -> None:
        if event_type not in HOST_EVENT_TYPES:
            raise ValueError(f"Unsupported host event type: {event_type}")
        event = {
            "type": event_type,
            "run_id": context.get("run_id"),
            "project_id": context.get("project_id"),
            "task_id": context.get("task_id"),
            "work_package_id": context.get("work_package_id"),
            "created_at": _now(),
            "payload": dict(payload),
        }
        _append_ndjson(Path(str(context["host_events_path"])), event)

    def _event_files(self) -> list[Path]:
        runs_root = self.data_dir / "runs"
        if not runs_root.is_dir():
            return []
        return sorted(path for path in runs_root.rglob("events.ndjson") if path.is_file())

    def _read_events(self, events_path: Path, after_index: int) -> list[tuple[int, dict[str, Any]]]:
        events: list[tuple[int, dict[str, Any]]] = []
        try:
            run_id = _relative_key(self.data_dir / "runs", events_path.parent)
        except Exception:
            run_id = events_path.parent.name
        for event_index, line in enumerate(events_path.read_text(encoding="utf-8").splitlines(), start=1):
            if event_index <= after_index or not line.strip():
                continue
            try:
                decoded = json.loads(line)
                if isinstance(decoded, dict):
                    decoded["event_id"] = _event_identity(run_id, event_index, decoded)
                else:
                    decoded = {"type": "INVALID_EVENT_JSON", "event_id": f"evt_{run_id}_{event_index}"}
            except json.JSONDecodeError:
                decoded = {"type": "INVALID_EVENT_JSON", "event_id": f"evt_{run_id}_{event_index}", "raw": line}
            events.append((event_index, decoded))
        return events

    def _location_for(self, events_path: Path, event_index: int) -> EventLocation:
        run_root = events_path.parent
        run_id = _relative_key(self.data_dir / "runs", run_root)
        return EventLocation(events_path=events_path, event_index=event_index, run_root=run_root, run_id=run_id)

    def _handoff_report_path(self, run_root: Path, package_id: int, payload: Mapping[str, Any]) -> str | None:
        raw_report = payload.get("report")
        if isinstance(raw_report, str) and raw_report.strip():
            return raw_report
        report_path = run_root / "handoff" / f"WP-{package_id}.report.md"
        if report_path.is_file():
            return str(report_path)
        reports = sorted((run_root / "handoff").glob("*.report.md")) if (run_root / "handoff").is_dir() else []
        return str(reports[-1]) if reports else None

    def _ledger_snapshot(self, project_id: int, task_id: int, package_id: int) -> dict[str, Any]:
        ledger_path = self.data_dir / "ledger.sqlite3"
        if not ledger_path.is_file():
            return {}
        snapshot: dict[str, Any] = {}
        conn = sqlite3.connect(ledger_path)
        conn.row_factory = sqlite3.Row
        try:
            snapshot["project"] = self._row_or_empty(conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone())
            snapshot["task"] = self._row_or_empty(conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone())
            snapshot["package"] = self._row_or_empty(conn.execute("SELECT * FROM work_packages WHERE id = ?", (package_id,)).fetchone())
            snapshot["latest_session"] = self._row_or_empty(
                conn.execute(
                    "SELECT * FROM mimo_sessions WHERE work_package_id = ? ORDER BY id DESC LIMIT 1",
                    (package_id,),
                ).fetchone()
            )
            snapshot["latest_submission"] = self._row_or_empty(
                conn.execute(
                    "SELECT * FROM submissions WHERE work_package_id = ? ORDER BY id DESC LIMIT 1",
                    (package_id,),
                ).fetchone()
            )
            snapshot["latest_review"] = self._row_or_empty(
                conn.execute(
                    "SELECT * FROM reviews WHERE target_type = 'work_package' AND target_id = ? ORDER BY id DESC LIMIT 1",
                    (package_id,),
                ).fetchone()
            )
        finally:
            conn.close()
        return snapshot

    def _row_or_empty(self, row: sqlite3.Row | None) -> dict[str, Any]:
        return dict(row) if row is not None else {}

    def _module_context(self, package: Mapping[str, Any]) -> tuple[str | None, str | None]:
        discovery = _loads_json(package.get("contract_discovery_json"), {})
        if isinstance(discovery, dict):
            module_refs = discovery.get("module_refs") or []
            verification_refs = discovery.get("verification_refs") or []
            module_id = str(module_refs[0]) if module_refs else None
            verification_id = str(verification_refs[0]) if verification_refs else None
            if module_id or verification_id:
                return module_id, verification_id
        route = _loads_json(package.get("session_routing_json"), {})
        module_id = str(route.get("workstream")) if isinstance(route, dict) and route.get("workstream") else None
        cache_anchor = str(package.get("cache_anchor") or "")
        if cache_anchor.startswith("GRACE:"):
            parts = cache_anchor.split(":", 2)
            if len(parts) == 3:
                module_id = module_id or parts[1]
                return module_id, parts[2]
        return module_id, None

    def _controller_metadata(self, run_root: Path) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        for filename in ("controller.json", "controller-session.json", "controller-state.json"):
            metadata.update(_read_json_file(run_root / filename))
        return metadata

    def _controller_session_id(self, metadata: Mapping[str, Any]) -> str | None:
        for key in ("controller_session_id", "session_id", "codex_session_id"):
            value = metadata.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        controller = metadata.get("controller")
        if isinstance(controller, dict):
            value = controller.get("session_id")
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _cursor_count(self, event_key: str) -> int:
        cursor = self._load_cursor()
        event_cursor = cursor.get("events", {}).get(event_key, {})
        try:
            return int(event_cursor.get("line_count", 0))
        except (AttributeError, ValueError):
            return 0

    def _advance_cursor(self, event_key: str, event_index: int) -> None:
        cursor = self._load_cursor()
        events = cursor.setdefault("events", {})
        existing = events.get(event_key, {})
        existing_count = int(existing.get("line_count", 0)) if isinstance(existing, dict) else 0
        events[event_key] = {"line_count": max(existing_count, event_index), "updated_at": _now()}
        self._save_cursor(cursor)

    def _load_cursor(self) -> dict[str, Any]:
        raw = _read_json_file(self.cursor_path)
        if raw.get("version") != 1 or not isinstance(raw.get("events", {}), dict):
            return {"version": 1, "events": {}}
        return raw

    def _save_cursor(self, cursor: Mapping[str, Any]) -> None:
        tmp_path = self.cursor_path.with_suffix(".json.tmp")
        _write_durable_text(tmp_path, json.dumps(dict(cursor), indent=2, sort_keys=True) + "\n")
        tmp_path.replace(self.cursor_path)

    def _run_lock_path(self, run_id: str) -> Path:
        return self.lock_root / _sanitize_lock_name(run_id)

    def _acquire_lock(self, lock_path: Path, location: EventLocation, event: Mapping[str, Any]) -> bool:
        try:
            lock_path.mkdir(parents=True, exist_ok=False)
        except FileExistsError:
            return False
        metadata = {
            "created_at": _now(),
            "pid": os.getpid(),
            "run_id": location.run_id,
            "event_index": location.event_index,
            "event_type": event.get("type"),
        }
        _write_durable_text(lock_path / "owner.json", json.dumps(metadata, indent=2, sort_keys=True) + "\n")
        return True

    def _release_lock(self, lock_path: Path) -> None:
        owner = lock_path / "owner.json"
        if owner.exists():
            owner.unlink()
        try:
            lock_path.rmdir()
        except OSError:
            pass


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Watch durable GRACE worker handoff events and start controller continuation.")
    parser.add_argument("--data-dir", default=None, help="GRACE_ORCHESTRATOR_DATA_DIR containing ledger.sqlite3 and runs/.")
    parser.add_argument("--once", action="store_true", help="Run one scan and exit.")
    parser.add_argument("--poll-interval-seconds", type=float, default=5.0, help="Polling interval for continuous host mode.")
    args = parser.parse_args(argv)
    config = HostContinuationConfig.from_environment(Path(args.data_dir) if args.data_dir else None)
    config = HostContinuationConfig(
        data_dir=config.data_dir,
        poll_interval_seconds=args.poll_interval_seconds,
        resume_command=config.resume_command,
        start_command=config.start_command,
        command_wait_seconds=config.command_wait_seconds,
    )
    supervisor = HostContinuationSupervisor(config)
    if args.once:
        print(json.dumps(supervisor.run_once(), indent=2, sort_keys=True))
        return 0
    supervisor.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
