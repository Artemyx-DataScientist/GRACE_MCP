"""SQLite schema and transaction owner for orchestration workflow truth."""

# FILE: src/grace_orchestrator/db.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Own the M-ORCH-LEDGER SQLite schema and short transactional boundary.
#   SCOPE: Schema installation, append-only audit triggers, and connection transactions.
#   DEPENDS: M-ORCH-LEDGER
#   LINKS: M-ORCH-LEDGER, V-M-ORCH-LEDGER, type-OrchestratorStore
#   ROLE: RUNTIME
#   MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   SCHEMA - local ledger tables and append-only audit triggers.
#   OrchestratorStore - initializes local SQLite and owns explicit transactions.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.3.1 - Added locked read helpers for shared ledger connection access.
# END_CHANGE_SUMMARY

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sqlite3
from threading import RLock
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS projects (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  repo_path TEXT NOT NULL,
  grace_path TEXT NOT NULL,
  main_branch TEXT NOT NULL,
  allowed_test_commands_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agents (
  id INTEGER PRIMARY KEY,
  project_id INTEGER NOT NULL REFERENCES projects(id),
  name TEXT NOT NULL,
  primary_role TEXT NOT NULL,
  capabilities_json TEXT NOT NULL,
  mimo_model TEXT,
  mimo_agent TEXT,
  availability TEXT NOT NULL CHECK(availability IN ('available', 'unavailable')),
  updated_at TEXT NOT NULL,
  UNIQUE(project_id, name)
);

CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY,
  project_id INTEGER NOT NULL REFERENCES projects(id),
  parent_task_id INTEGER REFERENCES tasks(id),
  created_by_role TEXT NOT NULL,
  title TEXT NOT NULL,
  objective TEXT NOT NULL,
  architecture_intent TEXT NOT NULL,
  constraints_json TEXT NOT NULL,
  non_goals_json TEXT NOT NULL,
  acceptance_criteria_json TEXT NOT NULL,
  allowed_files_json TEXT NOT NULL,
  forbidden_files_json TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS grace_artifacts (
  id INTEGER PRIMARY KEY,
  project_id INTEGER NOT NULL REFERENCES projects(id),
  task_id INTEGER NOT NULL REFERENCES tasks(id),
  artifact_type TEXT NOT NULL,
  path TEXT NOT NULL,
  content TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  revision INTEGER NOT NULL,
  created_by_agent TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(task_id, artifact_type, revision)
);

CREATE TABLE IF NOT EXISTS verification_plans (
  id INTEGER PRIMARY KEY,
  task_id INTEGER NOT NULL REFERENCES tasks(id),
  revision INTEGER NOT NULL,
  test_strategy TEXT NOT NULL,
  test_commands_json TEXT NOT NULL,
  risk_coverage_json TEXT NOT NULL,
  acceptance_mapping_json TEXT NOT NULL,
  created_by_agent TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(task_id, revision)
);

CREATE TABLE IF NOT EXISTS work_packages (
  id INTEGER PRIMARY KEY,
  task_id INTEGER NOT NULL REFERENCES tasks(id),
  title TEXT NOT NULL,
  objective TEXT NOT NULL,
  allowed_files_json TEXT NOT NULL,
  forbidden_files_json TEXT NOT NULL,
  assigned_junior_agent TEXT NOT NULL,
  assigned_pro_agent TEXT NOT NULL,
  worker_pro_available INTEGER NOT NULL DEFAULT 0 CHECK(worker_pro_available IN (0, 1)),
  operation_id TEXT NOT NULL DEFAULT '',
  authority_mode TEXT NOT NULL DEFAULT '',
  operation_root TEXT NOT NULL DEFAULT '',
  codex_required INTEGER NOT NULL DEFAULT 1 CHECK(codex_required IN (0, 1)),
  codex_instance_id TEXT NOT NULL DEFAULT '',
  glm_instance_id TEXT NOT NULL DEFAULT '',
  branch_worktree TEXT NOT NULL DEFAULT '',
  glm_scan_plan_report_json TEXT NOT NULL DEFAULT '{}',
  operation_isolation_json TEXT NOT NULL DEFAULT '{}',
  pro_api_assignment TEXT NOT NULL DEFAULT '',
  base_commit TEXT NOT NULL,
  contract_discovery_json TEXT NOT NULL DEFAULT '{}',
  test_surface_json TEXT NOT NULL DEFAULT '[]',
  rollback_boundary TEXT NOT NULL DEFAULT '',
  compact_report_format_json TEXT NOT NULL DEFAULT '[]',
  session_routing_json TEXT NOT NULL DEFAULT '{}',
  cache_anchor TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  claimed_by_agent TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS submissions (
  id INTEGER PRIMARY KEY,
  work_package_id INTEGER NOT NULL REFERENCES work_packages(id),
  submitted_by_agent TEXT NOT NULL,
  base_commit TEXT NOT NULL,
  head_commit TEXT NOT NULL,
  diff TEXT NOT NULL,
  diff_hash TEXT NOT NULL,
  summary TEXT NOT NULL,
  tests_run_json TEXT NOT NULL,
  files_changed_json TEXT NOT NULL,
  worker_report_json TEXT NOT NULL DEFAULT '{}',
  worker_report_validation_json TEXT NOT NULL DEFAULT '{}',
  risk_notes TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reviews (
  id INTEGER PRIMARY KEY,
  target_type TEXT NOT NULL,
  target_id INTEGER NOT NULL,
  reviewer_role TEXT NOT NULL,
  reviewer_agent TEXT NOT NULL,
  effective_role TEXT NOT NULL,
  decision TEXT NOT NULL,
  findings_json TEXT NOT NULL,
  required_fixes_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS role_delegations (
  id INTEGER PRIMARY KEY,
  project_id INTEGER NOT NULL REFERENCES projects(id),
  task_id INTEGER REFERENCES tasks(id),
  unavailable_role TEXT NOT NULL,
  substitute_actor TEXT NOT NULL,
  delegated_role TEXT NOT NULL,
  reason TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  created_by_actor TEXT NOT NULL,
  created_at TEXT NOT NULL,
  revoked_at TEXT
);

CREATE TABLE IF NOT EXISTS test_runs (
  id INTEGER PRIMARY KEY,
  task_id INTEGER NOT NULL REFERENCES tasks(id),
  work_package_id INTEGER REFERENCES work_packages(id),
  command_key TEXT NOT NULL,
  command_json TEXT NOT NULL,
  exit_code INTEGER NOT NULL,
  stdout_path TEXT NOT NULL,
  stderr_path TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mimo_sessions (
  id INTEGER PRIMARY KEY,
  project_id INTEGER NOT NULL REFERENCES projects(id),
  task_id INTEGER NOT NULL REFERENCES tasks(id),
  work_package_id INTEGER NOT NULL REFERENCES work_packages(id),
  requested_by_agent TEXT NOT NULL,
  assigned_agent TEXT NOT NULL,
  assigned_role TEXT NOT NULL,
  mimo_model TEXT NOT NULL,
  mimo_agent TEXT,
  mode TEXT NOT NULL CHECK(mode IN ('headless', 'tui')),
  lifecycle_state TEXT NOT NULL CHECK(lifecycle_state IN ('PREPARED', 'RUNNING', 'TUI_DETACHED', 'EXITED', 'FAILED', 'CANCELLED')),
  workspace_path TEXT,
  briefing_path TEXT,
  command_json TEXT,
  pid INTEGER,
  stdout_path TEXT,
  stderr_path TEXT,
  exit_code INTEGER,
  failure_reason TEXT,
  created_at TEXT NOT NULL,
  started_at TEXT,
  ended_at TEXT
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
  actor_name TEXT NOT NULL,
  operation TEXT NOT NULL,
  request_key TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  result_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(actor_name, operation, request_key)
);

CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY,
  event_type TEXT NOT NULL,
  actor_role TEXT NOT NULL,
  effective_role TEXT NOT NULL,
  actor_agent TEXT NOT NULL,
  target_type TEXT NOT NULL,
  target_id INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS audit_log_no_update
BEFORE UPDATE ON audit_log
BEGIN
  SELECT RAISE(ABORT, 'audit log is append-only');
END;

CREATE TRIGGER IF NOT EXISTS audit_log_no_delete
BEFORE DELETE ON audit_log
BEGIN
  SELECT RAISE(ABORT, 'audit log is append-only');
END;
"""


class OrchestratorStore:
    """Owns one local SQLite connection and explicit short transactions."""

    def __init__(self, database_path: Path) -> None:
        # START_CONTRACT: OrchestratorStore.__init__
        #   PURPOSE: Initialize a local ledger database and install invariant triggers.
        #   INPUTS: { database_path: Path - ledger file }
        #   OUTPUTS: { OrchestratorStore - ready store }
        #   SIDE_EFFECTS: Creates local database files and schema.
        #   LINKS: M-ORCH-LEDGER, V-M-ORCH-LEDGER
        # END_CONTRACT: OrchestratorStore.__init__
        database_path.parent.mkdir(parents=True, exist_ok=True)
        self.database_path = database_path
        self.connection = sqlite3.connect(
            database_path,
            isolation_level=None,
            check_same_thread=False,
        )
        self.connection.row_factory = sqlite3.Row
        self._lock = RLock()
        with self._lock:
            self.connection.executescript(SCHEMA)
            self._ensure_column("agents", "mimo_model", "TEXT")
            self._ensure_column("agents", "mimo_agent", "TEXT")
            self._ensure_column(
                "work_packages",
                "worker_pro_available",
                "INTEGER NOT NULL DEFAULT 0 CHECK(worker_pro_available IN (0, 1))",
            )
            self._ensure_column("work_packages", "operation_id", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column("work_packages", "authority_mode", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column("work_packages", "operation_root", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(
                "work_packages",
                "codex_required",
                "INTEGER NOT NULL DEFAULT 1 CHECK(codex_required IN (0, 1))",
            )
            self._ensure_column("work_packages", "codex_instance_id", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column("work_packages", "glm_instance_id", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column("work_packages", "branch_worktree", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column("work_packages", "glm_scan_plan_report_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("work_packages", "operation_isolation_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("work_packages", "pro_api_assignment", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column("work_packages", "contract_discovery_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("work_packages", "test_surface_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column("work_packages", "rollback_boundary", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column("work_packages", "compact_report_format_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column("work_packages", "session_routing_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("work_packages", "cache_anchor", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column("submissions", "worker_report_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("submissions", "worker_report_validation_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("mimo_sessions", "mimo_agent", "TEXT")
            self.connection.execute(
                "UPDATE work_packages SET worker_pro_available = 1 WHERE status = 'REPAIR_REQUIRED'"
            )
            self.connection.execute("PRAGMA journal_mode = WAL")

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        """Apply the one additive migration needed by an already-created local ledger."""

        columns = {
            str(row["name"])
            for row in self.connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in columns:
            self.connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def fetchone(self, sql: str, parameters: tuple[object, ...] = ()) -> sqlite3.Row | None:
        """Run a short read against the shared connection under the ledger lock."""

        with self._lock:
            return self.connection.execute(sql, parameters).fetchone()

    def fetchall(self, sql: str, parameters: tuple[object, ...] = ()) -> list[sqlite3.Row]:
        """Run a short read against the shared connection under the ledger lock."""

        with self._lock:
            return self.connection.execute(sql, parameters).fetchall()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        # START_CONTRACT: OrchestratorStore.transaction
        #   PURPOSE: Commit a short all-or-nothing ledger transition.
        #   INPUTS: none.
        #   OUTPUTS: { sqlite3.Connection - active immediate transaction }
        #   SIDE_EFFECTS: Begins, commits, or rolls back SQLite state.
        #   LINKS: M-ORCH-LEDGER, fn-appendAudit
        # END_CONTRACT: OrchestratorStore.transaction
        # START_BLOCK_OWN_LEDGER_TRANSACTION
        with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                yield self.connection
            except BaseException:
                self.connection.rollback()
                raise
            else:
                self.connection.commit()
        # END_BLOCK_OWN_LEDGER_TRANSACTION

    def close(self) -> None:
        with self._lock:
            self.connection.close()
