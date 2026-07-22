"""Comprehensive test suite verifying all P0, P1, and architectural audit resolutions."""

import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import pytest

from grace_orchestrator.db import OrchestratorStore
from grace_orchestrator.models import ActorIdentity, OrchestratorError, OrchestratorRole
from grace_orchestrator.service import OrchestratorService


def test_ack_attempt_id_mandatory_and_nonempty(tmp_path: Path) -> None:
    store_path = tmp_path / "ledger.sqlite3"
    service = OrchestratorService(store_path)
    actor = ActorIdentity(name="codex_lead", primary_role=OrchestratorRole.CODEX)

    with pytest.raises(TypeError):
        service.ack_continuation(actor, "cont_1", "evt_1")  # type: ignore[call-arg]

    with pytest.raises(OrchestratorError, match="attempt_id is required and cannot be empty"):
        service.ack_continuation(actor, "cont_1", "evt_1", attempt_id="")

    with pytest.raises(OrchestratorError, match="attempt_id is required and cannot be empty"):
        service.ack_continuation(actor, "cont_1", "evt_1", attempt_id="   ")


def test_resolve_attempt_id_mandatory_and_nonempty(tmp_path: Path) -> None:
    store_path = tmp_path / "ledger.sqlite3"
    service = OrchestratorService(store_path)
    actor = ActorIdentity(name="codex_lead", primary_role=OrchestratorRole.CODEX)

    with pytest.raises(TypeError):
        service.resolve_continuation(actor, "cont_1", "evt_1")  # type: ignore[call-arg]

    with pytest.raises(OrchestratorError, match="attempt_id is required and cannot be empty"):
        service.resolve_continuation(actor, "cont_1", "evt_1", attempt_id="")


def test_continuation_ack_race_protection(tmp_path: Path) -> None:
    store_path = tmp_path / "ledger.sqlite3"
    store = OrchestratorStore(store_path)
    with store.transaction() as conn:
        conn.execute(
            """INSERT INTO continuation_deliveries 
               (continuation_id, run_id, source_event_id, state, attempt_count, attempt_id, next_attempt_at, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("cont_race", "run_1", "evt_1", "CLAIMED", 1, "att_123", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )

    service = OrchestratorService(store_path)
    actor = ActorIdentity(name="codex_lead", primary_role=OrchestratorRole.CODEX)

    # Controller ACK arrives rapidly
    acked = service.ack_continuation(actor, "cont_race", "evt_1", attempt_id="att_123", controller_session_id="sess_1")
    assert acked["state"] == "ACKNOWLEDGED"

    # Late supervisor attempt to set CONTROLLER_STARTED must not overwrite ACKNOWLEDGED
    with store.transaction() as conn:
        cursor = conn.execute(
            """UPDATE continuation_deliveries 
               SET state = 'CONTROLLER_STARTED', lease_expires_at = '2026-12-31T00:00:00Z'
               WHERE continuation_id = ? AND attempt_id = ? AND state = 'CLAIMED'""",
            ("cont_race", "att_123"),
        )
        assert cursor.rowcount == 0

    row = store.fetchone("SELECT state FROM continuation_deliveries WHERE continuation_id = ?", ("cont_race",))
    assert row["state"] == "ACKNOWLEDGED"


def test_db_migration_v3_populated_database(tmp_path: Path) -> None:
    db_file = tmp_path / "legacy_v2.sqlite3"
    conn = sqlite3.connect(db_file)
    conn.execute("""
        CREATE TABLE mimo_sessions (
          id INTEGER PRIMARY KEY,
          project_id INTEGER NOT NULL,
          task_id INTEGER NOT NULL,
          work_package_id INTEGER NOT NULL,
          requested_by_agent TEXT NOT NULL,
          assigned_agent TEXT NOT NULL,
          assigned_role TEXT NOT NULL,
          mimo_model TEXT NOT NULL,
          mimo_agent TEXT,
          mode TEXT NOT NULL,
          lifecycle_state TEXT NOT NULL,
          workspace_path TEXT,
          briefing_path TEXT,
          command_json TEXT,
          pid INTEGER,
          process_started_at_os TEXT,
          executable_path TEXT,
          argv_hash TEXT,
          launch_nonce TEXT,
          stdout_path TEXT,
          stderr_path TEXT,
          exit_code INTEGER,
          failure_reason TEXT,
          created_at TEXT NOT NULL,
          started_at TEXT,
          ended_at TEXT
        );
    """)
    conn.execute("""
        INSERT INTO mimo_sessions (id, project_id, task_id, work_package_id, requested_by_agent, assigned_agent, assigned_role, mimo_model, mode, lifecycle_state, created_at)
        VALUES (1, 10, 20, 30, 'glm', 'mimo_worker', 'worker_junior', 'mimo-v1', 'headless', 'RUNNING', '2026-01-01T00:00:00Z');
    """)
    conn.execute("PRAGMA user_version = 2;")
    conn.commit()
    conn.close()

    # Opening via OrchestratorStore triggers migration v4
    store = OrchestratorStore(db_file)
    ver = store.fetchone("PRAGMA user_version")[0]
    assert ver == 4

    rows = store.fetchall("SELECT * FROM mimo_sessions")
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["lifecycle_state"] == "RUNNING"


def test_handoff_event_uuid_generation_and_pagination(tmp_path: Path) -> None:
    store_path = tmp_path / "ledger.sqlite3"
    service = OrchestratorService(store_path)
    actor = ActorIdentity(name="codex_lead", primary_role=OrchestratorRole.CODEX)

    repo_dir = tmp_path / "repo"
    repo_dir.mkdir(parents=True, exist_ok=True)
    grace_dir = repo_dir / ".grace"
    grace_dir.mkdir(parents=True, exist_ok=True)
    service.init_project(
        actor=actor,
        name="proj",
        repo_path=repo_dir,
        grace_path=grace_dir,
        main_branch="main",
        allowed_test_commands={"python": ["pytest"]},
    )
    service.create_codex_task(
        actor=actor,
        project_id=1,
        title="Task Title",
        objective="Task Desc",
        architecture_intent="Intent",
        constraints=["c1"],
        non_goals=["ng1"],
        acceptance_criteria=["ac1"],
        allowed_files=["*"],
        forbidden_files=[],
    )

    glm_actor = ActorIdentity(name="glm_planner", primary_role=OrchestratorRole.GLM)
    service.plan_task(actor=glm_actor, task_id=1)
    service.register_verification_plan(
        actor=glm_actor,
        task_id=1,
        test_strategy="Strategy",
        test_commands=["python"],
    )
    service.register_agent(
        actor=actor,
        project_id=1,
        name="junior1",
        primary_role=OrchestratorRole.WORKER_JUNIOR,
        capabilities=[OrchestratorRole.WORKER_JUNIOR],
        mimo_model="xiaomi/mimo-v2.5",
    )
    service.register_agent(
        actor=actor,
        project_id=1,
        name="pro1",
        primary_role=OrchestratorRole.WORKER_PRO,
        capabilities=[OrchestratorRole.WORKER_PRO],
        mimo_model="xiaomi/mimo-v2.5",
    )




    pkg = service.create_work_package(
        actor=glm_actor,
        task_id=1,
        title="Package Title",
        objective="Desc",
        allowed_files=["*"],
        forbidden_files=["none"],
        test_surface=["pytest"],
        commands_allowed=["python"],
        rollback_boundary="HEAD",
        stop_conditions=["tests_pass"],
        compact_report_format=[
            "summary of changes",
            "files modified",
            "test results",
            "commands run with exact results",
            "audit log items",
            "blockers or open risks",
            "verification status",
        ],
        assigned_junior_agent="junior1",
        assigned_pro_agent="pro1",
        base_commit="HEAD",
        contract_discovery={
            "status": "pass",
            "contracts_read": ["M-TEST"],
            "module_refs": ["M-TEST"],
            "verification_refs": ["V-M-TEST"],
            "missing_contracts": [],
        },
    )







    pkg_id = pkg["id"]

    # Append handoff events
    evt1 = service._append_handoff_event(1, 1, pkg_id, "WORKER_STARTED", "worker1", {"step": 1})
    evt2 = service._append_handoff_event(1, 1, pkg_id, "WORKER_BLOCKED", "worker1", {"step": 2})

    assert "event_id" in evt1 and evt1["event_id"].startswith("evt_")
    assert "event_id" in evt2 and evt2["event_id"].startswith("evt_")

    # Test pagination with UUID cursor
    page1 = service.list_handoff_events_page(pkg_id, limit=1)
    assert len(page1["items"]) == 1
    assert page1["items"][0]["event_id"] == evt1["event_id"]
    assert page1["has_more"] is True
    assert page1["next_after_id"] == evt1["event_id"]

    page2 = service.list_handoff_events_page(pkg_id, after_event_id=page1["next_after_id"], limit=1)
    assert len(page2["items"]) == 1
    assert page2["items"][0]["event_id"] == evt2["event_id"]

    # Unknown cursor raises error
    with pytest.raises(OrchestratorError, match="Unknown after_event_id cursor"):
        service.list_handoff_events_page(pkg_id, after_event_id="non_existent_evt")


def test_resource_access_isolation(tmp_path: Path) -> None:
    store_path = tmp_path / "ledger.sqlite3"
    service = OrchestratorService(store_path)

    worker_actor = ActorIdentity(name="junior_1", primary_role=OrchestratorRole.WORKER_JUNIOR)

    # Worker role cannot read review or grace artifacts
    with pytest.raises(OrchestratorError, match="Worker role worker_junior is not authorized to read review resources"):
        service.get_review(worker_actor, 1)

    with pytest.raises(OrchestratorError, match="Worker role worker_junior is not authorized to read GRACE artifacts"):
        service.get_latest_grace_artifact(worker_actor, 1, "requirements")


def test_cli_dashboard_execution(tmp_path: Path) -> None:
    store_path = tmp_path / "ledger.sqlite3"
    service = OrchestratorService(store_path)
    actor = ActorIdentity(name="codex_lead", primary_role=OrchestratorRole.CODEX)
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir(parents=True, exist_ok=True)
    grace_dir = repo_dir / ".grace"
    grace_dir.mkdir(parents=True, exist_ok=True)
    service.init_project(
        actor=actor,
        name="test_repo",
        repo_path=repo_dir,
        grace_path=grace_dir,
        main_branch="main",
        allowed_test_commands={"python": ["pytest"]},
    )

    # Test cli_dashboard as a subprocess
    cmd = [sys.executable, "-m", "grace_orchestrator.cli_dashboard", "--data-dir", str(tmp_path), "--json"]
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}
    res = subprocess.run(cmd, capture_output=True, text=True, env=env)
    assert res.returncode == 0
    data = json.loads(res.stdout)
    assert "projects_count" in data

