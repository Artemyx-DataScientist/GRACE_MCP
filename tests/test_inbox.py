from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import pytest

from grace_orchestrator.db import SCHEMA
from grace_orchestrator.models import ActorIdentity, OrchestratorRole, SubmissionEvidence
from grace_orchestrator.service import OrchestratorError, OrchestratorService
from conftest import packet_kwargs, worker_report


def _actor(name: str, role: OrchestratorRole) -> ActorIdentity:
    return ActorIdentity(name=name, primary_role=role)


def test_db_schema_v4_migration(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(SCHEMA)
        conn.execute("PRAGMA user_version = 3")
        conn.commit()

    _ = OrchestratorService(db_path)
    with sqlite3.connect(db_path) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
        assert ver == 4
        cursor = conn.execute("PRAGMA table_info(agents)")
        cols = {row[1] for row in cursor.fetchall()}
        assert {"runtime", "provider", "model", "reasoning_profile"}.issubset(cols)


def test_agent_registration_persists_metadata(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    (tmp_path / "grace").mkdir()
    project = service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {"unit": ["pytest"]})
    agent = service.register_agent(
        codex,
        project["id"],
        "worker-agent",
        OrchestratorRole.WORKER_PRO,
        [OrchestratorRole.WORKER_PRO],
        runtime="mimo-cli",
        provider="open-router",
        model="claude-3-5-sonnet",
        reasoning_profile="deep-reasoning",
    )
    assert agent["runtime"] == "mimo-cli"
    assert agent["provider"] == "open-router"
    assert agent["model"] == "claude-3-5-sonnet"
    assert agent["reasoning_profile"] == "deep-reasoning"


def test_inbox_next_and_list_priority(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    glm = _actor("glm", OrchestratorRole.GLM)
    junior = _actor("worker-jr", OrchestratorRole.WORKER_JUNIOR)
    pro = _actor("worker-pro", OrchestratorRole.WORKER_PRO)

    (tmp_path / "grace").mkdir()
    project = service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {"unit": ["pytest"]})
    service.register_agent(codex, project["id"], glm.name, OrchestratorRole.GLM, [OrchestratorRole.GLM])
    service.register_agent(codex, project["id"], junior.name, OrchestratorRole.WORKER_JUNIOR, [OrchestratorRole.WORKER_JUNIOR], mimo_model="xiaomi/mimo-v2.5")
    service.register_agent(codex, project["id"], pro.name, OrchestratorRole.WORKER_PRO, [OrchestratorRole.WORKER_PRO], mimo_model="xiaomi/mimo-v2.5-pro")

    task = service.create_codex_task(
        codex,
        project["id"],
        "Inbox Test Task",
        "Test priority order",
        "Test",
        [],
        [],
        [],
        ["src/**"],
        [],
    )
    service.plan_task(glm, task["id"])
    service.register_verification_plan(glm, task["id"], "plan", ["unit"])

    pkg = service.create_work_package(
        glm,
        task["id"],
        "WP Inbox",
        "Check inbox",
        ["src/**"],
        ["tests/protected/**"],
        junior.name,
        pro.name,
        "a" * 40,
        **packet_kwargs(),
    )
    service.assign_work_package(glm, pkg["id"])

    # Worker assigned -> priority 5
    jr_inbox = service.inbox_list(junior, project_id=project["id"])
    assert jr_inbox["count"] == 1
    assert jr_inbox["items"][0]["kind"] == "work_package"
    assert jr_inbox["items"][0]["next_action"]["tool"] == "workpackage.claim"

    # Claim WP -> priority 1
    service.claim_work_package(junior, pkg["id"])
    jr_next = service.inbox_next(junior, project_id=project["id"])
    assert jr_next["status"] == "item"
    assert jr_next["item"]["next_action"]["tool"] == "submission.create"

    # Envelope size clamping
    item_bytes = json.dumps(jr_next["item"]).encode("utf-8")
    assert len(item_bytes) <= 4096


def test_project_scoped_authorization(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    test_owner_unauth = _actor("test-owner-other", OrchestratorRole.TEST_OWNER)

    (tmp_path / "grace").mkdir()
    project = service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {"unit": ["pytest"]})

    # Unregistered test owner cannot access project resources
    with pytest.raises(OrchestratorError, match="authorization|authorized"):
        service.get_project(test_owner_unauth, project["id"])

    with pytest.raises(OrchestratorError, match="authorization|authorized"):
        service.get_orchestrator_status_snapshot(test_owner_unauth, project_id=project["id"])


def test_historical_submission_read_after_reset(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    glm = _actor("glm", OrchestratorRole.GLM)
    junior = _actor("worker-jr", OrchestratorRole.WORKER_JUNIOR)
    pro = _actor("worker-pro", OrchestratorRole.WORKER_PRO)

    (tmp_path / "grace").mkdir()
    project = service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {"unit": ["pytest"]})
    service.register_agent(codex, project["id"], glm.name, OrchestratorRole.GLM, [OrchestratorRole.GLM])
    service.register_agent(codex, project["id"], junior.name, OrchestratorRole.WORKER_JUNIOR, [OrchestratorRole.WORKER_JUNIOR], mimo_model="xiaomi/mimo-v2.5")
    service.register_agent(codex, project["id"], pro.name, OrchestratorRole.WORKER_PRO, [OrchestratorRole.WORKER_PRO], mimo_model="xiaomi/mimo-v2.5-pro")

    task = service.create_codex_task(
        codex,
        project["id"],
        "Hist Task",
        "Test historical submission",
        "Test",
        [],
        [],
        [],
        ["src/**"],
        [],
    )
    service.plan_task(glm, task["id"])
    service.register_verification_plan(glm, task["id"], "plan", ["unit"])
    pkg = service.create_work_package(
        glm,
        task["id"],
        "WP Hist",
        "Test hist",
        ["src/**"],
        ["tests/protected/**"],
        junior.name,
        pro.name,
        "a" * 40,
        **packet_kwargs(),
    )
    service.assign_work_package(glm, pkg["id"])
    service.claim_work_package(junior, pkg["id"])
    sub = service.submit_package(
        junior,
        pkg["id"],
        "submission 1",
        SubmissionEvidence(
            base_commit="a" * 40,
            head_commit="b" * 40,
            diff="diff --git a/a b/a",
            diff_hash="f" * 64,
            files_changed=["src/a.py"],
        ),
        [{"command_key": "unit", "exit_code": 0}],
        "none",
        worker_report=worker_report(task_id=task["id"], package_id=pkg["id"], files_changed=["src/a.py"]),
    )

    # Worker can read submission even after reset/reassignment
    read_sub = service.get_submission(junior, sub["id"])
    assert read_sub["id"] == sub["id"]
    assert read_sub["submitted_by_agent"] == junior.name
