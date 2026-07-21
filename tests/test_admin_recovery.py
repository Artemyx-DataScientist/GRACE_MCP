"""Tests for administrative recovery transitions, optimistic locking, and audit logging."""

import pytest

from grace_orchestrator.models import (
    ActorIdentity,
    ConflictError,
    OrchestratorError,
    OrchestratorRole,
    TaskStatus,
    WorkPackageStatus,
)
from grace_orchestrator.service import OrchestratorService
from conftest import packet_kwargs


def create_test_project_and_task(service: OrchestratorService, codex_actor: ActorIdentity, base_dir=None):
    if base_dir is None:
        repo_dir = service.data_root / "repo"
    else:
        repo_dir = base_dir / "repo"
    repo_dir.mkdir(parents=True, exist_ok=True)
    proj = service.init_project(
        codex_actor,
        name="test-recovery-proj",
        repo_path=repo_dir,
        grace_path=repo_dir,
        main_branch="main",
        allowed_test_commands={"fast": ["pytest"]},
    )
    task = service.create_codex_task(
        codex_actor,
        proj["id"],
        title="test-recovery-task",
        objective="Testing recovery functionality",
        architecture_intent="Modular recovery",
        constraints=["Unit test isolated"],
        non_goals=["no patch apply"],
        acceptance_criteria=["audit exists"],
        allowed_files=["src/**"],
        forbidden_files=[],
    )
    return proj, task


def test_force_transition_valid_task(tmp_path, monkeypatch):
    service = OrchestratorService(tmp_path / "data")
    codex_actor = ActorIdentity(name="codex-1", primary_role=OrchestratorRole.CODEX)
    proj, task = create_test_project_and_task(service, codex_actor)

    # Force transition from CODEX_TASK_CREATED to GLM_TESTS_PREPARED
    updated = service.force_transition(
        codex_actor,
        entity_type="task",
        entity_id=task["id"],
        target_status=TaskStatus.GLM_TESTS_PREPARED.value,
        reason="Administrative manual recovery for test setup",
        expected_current_status=TaskStatus.CODEX_TASK_CREATED.value,
    )
    assert updated["status"] == TaskStatus.GLM_TESTS_PREPARED.value

    # Verify audit entry was written
    audits = [e for e in service.list_audit() if e["event_type"] == "ADMIN_RECOVERY_EXECUTED"]
    assert len(audits) == 1
    assert audits[0]["target_id"] == task["id"]


def test_force_transition_optimistic_locking_mismatch(tmp_path):
    service = OrchestratorService(tmp_path / "data")
    codex_actor = ActorIdentity(name="codex-1", primary_role=OrchestratorRole.CODEX)
    proj, task = create_test_project_and_task(service, codex_actor)

    with pytest.raises(ConflictError, match="OPTIMISTIC_LOCK_MISMATCH"):
        service.force_transition(
            codex_actor,
            entity_type="task",
            entity_id=task["id"],
            target_status=TaskStatus.GLM_TESTS_PREPARED.value,
            reason="Administrative recovery with stale status",
            expected_current_status=TaskStatus.GLM_GRACE_PLANNED.value,  # Wrong expected status!
        )


def test_force_transition_unauthorized_role(tmp_path):
    service = OrchestratorService(tmp_path / "data")
    codex_actor = ActorIdentity(name="codex-1", primary_role=OrchestratorRole.CODEX)
    worker_actor = ActorIdentity(name="worker-1", primary_role=OrchestratorRole.WORKER_JUNIOR)
    proj, task = create_test_project_and_task(service, codex_actor)

    with pytest.raises(OrchestratorError, match="not authorized to perform administrative transitions"):
        service.force_transition(
            worker_actor,
            entity_type="task",
            entity_id=task["id"],
            target_status=TaskStatus.GLM_TESTS_PREPARED.value,
            reason="Worker trying to force transition",
            expected_current_status=TaskStatus.CODEX_TASK_CREATED.value,
        )


def test_force_transition_short_reason(tmp_path):
    service = OrchestratorService(tmp_path / "data")
    codex_actor = ActorIdentity(name="codex-1", primary_role=OrchestratorRole.CODEX)
    proj, task = create_test_project_and_task(service, codex_actor)

    with pytest.raises(OrchestratorError, match="at least 10 characters"):
        service.force_transition(
            codex_actor,
            entity_type="task",
            entity_id=task["id"],
            target_status=TaskStatus.GLM_TESTS_PREPARED.value,
            reason="too short",
            expected_current_status=TaskStatus.CODEX_TASK_CREATED.value,
        )


def test_force_transition_terminal_protection(tmp_path):
    service = OrchestratorService(tmp_path / "data")
    codex_actor = ActorIdentity(name="codex-1", primary_role=OrchestratorRole.CODEX)
    proj, task = create_test_project_and_task(service, codex_actor)

    # Move task to TASK_CLOSED
    service.force_transition(
        codex_actor,
        entity_type="task",
        entity_id=task["id"],
        target_status=TaskStatus.TASK_CLOSED.value,
        reason="Closing task for terminal test",
        expected_current_status=TaskStatus.CODEX_TASK_CREATED.value,
    )

    # Attempting to move out of terminal status without allow_terminal should fail
    with pytest.raises(OrchestratorError, match="terminal status"):
        service.force_transition(
            codex_actor,
            entity_type="task",
            entity_id=task["id"],
            target_status=TaskStatus.GLM_TESTS_PREPARED.value,
            reason="Trying to reopen without allow_terminal",
            expected_current_status=TaskStatus.TASK_CLOSED.value,
            allow_terminal=False,
        )

    # With allow_terminal=True, it succeeds
    reopened = service.force_transition(
        codex_actor,
        entity_type="task",
        entity_id=task["id"],
        target_status=TaskStatus.GLM_TESTS_PREPARED.value,
        reason="Explicit override reopening terminal task",
        expected_current_status=TaskStatus.TASK_CLOSED.value,
        allow_terminal=True,
    )
    assert reopened["status"] == TaskStatus.GLM_TESTS_PREPARED.value


def test_force_reset_work_package(tmp_path):
    service = OrchestratorService(tmp_path / "data")
    codex_actor = ActorIdentity(name="codex-1", primary_role=OrchestratorRole.CODEX)
    glm_actor = ActorIdentity(name="glm-1", primary_role=OrchestratorRole.GLM)
    proj, task = create_test_project_and_task(service, codex_actor)

    service.plan_task(glm_actor, task["id"])
    service.register_verification_plan(
        glm_actor,
        task_id=task["id"],
        test_strategy="automated tests",
        test_commands=["fast"],
    )

    service.register_agent(
        codex_actor,
        proj["id"],
        name="mimo-2.5",
        primary_role=OrchestratorRole.WORKER_JUNIOR,
        capabilities=[OrchestratorRole.WORKER_JUNIOR],
        mimo_model="xiaomi/mimo-v2.5",
    )
    service.register_agent(
        codex_actor,
        proj["id"],
        name="mimo-2.5-pro",
        primary_role=OrchestratorRole.WORKER_PRO,
        capabilities=[OrchestratorRole.WORKER_PRO],
        mimo_model="xiaomi/mimo-v2.5-pro",
    )
    pkg = service.create_work_package(
        glm_actor,
        task["id"],
        title="Test Package",
        objective="implement guards",
        allowed_files=["src/**"],
        forbidden_files=["tests/**"],
        assigned_junior_agent="mimo-2.5",
        assigned_pro_agent="mimo-2.5-pro",
        base_commit="a" * 40,
        **packet_kwargs(),
    )

    assigned = service.assign_work_package(glm_actor, pkg["id"])
    assert assigned["status"] == WorkPackageStatus.ASSIGNED.value

    reset_pkg = service.force_reset_work_package(
        codex_actor,
        package_id=pkg["id"],
        reason="Resetting stuck assigned package",
        expected_current_status=WorkPackageStatus.ASSIGNED.value,
    )

    assert reset_pkg["status"] == WorkPackageStatus.CREATED.value
    assert reset_pkg["claimed_by_agent"] is None

    # Check audit log
    audits = [e for e in service.list_audit() if e["event_type"] == "ADMIN_WORK_PACKAGE_RESET"]
    assert len(audits) == 1
