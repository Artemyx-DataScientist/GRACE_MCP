"""Tests expanding code coverage for policy.py validation functions."""

import pytest

from grace_orchestrator.models import OrchestratorError, TaskStatus, WorkPackageStatus
from grace_orchestrator.policy import (
    project_next_action,
    validate_contract_discovery,
    validate_execution_packet,
    validate_worker_report,
)
from grace_orchestrator.state_machine import (
    assert_administrative_transition,
    assert_task_transition,
    assert_work_package_transition,
)


def test_assert_task_transition_valid_and_invalid():
    assert_task_transition(TaskStatus.CODEX_TASK_CREATED, TaskStatus.GLM_GRACE_PLANNED)

    with pytest.raises(OrchestratorError, match="Invalid task transition"):
        assert_task_transition(TaskStatus.CODEX_TASK_CREATED, TaskStatus.CODEX_ACCEPTED)


def test_assert_work_package_transition_valid_and_invalid():
    assert_work_package_transition(WorkPackageStatus.CREATED, WorkPackageStatus.ASSIGNED)

    with pytest.raises(OrchestratorError, match="Invalid work-package transition"):
        assert_work_package_transition(WorkPackageStatus.CREATED, WorkPackageStatus.GLM_ACCEPTED)


def test_assert_administrative_transition_requirements():
    with pytest.raises(OrchestratorError, match="reason must be a descriptive non-empty string"):
        assert_administrative_transition(
            TaskStatus.WORK_PACKAGES_ASSIGNED,
            TaskStatus.GLM_GRACE_PLANNED,
            reason="short",
        )

    assert_administrative_transition(
        TaskStatus.WORK_PACKAGES_ASSIGNED,
        TaskStatus.GLM_GRACE_PLANNED,
        reason="Administrative rollback after stale state",
    )


def test_validate_execution_packet_gates():
    res = validate_execution_packet({})
    assert res["status"] == "blocked"
    assert any("missing required field" in issue for issue in res["issues"])


def test_validate_worker_report_gates():
    res = validate_worker_report(
        {},
        task_id=1,
        work_package_id=1,
        allowed_files=["src/**"],
        forbidden_files=["tests/**"],
    )
    assert res["status"] == "blocked"
    assert any("missing required field" in issue for issue in res["issues"])


def test_validate_contract_discovery_blocked():
    disc = {"status": "fail"}
    res = validate_contract_discovery(disc)
    assert res["status"] == "blocked"


def test_project_next_action_classifier():
    res = project_next_action("WORK_PACKAGES_ASSIGNED", [{"id": 1, "status": "GLM_ACCEPTED"}])
    assert res["action"] == "task.request_final_review"

    res_claim = project_next_action("WORK_PACKAGES_ASSIGNED", [{"id": 1, "status": "ASSIGNED"}])
    assert "workpackage.claim" in res_claim["action"]
