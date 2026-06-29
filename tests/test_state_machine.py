import pytest

# FILE: tests/test_state_machine.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Verify M-ORCH-DOMAIN task and package transition guards deterministically.
#   SCOPE: Legal path and forbidden transition assertions.
#   DEPENDS: M-ORCH-DOMAIN
#   LINKS: M-ORCH-DOMAIN, V-M-ORCH-DOMAIN
#   ROLE: TEST
#   MAP_MODE: LOCALS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   test_* - transition acceptance and repair-claim rejection scenarios.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.1.1 - Cover controller repair resubmission transition.
# END_CHANGE_SUMMARY

from grace_orchestrator.models import OrchestratorError, TaskStatus, WorkPackageStatus
from grace_orchestrator.state_machine import assert_task_transition, assert_work_package_transition


def test_task_machine_accepts_planned_path() -> None:
    assert_task_transition(TaskStatus.CODEX_TASK_CREATED, TaskStatus.GLM_GRACE_PLANNED)
    assert_task_transition(TaskStatus.GLM_GRACE_PLANNED, TaskStatus.GLM_TESTS_PREPARED)
    assert_task_transition(TaskStatus.GLM_GRACE_PLANNED, TaskStatus.GLM_ACCEPTED)
    assert_task_transition(TaskStatus.GLM_TESTS_PREPARED, TaskStatus.GLM_ACCEPTED)
    assert_task_transition(TaskStatus.GLM_ACCEPTED, TaskStatus.CODEX_FINAL_REVIEW)
    assert_task_transition(TaskStatus.CODEX_FINAL_REVIEW, TaskStatus.CODEX_ACCEPTED)
    assert_task_transition(TaskStatus.CODEX_ACCEPTED, TaskStatus.TASK_CLOSED)


def test_task_machine_rejects_skipped_acceptance() -> None:
    with pytest.raises(OrchestratorError, match="Invalid task transition"):
        assert_task_transition(TaskStatus.CODEX_TASK_CREATED, TaskStatus.CODEX_FINAL_REVIEW)


def test_pro_claim_requires_recorded_repair_transition() -> None:
    with pytest.raises(OrchestratorError, match="Invalid work-package transition"):
        assert_work_package_transition(WorkPackageStatus.ASSIGNED, WorkPackageStatus.CLAIMED_PRO)

    assert_work_package_transition(WorkPackageStatus.REPAIR_REQUIRED, WorkPackageStatus.CLAIMED_PRO)
    assert_work_package_transition(WorkPackageStatus.REPAIR_REQUIRED, WorkPackageStatus.SUBMITTED)
