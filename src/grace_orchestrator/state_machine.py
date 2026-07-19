"""Closed workflow transitions. Client code cannot request arbitrary promotion."""

# FILE: src/grace_orchestrator/state_machine.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Define the only legal M-ORCH-DOMAIN task and package transitions.
#   SCOPE: Transition lookup and deterministic rejection; no persistence.
#   DEPENDS: M-ORCH-DOMAIN
#   LINKS: M-ORCH-DOMAIN, V-M-ORCH-DOMAIN, type-TaskStatus, type-WorkPackageStatus
#   ROLE: RUNTIME
#   MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   logger - stable transition-guard telemetry sink.
#   TASK_TRANSITIONS - declared parent-task graph.
#   WORK_PACKAGE_TRANSITIONS - declared package graph.
#   assert_task_transition - rejects skipped task gates.
#   assert_work_package_transition - rejects invalid worker and repair states.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.1.4 - Allow an accepted package wave to reopen package creation before the single final review.
# END_CHANGE_SUMMARY

from __future__ import annotations

import logging

from .models import OrchestratorError, TaskStatus, WorkPackageStatus

logger = logging.getLogger(__name__)


TASK_TRANSITIONS: dict[TaskStatus, set[TaskStatus]] = {
    TaskStatus.CODEX_TASK_CREATED: {TaskStatus.GLM_GRACE_PLANNED},
    TaskStatus.GLM_GRACE_PLANNED: {
        TaskStatus.GLM_TESTS_PREPARED,
        TaskStatus.GLM_ACCEPTED,
    },
    TaskStatus.GLM_TESTS_PREPARED: {
        TaskStatus.WORK_PACKAGES_CREATED,
        TaskStatus.GLM_ACCEPTED,
    },
    TaskStatus.WORK_PACKAGES_CREATED: {
        TaskStatus.WORK_PACKAGES_ASSIGNED,
        TaskStatus.GLM_TESTS_PREPARED,
    },
    TaskStatus.WORK_PACKAGES_ASSIGNED: {
        TaskStatus.GLM_TESTS_PREPARED,
        TaskStatus.GLM_ACCEPTED,
        TaskStatus.GLM_REJECTED_REPAIR_REQUIRED,
    },
    TaskStatus.GLM_REJECTED_REPAIR_REQUIRED: {
        TaskStatus.GLM_TESTS_PREPARED,
        TaskStatus.WORK_PACKAGES_ASSIGNED,
        TaskStatus.GLM_ACCEPTED,
    },
    TaskStatus.GLM_ACCEPTED: {
        TaskStatus.WORK_PACKAGES_CREATED,
        TaskStatus.CODEX_FINAL_REVIEW,
    },
    TaskStatus.CODEX_FINAL_REVIEW: {
        TaskStatus.CODEX_ACCEPTED,
        TaskStatus.CODEX_REJECTED_REPAIR_REQUIRED,
    },
    TaskStatus.CODEX_REJECTED_REPAIR_REQUIRED: {
        TaskStatus.GLM_GRACE_PLANNED,
        TaskStatus.WORK_PACKAGES_ASSIGNED,
    },
    TaskStatus.CODEX_ACCEPTED: {TaskStatus.TASK_CLOSED},
    TaskStatus.TASK_CLOSED: {TaskStatus.NEXT_TASK_READY},
}


WORK_PACKAGE_TRANSITIONS: dict[WorkPackageStatus, set[WorkPackageStatus]] = {
    WorkPackageStatus.CREATED: {WorkPackageStatus.ASSIGNED, WorkPackageStatus.CANCELLED},
    WorkPackageStatus.ASSIGNED: {WorkPackageStatus.CLAIMED_JUNIOR, WorkPackageStatus.CANCELLED},
    WorkPackageStatus.CLAIMED_JUNIOR: {WorkPackageStatus.SUBMITTED},
    WorkPackageStatus.CLAIMED_PRO: {WorkPackageStatus.SUBMITTED},
    WorkPackageStatus.SUBMITTED: {WorkPackageStatus.GLM_REVIEW_IN_PROGRESS},
    WorkPackageStatus.GLM_REVIEW_IN_PROGRESS: {
        WorkPackageStatus.GLM_ACCEPTED,
        WorkPackageStatus.REPAIR_REQUIRED,
    },
    WorkPackageStatus.REPAIR_REQUIRED: {
        WorkPackageStatus.CLAIMED_JUNIOR,
        WorkPackageStatus.CLAIMED_PRO,
        WorkPackageStatus.SUBMITTED,
        WorkPackageStatus.CANCELLED,
    },
}


def assert_task_transition(current: TaskStatus, target: TaskStatus) -> None:
    # START_CONTRACT: assert_task_transition
    #   PURPOSE: Accept only a declared parent-task state transition.
    #   INPUTS: { current: TaskStatus, target: TaskStatus }
    #   OUTPUTS: { None - returns on valid transition }
    #   SIDE_EFFECTS: Raises OrchestratorError on invalid input.
    #   LINKS: M-ORCH-DOMAIN, V-M-ORCH-DOMAIN
    # END_CONTRACT: assert_task_transition
    # START_BLOCK_VALIDATE_TASK_TRANSITION
    if target not in TASK_TRANSITIONS.get(current, set()):
        raise OrchestratorError(f"Invalid task transition: {current.value} -> {target.value}")
    logger.info("[GraceOrchestrator][domain][TRANSITION_GUARD] accepted task transition", extra={"from_status": current.value, "to_status": target.value})
    # END_BLOCK_VALIDATE_TASK_TRANSITION


def assert_work_package_transition(current: WorkPackageStatus, target: WorkPackageStatus) -> None:
    # START_CONTRACT: assert_work_package_transition
    #   PURPOSE: Accept only a declared work-package lifecycle transition.
    #   INPUTS: { current: WorkPackageStatus, target: WorkPackageStatus }
    #   OUTPUTS: { None - returns on valid transition }
    #   SIDE_EFFECTS: Raises OrchestratorError on invalid input.
    #   LINKS: M-ORCH-DOMAIN, V-M-ORCH-DOMAIN
    # END_CONTRACT: assert_work_package_transition
    # START_BLOCK_VALIDATE_WORK_PACKAGE_TRANSITION
    if target not in WORK_PACKAGE_TRANSITIONS.get(current, set()):
        raise OrchestratorError(
            f"Invalid work-package transition: {current.value} -> {target.value}"
        )
    logger.info("[GraceOrchestrator][domain][TRANSITION_GUARD] accepted package transition", extra={"from_status": current.value, "to_status": target.value})
    # END_BLOCK_VALIDATE_WORK_PACKAGE_TRANSITION
