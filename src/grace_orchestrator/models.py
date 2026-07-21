"""Typed contracts for the local orchestration ledger."""

# FILE: src/grace_orchestrator/models.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Define validated actor, role, state, and evidence types for M-ORCH-DOMAIN.
#   SCOPE: Local DTOs and deterministic value helpers; no ledger mutation.
#   DEPENDS: M-ORCH-DOMAIN
#   LINKS: M-ORCH-DOMAIN, V-M-ORCH-DOMAIN, type-OrchestratorRole
#   ROLE: TYPES
#   MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   OrchestratorError - client-safe domain rejection.
#   OrchestratorRole - closed server-side authorization roles.
#   ActorIdentity - process-bound caller identity.
#   TaskStatus - parent workflow lifecycle type.
#   WorkPackageStatus - package workflow lifecycle type.
#   MimoLaunchMode - closed execution presentation modes for the local Mimo bridge.
#   MimoSessionStatus - externally observed Mimo-session lifecycle values.
#   SubmissionEvidence - server-derived Git evidence contract.
#   TestRunResult - persisted command execution evidence.
#   ProjectInitInput - validated project creation DTO.
#   stable_hash - stable content hash helper.
#   json_object - narrow decoded-object validator.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.1.0 - Added typed workflow contracts for the local ledger.
# END_CHANGE_SUMMARY

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from hashlib import sha256
from os import environ
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class OrchestratorError(ValueError):
    """A client-safe rejection of an orchestration operation."""


class ConflictError(OrchestratorError):
    """A rejection due to optimistic locking mismatch or stale status."""


class OrchestratorRole(StrEnum):
    USER = "user"
    CODEX = "codex"
    GLM = "glm"
    TEST_OWNER = "test_owner"
    WORKER_JUNIOR = "worker_junior"
    WORKER_PRO = "worker_pro"


class TaskStatus(StrEnum):
    PROJECT_INITIALIZED = "PROJECT_INITIALIZED"
    CODEX_TASK_CREATED = "CODEX_TASK_CREATED"
    GLM_GRACE_PLANNED = "GLM_GRACE_PLANNED"
    GLM_TESTS_PREPARED = "GLM_TESTS_PREPARED"
    WORK_PACKAGES_CREATED = "WORK_PACKAGES_CREATED"
    WORK_PACKAGES_ASSIGNED = "WORK_PACKAGES_ASSIGNED"
    GLM_REVIEW_IN_PROGRESS = "GLM_REVIEW_IN_PROGRESS"
    GLM_ACCEPTED = "GLM_ACCEPTED"
    GLM_REJECTED_REPAIR_REQUIRED = "GLM_REJECTED_REPAIR_REQUIRED"
    CODEX_FINAL_REVIEW = "CODEX_FINAL_REVIEW"
    CODEX_ACCEPTED = "CODEX_ACCEPTED"
    CODEX_REJECTED_REPAIR_REQUIRED = "CODEX_REJECTED_REPAIR_REQUIRED"
    HUMAN_INTERVENTION_REQUIRED = "HUMAN_INTERVENTION_REQUIRED"
    TASK_CLOSED = "TASK_CLOSED"
    NEXT_TASK_READY = "NEXT_TASK_READY"


class WorkPackageStatus(StrEnum):
    CREATED = "CREATED"
    ASSIGNED = "ASSIGNED"
    CLAIMED_JUNIOR = "CLAIMED_JUNIOR"
    SUBMITTED = "SUBMITTED"
    GLM_REVIEW_IN_PROGRESS = "GLM_REVIEW_IN_PROGRESS"
    GLM_ACCEPTED = "GLM_ACCEPTED"
    REPAIR_REQUIRED = "REPAIR_REQUIRED"
    HUMAN_INTERVENTION_REQUIRED = "HUMAN_INTERVENTION_REQUIRED"
    CLAIMED_PRO = "CLAIMED_PRO"
    CANCELLED = "CANCELLED"


class MimoLaunchMode(StrEnum):
    """The only Mimo launch modes allowed through the orchestration boundary."""

    HEADLESS = "headless"
    TUI = "tui"


class MimoSessionStatus(StrEnum):
    """Evidence state for an external Mimo process, not a package acceptance state."""

    PREPARED = "PREPARED"
    RUNNING = "RUNNING"
    TUI_DETACHED = "TUI_DETACHED"
    EXITED = "EXITED"
    WORKER_CRASHED = "WORKER_CRASHED"
    WORKER_EXITED_WITHOUT_HANDOFF = "WORKER_EXITED_WITHOUT_HANDOFF"
    WORKER_UNRESPONSIVE = "WORKER_UNRESPONSIVE"
    WATCHDOG_UNCERTAIN = "WATCHDOG_UNCERTAIN"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class HostContinuationEventType(StrEnum):
    """Event types tracking host continuation delivery lifecycle."""

    CONTINUATION_DISCOVERED = "CONTINUATION_DISCOVERED"
    CONTINUATION_CONTROLLER_STARTED = "CONTINUATION_CONTROLLER_STARTED"
    CONTINUATION_ACKNOWLEDGED = "CONTINUATION_ACKNOWLEDGED"
    CONTINUATION_RESOLVED = "CONTINUATION_RESOLVED"
    CONTINUATION_RETRY_SCHEDULED = "CONTINUATION_RETRY_SCHEDULED"
    CONTINUATION_DEAD_LETTERED = "CONTINUATION_DEAD_LETTERED"


class ContinuationDeliveryState(StrEnum):
    """Durable state of a continuation delivery record."""

    PENDING = "PENDING"
    CLAIMED = "CLAIMED"
    CONTROLLER_STARTED = "CONTROLLER_STARTED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"
    RETRY_WAIT = "RETRY_WAIT"
    DEAD_LETTER = "DEAD_LETTER"


class ExecutionRuntime(StrEnum):
    """Canonical execution runtime hosts in GRACE."""

    CODEX = "codex"
    ANTIGRAVITY = "antigravity"
    MIMO_TUI = "mimo_tui"
    EXTERNAL = "external"


@dataclass(frozen=True, slots=True)
class ActorIdentity:
    """Identity bound at server startup or registration, never trusted from client self-promotion."""

    name: str
    primary_role: OrchestratorRole
    actor_id: str = ""
    granted_role: OrchestratorRole | None = None
    requested_role: OrchestratorRole | None = None
    runtime: ExecutionRuntime | None = None
    provider: str | None = None
    model: str | None = None
    reasoning_profile: str | None = None

    def __post_init__(self) -> None:
        if not object.__getattribute__(self, "actor_id"):
            object.__setattr__(self, "actor_id", f"actor-{self.name}")
        if object.__getattribute__(self, "granted_role") is None:
            object.__setattr__(self, "granted_role", self.primary_role)

    @classmethod
    # START_CONTRACT: ActorIdentity.from_environment
    #   PURPOSE: Load the process-bound actor identity without client role input.
    #   INPUTS: none.
    #   OUTPUTS: ActorIdentity - configured local actor.
    #   SIDE_EFFECTS: Reads environment only.
    #   LINKS: M-ORCH-DOMAIN, fn-requireRole
    # END_CONTRACT: ActorIdentity.from_environment
    def from_environment(cls) -> "ActorIdentity":
        actor_id = environ.get("GRACE_ORCHESTRATOR_ACTOR_ID", "").strip()
        name = environ.get("GRACE_ORCHESTRATOR_ACTOR_NAME", "").strip()
        raw_role = (
            environ.get("GRACE_ORCHESTRATOR_ACTOR_ROLE", "").strip()
            or environ.get("GRACE_ORCHESTRATOR_REQUESTED_ROLE", "").strip()
        )
        raw_runtime = environ.get("GRACE_ORCHESTRATOR_RUNTIME", "").strip()
        provider = environ.get("GRACE_ORCHESTRATOR_PROVIDER", "").strip() or None
        model = environ.get("GRACE_ORCHESTRATOR_MODEL", "").strip() or None
        reasoning_profile = environ.get("GRACE_ORCHESTRATOR_REASONING_PROFILE", "").strip() or None

        if not name or not raw_role:
            raise OrchestratorError(
                "ACTOR_IDENTITY_UNCONFIGURED: set GRACE_ORCHESTRATOR_ACTOR_NAME and "
                "GRACE_ORCHESTRATOR_ACTOR_ROLE before starting the server"
            )
        try:
            role = OrchestratorRole(raw_role)
        except ValueError as error:
            raise OrchestratorError(f"Unknown configured actor role: {raw_role}") from error

        parsed_runtime: ExecutionRuntime | None = None
        if raw_runtime:
            raw_lowered = raw_runtime.lower()
            if raw_lowered in {"antigravity", "google/antigravity"}:
                parsed_runtime = ExecutionRuntime.ANTIGRAVITY
            elif raw_lowered in {"codex", "openai/codex"}:
                parsed_runtime = ExecutionRuntime.CODEX
            elif raw_lowered in {"mimo", "mimo_tui"}:
                parsed_runtime = ExecutionRuntime.MIMO_TUI
            else:
                try:
                    parsed_runtime = ExecutionRuntime(raw_runtime)
                except ValueError:
                    parsed_runtime = ExecutionRuntime.EXTERNAL

        effective_actor_id = actor_id or f"actor-{name}"

        return cls(
            actor_id=effective_actor_id,
            name=name,
            primary_role=role,
            granted_role=role,
            requested_role=role,
            runtime=parsed_runtime,
            provider=provider,
            model=model,
            reasoning_profile=reasoning_profile,
        )


@dataclass(frozen=True, slots=True)
class SubmissionEvidence:
    base_commit: str
    head_commit: str
    diff: str
    diff_hash: str
    files_changed: list[str]


@dataclass(frozen=True, slots=True)
class TestRunResult:
    command_key: str
    exit_code: int
    stdout_path: Path
    stderr_path: Path


class ProjectInitInput(BaseModel):
    """Pydantic DTO used by the MCP entry point before a project is registered."""

    name: str = Field(min_length=1, max_length=200)
    repo_path: str
    grace_path: str
    main_branch: str = Field(min_length=1, max_length=200)
    allowed_test_commands: dict[str, list[str]] = Field(default_factory=dict)


def stable_hash(payload: str) -> str:
    # START_CONTRACT: stable_hash
    #   PURPOSE: Produce a stable content hash for ledger evidence.
    #   INPUTS: { payload: str - UTF-8 content }
    #   OUTPUTS: { str - SHA-256 hex digest }
    #   SIDE_EFFECTS: none.
    #   LINKS: M-ORCH-LEDGER
    # END_CONTRACT: stable_hash
    return sha256(payload.encode("utf-8")).hexdigest()


def json_object(value: Any) -> dict[str, Any]:
    # START_CONTRACT: json_object
    #   PURPOSE: Reject non-object JSON projections at the domain boundary.
    #   INPUTS: { value: Any - decoded JSON candidate }
    #   OUTPUTS: { dict - validated object }
    #   SIDE_EFFECTS: none.
    #   LINKS: M-ORCH-DOMAIN
    # END_CONTRACT: json_object
    """Narrow helper for values returned from SQLite JSON columns."""

    if isinstance(value, dict):
        return value
    raise OrchestratorError("Expected JSON object")
