"""Trusted in-process hooks for post-mutation orchestration policy."""

# FILE: src/grace_orchestrator/hooks.py
# VERSION: 0.3.0
# START_MODULE_CONTRACT
#   PURPOSE: Dispatch trusted post-mutation workflow hooks without exposing arbitrary client promotion or shell execution.
#   SCOPE: Event registration, synchronous dispatch, audit callbacks, scope callbacks, and hook policy ordering.
#   DEPENDS: M-ORCH-DOMAIN
#   LINKS: M-ORCH-HOOKS, V-M-ORCH-HOOKS, type-HookRegistry, fn-dispatchHook
#   ROLE: RUNTIME
#   MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   logger - stable hook-dispatch telemetry sink.
#   HookEvent - closed internal events emitted by approved service mutations.
#   HookHandler - callback shape accepted by the local trusted registry.
#   HookContext - transactional effect callbacks supplied only by OrchestratorService.
#   HookRegistry - trusted in-process synchronous event bus.
#   install_default_hooks - installs the fixed GRACE policy handlers.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.3.0 - Added trusted post-mutation hooks for workflow gates and acceptance policy.
# END_CHANGE_SUMMARY

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import logging
from typing import Any, Callable, Mapping

from .models import OrchestratorError

logger = logging.getLogger(__name__)

class HookEvent(StrEnum):
    """Closed event set; clients cannot submit arbitrary hook names."""

    TASK_CREATED = "on_task_created"
    GRACE_ARTIFACT_UPSERTED = "on_grace_artifact_upserted"
    WORKPACKAGE_CREATED = "on_workpackage_created"
    SUBMISSION_CREATED = "on_submission_created"
    GLM_REJECTED = "on_glm_rejected"
    GLM_ACCEPTED = "on_glm_accepted"
    CODEX_REJECTED = "on_codex_rejected"
    CODEX_ACCEPTED = "on_codex_accepted"
    GATE_PROMOTED = "gate.promote"


HookHandler = Callable[["HookContext"], None]


@dataclass(frozen=True, slots=True)
class HookContext:
    """Only service-owned callbacks can mutate the current SQLite transaction."""

    event: HookEvent
    project_id: int
    task_id: int
    work_package_id: int | None
    payload: Mapping[str, Any]
    audit: Callable[[str, Mapping[str, Any]], None]
    validate_scope: Callable[[], None]
    enable_worker_pro: Callable[[], None]
    require_grace_artifacts: Callable[[], None]
    close_task: Callable[[], None]


class HookRegistry:
    """A synchronous, trusted EventBus: handlers run inside the originating ledger transaction."""

    def __init__(self) -> None:
        self._handlers: dict[HookEvent, list[HookHandler]] = {event: [] for event in HookEvent}

    def register(self, event: HookEvent, handler: HookHandler) -> None:
        # START_CONTRACT: HookRegistry.register
        #   PURPOSE: Register one trusted in-process handler for a closed hook event.
        #   INPUTS: { event: HookEvent, handler: HookHandler }
        #   OUTPUTS: { None }
        #   SIDE_EFFECTS: Mutates only the local registry, never workflow state.
        #   LINKS: M-ORCH-HOOKS, V-M-ORCH-HOOKS
        # END_CONTRACT: HookRegistry.register
        if not isinstance(event, HookEvent):
            raise OrchestratorError("Hook registration requires a documented HookEvent")
        self._handlers[event].append(handler)

    def dispatch(self, context: HookContext) -> None:
        # START_CONTRACT: HookRegistry.dispatch
        #   PURPOSE: Run trusted hook handlers synchronously inside the originating service transaction.
        #   INPUTS: { context: HookContext - service-owned mutation context }
        #   OUTPUTS: { None - returns only after all handlers succeed }
        #   SIDE_EFFECTS: Invokes only registered Python callbacks; no subprocess or shell surface exists.
        #   LINKS: M-ORCH-HOOKS, V-M-ORCH-HOOKS, fn-dispatchHook
        # END_CONTRACT: HookRegistry.dispatch
        # START_BLOCK_DISPATCH_TRUSTED_POST_MUTATION_HOOKS
        logger.info(
            "[GraceOrchestrator][hooks][DISPATCH_TRUSTED_POST_MUTATION_HOOKS] dispatching trusted hook",
            extra={
                "event": context.event.value,
                "task_id": context.task_id,
                "work_package_id": context.work_package_id,
            },
        )
        for handler in tuple(self._handlers[context.event]):
            handler(context)
        # END_BLOCK_DISPATCH_TRUSTED_POST_MUTATION_HOOKS


def install_default_hooks(registry: HookRegistry) -> None:
    """Install the fixed policy handlers owned by the orchestrator, not by MCP clients."""

    registry.register(HookEvent.TASK_CREATED, _on_task_created)
    registry.register(HookEvent.GRACE_ARTIFACT_UPSERTED, _on_grace_artifact_upserted)
    registry.register(HookEvent.WORKPACKAGE_CREATED, _on_workpackage_created)
    registry.register(HookEvent.SUBMISSION_CREATED, _on_submission_created)
    registry.register(HookEvent.GLM_REJECTED, _on_glm_rejected)
    registry.register(HookEvent.GLM_ACCEPTED, _on_glm_accepted)
    registry.register(HookEvent.CODEX_REJECTED, _on_codex_rejected)
    registry.register(HookEvent.CODEX_ACCEPTED, _on_codex_accepted)
    registry.register(HookEvent.GATE_PROMOTED, _on_gate_promoted)


def _audit_scope_hook(context: HookContext, hook_name: str, payload: Mapping[str, Any] | None = None) -> None:
    context.validate_scope()
    context.audit(hook_name, dict(payload or {}))


def _on_task_created(context: HookContext) -> None:
    _audit_scope_hook(context, HookEvent.TASK_CREATED.value)


def _on_grace_artifact_upserted(context: HookContext) -> None:
    _audit_scope_hook(context, HookEvent.GRACE_ARTIFACT_UPSERTED.value)


def _on_workpackage_created(context: HookContext) -> None:
    _audit_scope_hook(context, HookEvent.WORKPACKAGE_CREATED.value)


def _on_submission_created(context: HookContext) -> None:
    _audit_scope_hook(context, HookEvent.SUBMISSION_CREATED.value)


def _on_glm_rejected(context: HookContext) -> None:
    context.validate_scope()
    context.enable_worker_pro()
    context.audit(HookEvent.GLM_REJECTED.value, {"worker_pro_available": True})


def _on_glm_accepted(context: HookContext) -> None:
    _audit_scope_hook(context, HookEvent.GLM_ACCEPTED.value)


def _on_codex_rejected(context: HookContext) -> None:
    _audit_scope_hook(context, HookEvent.CODEX_REJECTED.value)


def _on_codex_accepted(context: HookContext) -> None:
    _audit_scope_hook(context, HookEvent.CODEX_ACCEPTED.value)
    context.close_task()


def _on_gate_promoted(context: HookContext) -> None:
    context.validate_scope()
    if context.payload.get("to_status") == "CODEX_FINAL_REVIEW":
        context.require_grace_artifacts()
    context.audit(HookEvent.GATE_PROMOTED.value, dict(context.payload))
