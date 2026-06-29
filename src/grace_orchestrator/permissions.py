"""Role checks based on bound identity and auditable temporary delegation."""

# FILE: src/grace_orchestrator/permissions.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Resolve primary or active delegated role authority for M-ORCH-DOMAIN.
#   SCOPE: Read-only authorization against supplied delegation records.
#   DEPENDS: M-ORCH-DOMAIN
#   LINKS: M-ORCH-DOMAIN, V-M-ORCH-DOMAIN, fn-requireRole
#   ROLE: RUNTIME
#   MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   logger - stable domain authorization telemetry sink.
#   authorize_role - validates process identity against primary or non-expired delegation.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.1.0 - Added deterministic fallback-role authorization.
# END_CHANGE_SUMMARY

from __future__ import annotations

from datetime import UTC, datetime
import logging
from typing import Mapping, Sequence

from .models import ActorIdentity, OrchestratorError, OrchestratorRole

logger = logging.getLogger(__name__)


def authorize_role(
    actor: ActorIdentity,
    required_role: OrchestratorRole,
    delegations: Sequence[Mapping[str, object]],
    now: datetime | None = None,
) -> OrchestratorRole:
    # START_CONTRACT: authorize_role
    #   PURPOSE: Return a required effective role only for primary or active delegated authority.
    #   INPUTS: { actor: ActorIdentity, required_role: OrchestratorRole, delegations: records }
    #   OUTPUTS: { OrchestratorRole - effective authorized role }
    #   SIDE_EFFECTS: Raises OrchestratorError on missing or expired authority.
    #   LINKS: M-ORCH-DOMAIN, V-M-ORCH-DOMAIN
    # END_CONTRACT: authorize_role
    """Return effective role or reject without accepting client-provided authority."""

    # START_BLOCK_RESOLVE_PRIMARY_OR_DELEGATED_ROLE
    if actor.primary_role == required_role:
        logger.info("[GraceOrchestrator][domain][ROLE_AUTHORIZATION] primary role accepted", extra={"actor": actor.name, "role": required_role.value})
        return required_role

    now = now or datetime.now(UTC)
    for delegation in delegations:
        if delegation.get("substitute_actor") != actor.name:
            continue
        if delegation.get("delegated_role") != required_role.value:
            continue
        if delegation.get("revoked_at") is not None:
            continue
        expires_at = delegation.get("expires_at")
        if not isinstance(expires_at, str):
            continue
        try:
            expiry = datetime.fromisoformat(expires_at)
        except ValueError:
            continue
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=UTC)
        if expiry > now:
            logger.info("[GraceOrchestrator][domain][ROLE_AUTHORIZATION] delegated role accepted", extra={"actor": actor.name, "role": required_role.value})
            return required_role

    raise OrchestratorError(
        f"Actor {actor.name!r} requires role {required_role.value}; no active delegation exists"
    )
    # END_BLOCK_RESOLVE_PRIMARY_OR_DELEGATED_ROLE
