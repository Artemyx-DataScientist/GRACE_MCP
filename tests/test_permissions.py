from datetime import UTC, datetime, timedelta

import pytest

# FILE: tests/test_permissions.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Verify M-ORCH-DOMAIN primary and fallback role authorization.
#   SCOPE: Primary role, active delegation, test-owner fallback, and expiry checks.
#   DEPENDS: M-ORCH-DOMAIN
#   LINKS: M-ORCH-DOMAIN, V-M-ORCH-DOMAIN
#   ROLE: TEST
#   MAP_MODE: LOCALS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   test_* - deterministic authorization and delegation-expiry evidence.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.1.0 - Added Codex-to-GLM/test-owner fallback evidence.
# END_CHANGE_SUMMARY

from grace_orchestrator.models import ActorIdentity, OrchestratorError, OrchestratorRole
from grace_orchestrator.permissions import authorize_role


def test_primary_role_is_authorized_without_delegation() -> None:
    actor = ActorIdentity(name="codex", primary_role=OrchestratorRole.CODEX)
    effective = authorize_role(actor, OrchestratorRole.CODEX, delegations=[])
    assert effective == OrchestratorRole.CODEX


def test_codex_fallback_requires_active_auditable_delegation() -> None:
    actor = ActorIdentity(name="codex", primary_role=OrchestratorRole.CODEX)
    with pytest.raises(OrchestratorError, match="requires role glm"):
        authorize_role(actor, OrchestratorRole.GLM, delegations=[])

    effective = authorize_role(
        actor,
        OrchestratorRole.GLM,
        delegations=[
            {
                "substitute_actor": "codex",
                "delegated_role": "glm",
                "expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
                "revoked_at": None,
            }
        ],
    )
    assert effective == OrchestratorRole.GLM

    test_owner = authorize_role(
        actor,
        OrchestratorRole.TEST_OWNER,
        delegations=[
            {
                "substitute_actor": "codex",
                "delegated_role": "test_owner",
                "expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
                "revoked_at": None,
            }
        ],
    )
    assert test_owner == OrchestratorRole.TEST_OWNER


def test_expired_delegation_is_not_authority() -> None:
    actor = ActorIdentity(name="codex", primary_role=OrchestratorRole.CODEX)
    with pytest.raises(OrchestratorError, match="requires role glm"):
        authorize_role(
            actor,
            OrchestratorRole.GLM,
            delegations=[
                {
                    "substitute_actor": "codex",
                    "delegated_role": "glm",
                    "expires_at": (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
                    "revoked_at": None,
                }
            ],
        )
