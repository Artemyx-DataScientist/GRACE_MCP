"""Local GRACE orchestration control-plane package."""

# FILE: src/grace_orchestrator/__init__.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Re-export the minimal M-ORCH public Python surface.
#   SCOPE: Barrel imports only.
#   DEPENDS: M-ORCH-DOMAIN, M-ORCH-LEDGER, M-ORCH-HOOKS
#   LINKS: M-ORCH-DOMAIN, M-ORCH-LEDGER, M-ORCH-HOOKS
#   ROLE: BARREL
#   MAP_MODE: SUMMARY
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   package exports - ActorIdentity, OrchestratorRole, HookEvent, HookRegistry, and OrchestratorService.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.3.0 - Added trusted HookRegistry exports for local tool consumers.
# END_CHANGE_SUMMARY

from .hooks import HookEvent, HookRegistry
from .models import ActorIdentity, OrchestratorRole
from .service import OrchestratorService

__all__ = ["ActorIdentity", "HookEvent", "HookRegistry", "OrchestratorRole", "OrchestratorService"]
