"""STDIO entry point. stdout remains owned by MCP JSON-RPC."""

# FILE: src/grace_orchestrator/__main__.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Start M-ORCH-MCP-SERVER over STDIO while reserving stdout for JSON-RPC.
#   SCOPE: Configure stderr logging, load bound identity, and run FastMCP STDIO transport.
#   DEPENDS: M-ORCH-MCP-SERVER
#   LINKS: M-ORCH-MCP-SERVER, V-M-ORCH-MCP-SERVER, fn-stdioMain
#   ROLE: SCRIPT
#   MAP_MODE: LOCALS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   main - process entry point for actor-bound STDIO server.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.1.0 - Added stderr-only STDIO server entry point.
# END_CHANGE_SUMMARY

from __future__ import annotations

import logging
from os import environ
from pathlib import Path
import sys

from .models import ActorIdentity
from .server import create_server

logger = logging.getLogger(__name__)


def main() -> None:
    # START_CONTRACT: main
    #   PURPOSE: Start the configured actor-bound FastMCP STDIO process.
    #   INPUTS: Environment identity and data directory variables.
    #   OUTPUTS: { None - blocks while the STDIO server runs }
    #   SIDE_EFFECTS: Configures stderr logging and creates local ledger state.
    #   LINKS: M-ORCH-MCP-SERVER, V-M-ORCH-MCP-SERVER
    # END_CONTRACT: main
    # START_BLOCK_START_STDIO_SERVER
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(levelname)s %(message)s")
    actor = ActorIdentity.from_environment()
    data_dir = Path(environ.get("GRACE_ORCHESTRATOR_DATA_DIR", ".grace-orchestrator-state"))
    logger.info("[GraceOrchestrator][mcp][STDIO_TRANSPORT] starting stdio server")
    create_server(actor, data_dir).run(transport="stdio")
    # END_BLOCK_START_STDIO_SERVER


if __name__ == "__main__":
    main()
