import asyncio
import os
from pathlib import Path
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# FILE: tests/test_server_surface.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Verify actual M-ORCH-MCP-SERVER tools, resources, prompts, and STDIO initialization.
#   SCOPE: FastMCP registration plus local JSON-RPC client smoke.
#   DEPENDS: M-ORCH-MCP-SERVER
#   LINKS: M-ORCH-MCP-SERVER, V-M-ORCH-MCP-SERVER
#   ROLE: TEST
#   MAP_MODE: LOCALS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   test_server_declares_required_mcp_surface - registered tool/resource/prompt assertions.
#   test_stdio_server_initializes_without_stdout_log_noise - actual STDIO handshake smoke.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.2.0 - Extended FastMCP registry evidence for Mimo role profiles and dispatch.
# END_CHANGE_SUMMARY

from grace_orchestrator.models import ActorIdentity, OrchestratorRole
from grace_orchestrator.server import REQUIRED_PROMPTS, REQUIRED_RESOURCES, REQUIRED_TOOLS, create_server


def test_server_declares_required_mcp_surface(tmp_path) -> None:
    server = create_server(
        ActorIdentity(name="codex", primary_role=OrchestratorRole.CODEX),
        tmp_path,
    )
    assert server.name == "grace-orchestrator-mcp"
    tool_names = {tool.name for tool in asyncio.run(server.list_tools())}
    assert REQUIRED_TOOLS <= tool_names
    assert "gate.promote" not in tool_names
    assert REQUIRED_RESOURCES <= {
        resource.uriTemplate for resource in asyncio.run(server.list_resource_templates())
    }
    for prompt_name in REQUIRED_PROMPTS:
        prompt = asyncio.run(server.get_prompt(prompt_name, None))
        assert prompt.messages


def test_stdio_server_initializes_without_stdout_log_noise(tmp_path: Path) -> None:
    async def smoke() -> set[str]:
        parameters = StdioServerParameters(
            command=sys.executable,
            args=["-m", "grace_orchestrator"],
            cwd=tmp_path,
            env={
                **os.environ,
                "GRACE_ORCHESTRATOR_ACTOR_NAME": "codex",
                "GRACE_ORCHESTRATOR_ACTOR_ROLE": "codex",
                "GRACE_ORCHESTRATOR_DATA_DIR": str(tmp_path / "state"),
            },
        )
        async with stdio_client(parameters) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                tools = await session.list_tools()
                return {tool.name for tool in tools.tools}

    assert REQUIRED_TOOLS <= asyncio.run(smoke())
