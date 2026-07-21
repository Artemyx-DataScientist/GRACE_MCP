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
from grace_orchestrator.server import (
    ADMINISTRATIVE_TOOLS,
    ALL_REQUIRED_TOOLS,
    REQUIRED_PROMPTS,
    REQUIRED_RESOURCES,
    REQUIRED_TOOLS,
    REQUIRED_TOOLS_BY_ROLE,
    create_server,
)


def test_server_declares_required_mcp_surface(tmp_path) -> None:
    server = create_server(
        ActorIdentity(name="codex", primary_role=OrchestratorRole.CODEX),
        tmp_path,
    )
    assert server.name == "grace-orchestrator-mcp"
    tool_names = {tool.name for tool in asyncio.run(server.list_tools())}

    # Bi-directional surface assertions
    assert tool_names == ALL_REQUIRED_TOOLS
    assert REQUIRED_TOOLS == ALL_REQUIRED_TOOLS
    assert "gate.promote" not in tool_names

    # Administrative tool availability for CODEX and USER
    assert ADMINISTRATIVE_TOOLS <= REQUIRED_TOOLS_BY_ROLE[OrchestratorRole.CODEX]
    assert ADMINISTRATIVE_TOOLS <= REQUIRED_TOOLS_BY_ROLE[OrchestratorRole.USER]

    # Role tool subset invariants
    for role in OrchestratorRole:
        expected_tools = REQUIRED_TOOLS_BY_ROLE[role]
        assert expected_tools <= ALL_REQUIRED_TOOLS

    assert REQUIRED_RESOURCES <= {
        resource.uriTemplate for resource in asyncio.run(server.list_resource_templates())
    }
    for prompt_name in REQUIRED_PROMPTS:
        prompt = asyncio.run(server.get_prompt(prompt_name, None))
        assert prompt.messages


def test_stdio_server_initializes_without_stdout_log_noise(tmp_path: Path) -> None:
    src_path = str(Path(__file__).resolve().parents[1] / "src")

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
                "PYTHONPATH": os.pathsep.join(
                    part for part in [src_path, os.environ.get("PYTHONPATH", "")] if part
                ),
            },
        )
        async with stdio_client(parameters) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                tools = await session.list_tools()
                return {tool.name for tool in tools.tools}

    assert REQUIRED_TOOLS <= asyncio.run(smoke())


def test_worker_scoped_surface_isolation(tmp_path) -> None:
    worker_server = create_server(
        ActorIdentity(name="junior_1", primary_role=OrchestratorRole.WORKER_JUNIOR),
        tmp_path,
    )
    worker_tools = {tool.name for tool in asyncio.run(worker_server.list_tools())}
    expected_worker_tools = REQUIRED_TOOLS_BY_ROLE[OrchestratorRole.WORKER_JUNIOR]

    assert worker_tools == expected_worker_tools
    assert "task.create_codex_task" not in worker_tools
    assert "workpackage.create" not in worker_tools
    assert "task.force_transition" not in worker_tools
    assert "workpackage.claim" in worker_tools
    assert "submission.create" in worker_tools
