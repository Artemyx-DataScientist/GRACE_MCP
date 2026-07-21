"""Tests expanding coverage for server.py error handlers, prompts, and resources."""


from grace_orchestrator.models import ActorIdentity, OrchestratorRole
from grace_orchestrator.server import _plain, create_server


def test_plain_helper():
    data = {"status": "ok"}
    wrapped = _plain(data)
    assert wrapped == data


def test_whoami_null_safe_identity_handling(tmp_path):
    actor = ActorIdentity(name="test_actor", primary_role=OrchestratorRole.CODEX)
    server = create_server(actor, tmp_path)

    tool = server._tool_manager._tools["orchestrator.whoami"]
    res = tool.fn()

    assert res["primary_role"] == "codex"
    assert res["granted_role"] == "codex"
    assert res["requested_role"] == "codex"
