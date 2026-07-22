"""End-to-end contract tests for the passive GUI-neutral orchestration inbox."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import sys
from typing import Any

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.server.fastmcp.exceptions import ToolError

from conftest import packet_kwargs
from grace_orchestrator.models import ActorIdentity, OrchestratorRole
from grace_orchestrator.server import create_server
from grace_orchestrator.service import OrchestratorService


def _actor(name: str, role: OrchestratorRole) -> ActorIdentity:
    return ActorIdentity(name=name, primary_role=role)


def _base_case(tmp_path: Path, *, task_objective: str = "bounded objective") -> dict[str, Any]:
    repo = tmp_path / "repo"
    repo.mkdir()
    grace = repo / "grace"
    grace.mkdir()
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    glm = _actor("glm-project", OrchestratorRole.GLM)
    junior = _actor("worker-junior", OrchestratorRole.WORKER_JUNIOR)
    pro = _actor("worker-pro", OrchestratorRole.WORKER_PRO)
    project = service.init_project(
        codex,
        "contract-project",
        repo,
        grace,
        "main",
        {"unit": [sys.executable, "-c", "from pathlib import Path; print(Path.cwd())"]},
    )
    service.register_agent(codex, project["id"], glm.name, glm.primary_role, [glm.primary_role])
    service.register_agent(
        codex,
        project["id"],
        junior.name,
        junior.primary_role,
        [junior.primary_role],
        mimo_model="xiaomi/mimo-v2.5",
    )
    service.register_agent(
        codex,
        project["id"],
        pro.name,
        pro.primary_role,
        [pro.primary_role],
        mimo_model="xiaomi/mimo-v2.5-pro",
    )
    task = service.create_codex_task(
        codex,
        project["id"],
        "Contract task",
        task_objective,
        "GUI-neutral passive routing",
        [],
        [],
        ["Every projected action is executable by the bound actor"],
        ["src/**"],
        ["tests/protected/**"],
    )
    service.plan_task(glm, task["id"])
    service.register_verification_plan(glm, task["id"], "deterministic", ["unit"])
    return {
        "root": tmp_path,
        "repo": repo,
        "grace": grace,
        "service": service,
        "codex": codex,
        "glm": glm,
        "junior": junior,
        "pro": pro,
        "project": project,
        "task": task,
    }


def _create_package(
    case: dict[str, Any],
    *,
    title: str = "Contract package",
    objective: str = "implement the bounded change",
    junior: ActorIdentity | None = None,
    allowed_files: list[str] | None = None,
    assign: bool = True,
) -> dict[str, Any]:
    worker = junior or case["junior"]
    package = case["service"].create_work_package(
        case["glm"],
        case["task"]["id"],
        title,
        objective,
        allowed_files or ["src/contract/**"],
        ["tests/protected/**"],
        worker.name,
        case["pro"].name,
        "a" * 40,
        **packet_kwargs(),
    )
    if assign:
        package = case["service"].assign_work_package(case["glm"], package["id"])
    return package


def _insert_session(
    case: dict[str, Any],
    package: dict[str, Any],
    assigned_actor: ActorIdentity,
    workspace: Path,
) -> int:
    workspace.mkdir(parents=True, exist_ok=True)
    with case["service"].store.transaction() as conn:
        cursor = conn.execute(
            """INSERT INTO mimo_sessions (
                 project_id, task_id, work_package_id, requested_by_agent, assigned_agent,
                 assigned_role, mimo_model, mode, lifecycle_state, workspace_path, briefing_path,
                 command_json, pid, stdout_path, stderr_path, exit_code, failure_reason,
                 created_at, started_at, ended_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                case["project"]["id"],
                case["task"]["id"],
                package["id"],
                case["glm"].name,
                assigned_actor.name,
                assigned_actor.primary_role.value,
                "xiaomi/mimo-v2.5",
                "tui",
                "TUI_DETACHED",
                str(workspace),
                None,
                "[]",
                None,
                None,
                None,
                None,
                None,
                "2026-07-22T00:00:00+00:00",
                "2026-07-22T00:00:00+00:00",
                None,
            ),
        )
        return int(cursor.lastrowid or 0)


def _call_tool(server: Any, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    result = asyncio.run(server.call_tool(name, arguments))
    assert isinstance(result, tuple)
    structured = result[1]
    assert isinstance(structured, dict)
    return structured


def test_gui_neutral_inbox_state_is_shared_between_server_instances(tmp_path: Path) -> None:
    case = _base_case(tmp_path)
    package = _create_package(case)
    src_path = str(Path(__file__).resolve().parents[1] / "src")
    parameters = StdioServerParameters(
        command=sys.executable,
        args=["-m", "grace_orchestrator"],
        cwd=case["repo"],
        env={
            **os.environ,
            "GRACE_ORCHESTRATOR_ACTOR_NAME": case["junior"].name,
            "GRACE_ORCHESTRATOR_ACTOR_ROLE": case["junior"].primary_role.value,
            "GRACE_ORCHESTRATOR_DATA_DIR": str(tmp_path),
            "PYTHONPATH": os.pathsep.join(
                part for part in [src_path, os.environ.get("PYTHONPATH", "")] if part
            ),
        },
    )

    async def exercise_two_gui_backends() -> tuple[dict[str, Any], dict[str, Any]]:
        async with stdio_client(parameters) as (first_read, first_write):
            async with ClientSession(first_read, first_write) as first_gui:
                await first_gui.initialize()
                async with stdio_client(parameters) as (second_read, second_write):
                    async with ClientSession(second_read, second_write) as second_gui:
                        await second_gui.initialize()
                        before_result = await first_gui.call_tool(
                            "inbox.next", {"project_id": case["project"]["id"]}
                        )
                        await first_gui.call_tool(
                            "workpackage.claim", {"work_package_id": package["id"]}
                        )
                        after_result = await second_gui.call_tool(
                            "inbox.next", {"project_id": case["project"]["id"]}
                        )
                        assert isinstance(before_result.structuredContent, dict)
                        assert isinstance(after_result.structuredContent, dict)
                        return before_result.structuredContent, after_result.structuredContent

    before_response, after_response = asyncio.run(exercise_two_gui_backends())

    assert before_response["item"]["next_action"]["tool"] == "workpackage.claim"
    assert after_response["item"]["next_action"]["tool"] == "submission.create"


def test_inbox_item_id_is_stable_across_state_transitions(tmp_path: Path) -> None:
    case = _base_case(tmp_path)
    package = _create_package(case)
    before = case["service"].inbox_next(case["junior"], case["project"]["id"])["item"]

    case["service"].claim_work_package(case["junior"], package["id"])
    after = case["service"].inbox_next(case["junior"], case["project"]["id"])["item"]

    assert before["item_id"] == after["item_id"]


def test_inbox_envelope_never_exceeds_4096_utf8_bytes(tmp_path: Path) -> None:
    long_text = "контекст" * 1200
    case = _base_case(tmp_path, task_objective=long_text)
    _create_package(case, objective=long_text)

    item = case["service"].inbox_next(case["junior"], case["project"]["id"])["item"]
    encoded = json.dumps(item, ensure_ascii=False, sort_keys=True).encode("utf-8")

    assert len(encoded) <= 4096
    assert item["project_id"] == case["project"]["id"]
    assert item["task_id"] == case["task"]["id"]
    assert item["work_package_id"] is not None
    assert item["next_action"]["tool"] == "workpackage.claim"


def _delegated_test_owner_case(tmp_path: Path) -> dict[str, Any]:
    repo = tmp_path / "repo"
    repo.mkdir()
    grace = repo / "grace"
    grace.mkdir()
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    test_owner = _actor("test-owner", OrchestratorRole.TEST_OWNER)
    project = service.init_project(codex, "delegation-project", repo, grace, "main", {})
    service.register_agent(
        codex,
        project["id"],
        test_owner.name,
        test_owner.primary_role,
        [OrchestratorRole.TEST_OWNER, OrchestratorRole.GLM],
    )
    task = service.create_codex_task(
        codex,
        project["id"],
        "Delegated task",
        "must be planned by an effective GLM",
        "role-bound action projection",
        [],
        [],
        [],
        ["src/**"],
        ["tests/**"],
    )
    return {
        "service": service,
        "codex": codex,
        "test_owner": test_owner,
        "project": project,
        "task": task,
    }


def test_inbox_does_not_treat_capability_as_effective_role(tmp_path: Path) -> None:
    case = _delegated_test_owner_case(tmp_path)

    actions = {
        item["next_action"]["tool"]
        for item in case["service"].inbox_list(case["test_owner"], case["project"]["id"])["items"]
    }

    assert "task.plan" not in actions


def test_inbox_excludes_action_after_delegation_expires(tmp_path: Path) -> None:
    case = _delegated_test_owner_case(tmp_path)
    delegation = case["service"].delegate_role(
        case["codex"],
        case["project"]["id"],
        case["task"]["id"],
        OrchestratorRole.GLM,
        case["test_owner"].name,
        OrchestratorRole.GLM,
        "Temporary GLM substitution for contract verification",
        datetime.now(UTC) + timedelta(hours=1),
    )
    active_actions = {
        item["next_action"]["tool"]
        for item in case["service"].inbox_list(case["test_owner"], case["project"]["id"])["items"]
    }
    assert "task.plan" in active_actions

    with case["service"].store.transaction() as conn:
        conn.execute(
            "UPDATE role_delegations SET expires_at = ? WHERE id = ?",
            ((datetime.now(UTC) - timedelta(seconds=1)).isoformat(), delegation["id"]),
        )
    expired_actions = {
        item["next_action"]["tool"]
        for item in case["service"].inbox_list(case["test_owner"], case["project"]["id"])["items"]
    }

    assert "task.plan" not in expired_actions


def test_codex_inbox_does_not_offer_undelegated_glm_action(tmp_path: Path) -> None:
    case = _base_case(tmp_path)
    fresh = case["service"].create_codex_task(
        case["codex"],
        case["project"]["id"],
        "Fresh task",
        "still needs GLM planning",
        "separate authority",
        [],
        [],
        [],
        ["src/**"],
        ["tests/**"],
    )

    actions = [
        item["next_action"]["tool"]
        for item in case["service"].inbox_list(case["codex"], case["project"]["id"])["items"]
        if item["task_id"] == fresh["id"]
    ]

    assert "task.plan" not in actions


@pytest.fixture
def populated_project_case(tmp_path: Path) -> dict[str, Any]:
    case = _base_case(tmp_path)
    package = _create_package(case)
    session_id = _insert_session(case, package, case["junior"], tmp_path / "worker-workspace")
    case["service"].upsert_artifact(
        case["glm"],
        case["project"]["id"],
        case["task"]["id"],
        "requirements",
        "<requirements>private</requirements>",
        "docs/requirements.xml",
    )
    with case["service"].store.transaction() as conn:
        submission_cursor = conn.execute(
            """INSERT INTO submissions (
                 work_package_id, submitted_by_agent, base_commit, head_commit, diff, diff_hash,
                 summary, tests_run_json, files_changed_json, worker_report_json,
                 worker_report_validation_json, risk_notes, created_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                package["id"],
                case["junior"].name,
                "a" * 40,
                "b" * 40,
                "diff --git a/src/a.py b/src/a.py",
                "f" * 64,
                "private submission",
                "[]",
                '["src/a.py"]',
                "{}",
                "{}",
                "none",
                "2026-07-22T00:00:00+00:00",
            ),
        )
        review_cursor = conn.execute(
            """INSERT INTO reviews (
                 target_type, target_id, reviewer_role, reviewer_agent, effective_role,
                 decision, findings_json, required_fixes_json, created_at
               ) VALUES ('task', ?, 'glm', ?, 'glm', 'accepted', '[]', '[]', ?)""",
            (case["task"]["id"], case["glm"].name, "2026-07-22T00:00:00+00:00"),
        )
    case.update(
        {
            "package": package,
            "session_id": session_id,
            "submission_id": int(submission_cursor.lastrowid or 0),
            "review_id": int(review_cursor.lastrowid or 0),
        }
    )
    yield case
    case["service"].close()


@pytest.mark.parametrize(
    "surface",
    [
        "task_tool",
        "task_next_action_tool",
        "session_tool",
        "project_resource",
        "project_active_resource",
        "task_resource",
        "task_summary_resource",
        "work_package_resource",
        "work_package_summary_resource",
        "session_resource",
        "review_resource",
        "submission_resource",
        "grace_resource",
    ],
)
def test_unregistered_glm_cannot_read_project_objects_over_public_mcp(
    populated_project_case: dict[str, Any],
    surface: str,
) -> None:
    case = populated_project_case
    outsider = _actor("glm-unregistered", OrchestratorRole.GLM)
    server = create_server(outsider, case["root"])

    if surface == "task_tool":
        with pytest.raises(ToolError):
            asyncio.run(server.call_tool("task.get", {"task_id": case["task"]["id"]}))
    elif surface == "task_next_action_tool":
        with pytest.raises(ToolError):
            asyncio.run(server.call_tool("task.get_next_action", {"task_id": case["task"]["id"]}))
    elif surface == "session_tool":
        with pytest.raises(ToolError):
            asyncio.run(server.call_tool("mimo.get_session", {"session_id": case["session_id"]}))
    elif surface == "project_resource":
        with pytest.raises(ValueError, match="authorized|authorization|requires role"):
            asyncio.run(server.read_resource(f"orchestrator://project/{case['project']['id']}"))
    elif surface == "project_active_resource":
        with pytest.raises(ValueError, match="authorized|authorization|requires role"):
            asyncio.run(server.read_resource(f"orchestrator://project/{case['project']['id']}/active"))
    elif surface == "task_resource":
        with pytest.raises(ValueError, match="authorized|authorization|requires role"):
            asyncio.run(server.read_resource(f"orchestrator://task/{case['task']['id']}"))
    elif surface == "task_summary_resource":
        with pytest.raises(ValueError, match="authorized|authorization|requires role"):
            asyncio.run(server.read_resource(f"orchestrator://task/{case['task']['id']}/summary"))
    elif surface == "work_package_resource":
        with pytest.raises(ValueError, match="authorized|authorization|requires role"):
            asyncio.run(server.read_resource(f"orchestrator://workpackage/{case['package']['id']}"))
    elif surface == "work_package_summary_resource":
        with pytest.raises(ValueError, match="authorized|authorization|requires role"):
            asyncio.run(server.read_resource(f"orchestrator://workpackage/{case['package']['id']}/summary"))
    elif surface == "session_resource":
        with pytest.raises(ValueError, match="authorized|authorization|requires role"):
            asyncio.run(server.read_resource(f"orchestrator://mimo-session/{case['session_id']}"))
    elif surface == "review_resource":
        with pytest.raises(ValueError, match="authorized|authorization|requires role"):
            asyncio.run(server.read_resource(f"orchestrator://review/{case['review_id']}"))
    elif surface == "submission_resource":
        with pytest.raises(ValueError, match="authorized|authorization|requires role"):
            asyncio.run(server.read_resource(f"orchestrator://submission/{case['submission_id']}"))
    else:
        with pytest.raises(ValueError, match="authorized|authorization|requires role"):
            asyncio.run(server.read_resource(f"grace://project/{case['project']['id']}/requirements.xml"))


def test_project_snapshot_never_contains_foreign_project_audit_events(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    glm = _actor("glm-project-one", OrchestratorRole.GLM)
    repo_one = tmp_path / "repo-one"
    repo_two = tmp_path / "repo-two"
    repo_one.mkdir()
    repo_two.mkdir()
    project_one = service.init_project(codex, "one", repo_one, repo_one, "main", {})
    service.register_agent(codex, project_one["id"], glm.name, glm.primary_role, [glm.primary_role])
    project_two = service.init_project(codex, "two", repo_two, repo_two, "main", {})
    foreign_task = service.create_codex_task(
        codex,
        project_two["id"],
        "foreign task",
        "must remain private",
        "project isolation",
        [],
        [],
        [],
        ["src/**"],
        ["tests/**"],
    )

    snapshot = service.get_orchestrator_status_snapshot(glm, project_one["id"])

    assert not any(
        event["target_type"] == "task" and event["target_id"] == foreign_task["id"]
        for event in snapshot["recent_audit_events"]
    )


def test_worker_task_summary_ignores_inaccessible_sibling_sessions(tmp_path: Path) -> None:
    case = _base_case(tmp_path)
    sibling = _actor("worker-sibling", OrchestratorRole.WORKER_JUNIOR)
    case["service"].register_agent(
        case["codex"],
        case["project"]["id"],
        sibling.name,
        sibling.primary_role,
        [sibling.primary_role],
        mimo_model="xiaomi/mimo-v2.5",
    )
    own_package = _create_package(case, title="Own package", allowed_files=["src/own/**"], assign=False)
    sibling_package = _create_package(
        case,
        title="Sibling package",
        junior=sibling,
        allowed_files=["src/sibling/**"],
        assign=False,
    )
    case["service"].assign_work_package(case["glm"], own_package["id"])
    case["service"].assign_work_package(case["glm"], sibling_package["id"])
    _insert_session(case, sibling_package, sibling, tmp_path / "sibling-workspace")

    summary = case["service"].get_task_summary(case["junior"], case["task"]["id"])

    assert summary["id"] == case["task"]["id"]
    assert summary["package_counts"]["total"] == 2


def test_registered_test_root_is_server_selected(tmp_path: Path) -> None:
    case = _base_case(tmp_path)
    package = _create_package(case)

    repo_result = case["service"].run_allowed_test(
        case["junior"],
        case["project"]["id"],
        case["task"]["id"],
        package["id"],
        "unit",
    )
    assert Path(repo_result.stdout_path).read_text(encoding="utf-8").strip() == str(case["repo"].resolve())

    workspace = tmp_path / "selected-workspace"
    _insert_session(case, package, case["junior"], workspace)
    workspace_result = case["service"].run_allowed_test(
        case["junior"],
        case["project"]["id"],
        case["task"]["id"],
        package["id"],
        "unit",
    )
    assert Path(workspace_result.stdout_path).read_text(encoding="utf-8").strip() == str(workspace.resolve())


def test_public_mcp_handoff_cursor_round_trips_as_string(tmp_path: Path) -> None:
    case = _base_case(tmp_path)
    package = _create_package(case, assign=False)
    first = case["service"]._append_handoff_event(
        case["project"]["id"],
        case["task"]["id"],
        package["id"],
        "WORKER_STARTED",
        case["junior"].name,
        {"step": 1},
    )
    second = case["service"]._append_handoff_event(
        case["project"]["id"],
        case["task"]["id"],
        package["id"],
        "WORKER_BLOCKED",
        case["junior"].name,
        {"step": 2},
    )
    server = create_server(case["glm"], tmp_path)

    first_page = _call_tool(
        server,
        "handoff.list_events_page",
        {"work_package_id": package["id"], "limit": 1},
    )
    second_page = _call_tool(
        server,
        "handoff.list_events_page",
        {
            "work_package_id": package["id"],
            "after_event_id": first_page["next_after_id"],
            "limit": 1,
        },
    )

    assert first_page["items"][0]["event_id"] == first["event_id"]
    assert isinstance(first_page["next_after_id"], str)
    assert second_page["items"][0]["event_id"] == second["event_id"]
