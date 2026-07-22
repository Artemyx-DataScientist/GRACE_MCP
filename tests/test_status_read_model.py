"""Tests for Orchestrator Status Read-Model and CLI Dashboard."""

from conftest import packet_kwargs
from grace_orchestrator.cli_dashboard import render_ascii_tree
from grace_orchestrator.models import ActorIdentity, OrchestratorRole
from grace_orchestrator.service import OrchestratorService


def test_status_snapshot_empty_db(tmp_path):
    service = OrchestratorService(tmp_path / "data")
    snapshot = service.get_orchestrator_status_snapshot()

    assert snapshot["projects_count"] == 0
    assert snapshot["projects"] == []

    tree = render_ascii_tree(snapshot)
    assert "No registered projects found" in tree


def test_status_snapshot_with_data(tmp_path):
    service = OrchestratorService(tmp_path / "data")
    codex_actor = ActorIdentity(name="codex-1", primary_role=OrchestratorRole.CODEX)
    glm_actor = ActorIdentity(name="glm-1", primary_role=OrchestratorRole.GLM)

    repo_dir = tmp_path / "repo"
    repo_dir.mkdir(parents=True, exist_ok=True)

    proj = service.init_project(
        codex_actor,
        name="test-dashboard-proj",
        repo_path=repo_dir,
        grace_path=repo_dir,
        main_branch="main",
        allowed_test_commands={"fast": ["pytest"]},
    )
    service.register_agent(
        codex_actor,
        proj["id"],
        name="glm-1",
        primary_role=OrchestratorRole.GLM,
        capabilities=[OrchestratorRole.GLM],
    )

    task = service.create_codex_task(
        codex_actor,
        proj["id"],
        title="dashboard-task",
        objective="test status dashboard",
        architecture_intent="dashboard read-model",
        constraints=["unit-test"],
        non_goals=[],
        acceptance_criteria=["dashboard works"],
        allowed_files=["src/**"],
        forbidden_files=[],
    )

    service.plan_task(glm_actor, task["id"])
    service.register_verification_plan(
        glm_actor,
        task_id=task["id"],
        test_strategy="automated",
        test_commands=["fast"],
    )

    service.register_agent(
        codex_actor,
        proj["id"],
        name="junior-1",
        primary_role=OrchestratorRole.WORKER_JUNIOR,
        capabilities=[OrchestratorRole.WORKER_JUNIOR],
        mimo_model="xiaomi/mimo-v2.5",
    )
    service.register_agent(
        codex_actor,
        proj["id"],
        name="pro-1",
        primary_role=OrchestratorRole.WORKER_PRO,
        capabilities=[OrchestratorRole.WORKER_PRO],
        mimo_model="xiaomi/mimo-v2.5-pro",
    )

    service.create_work_package(
        glm_actor,
        task["id"],
        title="Dashboard Package",
        objective="implement read-model",
        allowed_files=["src/**"],
        forbidden_files=["tests/**"],
        assigned_junior_agent="junior-1",
        assigned_pro_agent="pro-1",
        base_commit="a" * 40,
        **packet_kwargs(),
    )

    snapshot = service.get_orchestrator_status_snapshot(project_id=proj["id"])

    assert snapshot["projects_count"] == 1
    assert snapshot["projects"][0]["name"] == "test-dashboard-proj"
    assert len(snapshot["projects"][0]["tasks"]) == 1
    assert snapshot["projects"][0]["tasks"][0]["title"] == "dashboard-task"

    # Test ASCII tree rendering
    tree_ascii = render_ascii_tree(snapshot, use_ascii=True)
    assert "+--" in tree_ascii or "|--" in tree_ascii
    assert "test-dashboard-proj" in tree_ascii
    assert "dashboard-task" in tree_ascii
    assert "Dashboard Package" in tree_ascii
