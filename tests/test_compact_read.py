"""Tests for compact read tools, pagination, and centralized next action classification."""


from conftest import packet_kwargs
from grace_orchestrator.models import ActorIdentity, OrchestratorRole
from grace_orchestrator.policy import project_next_action
from grace_orchestrator.service import OrchestratorService


def test_project_next_action_classifier():
    # CODEX_TASK_CREATED -> task.plan
    res1 = project_next_action("CODEX_TASK_CREATED")
    assert res1["action"] == "task.plan"
    assert res1["role"] == "glm"

    # Blocked package -> force_reset or force_transition
    res2 = project_next_action(
        "WORK_PACKAGES_CREATED",
        [{"id": 10, "status": "HUMAN_INTERVENTION_REQUIRED"}],
    )
    assert "force_reset" in res2["action"]
    assert res2["role"] == "user_or_codex"

    # CREATED package -> workpackage.assign
    res3 = project_next_action(
        "WORK_PACKAGES_CREATED",
        [{"id": 12, "status": "CREATED"}],
    )
    assert "workpackage.assign" in res3["action"]
    assert res3["role"] == "glm"


def test_get_task_and_package_summaries(tmp_path):
    service = OrchestratorService(tmp_path / "data")
    codex = ActorIdentity(name="codex", primary_role=OrchestratorRole.CODEX)
    glm = ActorIdentity(name="glm", primary_role=OrchestratorRole.GLM)

    repo_dir = tmp_path / "repo"
    repo_dir.mkdir(parents=True, exist_ok=True)

    proj = service.init_project(codex, "summary-proj", repo_dir, repo_dir, "main", {"fast": ["pytest"]})
    task = service.create_codex_task(
        codex, proj["id"], "Summary Task", "obj", "intent", [], [], ["criteria"], ["src/**"], []
    )
    service.plan_task(glm, task["id"])
    service.register_verification_plan(glm, task["id"], "automated", ["fast"])

    service.register_agent(codex, proj["id"], "j1", OrchestratorRole.WORKER_JUNIOR, [OrchestratorRole.WORKER_JUNIOR], mimo_model="xiaomi/mimo-v2.5")
    service.register_agent(codex, proj["id"], "p1", OrchestratorRole.WORKER_PRO, [OrchestratorRole.WORKER_PRO], mimo_model="xiaomi/mimo-v2.5-pro")

    pkg = service.create_work_package(
        glm, task["id"], "Pkg 1", "obj", ["src/**"], ["tests/**"], "j1", "p1", "a" * 40, **packet_kwargs()
    )

    t_summary = service.get_task_summary(task["id"])
    assert t_summary["package_counts"]["total"] == 1
    assert t_summary["package_counts"]["active"] == 1
    assert t_summary["active_package_ids"] == [pkg["id"]]
    assert t_summary["next_action"]["action"].startswith("workpackage.assign")

    pkg_summary = service.get_work_package_summary(pkg["id"])
    assert pkg_summary["id"] == pkg["id"]
    assert pkg_summary["title"] == "Pkg 1"


def test_paginated_handoff_and_audit_log(tmp_path):
    service = OrchestratorService(tmp_path / "data")
    codex = ActorIdentity(name="codex", primary_role=OrchestratorRole.CODEX)
    glm = ActorIdentity(name="glm", primary_role=OrchestratorRole.GLM)
    junior = ActorIdentity(name="j1", primary_role=OrchestratorRole.WORKER_JUNIOR)

    repo_dir = tmp_path / "repo"
    repo_dir.mkdir(parents=True, exist_ok=True)

    proj = service.init_project(codex, "page-proj", repo_dir, repo_dir, "main", {"fast": ["pytest"]})
    task = service.create_codex_task(
        codex, proj["id"], "Page Task", "obj", "intent", [], [], ["criteria"], ["src/**"], []
    )
    service.plan_task(glm, task["id"])
    service.register_verification_plan(glm, task["id"], "automated", ["fast"])

    service.register_agent(codex, proj["id"], "j1", OrchestratorRole.WORKER_JUNIOR, [OrchestratorRole.WORKER_JUNIOR], mimo_model="xiaomi/mimo-v2.5")
    service.register_agent(codex, proj["id"], "p1", OrchestratorRole.WORKER_PRO, [OrchestratorRole.WORKER_PRO], mimo_model="xiaomi/mimo-v2.5-pro")

    pkg = service.create_work_package(
        glm, task["id"], "Pkg 1", "obj", ["src/**"], ["tests/**"], "j1", "p1", "a" * 40, **packet_kwargs()
    )
    service.assign_work_package(glm, pkg["id"])
    service.claim_work_package(junior, pkg["id"])

    # Report 5 events
    for i in range(5):
        service.report_worker_handoff_event(junior, pkg["id"], "WORKER_BLOCKED", f"step {i}")

    page1 = service.list_handoff_events_page(pkg["id"], after_event_id=0, limit=2)
    assert len(page1["items"]) == 2
    assert page1["has_more"] is True
    assert page1["next_after_id"] is not None

    page2 = service.list_handoff_events_page(pkg["id"], after_event_id=page1["next_after_id"], limit=5)
    assert len(page2["items"]) == 3
    assert page2["has_more"] is False

    # Test audit pagination
    audit_page = service.list_audit_page(task_id=task["id"], after_audit_id=0, limit=3)
    assert len(audit_page["items"]) == 3
    assert audit_page["has_more"] is True
