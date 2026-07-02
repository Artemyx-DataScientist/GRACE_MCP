from pathlib import Path
import threading

import pytest

# FILE: tests/test_hooks.py
# VERSION: 0.3.0
# START_MODULE_CONTRACT
#   PURPOSE: Verify M-ORCH-HOOKS transactional audit, scope, repair, final-gate, and auto-close behavior.
#   SCOPE: Trusted named hook effects only; no external process or provider call.
#   DEPENDS: M-ORCH-DOMAIN, M-ORCH-HOOKS, M-ORCH-LEDGER, M-ORCH-REPO-BOUNDARY
#   LINKS: M-ORCH-HOOKS, V-M-ORCH-HOOKS
#   ROLE: TEST
#   MAP_MODE: LOCALS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   _ready_package - creates a single assigned package under distinct Codex/GLM/worker identities.
#   _submit - produces deterministic server-shaped submission evidence.
#   _upsert_required_artifacts - supplies canonical final-gate artifacts.
#   test_* - named hook effects and failure rollback evidence.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.3.1 - Cover ten-minute handoff wait bound.
# END_CHANGE_SUMMARY

from grace_orchestrator.hooks import HookEvent, HookRegistry
from grace_orchestrator.models import (
    ActorIdentity,
    OrchestratorError,
    OrchestratorRole,
    SubmissionEvidence,
    TaskStatus,
)
from grace_orchestrator.service import GRACE_ARTIFACT_PATHS, OrchestratorService
from conftest import packet_kwargs, worker_report


def _actor(name: str, role: OrchestratorRole) -> ActorIdentity:
    return ActorIdentity(name=name, primary_role=role)


def _ready_package(tmp_path: Path) -> tuple[OrchestratorService, dict[str, object], dict[str, object], ActorIdentity, ActorIdentity, ActorIdentity, ActorIdentity]:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    glm = _actor("glm", OrchestratorRole.GLM)
    junior = _actor("mimo-junior", OrchestratorRole.WORKER_JUNIOR)
    pro = _actor("mimo-pro", OrchestratorRole.WORKER_PRO)
    (tmp_path / "grace").mkdir()
    project = service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {"unit": ["python", "-m", "pytest"]})
    service.register_agent(codex, project["id"], glm.name, OrchestratorRole.GLM, [OrchestratorRole.GLM])
    service.register_agent(codex, project["id"], junior.name, OrchestratorRole.WORKER_JUNIOR, [OrchestratorRole.WORKER_JUNIOR])
    service.register_agent(codex, project["id"], pro.name, OrchestratorRole.WORKER_PRO, [OrchestratorRole.WORKER_PRO])
    task = service.create_codex_task(
        codex,
        project["id"],
        "hook task",
        "exercise hook policy",
        "ledger owns acceptance",
        ["no shell"],
        ["no client promotion"],
        ["hook evidence"],
        ["src/**"],
        ["tests/**"],
    )
    service.plan_task(glm, task["id"])
    service.register_verification_plan(glm, task["id"], "deterministic", ["unit"])
    package = service.create_work_package(
        glm,
        task["id"],
        "package",
        "bounded work",
        ["src/**"],
        [],
        junior.name,
        pro.name,
        "a" * 40,
        **packet_kwargs(),
    )
    service.assign_work_package(glm, package["id"])
    return service, project, task, codex, glm, junior, pro


def _submit(service: OrchestratorService, actor: ActorIdentity, package_id: int, head: str) -> dict[str, object]:
    return service.submit_package(
        actor,
        package_id,
        "bounded implementation",
        SubmissionEvidence(
            base_commit="a" * 40,
            head_commit=head * 40,
            diff="diff --git a/src/hook.py b/src/hook.py",
            diff_hash="f" * 64,
            files_changed=["src/hook.py"],
        ),
        [{"command_key": "unit", "exit_code": 0}],
        "none",
        worker_report=worker_report(
            task_id=service.get_work_package(package_id)["task_id"],
            package_id=package_id,
            files_changed=["src/hook.py"],
        ),
    )


def _upsert_required_artifacts(
    service: OrchestratorService,
    glm: ActorIdentity,
    project_id: int,
    task_id: int,
) -> None:
    for artifact_type, filename in {
        "requirements": "requirements.xml",
        "technology": "technology.xml",
        "development_plan": "development-plan.xml",
        "verification_plan": "verification-plan.xml",
        "knowledge_graph": "knowledge-graph.xml",
        "operational_packets": "operational-packets.xml",
    }.items():
        service.upsert_artifact(
            glm,
            project_id,
            task_id,
            artifact_type,
            f"<{artifact_type} />",
            f"grace/{filename}",
        )


def _accepted_task(
    tmp_path: Path,
    *,
    artifacts: bool,
) -> tuple[OrchestratorService, dict[str, object], dict[str, object], ActorIdentity, ActorIdentity]:
    service, project, task, codex, glm, junior, pro = _ready_package(tmp_path)
    package = service.get_task(task["id"])["work_packages"][0]
    service.claim_work_package(junior, package["id"])
    _submit(service, junior, package["id"], "b")
    service.review_package(glm, package["id"], "rejected_repair_required", ["repair"], ["repair"])
    rejected_package = service.get_work_package(package["id"])
    assert rejected_package["worker_pro_available"] is True
    service.claim_work_package(pro, package["id"])
    _submit(service, pro, package["id"], "c")
    service.review_package(glm, package["id"], "accepted", [], [])
    if artifacts:
        _upsert_required_artifacts(service, glm, project["id"], task["id"])
    return service, project, task, codex, glm


def test_named_hooks_audit_scope_enable_repair_and_auto_close(tmp_path: Path) -> None:
    service, project, task, codex, glm = _accepted_task(tmp_path, artifacts=True)

    service.request_final_review(codex, task["id"])
    service.final_review(codex, task["id"], "accepted", [], [])

    completed = service.get_task(task["id"])
    events = {event["event_type"] for event in service.list_audit(task_id=task["id"])}
    assert completed["status"] == TaskStatus.TASK_CLOSED.value
    assert {
        "hook.on_task_created",
        "hook.on_grace_artifact_upserted",
        "hook.on_workpackage_created",
        "hook.on_submission_created",
        "hook.on_glm_rejected",
        "hook.on_glm_accepted",
        "hook.on_codex_accepted",
        "hook.gate.promote",
        "task.closed_by_hook",
    } <= events
    assert service.close_task(codex, task["id"])["status"] == TaskStatus.TASK_CLOSED.value
    assert project["id"] == completed["project_id"]


def test_final_gate_rejects_missing_artifacts_without_promoting_task(tmp_path: Path) -> None:
    service, _project, task, codex, _glm = _accepted_task(tmp_path, artifacts=False)

    with pytest.raises(OrchestratorError, match="requires GRACE artifacts"):
        service.request_final_review(codex, task["id"])

    assert service.get_task(task["id"])["status"] == TaskStatus.GLM_ACCEPTED.value


def test_final_gate_auto_imports_existing_canonical_grace_docs(tmp_path: Path) -> None:
    service, project, task, codex, _glm = _accepted_task(tmp_path, artifacts=False)
    repo_root = Path(str(project["repo_path"]))
    for artifact_type, relative_path in GRACE_ARTIFACT_PATHS.items():
        artifact_path = repo_root / relative_path
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(f"<{artifact_type} />", encoding="utf-8")

    reviewed = service.request_final_review(codex, task["id"])

    assert reviewed["status"] == TaskStatus.CODEX_FINAL_REVIEW.value
    artifacts = service.get_task(task["id"])["grace_artifacts"]
    assert {artifact["artifact_type"] for artifact in artifacts} == set(GRACE_ARTIFACT_PATHS)
    assert {artifact["created_by_agent"] for artifact in artifacts} == {"system:auto-import"}


def test_submission_and_controller_review_materialize_closed_handoff_events(tmp_path: Path) -> None:
    service, project, task, codex, glm, junior, _pro = _ready_package(tmp_path)
    package = service.get_task(task["id"])["work_packages"][0]
    service.claim_work_package(junior, package["id"])

    submission = _submit(service, junior, package["id"], "b")

    assert submission["handoff_event"]["type"] == "WORKER_READY_FOR_REVIEW"
    assert Path(submission["handoff_report_path"]).is_file()
    assert service.list_handoff_events(codex, package["id"])[0]["type"] == "WORKER_READY_FOR_REVIEW"

    review = service.review_package(glm, package["id"], "rejected_repair_required", ["root mismatch"], ["repair browse root"])

    assert review["handoff_event"]["type"] == "CONTROLLER_REWORK_REQUESTED"
    assert [event["type"] for event in service.list_handoff_events(codex, package["id"])] == [
        "WORKER_READY_FOR_REVIEW",
        "CONTROLLER_REWORK_REQUESTED",
    ]
    assert project["id"] == task["project_id"]


def test_controller_wait_is_woken_by_worker_ready_for_review(tmp_path: Path) -> None:
    service, _project, task, codex, _glm, junior, _pro = _ready_package(tmp_path)
    package = service.get_task(task["id"])["work_packages"][0]
    service.claim_work_package(junior, package["id"])
    result: dict[str, object] = {}
    entered_wait = threading.Event()

    def wait_for_worker() -> None:
        entered_wait.set()
        result.update(service.wait_for_handoff_event(codex, package["id"], after_event_count=0, timeout_seconds=2))

    thread = threading.Thread(target=wait_for_worker)
    thread.start()
    assert entered_wait.wait(timeout=1)
    _submit(service, junior, package["id"], "b")
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert result["status"] == "event"
    assert [event["type"] for event in result["events"]] == ["WORKER_READY_FOR_REVIEW"]


def test_controller_wait_accepts_ten_minute_bound_without_sleeping(tmp_path: Path) -> None:
    service, _project, task, codex, _glm, junior, _pro = _ready_package(tmp_path)
    package = service.get_task(task["id"])["work_packages"][0]
    service.claim_work_package(junior, package["id"])
    _submit(service, junior, package["id"], "b")

    result = service.wait_for_handoff_event(codex, package["id"], after_event_count=0, timeout_seconds=600)

    assert result["status"] == "event"
    with pytest.raises(OrchestratorError, match="between 1 and 600"):
        service.wait_for_handoff_event(codex, package["id"], after_event_count=1, timeout_seconds=601)


def test_controller_wait_returns_structured_timeout(tmp_path: Path) -> None:
    service, _project, task, codex, _glm, _junior, _pro = _ready_package(tmp_path)
    package = service.get_task(task["id"])["work_packages"][0]

    result = service.wait_for_handoff_event(codex, package["id"], after_event_count=0, timeout_seconds=1)

    assert result["status"] == "timeout"
    assert result["event_count"] == 0
    assert result["events"] == []


def test_scope_hook_rolls_back_a_task_with_path_escape(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    project = service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {})

    with pytest.raises(OrchestratorError, match="scope pattern is not project-relative"):
        service.create_codex_task(
            codex,
            project["id"],
            "bad scope",
            "must roll back",
            "none",
            [],
            [],
            [],
            ["../outside/**"],
            [],
        )

    assert service.store.connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 0


def test_codex_rejection_hook_keeps_task_open_for_repair(tmp_path: Path) -> None:
    service, _project, task, codex, _glm = _accepted_task(tmp_path, artifacts=True)
    service.request_final_review(codex, task["id"])

    service.final_review(codex, task["id"], "rejected_repair_required", ["repair"], ["repair"])

    task_after = service.get_task(task["id"])
    events = {event["event_type"] for event in service.list_audit(task_id=task["id"])}
    assert task_after["status"] == TaskStatus.CODEX_REJECTED_REPAIR_REQUIRED.value
    assert "hook.on_codex_rejected" in events
    assert "task.closed_by_hook" not in events


def test_registry_rejects_undocumented_event_registration() -> None:
    registry = HookRegistry()

    with pytest.raises(OrchestratorError, match="documented HookEvent"):
        registry.register("arbitrary.shell", lambda _context: None)  # type: ignore[arg-type]

    assert HookEvent.GATE_PROMOTED.value == "gate.promote"
