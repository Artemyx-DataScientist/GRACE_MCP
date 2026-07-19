from os import name as os_name
from pathlib import Path
import subprocess
import sys

import pytest

# FILE: tests/test_mimo_bridge.py
# VERSION: 0.2.0
# START_MODULE_CONTRACT
#   PURPOSE: Verify isolated Mimo dispatch, role-bound connection profiles, and non-shell launch evidence.
#   SCOPE: Detached worktree creation, briefing/session records, process observation, and missing-model rejection.
#   DEPENDS: M-ORCH-DOMAIN, M-ORCH-REPO-BOUNDARY, M-ORCH-MIMO-EXECUTOR, M-ORCH-LEDGER
#   LINKS: M-ORCH-MIMO-EXECUTOR, V-M-ORCH-MIMO-EXECUTOR
#   ROLE: TEST
#   MAP_MODE: LOCALS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   FakeMimoRunner - deterministic Mimo process boundary fake.
#   test_* - isolated workspace, role profile, lifecycle, and missing-model coverage.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.2.7 - Cover bounded worker-report preflight for the claimed shared Codex actor.
# END_CHANGE_SUMMARY

from grace_orchestrator.mimo import MimoLaunchResult, MimoRunner
from grace_orchestrator.models import (
    ActorIdentity,
    MimoLaunchMode,
    MimoSessionStatus,
    OrchestratorError,
    OrchestratorRole,
    TaskStatus,
    WorkPackageStatus,
)
from grace_orchestrator.repo import RepositoryBoundary
from grace_orchestrator.service import OrchestratorService
from conftest import packet_kwargs, worker_report


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout.strip()


class FakeMimoRunner:
    """Records fixed-argv requests; it never contacts a model provider in a deterministic test."""

    def __init__(self) -> None:
        self.launches: list[dict[str, object]] = []
        self.exit_code: int | None = None

    def launch(
        self,
        *,
        session_id: int,
        mode: MimoLaunchMode,
        model: str,
        agent: str | None = None,
        workspace_path: Path,
        briefing_path: Path,
    ) -> MimoLaunchResult:
        self.launches.append(
            {
                "session_id": session_id,
                "mode": mode,
                "model": model,
                "agent": agent,
                "workspace_path": workspace_path,
                "briefing_path": briefing_path,
            }
        )
        return MimoLaunchResult(
            argv=["mimo", "run", "--agent", agent or "", "--model", model, "--file", str(briefing_path)],
            pid=4242,
            stdout_path=None,
            stderr_path=None,
            detached_tui=mode == MimoLaunchMode.TUI,
        )

    def poll(self, session_id: int) -> int | None:
        return self.exit_code

    def cancel(self, session_id: int) -> int:
        return 143


def _actor(name: str, role: OrchestratorRole) -> ActorIdentity:
    return ActorIdentity(name=name, primary_role=role)


def _ready_service(tmp_path: Path, *, junior_model: str | None = "xiaomi/mimo-v2.5") -> tuple[OrchestratorService, FakeMimoRunner, ActorIdentity, dict[str, object], dict[str, object]]:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "docs").mkdir()
    (repo / "src").mkdir()
    (repo / "src" / "worker.py").write_text("value = 1\n", encoding="utf-8")
    _git(repo, "init")
    _git(repo, "config", "user.email", "orchestrator@example.invalid")
    _git(repo, "config", "user.name", "Orchestrator Test")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "base")
    base_commit = _git(repo, "rev-parse", "HEAD")

    runner = FakeMimoRunner()
    service = OrchestratorService(tmp_path / "state" / "ledger.sqlite3", mimo_runner=runner)  # type: ignore[arg-type]
    codex = _actor("codex", OrchestratorRole.CODEX)
    glm = _actor("glm-5.2", OrchestratorRole.GLM)
    project = service.init_project(codex, "demo", repo, repo / "docs", "main", {"unit": [sys.executable, "-c", "print('ok')"]})
    service.register_agent(codex, project["id"], glm.name, OrchestratorRole.GLM, [OrchestratorRole.GLM, OrchestratorRole.TEST_OWNER], mimo_model="zai-coding-plan/glm-5.2")
    service.register_agent(codex, project["id"], "mimo-2.5", OrchestratorRole.WORKER_JUNIOR, [OrchestratorRole.WORKER_JUNIOR], mimo_model=junior_model)
    service.register_agent(codex, project["id"], "mimo-2.5-pro", OrchestratorRole.WORKER_PRO, [OrchestratorRole.WORKER_PRO], mimo_model="xiaomi/mimo-v2.5-pro")
    task = service.create_codex_task(codex, project["id"], "Mimo bridge", "dispatch isolated work", "ledger owns state", ["test-first"], ["automatic acceptance"], ["audited session"], ["src/**"], ["tests/**"])
    service.plan_task(glm, task["id"])
    service.register_verification_plan(glm, task["id"], "deterministic", ["unit"])
    package = service.create_work_package(
        glm,
        task["id"],
        "worker",
        "implement bounded change",
        ["src/**"],
        [],
        "mimo-2.5",
        "mimo-2.5-pro",
        base_commit,
        **packet_kwargs(module_id="M-ORCH-MIMO-EXECUTOR", verification_id="V-M-ORCH-MIMO-EXECUTOR"),
    )
    service.assign_work_package(glm, package["id"])
    return service, runner, glm, task, package


def test_mimo_dispatch_creates_isolated_worktree_and_audited_briefing(tmp_path: Path) -> None:
    service, runner, glm, task, package = _ready_service(tmp_path)

    session = service.launch_mimo_session(glm, package["id"], MimoLaunchMode.HEADLESS)

    workspace = Path(session["workspace_path"])
    briefing = Path(session["briefing_path"])
    assert session["lifecycle_state"] == MimoSessionStatus.RUNNING.value
    assert workspace != Path(service.get_project(task["project_id"])["repo_path"])
    assert (workspace / "src" / "worker.py").read_text(encoding="utf-8") == "value = 1\n"
    assert "Work-package id" in briefing.read_text(encoding="utf-8")
    assert runner.launches[0]["model"] == "xiaomi/mimo-v2.5"
    assert runner.launches[0]["agent"] == "build"
    assert session["mimo_agent"] == "build"
    assert session["handoff_event"]["type"] == "WORKER_STARTED"
    assert any(event["event_type"] == "mimo.session_launched" for event in service.list_audit(task_id=task["id"]))

    runner.exit_code = 0
    observed = service.poll_mimo_session(glm, session["id"])
    assert observed["lifecycle_state"] == MimoSessionStatus.EXITED.value
    assert observed["exit_code"] == 0


def test_work_package_creation_rejects_missing_registered_model_before_dispatch(tmp_path: Path) -> None:
    with pytest.raises(OrchestratorError, match="explicit provider/model"):
        _ready_service(tmp_path, junior_model=None)


def test_model_less_shared_codex_actor_can_receive_and_claim_package_without_mimo_launch(
    tmp_path: Path,
) -> None:
    service, runner, glm, task, old_package = _ready_service(tmp_path)
    codex = _actor("codex", OrchestratorRole.CODEX)
    service.cancel_work_package(glm, old_package["id"], "replace MiMo route with shared Codex")
    service.register_agent(
        codex,
        task["project_id"],
        codex.name,
        OrchestratorRole.CODEX,
        [
            OrchestratorRole.CODEX,
            OrchestratorRole.TEST_OWNER,
            OrchestratorRole.WORKER_JUNIOR,
            OrchestratorRole.WORKER_PRO,
        ],
    )

    package = service.create_work_package(
        glm,
        task["id"],
        "shared Codex worker",
        "execute bounded change in the current Terra or Luna conversation",
        ["src/**"],
        [],
        codex.name,
        codex.name,
        old_package["base_commit"],
        **packet_kwargs(
            module_id="M-ORCH-MIMO-EXECUTOR",
            verification_id="V-M-ORCH-MIMO-EXECUTOR",
        ),
    )
    assigned = service.assign_work_package(glm, package["id"])

    with pytest.raises(OrchestratorError, match="must never be launched through MiMo"):
        service.launch_mimo_session(glm, package["id"], MimoLaunchMode.TUI)

    claimed = service.claim_work_package(codex, package["id"])
    report_gate = service.validate_worker_report(
        codex,
        package["id"],
        worker_report(
            task_id=task["id"],
            package_id=package["id"],
            files_changed=["src/worker.py"],
            module_id="M-ORCH-MIMO-EXECUTOR",
        ),
        evidence_files=["src/worker.py"],
    )

    assert assigned["assigned_junior_agent"] == codex.name
    assert assigned["assigned_pro_agent"] == codex.name
    assert claimed["status"] == WorkPackageStatus.CLAIMED_JUNIOR.value
    assert claimed["claimed_by_agent"] == codex.name
    assert report_gate["status"] == "pass"
    assert runner.launches == []


def test_zai_glm_flash_worker_backend_dispatches_without_glm_planner_authority(tmp_path: Path) -> None:
    service, runner, glm, task, package = _ready_service(tmp_path, junior_model="zai/glm-4.7-flash")
    codex = _actor("codex", OrchestratorRole.CODEX)

    profile = service.mimo_connection_profile(codex, task["project_id"], "mimo-2.5")
    session = service.launch_mimo_session(glm, package["id"], MimoLaunchMode.HEADLESS)

    assert profile["backend_family"] == "glm_worker"
    assert profile["mimo_model"] == "zai/glm-4.7-flash"
    assert session["assigned_role"] == OrchestratorRole.WORKER_JUNIOR.value
    assert session["mimo_model"] == "zai/glm-4.7-flash"
    assert runner.launches[0]["model"] == "zai/glm-4.7-flash"


def test_paid_zai_glm_backend_is_not_worker_backend_by_default(tmp_path: Path) -> None:
    with pytest.raises(OrchestratorError, match="approved Z.ai GLM worker backend"):
        _ready_service(tmp_path, junior_model="zai/glm-5.2")


def test_free_mimo_auto_worker_profile_uses_registered_auto_backend(tmp_path: Path) -> None:
    service, _runner, _glm, task, _package = _ready_service(tmp_path)
    codex = _actor("codex", OrchestratorRole.CODEX)
    service.register_agent(
        codex,
        task["project_id"],
        "mimo-auto-junior",
        OrchestratorRole.WORKER_JUNIOR,
        [OrchestratorRole.WORKER_JUNIOR],
        mimo_model="mimo-auto-junior",
        mimo_agent="build-junior",
    )

    profile = service.mimo_connection_profile(codex, task["project_id"], "mimo-auto-junior")

    assert profile["env"]["GRACE_ORCHESTRATOR_ACTOR_NAME"] == "mimo-auto-junior"
    assert profile["mimo_agent"] == "build-junior"
    assert profile["mimo_model"] == "mimo-auto-junior"
    assert profile["backend_family"] == "mimo_auto"
    assert "without --model" in profile["note"]


def test_controller_can_assign_luna_and_external_codex_worker_cannot_launch_through_mimo(tmp_path: Path) -> None:
    service, _runner, glm, task, package = _ready_service(tmp_path)
    codex = _actor("codex", OrchestratorRole.CODEX)
    service.register_agent(
        codex,
        task["project_id"],
        "luna",
        OrchestratorRole.WORKER_JUNIOR,
        [OrchestratorRole.WORKER_JUNIOR],
        mimo_model="openai/codex-luna",
    )

    reassigned = service.reassign_work_package_by_controller(
        codex, package["id"], "luna", "Sol explicitly assigned Luna"
    )
    profile = service.mimo_connection_profile(codex, task["project_id"], "luna")

    assert reassigned["assigned_junior_agent"] == "luna"
    assert reassigned["status"] == WorkPackageStatus.ASSIGNED.value
    assert profile["transport"] == "external_codex"
    assert profile["env"]["GRACE_ORCHESTRATOR_ACTOR_NAME"] == "luna"
    assert "MiMo launch is forbidden" in profile["note"]
    assert any(
        event["event_type"] == "work_package.reassigned_by_authority"
        for event in service.list_audit(task_id=task["id"])
    )
    with pytest.raises(OrchestratorError, match="cannot be launched through mimo.launch_package"):
        service.launch_mimo_session(glm, package["id"], MimoLaunchMode.TUI)


def test_terra_is_accepted_as_an_explicit_external_pro_backend(tmp_path: Path) -> None:
    service, _runner, _glm, task, _package = _ready_service(tmp_path)
    codex = _actor("codex", OrchestratorRole.CODEX)

    terra = service.register_agent(
        codex,
        task["project_id"],
        "terra",
        OrchestratorRole.WORKER_PRO,
        [OrchestratorRole.WORKER_PRO],
        mimo_model="openai/codex-terra",
    )

    assert terra["primary_role"] == OrchestratorRole.WORKER_PRO.value


def test_controller_cannot_reassign_an_independent_glm_direct_package(tmp_path: Path) -> None:
    service, _runner, _glm, task, package = _ready_service(tmp_path)
    codex = _actor("codex", OrchestratorRole.CODEX)
    with service.store.transaction() as conn:
        conn.execute(
            "UPDATE work_packages SET authority_mode = 'glm_direct', codex_required = 0 WHERE id = ?",
            (package["id"],),
        )

    with pytest.raises(OrchestratorError, match="independent glm_direct"):
        service.reassign_work_package_by_controller(codex, package["id"], "mimo-2.5", "not authorized")


def test_effective_glm_can_reassign_an_independent_glm_direct_package(tmp_path: Path) -> None:
    service, _runner, glm, task, package = _ready_service(tmp_path)
    codex = _actor("codex", OrchestratorRole.CODEX)
    service.register_agent(
        codex,
        task["project_id"],
        "luna",
        OrchestratorRole.WORKER_JUNIOR,
        [OrchestratorRole.WORKER_JUNIOR],
        mimo_model="openai/codex-luna",
    )
    with service.store.transaction() as conn:
        conn.execute(
            "UPDATE work_packages SET authority_mode = 'glm_direct', codex_required = 0 WHERE id = ?",
            (package["id"],),
        )

    reassigned = service.reassign_work_package(
        glm, package["id"], "luna", "GLM root selected Luna"
    )

    assert reassigned["assigned_junior_agent"] == "luna"
    assert any(
        event["event_type"] == "work_package.reassigned_by_authority"
        and event["effective_role"] == OrchestratorRole.GLM.value
        for event in service.list_audit(task_id=task["id"])
    )


def test_exact_package_assignment_allows_multirole_codex_actor_to_claim_as_junior(tmp_path: Path) -> None:
    service, _runner, _glm, task, package = _ready_service(tmp_path)
    codex = _actor("codex", OrchestratorRole.CODEX)
    service.register_agent(
        codex,
        task["project_id"],
        codex.name,
        OrchestratorRole.CODEX,
        [OrchestratorRole.CODEX, OrchestratorRole.WORKER_JUNIOR],
        mimo_model="openai/codex-luna",
    )
    with service.store.transaction() as conn:
        conn.execute(
            "UPDATE work_packages SET assigned_junior_agent = ? WHERE id = ?",
            (codex.name, package["id"]),
        )

    claimed = service.claim_work_package(codex, package["id"])

    assert claimed["status"] == WorkPackageStatus.CLAIMED_JUNIOR.value
    assert claimed["claimed_by_agent"] == codex.name


def test_shared_codex_actor_can_never_be_launched_through_mimo(tmp_path: Path) -> None:
    service, _runner, glm, task, package = _ready_service(tmp_path)
    codex = _actor("codex", OrchestratorRole.CODEX)
    service.register_agent(
        codex,
        task["project_id"],
        codex.name,
        OrchestratorRole.CODEX,
        [OrchestratorRole.CODEX, OrchestratorRole.WORKER_JUNIOR],
        mimo_model="mimo-auto-junior",
    )
    with service.store.transaction() as conn:
        conn.execute(
            "UPDATE work_packages SET assigned_junior_agent = ? WHERE id = ?",
            (codex.name, package["id"]),
        )

    with pytest.raises(OrchestratorError, match="must never be launched through MiMo"):
        service.launch_mimo_session(glm, package["id"], MimoLaunchMode.TUI)


def test_cancel_last_active_package_restores_prepared_state_and_allows_recreation(tmp_path: Path) -> None:
    service, _runner, glm, task, package = _ready_service(tmp_path)

    cancelled = service.cancel_work_package(glm, package["id"], "replace worker topology")
    recovered_task = service.get_task(task["id"])

    assert cancelled["status"] == WorkPackageStatus.CANCELLED.value
    assert recovered_task["status"] == TaskStatus.GLM_TESTS_PREPARED.value

    replacement = service.create_work_package(
        glm,
        task["id"],
        "replacement",
        "recreated after cancel-all",
        ["src/**"],
        [],
        "mimo-2.5",
        "mimo-2.5-pro",
        _git(Path(service.get_project(task["project_id"])["repo_path"]), "rev-parse", "HEAD"),
        **packet_kwargs(module_id="M-ORCH-MIMO-EXECUTOR", verification_id="V-M-ORCH-MIMO-EXECUTOR"),
    )

    assert replacement["status"] == WorkPackageStatus.CREATED.value


def test_controller_can_repair_a_legacy_cancel_all_stuck_task(tmp_path: Path) -> None:
    service, _runner, glm, task, package = _ready_service(tmp_path)
    codex = _actor("codex", OrchestratorRole.CODEX)
    service.cancel_work_package(glm, package["id"], "cancel old topology")
    with service.store.transaction() as conn:
        conn.execute(
            "UPDATE tasks SET status = ? WHERE id = ?",
            (TaskStatus.WORK_PACKAGES_ASSIGNED.value, task["id"]),
        )

    repaired = service.recover_task_after_cancel_all(
        codex, task["id"], "legacy cancel-all left the task stuck"
    )

    assert repaired["status"] == TaskStatus.GLM_TESTS_PREPARED.value
    assert any(
        event["event_type"] == "task.cancel_all_state_repaired"
        for event in service.list_audit(task_id=task["id"])
    )


def test_detached_tui_session_is_not_cancellable_by_the_service(tmp_path: Path) -> None:
    service, _runner, glm, _task, package = _ready_service(tmp_path)

    session = service.launch_mimo_session(glm, package["id"], MimoLaunchMode.TUI)

    assert session["lifecycle_state"] == MimoSessionStatus.TUI_DETACHED.value
    with pytest.raises(OrchestratorError, match="Only a running headless"):
        service.cancel_mimo_session(glm, session["id"])
    closed = service.record_detached_mimo_session_closed(glm, session["id"], "operator closed the window")
    assert closed["lifecycle_state"] == MimoSessionStatus.EXITED.value


def test_mimo_dispatch_rejects_a_second_active_session_for_one_package(tmp_path: Path) -> None:
    service, _runner, glm, _task, package = _ready_service(tmp_path)
    first = service.launch_mimo_session(glm, package["id"], MimoLaunchMode.HEADLESS)

    with pytest.raises(OrchestratorError, match=f"active Mimo session: {first['id']}"):
        service.launch_mimo_session(glm, package["id"], MimoLaunchMode.HEADLESS)


def test_mimo_pro_repair_starts_from_the_rejected_worker_commit(tmp_path: Path) -> None:
    service, runner, glm, task, package = _ready_service(tmp_path)
    first = service.launch_mimo_session(glm, package["id"], MimoLaunchMode.HEADLESS)
    worker = _actor("mimo-2.5", OrchestratorRole.WORKER_JUNIOR)
    service.claim_work_package(worker, package["id"])
    runner.exit_code = 0
    service.poll_mimo_session(glm, first["id"])

    repo = Path(service.get_project(task["project_id"])["repo_path"])
    (repo / "src" / "worker.py").write_text("value = 2\n", encoding="utf-8")
    _git(repo, "add", "src/worker.py")
    _git(repo, "commit", "-m", "worker change")
    repaired_from = _git(repo, "rev-parse", "HEAD")
    evidence = RepositoryBoundary(repo).derive_submission(package["base_commit"], repaired_from)
    service.submit_package(
        worker,
        package["id"],
        "worker result",
        evidence,
        [],
        "",
        worker_report=worker_report(
            task_id=task["id"],
            package_id=package["id"],
            files_changed=["src/worker.py"],
            module_id="M-ORCH-MIMO-EXECUTOR",
        ),
    )
    service.review_package(glm, package["id"], "rejected_repair_required", ["needs repair"], ["repair it"])

    repair = service.launch_mimo_session(glm, package["id"], MimoLaunchMode.HEADLESS)

    assert _git(Path(repair["workspace_path"]), "rev-parse", "HEAD") == repaired_from
    briefing = Path(repair["briefing_path"]).read_text(encoding="utf-8")
    assert f"Repair source commit: {repaired_from}" in briefing
    assert "Latest rejection findings: ['needs repair']" in briefing
    assert "Required repair fixes: ['repair it']" in briefing


def test_mimo_repair_uses_paid_junior_when_pro_is_unavailable(tmp_path: Path) -> None:
    service, runner, glm, task, package = _ready_service(tmp_path, junior_model="xiaomi/mimo-v2.5")
    codex = _actor("codex", OrchestratorRole.CODEX)
    service.set_agent_availability(codex, task["project_id"], "mimo-2.5-pro", "unavailable")
    first = service.launch_mimo_session(glm, package["id"], MimoLaunchMode.HEADLESS)
    worker = _actor("mimo-2.5", OrchestratorRole.WORKER_JUNIOR)
    service.claim_work_package(worker, package["id"])
    runner.exit_code = 0
    service.poll_mimo_session(glm, first["id"])

    repo = Path(service.get_project(task["project_id"])["repo_path"])
    (repo / "src" / "worker.py").write_text("value = 2\n", encoding="utf-8")
    _git(repo, "add", "src/worker.py")
    _git(repo, "commit", "-m", "worker change")
    rejected_head = _git(repo, "rev-parse", "HEAD")
    evidence = RepositoryBoundary(repo).derive_submission(package["base_commit"], rejected_head)
    service.submit_package(
        worker,
        package["id"],
        "worker result",
        evidence,
        [],
        "",
        worker_report=worker_report(
            task_id=task["id"],
            package_id=package["id"],
            files_changed=["src/worker.py"],
            module_id="M-ORCH-MIMO-EXECUTOR",
        ),
    )
    service.review_package(glm, package["id"], "rejected_repair_required", ["needs repair"], ["repair it"])

    with pytest.raises(OrchestratorError, match="not available"):
        service.claim_work_package(_actor("mimo-2.5-pro", OrchestratorRole.WORKER_PRO), package["id"])

    repair = service.launch_mimo_session(glm, package["id"], MimoLaunchMode.TUI)

    assert repair["assigned_agent"] == "mimo-2.5"
    assert repair["assigned_role"] == OrchestratorRole.WORKER_JUNIOR.value
    assert repair["mimo_model"] == "xiaomi/mimo-v2.5"
    assert repair["mimo_agent"] == "build"
    assert repair["lifecycle_state"] == MimoSessionStatus.TUI_DETACHED.value
    assert runner.launches[-1]["model"] == "xiaomi/mimo-v2.5"
    assert runner.launches[-1]["agent"] == "build"
    assert _git(Path(repair["workspace_path"]), "rev-parse", "HEAD") == rejected_head
    briefing = Path(repair["briefing_path"]).read_text(encoding="utf-8")
    assert "Registered agent: mimo-2.5" in briefing
    assert "Bound role required: worker_junior" in briefing
    assert "Selected provider/model backend: xiaomi/mimo-v2.5" in briefing
    assert "Selected MiMoCode TUI agent: build" in briefing
    assert f"Repair source commit: {rejected_head}" in briefing
    assert "Latest rejection findings: ['needs repair']" in briefing
    assert "Required repair fixes: ['repair it']" in briefing
    claimed = service.claim_work_package(worker, package["id"])
    assert claimed["status"] == WorkPackageStatus.CLAIMED_JUNIOR.value
    assert claimed["claimed_by_agent"] == "mimo-2.5"


def test_mimo_repair_tui_launch_ignores_superseded_detached_tui(tmp_path: Path) -> None:
    service, runner, glm, task, package = _ready_service(tmp_path, junior_model="xiaomi/mimo-v2.5")
    codex = _actor("codex", OrchestratorRole.CODEX)
    service.set_agent_availability(codex, task["project_id"], "mimo-2.5-pro", "unavailable")
    first = service.launch_mimo_session(glm, package["id"], MimoLaunchMode.TUI)
    assert first["lifecycle_state"] == MimoSessionStatus.TUI_DETACHED.value
    worker = _actor("mimo-2.5", OrchestratorRole.WORKER_JUNIOR)
    service.claim_work_package(worker, package["id"])

    repo = Path(service.get_project(task["project_id"])["repo_path"])
    (repo / "src" / "worker.py").write_text("value = 2\n", encoding="utf-8")
    _git(repo, "add", "src/worker.py")
    _git(repo, "commit", "-m", "worker change")
    rejected_head = _git(repo, "rev-parse", "HEAD")
    evidence = RepositoryBoundary(repo).derive_submission(package["base_commit"], rejected_head)
    service.submit_package(
        worker,
        package["id"],
        "worker result",
        evidence,
        [],
        "",
        worker_report=worker_report(
            task_id=task["id"],
            package_id=package["id"],
            files_changed=["src/worker.py"],
            module_id="M-ORCH-MIMO-EXECUTOR",
        ),
    )
    service.review_package(glm, package["id"], "rejected_repair_required", ["needs repair"], ["repair it"])

    repair = service.launch_mimo_session(glm, package["id"], MimoLaunchMode.TUI)

    assert repair["id"] != first["id"]
    assert repair["assigned_agent"] == "mimo-2.5"
    assert repair["assigned_role"] == OrchestratorRole.WORKER_JUNIOR.value
    assert repair["mimo_model"] == "xiaomi/mimo-v2.5"
    assert repair["mimo_agent"] == "build"
    assert repair["lifecycle_state"] == MimoSessionStatus.TUI_DETACHED.value
    assert len(runner.launches) == 2
    assert _git(Path(repair["workspace_path"]), "rev-parse", "HEAD") == rejected_head
    with pytest.raises(OrchestratorError, match=f"active Mimo session: {repair['id']}"):
        service.launch_mimo_session(glm, package["id"], MimoLaunchMode.TUI)


def test_controller_can_recover_orphaned_prepared_session_without_accepting_work(tmp_path: Path) -> None:
    service, _runner, glm, task, package = _ready_service(tmp_path)
    with service.store.transaction() as conn:
        cursor = conn.execute(
            """INSERT INTO mimo_sessions (
                project_id, task_id, work_package_id, requested_by_agent, assigned_agent,
                assigned_role, mimo_model, mode, lifecycle_state, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                task["project_id"],
                task["id"],
                package["id"],
                glm.name,
                "mimo-2.5",
                OrchestratorRole.WORKER_JUNIOR.value,
                "xiaomi/mimo-v2.5",
                MimoLaunchMode.HEADLESS.value,
                MimoSessionStatus.PREPARED.value,
                "2026-06-20T00:00:00+00:00",
            ),
        )
        session_id = int(cursor.lastrowid)

    recovered = service.recover_prepared_mimo_session(glm, session_id, "controller observed an interrupted preflight")

    assert recovered["lifecycle_state"] == MimoSessionStatus.FAILED.value
    assert recovered["failure_reason"] == "controller observed an interrupted preflight"
    assert any(event["event_type"] == "mimo.prepared_session_recovered" for event in service.list_audit(task_id=task["id"]))


def test_controller_can_recover_an_absent_headless_process_without_accepting_work(tmp_path: Path) -> None:
    service, _runner, glm, task, package = _ready_service(tmp_path)
    with service.store.transaction() as conn:
        cursor = conn.execute(
            """INSERT INTO mimo_sessions (
                project_id, task_id, work_package_id, requested_by_agent, assigned_agent,
                assigned_role, mimo_model, mode, lifecycle_state, pid, created_at, started_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                task["project_id"],
                task["id"],
                package["id"],
                glm.name,
                "mimo-2.5",
                OrchestratorRole.WORKER_JUNIOR.value,
                "xiaomi/mimo-v2.5",
                MimoLaunchMode.HEADLESS.value,
                MimoSessionStatus.RUNNING.value,
                999_999,
                "2026-06-20T00:00:00+00:00",
                "2026-06-20T00:00:01+00:00",
            ),
        )
        session_id = int(cursor.lastrowid)

    recovered = service.recover_orphaned_running_mimo_session(glm, session_id, "controller observed the dispatched process had exited")

    assert recovered["lifecycle_state"] == MimoSessionStatus.FAILED.value
    assert recovered["failure_reason"] == "controller observed the dispatched process had exited"
    assert any(event["event_type"] == "mimo.running_session_recovered" for event in service.list_audit(task_id=task["id"]))


def test_mimo_profile_binds_each_registered_agent_identity(tmp_path: Path) -> None:
    service, _runner, _glm, task, _package = _ready_service(tmp_path)
    codex = _actor("codex", OrchestratorRole.CODEX)

    profile = service.mimo_connection_profile(codex, task["project_id"], "mimo-2.5")

    assert profile["transport"] == "stdio"
    assert profile["command"] == sys.executable
    assert profile["env"]["GRACE_ORCHESTRATOR_ACTOR_NAME"] == "mimo-2.5"
    assert profile["env"]["GRACE_ORCHESTRATOR_ACTOR_ROLE"] == OrchestratorRole.WORKER_JUNIOR.value
    assert profile["mimo_agent"] == "build"
    assert profile["mimo_model"] == "xiaomi/mimo-v2.5"
    assert profile["backend_family"] == "mimo"


def test_mimo_runner_builds_shell_free_headless_argv(tmp_path: Path) -> None:
    runner = MimoRunner(tmp_path, command="mimo")
    argv = runner.build_command(
        mode=MimoLaunchMode.HEADLESS,
        model="xiaomi/mimo-v2.5",
        workspace_path=tmp_path / "workspace",
        briefing_path=tmp_path / "briefing.md",
        session_id=7,
    )

    assert argv[1:4] == ["run", "--model", "xiaomi/mimo-v2.5"]
    if os_name == "nt":
        assert argv[0].lower().endswith("mimo.exe")
    assert "--trust" not in argv
    assert "--file" in argv
    assert "--dangerously-skip-permissions" not in argv


def test_mimo_runner_builds_trusted_tui_argv(tmp_path: Path) -> None:
    runner = MimoRunner(tmp_path, command="mimo")
    argv = runner.build_command(
        mode=MimoLaunchMode.TUI,
        model="xiaomi/mimo-v2.5",
        workspace_path=tmp_path / "workspace",
        briefing_path=tmp_path / "briefing.md",
        session_id=7,
    )

    assert argv[1:4] == ["--model", "xiaomi/mimo-v2.5", "--trust"]
    assert "--prompt" in argv
    assert "--dangerously-skip-permissions" not in argv


def test_mimo_runner_builds_free_auto_tui_without_model_flag(tmp_path: Path) -> None:
    runner = MimoRunner(tmp_path, command="mimo")

    argv = runner.build_command(
        mode=MimoLaunchMode.TUI,
        model="mimo-auto-junior",
        agent="build-junior",
        workspace_path=tmp_path / "workspace",
        briefing_path=tmp_path / "briefing.md",
        session_id=7,
    )

    assert argv[1:4] == ["--agent", "build-junior", "--trust"]
    assert "--model" not in argv
    assert "--prompt" in argv
    assert "--dangerously-skip-permissions" not in argv


def test_mimo_runner_rejects_free_auto_headless(tmp_path: Path) -> None:
    runner = MimoRunner(tmp_path, command="mimo")

    with pytest.raises(OrchestratorError, match="free TUI backend"):
        runner.build_command(
            mode=MimoLaunchMode.HEADLESS,
            model="mimo-auto-junior",
            workspace_path=tmp_path / "workspace",
            briefing_path=tmp_path / "briefing.md",
            session_id=7,
        )


def test_mimo_runner_rejects_generic_auto_aliases(tmp_path: Path) -> None:
    runner = MimoRunner(tmp_path, command="mimo")

    for model in ("auto", "auto-junior", "default"):
        with pytest.raises(OrchestratorError, match="legacy implicit aliases are blocked"):
            runner.build_command(
                mode=MimoLaunchMode.TUI,
                model=model,
                workspace_path=tmp_path / "workspace",
                briefing_path=tmp_path / "briefing.md",
                session_id=7,
            )


def test_mimo_runner_pins_mimocode_agent_with_explicit_model_for_tui(tmp_path: Path) -> None:
    runner = MimoRunner(tmp_path, command="mimo")
    argv = runner.build_command(
        mode=MimoLaunchMode.TUI,
        model="xiaomi/mimo-v2.5",
        agent="build",
        workspace_path=tmp_path / "workspace",
        briefing_path=tmp_path / "briefing.md",
        session_id=7,
    )

    assert argv[1:6] == ["--agent", "build", "--model", "xiaomi/mimo-v2.5", "--trust"]
