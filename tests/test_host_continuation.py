from __future__ import annotations

import json
import os
from pathlib import Path
import sys

from grace_orchestrator.host_continuation import (
    HostContinuationConfig,
    HostContinuationSupervisor,
    _split_configured_command,
)
from grace_orchestrator.models import ActorIdentity, OrchestratorRole, SubmissionEvidence
from grace_orchestrator.service import OrchestratorService
from conftest import packet_kwargs, worker_report


def _actor(name: str, role: OrchestratorRole) -> ActorIdentity:
    return ActorIdentity(name=name, primary_role=role)


def _ready_package(tmp_path: Path) -> tuple[OrchestratorService, dict[str, object], dict[str, object], dict[str, object], ActorIdentity, ActorIdentity]:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    glm = _actor("glm", OrchestratorRole.GLM)
    junior = _actor("mimo-auto-junior", OrchestratorRole.WORKER_JUNIOR)
    pro = _actor("mimo-pro", OrchestratorRole.WORKER_PRO)
    (tmp_path / "grace").mkdir()
    project = service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {"unit": ["python", "-m", "pytest"]})
    service.register_agent(codex, project["id"], glm.name, OrchestratorRole.GLM, [OrchestratorRole.GLM], mimo_model="zai-coding-plan/glm-5.2")
    service.register_agent(codex, project["id"], junior.name, OrchestratorRole.WORKER_JUNIOR, [OrchestratorRole.WORKER_JUNIOR], mimo_model="xiaomi/mimo-v2.5")
    service.register_agent(codex, project["id"], pro.name, OrchestratorRole.WORKER_PRO, [OrchestratorRole.WORKER_PRO], mimo_model="xiaomi/mimo-v2.5-pro")
    task = service.create_codex_task(
        codex,
        project["id"],
        "host continuation",
        "resume controller after worker handoff",
        "host owns wakeup; MCP owns review ledger",
        ["durable files only"],
        ["no in-memory recovery"],
        ["controller event emitted"],
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
    service.claim_work_package(junior, package["id"])
    return service, project, task, package, junior, glm


def _submit(service: OrchestratorService, actor: ActorIdentity, package_id: int) -> dict[str, object]:
    task_id = service.get_work_package(package_id)["task_id"]
    return service.submit_package(
        actor,
        package_id,
        "bounded implementation",
        SubmissionEvidence(
            base_commit="a" * 40,
            head_commit="b" * 40,
            diff="diff --git a/src/hook.py b/src/hook.py",
            diff_hash="f" * 64,
            files_changed=["src/hook.py"],
        ),
        [{"command_key": "unit", "exit_code": 0}],
        "none",
        worker_report=worker_report(task_id=task_id, package_id=package_id, files_changed=["src/hook.py"]),
    )


def _host_events(run_root: Path) -> list[dict[str, object]]:
    path = run_root / "host-events.ndjson"
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _start_command(tmp_path: Path, exit_code: int = 0) -> str:
    script = tmp_path / f"controller-start-{exit_code}.py"
    script.write_text(
        "\n".join(
            [
                "from pathlib import Path",
                "import os",
                "import sys",
                "marker = os.environ.get('GRACE_TEST_MARKER')",
                "if marker:",
                "    Path(marker).write_text(Path(sys.argv[1]).read_text(encoding='utf-8'), encoding='utf-8')",
                f"raise SystemExit({exit_code})",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return f'"{sys.executable}" "{script}" "{{prompt_file}}"'


def test_windows_command_split_preserves_prompt_path_backslashes() -> None:
    if os.name != "nt":
        return

    prompt_path = r"D:\Users\Artemyx\grace-orchestrator-state\host-continuation\prompts\abc.md"
    assert _split_configured_command(f"codex {prompt_path}") == ["codex", prompt_path]

    quoted = _split_configured_command(
        r'"C:\Program Files\Codex\codex.exe" "D:\Users\Artemyx\path with spaces\abc.md"'
    )
    assert quoted == [
        r"C:\Program Files\Codex\codex.exe",
        r"D:\Users\Artemyx\path with spaces\abc.md",
    ]


def test_host_detects_worker_ready_for_review(tmp_path: Path, monkeypatch) -> None:
    service, _project, _task, package, junior, _glm = _ready_package(tmp_path)
    submission = _submit(service, junior, package["id"])
    run_root = Path(submission["handoff_event"]["run_root"])
    marker = tmp_path / "controller-started.txt"
    monkeypatch.setenv("GRACE_TEST_MARKER", str(marker))

    result = HostContinuationSupervisor(
        HostContinuationConfig(data_dir=tmp_path, start_command=_start_command(tmp_path), command_wait_seconds=2)
    ).run_once()

    assert result["processed_count"] == 1
    events = _host_events(run_root)
    assert [event["type"] for event in events] == [
        "HOST_CONTINUATION_DETECTED",
        "HOST_CONTROLLER_LOGICAL_CONTINUATION_STARTED",
    ]
    assert "Continue GRACE controller review" in marker.read_text(encoding="utf-8")


def test_host_does_not_process_same_event_twice(tmp_path: Path, monkeypatch) -> None:
    service, _project, _task, package, junior, _glm = _ready_package(tmp_path)
    submission = _submit(service, junior, package["id"])
    run_root = Path(submission["handoff_event"]["run_root"])
    marker = tmp_path / "controller-started.txt"
    monkeypatch.setenv("GRACE_TEST_MARKER", str(marker))
    supervisor = HostContinuationSupervisor(
        HostContinuationConfig(data_dir=tmp_path, start_command=_start_command(tmp_path), command_wait_seconds=2)
    )

    first = supervisor.run_once()
    second = supervisor.run_once()

    assert first["processed_count"] == 1
    assert second["processed_count"] == 0
    assert [event["type"] for event in _host_events(run_root)].count("HOST_CONTROLLER_LOGICAL_CONTINUATION_STARTED") == 1


def test_host_lock_prevents_duplicate_continuation(tmp_path: Path, monkeypatch) -> None:
    service, _project, _task, package, junior, _glm = _ready_package(tmp_path)
    submission = _submit(service, junior, package["id"])
    run_root = Path(submission["handoff_event"]["run_root"])
    marker = tmp_path / "controller-started.txt"
    monkeypatch.setenv("GRACE_TEST_MARKER", str(marker))
    supervisor = HostContinuationSupervisor(
        HostContinuationConfig(data_dir=tmp_path, start_command=_start_command(tmp_path), command_wait_seconds=2)
    )
    lock_path = supervisor._run_lock_path(run_root.relative_to(tmp_path / "runs").as_posix())
    lock_path.mkdir(parents=True)

    result = supervisor.run_once()

    assert result["processed_count"] == 0
    assert result["locked_count"] == 1
    assert not marker.exists()
    assert _host_events(run_root) == []


def test_host_emits_failure_event_when_start_command_fails(tmp_path: Path) -> None:
    service, _project, _task, package, junior, _glm = _ready_package(tmp_path)
    submission = _submit(service, junior, package["id"])
    run_root = Path(submission["handoff_event"]["run_root"])

    HostContinuationSupervisor(
        HostContinuationConfig(data_dir=tmp_path, start_command=_start_command(tmp_path, exit_code=7), command_wait_seconds=2)
    ).run_once()

    events = _host_events(run_root)
    assert "HOST_CONTROLLER_RESUME_FAILED" in [event["type"] for event in events]
    failure = [event for event in events if event["type"] == "HOST_CONTROLLER_RESUME_FAILED"][-1]
    assert failure["payload"]["attempted_mode"] == "logical"
    assert failure["payload"]["exit_code"] == 7


def test_host_builds_controller_prompt_from_durable_run_data(tmp_path: Path, monkeypatch) -> None:
    service, _project, _task, package, junior, _glm = _ready_package(tmp_path)
    workspace = tmp_path / "worktrees" / "package-1"
    workspace.mkdir(parents=True)
    with service.store.transaction() as conn:
        conn.execute(
            """INSERT INTO mimo_sessions (
                project_id, task_id, work_package_id, requested_by_agent, assigned_agent,
                assigned_role, mimo_model, mode, lifecycle_state, workspace_path, briefing_path,
                command_json, pid, stdout_path, stderr_path, exit_code, failure_reason,
                created_at, started_at, ended_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                1,
                package["task_id"],
                package["id"],
                "glm",
                junior.name,
                OrchestratorRole.WORKER_JUNIOR.value,
                "mimo-auto-junior",
                "tui",
                "TUI_DETACHED",
                str(workspace),
                str(tmp_path / "briefing.md"),
                "[]",
                None,
                None,
                None,
                None,
                None,
                "2026-06-28T00:00:00+00:00",
                "2026-06-28T00:00:00+00:00",
                None,
            ),
        )
    submission = _submit(service, junior, package["id"])
    run_root = Path(submission["handoff_event"]["run_root"])
    (run_root / "controller.json").write_text(
        json.dumps({"controller_session_id": "codex-session-123", "actor": "codex"}, sort_keys=True),
        encoding="utf-8",
    )
    marker = tmp_path / "controller-started.txt"
    monkeypatch.setenv("GRACE_TEST_MARKER", str(marker))

    HostContinuationSupervisor(
        HostContinuationConfig(data_dir=tmp_path, start_command=_start_command(tmp_path), command_wait_seconds=2)
    ).run_once()

    prompt = marker.read_text(encoding="utf-8")
    assert str(submission["handoff_report_path"]) in prompt
    assert str(workspace) in prompt
    assert "codex-session-123" in prompt
    assert "M-ORCH-LEDGER" in prompt
    assert "ACCEPTED / REWORK_REQUIRED / BLOCKED_WAITING_USER" in prompt
    assert "review.glm_submit" in prompt
