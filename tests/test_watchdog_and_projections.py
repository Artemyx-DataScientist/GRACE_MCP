"""Tests for Watchdog failure modes, Circuit Breakers, and Compact Context Projections."""


from conftest import packet_kwargs, worker_report
from grace_orchestrator.models import ActorIdentity, OrchestratorRole, SubmissionEvidence, WorkPackageStatus
from grace_orchestrator.policy import (
    calculate_rejection_fingerprint,
    compact_worker_report_for_context,
    create_compact_diff_projection,
    create_compact_log_projection,
)
from grace_orchestrator.service import OrchestratorService


def test_rejection_fingerprint_stability():
    f1 = calculate_rejection_fingerprint(["Fix missing module", "Syntax error in src/main.py"])
    f2 = calculate_rejection_fingerprint(["Syntax error in src/main.py", "Fix missing module"])
    assert f1 == f2
    assert len(f1) == 16


def test_compact_log_projection_small_log():
    log = "Line 1\nLine 2\nLine 3"
    compact = create_compact_log_projection(log)
    assert compact == log


def test_compact_log_projection_large_log_with_diagnostics():
    lines = [f"Normal execution line {i}" for i in range(100)]
    lines[45] = "Traceback (most recent call last):"
    lines[46] = "  File 'src/app.py', line 12, in main"
    lines[47] = "ValueError: CRITICAL ERROR in component"

    full_log = "\n".join(lines)
    compact = create_compact_log_projection(full_log, artifact_ref="artifacts/logs/test.log", sha256_hash="abc123hash")

    assert "BEGIN OUTPUT: first 20 of 100 lines" in compact
    assert "OMITTED 50 LINES" in compact
    assert "EXTRACTED DIAGNOSTICS" in compact
    assert "ValueError: CRITICAL ERROR in component" in compact
    assert "END OUTPUT: last 30 lines" in compact
    assert "FULL LOG ARTIFACT: artifacts/logs/test.log (SHA-256: abc123hash)" in compact


def test_compact_diff_projection():
    diff = "--- a/foo.py\n+++ b/foo.py\n@@ -1,3 +1,4 @@\n-old line\n+new line 1\n+new line 2"
    compact = create_compact_diff_projection(["foo.py"], diff, artifact_ref="artifacts/diffs/1.diff", sha256_hash="diffhash")

    assert "COMPACT DIFF SUMMARY" in compact
    assert "foo.py" in compact
    assert "+2 / -1 lines" in compact
    assert "FULL DIFF ARTIFACT: artifacts/diffs/1.diff" in compact


def test_compact_worker_report_pure_projection():
    big_output = "\n".join([f"Output line {i}" for i in range(100)])
    original_report = {
        "commands run with exact results": [
            {"command": "pytest", "stdout": big_output}
        ]
    }

    projected = compact_worker_report_for_context(original_report)

    # Ensure original is unchanged
    assert original_report["commands run with exact results"][0]["stdout"] == big_output

    # Ensure projected contains compact log
    proj_out = projected["commands run with exact results"][0]["output"]
    assert "OMITTED" in proj_out


def test_circuit_breaker_triggers_human_intervention(tmp_path):
    service = OrchestratorService(tmp_path / "data")
    codex_actor = ActorIdentity(name="codex-1", primary_role=OrchestratorRole.CODEX)
    glm_actor = ActorIdentity(name="glm-1", primary_role=OrchestratorRole.GLM)
    junior_actor = ActorIdentity(name="junior-1", primary_role=OrchestratorRole.WORKER_JUNIOR)

    repo_dir = tmp_path / "repo"
    repo_dir.mkdir(parents=True, exist_ok=True)

    proj = service.init_project(
        codex_actor,
        name="test-cb-proj",
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

    task = service.create_codex_task(
        codex_actor,
        proj["id"],
        title="cb-task",
        objective="test circuit breaker",
        architecture_intent="circuit breaker",
        constraints=["unit-test"],
        non_goals=[],
        acceptance_criteria=["cb works"],
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

    pkg = service.create_work_package(
        glm_actor,
        task["id"],
        title="CB Package",
        objective="implement guards",
        allowed_files=["src/**"],
        forbidden_files=["tests/**"],
        assigned_junior_agent="junior-1",
        assigned_pro_agent="pro-1",
        base_commit="a" * 40,
        **packet_kwargs(),
    )

    pro_actor = ActorIdentity(name="pro-1", primary_role=OrchestratorRole.WORKER_PRO)

    # 1st cycle: Assign -> Claim -> Submit -> Reject -> REPAIR_REQUIRED
    service.assign_work_package(glm_actor, pkg["id"])
    service.claim_work_package(junior_actor, pkg["id"])
    service.submit_package(
        junior_actor,
        pkg["id"],
        summary="sub 1",
        evidence=SubmissionEvidence("a"*40, "b"*40, "diff", "hash1", ["src/a.py"]),
        tests_run=[],
        risk_notes="none",
        worker_report=worker_report(task_id=task["id"], package_id=pkg["id"], files_changed=["src/a.py"]),
    )
    service.review_package(glm_actor, pkg["id"], decision="rejected_repair_required", findings=["f1"], required_fixes=["fix1"])
    assert service.get_work_package(pkg["id"])["status"] == WorkPackageStatus.REPAIR_REQUIRED.value

    # 2nd cycle: Claim by Pro -> Submit -> Reject -> REPAIR_REQUIRED
    service.claim_work_package(pro_actor, pkg["id"])
    service.submit_package(
        pro_actor,
        pkg["id"],
        summary="sub 2",
        evidence=SubmissionEvidence("a"*40, "c"*40, "diff", "hash2", ["src/a.py"]),
        tests_run=[],
        risk_notes="none",
        worker_report=worker_report(task_id=task["id"], package_id=pkg["id"], files_changed=["src/a.py"]),
    )
    service.review_package(glm_actor, pkg["id"], decision="rejected_repair_required", findings=["f2"], required_fixes=["fix2"])
    assert service.get_work_package(pkg["id"])["status"] == WorkPackageStatus.REPAIR_REQUIRED.value

    # 3rd cycle: Claim by Pro -> Submit -> Reject -> HUMAN_INTERVENTION_REQUIRED (Circuit Breaker Triggered!)
    service.claim_work_package(pro_actor, pkg["id"])
    service.submit_package(
        pro_actor,
        pkg["id"],
        summary="sub 3",
        evidence=SubmissionEvidence("a"*40, "d"*40, "diff", "hash3", ["src/a.py"]),
        tests_run=[],
        risk_notes="none",
        worker_report=worker_report(task_id=task["id"], package_id=pkg["id"], files_changed=["src/a.py"]),
    )
    service.review_package(glm_actor, pkg["id"], decision="rejected_repair_required", findings=["f3"], required_fixes=["fix3"])
    assert service.get_work_package(pkg["id"])["status"] == WorkPackageStatus.HUMAN_INTERVENTION_REQUIRED.value
