from datetime import UTC, datetime, timedelta
from pathlib import Path
import sqlite3

import pytest

# FILE: tests/test_task_flow.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Verify M-ORCH ledger acceptance flow, audit immutability, and fallback eligibility.
#   SCOPE: End-to-end local task, premature-final-review, audit mutation, and fallback rejection.
#   DEPENDS: M-ORCH-DOMAIN, M-ORCH-LEDGER, M-ORCH-REPO-BOUNDARY
#   LINKS: M-ORCH-LEDGER, V-M-ORCH-LEDGER
#   ROLE: TEST
#   MAP_MODE: LOCALS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   test_codex_fallback_can_drive_a_complete_local_acceptance_flow - full deterministic acceptance path.
#   test_* - final gate, append-only audit, and fallback capability guards.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.1.0 - Added end-to-end local acceptance and fallback registry coverage.
# END_CHANGE_SUMMARY

from grace_orchestrator.models import ActorIdentity, OrchestratorError, OrchestratorRole, SubmissionEvidence, TaskStatus, WorkPackageStatus
from grace_orchestrator.service import OrchestratorService
from conftest import packet_kwargs, worker_report


def _actor(name: str, role: OrchestratorRole) -> ActorIdentity:
    return ActorIdentity(name=name, primary_role=role)


def test_codex_can_register_a_non_empty_project_test_command_allowlist(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    project = service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {})

    updated = service.set_allowed_test_commands(
        codex,
        project["id"],
        {"unit": ["python", "-m", "pytest"]},
    )

    assert updated["allowed_test_commands"] == {"unit": ["python", "-m", "pytest"]}
    assert "project.test_commands_registered" in {
        event["event_type"] for event in service.list_audit()
    }


def test_codex_fallback_can_drive_a_complete_local_acceptance_flow(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    junior = _actor("mimo-2.5", OrchestratorRole.WORKER_JUNIOR)
    pro = _actor("mimo-2.5-pro", OrchestratorRole.WORKER_PRO)

    project = service.init_project(
        codex,
        name="demo",
        repo_path=tmp_path,
        grace_path=tmp_path / "grace",
        main_branch="main",
        allowed_test_commands={"unit": ["python", "-m", "pytest"]},
    )
    service.register_agent(
        codex,
        project["id"],
        name="codex",
        primary_role=OrchestratorRole.CODEX,
        capabilities=[OrchestratorRole.CODEX, OrchestratorRole.GLM, OrchestratorRole.TEST_OWNER],
    )
    service.register_agent(
        codex,
        project["id"],
        name="glm-5.2",
        primary_role=OrchestratorRole.GLM,
        capabilities=[OrchestratorRole.GLM, OrchestratorRole.TEST_OWNER],
        availability="unavailable",
        mimo_model="zai-coding-plan/glm-5.2",
    )
    service.register_agent(
        codex,
        project["id"],
        name=junior.name,
        primary_role=OrchestratorRole.WORKER_JUNIOR,
        capabilities=[OrchestratorRole.WORKER_JUNIOR],
        mimo_model="xiaomi/mimo-v2.5",
    )
    service.register_agent(
        codex,
        project["id"],
        name=pro.name,
        primary_role=OrchestratorRole.WORKER_PRO,
        capabilities=[OrchestratorRole.WORKER_PRO],
        mimo_model="xiaomi/mimo-v2.5-pro",
    )
    task = service.create_codex_task(
        codex,
        project["id"],
        title="ledger MVP",
        objective="record a bounded workflow",
        architecture_intent="ledger owns workflow state",
        constraints=["no model calls"],
        non_goals=["no patch apply"],
        acceptance_criteria=["audit exists"],
        allowed_files=["src/**"],
        forbidden_files=["tests/**"],
    )
    service.delegate_role(
        codex,
        project["id"],
        task["id"],
        unavailable_role=OrchestratorRole.GLM,
        substitute_actor="codex",
        delegated_role=OrchestratorRole.GLM,
        reason="GLM unavailable during bootstrap",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    service.delegate_role(
        codex,
        project["id"],
        task["id"],
        unavailable_role=OrchestratorRole.TEST_OWNER,
        substitute_actor="codex",
        delegated_role=OrchestratorRole.TEST_OWNER,
        reason="GLM test owner unavailable during bootstrap",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    service.plan_task(codex, task["id"])
    service.register_verification_plan(codex, task["id"], test_strategy="deterministic", test_commands=["unit"])
    package = service.create_work_package(
        codex,
        task["id"],
        title="domain",
        objective="implement guards",
        allowed_files=["src/**"],
        forbidden_files=["tests/**"],
        assigned_junior_agent=junior.name,
        assigned_pro_agent=pro.name,
        base_commit="a" * 40,
        **packet_kwargs(),
    )
    second_package = service.create_work_package(
        codex,
        task["id"],
        title="repository",
        objective="implement confinement",
        allowed_files=["src/**"],
        forbidden_files=["tests/**"],
        assigned_junior_agent=junior.name,
        assigned_pro_agent=pro.name,
        base_commit="a" * 40,
        **packet_kwargs(),
    )
    service.assign_work_package(codex, package["id"])
    service.assign_work_package(codex, second_package["id"])
    service.claim_work_package(junior, package["id"])
    service.submit_package(
        junior,
        package["id"],
        summary="implemented guards",
        evidence=SubmissionEvidence(
            base_commit="a" * 40,
            head_commit="b" * 40,
            diff="diff --git a/src/a.py b/src/a.py",
            diff_hash="f" * 64,
            files_changed=["src/a.py"],
        ),
        tests_run=[{"command_key": "unit", "exit_code": 0}],
        risk_notes="none",
        worker_report=worker_report(task_id=task["id"], package_id=package["id"], files_changed=["src/a.py"]),
    )
    service.review_package(codex, package["id"], decision="accepted", findings=[], required_fixes=[])
    service.claim_work_package(junior, second_package["id"])
    service.submit_package(
        junior,
        second_package["id"],
        summary="implemented confinement",
        evidence=SubmissionEvidence(
            base_commit="a" * 40,
            head_commit="c" * 40,
            diff="diff --git a/src/b.py b/src/b.py",
            diff_hash="e" * 64,
            files_changed=["src/b.py"],
        ),
        tests_run=[{"command_key": "unit", "exit_code": 0}],
        risk_notes="none",
        worker_report=worker_report(task_id=task["id"], package_id=second_package["id"], files_changed=["src/b.py"]),
    )
    service.review_package(
        codex,
        second_package["id"],
        decision="rejected_repair_required",
        findings=["scope evidence needs a repair"],
        required_fixes=["submit corrected package"],
    )
    service.claim_work_package(pro, second_package["id"])
    service.submit_package(
        pro,
        second_package["id"],
        summary="repaired confinement",
        evidence=SubmissionEvidence(
            base_commit="a" * 40,
            head_commit="d" * 40,
            diff="diff --git a/src/b.py b/src/b.py",
            diff_hash="d" * 64,
            files_changed=["src/b.py"],
        ),
        tests_run=[{"command_key": "unit", "exit_code": 0}],
        risk_notes="repair reviewed",
        worker_report=worker_report(task_id=task["id"], package_id=second_package["id"], files_changed=["src/b.py"]),
    )
    service.review_package(codex, second_package["id"], decision="accepted", findings=[], required_fixes=[])
    assert service.get_task(task["id"])["status"] == TaskStatus.GLM_ACCEPTED.value
    for artifact_type, filename in {
        "requirements": "requirements.xml",
        "technology": "technology.xml",
        "development_plan": "development-plan.xml",
        "verification_plan": "verification-plan.xml",
        "knowledge_graph": "knowledge-graph.xml",
        "operational_packets": "operational-packets.xml",
    }.items():
        service.upsert_artifact(
            codex,
            project["id"],
            task["id"],
            artifact_type,
            f"<{artifact_type} />",
            f"grace/{filename}",
        )
    service.request_final_review(codex, task["id"])
    service.final_review(codex, task["id"], decision="accepted", findings=[], required_fixes=[])
    service.close_task(codex, task["id"])
    assert service.get_task(task["id"])["status"] == TaskStatus.TASK_CLOSED.value
    assert len(service.list_audit(task_id=task["id"])) >= 10


def test_codex_controller_repair_can_replace_unavailable_pro_submission(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    junior = _actor("mimo-auto-junior", OrchestratorRole.WORKER_JUNIOR)
    pro = _actor("mimo-2.5-pro", OrchestratorRole.WORKER_PRO)

    project = service.init_project(
        codex,
        name="demo",
        repo_path=tmp_path,
        grace_path=tmp_path / "grace",
        main_branch="main",
        allowed_test_commands={"unit": ["python", "-m", "pytest"]},
    )
    service.register_agent(
        codex,
        project["id"],
        name="codex",
        primary_role=OrchestratorRole.CODEX,
        capabilities=[OrchestratorRole.CODEX, OrchestratorRole.GLM, OrchestratorRole.TEST_OWNER],
    )
    service.register_agent(
        codex,
        project["id"],
        name=junior.name,
        primary_role=OrchestratorRole.WORKER_JUNIOR,
        capabilities=[OrchestratorRole.WORKER_JUNIOR],
        mimo_model="xiaomi/mimo-v2.5",
    )
    service.register_agent(
        codex,
        project["id"],
        name=pro.name,
        primary_role=OrchestratorRole.WORKER_PRO,
        capabilities=[OrchestratorRole.WORKER_PRO],
        mimo_model="xiaomi/mimo-v2.5-pro",
        availability="unavailable",
    )
    task = service.create_codex_task(
        codex,
        project["id"],
        title="controller repair",
        objective="repair a rejected package without paid pro worker",
        architecture_intent="Codex may submit audited repair evidence after failed worker handoff",
        constraints=["controller repair must preserve package scope"],
        non_goals=["no direct acceptance without submission"],
        acceptance_criteria=["package returns to GLM review"],
        allowed_files=["src/**"],
        forbidden_files=["tests/**"],
    )
    service.delegate_role(
        codex,
        project["id"],
        task["id"],
        unavailable_role=OrchestratorRole.GLM,
        substitute_actor="codex",
        delegated_role=OrchestratorRole.GLM,
        reason="GLM unavailable during bootstrap",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    service.plan_task(codex, task["id"])
    service.register_verification_plan(codex, task["id"], test_strategy="deterministic", test_commands=["unit"])
    package = service.create_work_package(
        codex,
        task["id"],
        title="package",
        objective="bounded implementation",
        allowed_files=["src/**"],
        forbidden_files=["tests/**"],
        assigned_junior_agent=junior.name,
        assigned_pro_agent=pro.name,
        base_commit="a" * 40,
        **packet_kwargs(),
    )
    service.assign_work_package(codex, package["id"])
    service.claim_work_package(junior, package["id"])
    service.submit_package(
        junior,
        package["id"],
        summary="junior implementation",
        evidence=SubmissionEvidence(
            base_commit="a" * 40,
            head_commit="b" * 40,
            diff="diff --git a/src/a.py b/src/a.py",
            diff_hash="b" * 64,
            files_changed=["src/a.py"],
        ),
        tests_run=[{"command_key": "unit", "exit_code": 0}],
        risk_notes="needs repair",
        worker_report=worker_report(task_id=task["id"], package_id=package["id"], files_changed=["src/a.py"]),
    )
    service.review_package(
        codex,
        package["id"],
        decision="rejected_repair_required",
        findings=["worker report contradicted diff"],
        required_fixes=["controller repair required"],
    )
    assert service.get_task(task["id"])["status"] == TaskStatus.GLM_REJECTED_REPAIR_REQUIRED.value

    with pytest.raises(OrchestratorError, match="not available"):
        service.claim_work_package(pro, package["id"])

    repair = service.submit_controller_repair(
        codex,
        package["id"],
        summary="controller repaired scope and tests",
        evidence=SubmissionEvidence(
            base_commit="a" * 40,
            head_commit="c" * 40,
            diff="diff --git a/src/a.py b/src/a.py",
            diff_hash="c" * 64,
            files_changed=["src/a.py"],
        ),
        tests_run=[{"command_key": "unit", "exit_code": 0}],
        risk_notes="Pro worker unavailable; Codex repaired under controller authority.",
        controller_report=worker_report(task_id=task["id"], package_id=package["id"], files_changed=["src/a.py"]),
    )

    assert repair["submitted_by_agent"] == "codex"
    assert service.get_work_package(package["id"])["status"] == WorkPackageStatus.SUBMITTED.value
    assert repair["handoff_event"]["type"] == "CONTROLLER_REPAIR_SUBMITTED"
    repeated_rejection = service.review_package(
        codex,
        package["id"],
        decision="rejected_repair_required",
        findings=["controller repair still missed the contract"],
        required_fixes=["repair again"],
    )
    assert repeated_rejection["decision"] == "rejected_repair_required"
    assert service.get_task(task["id"])["status"] == TaskStatus.GLM_REJECTED_REPAIR_REQUIRED.value

    repair = service.submit_controller_repair(
        codex,
        package["id"],
        summary="controller repaired scope and tests again",
        evidence=SubmissionEvidence(
            base_commit="a" * 40,
            head_commit="d" * 40,
            diff="diff --git a/src/a.py b/src/a.py",
            diff_hash="d" * 64,
            files_changed=["src/a.py"],
        ),
        tests_run=[{"command_key": "unit", "exit_code": 0}],
        risk_notes="Pro worker unavailable; Codex repaired under controller authority after repeated rejection.",
        controller_report=worker_report(task_id=task["id"], package_id=package["id"], files_changed=["src/a.py"]),
    )
    assert service.get_work_package(package["id"])["status"] == WorkPackageStatus.SUBMITTED.value
    service.review_package(codex, package["id"], decision="accepted", findings=[], required_fixes=[])
    assert service.get_task(task["id"])["status"] == TaskStatus.GLM_ACCEPTED.value


def test_cancelled_superseded_package_does_not_block_accepted_sibling(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    glm = _actor("glm-5.2", OrchestratorRole.GLM)
    junior = _actor("mimo-2.5", OrchestratorRole.WORKER_JUNIOR)
    pro = _actor("mimo-2.5-pro", OrchestratorRole.WORKER_PRO)
    project = service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {"unit": ["python", "-m", "pytest"]})
    service.register_agent(codex, project["id"], "glm-5.2", OrchestratorRole.GLM, [OrchestratorRole.GLM], mimo_model="zai-coding-plan/glm-5.2")
    service.register_agent(codex, project["id"], junior.name, OrchestratorRole.WORKER_JUNIOR, [OrchestratorRole.WORKER_JUNIOR], mimo_model="xiaomi/mimo-v2.5")
    service.register_agent(codex, project["id"], pro.name, OrchestratorRole.WORKER_PRO, [OrchestratorRole.WORKER_PRO], mimo_model="xiaomi/mimo-v2.5-pro")
    task = service.create_codex_task(codex, project["id"], "superseded package", "replace stale v1", "ledger owns package lifecycle", [], [], ["v2 accepted"], ["src/**"], ["tests/**"])
    service.plan_task(glm, task["id"])
    service.register_verification_plan(glm, task["id"], test_strategy="unit", test_commands=["unit"])
    stale = service.create_work_package(
        glm,
        task["id"],
        title="v1 stale",
        objective="old packet",
        allowed_files=["src/a.py"],
        forbidden_files=["tests/**"],
        assigned_junior_agent=junior.name,
        assigned_pro_agent=pro.name,
        base_commit="a" * 40,
        **{**packet_kwargs(), "cache_anchor": "stale"},
    )
    replacement = service.create_work_package(
        glm,
        task["id"],
        title="v2 replacement",
        objective="replacement packet",
        allowed_files=["src/b.py"],
        forbidden_files=["tests/**"],
        assigned_junior_agent=junior.name,
        assigned_pro_agent=pro.name,
        base_commit="a" * 40,
        **{**packet_kwargs(), "cache_anchor": "replacement"},
    )
    service.assign_work_package(glm, stale["id"])
    service.assign_work_package(glm, replacement["id"])
    service.claim_work_package(junior, replacement["id"])
    service.submit_package(
        junior,
        replacement["id"],
        summary="implemented replacement",
        evidence=SubmissionEvidence(
            base_commit="a" * 40,
            head_commit="b" * 40,
            diff="diff --git a/src/b.py b/src/b.py",
            diff_hash="b" * 64,
            files_changed=["src/b.py"],
        ),
        tests_run=[{"command_key": "unit", "exit_code": 0}],
        risk_notes="none",
        worker_report=worker_report(task_id=task["id"], package_id=replacement["id"], files_changed=["src/b.py"]),
    )
    service.review_package(glm, replacement["id"], decision="accepted", findings=[], required_fixes=[])
    assert service.acceptance_review_gate(codex, task["id"])["status"] == "blocked"

    cancelled = service.cancel_work_package(glm, stale["id"], "superseded by replacement package")

    assert cancelled["status"] == WorkPackageStatus.CANCELLED.value
    assert service.get_task(task["id"])["status"] == TaskStatus.GLM_ACCEPTED.value
    assert service.acceptance_review_gate(codex, task["id"])["status"] == "pass"


def test_controller_owned_task_completion_can_reach_final_review_without_package(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    glm = _actor("glm-5.2", OrchestratorRole.GLM)
    project = service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {})
    task = service.create_codex_task(
        codex,
        project["id"],
        title="controller slice",
        objective="close a controller-owned test-owner slice",
        architecture_intent="Codex acts as temporary GLM/test-owner fallback without a worker package",
        constraints=["no junior substitution"],
        non_goals=["no fake worker submission"],
        acceptance_criteria=["controller evidence is audited"],
        allowed_files=["src/**"],
        forbidden_files=["forbidden/**"],
    )
    service.plan_task(glm, task["id"])
    blocked_gate = service.acceptance_review_gate(codex, task["id"])
    assert blocked_gate["status"] == "blocked"
    assert "audited controller task completion" in blocked_gate["issues"][0]

    completion = service.submit_controller_task_completion(
        codex,
        task["id"],
        summary="controller-owned slice implemented and verified",
        evidence=SubmissionEvidence(
            base_commit="a" * 40,
            head_commit="b" * 40,
            diff="diff --git a/src/a.py b/src/a.py",
            diff_hash="b" * 64,
            files_changed=["src/a.py"],
        ),
        tests_run=[{"command_key": "manual", "exit_code": 0}],
        risk_notes="No worker package required; Codex owns protected tests and final acceptance.",
        controller_report=worker_report(task_id=task["id"], package_id=0, files_changed=["src/a.py"]),
    )

    assert completion["decision"] == "controller_completed"
    assert completion["controller_completion"]["evidence"]["files_changed"] == ["src/a.py"]
    assert service.get_task(task["id"])["status"] == TaskStatus.GLM_ACCEPTED.value
    assert service.acceptance_review_gate(codex, task["id"])["status"] == "pass"
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
            project["id"],
            task["id"],
            artifact_type,
            f"<{artifact_type} />",
            f"grace/{filename}",
        )
    service.request_final_review(codex, task["id"])
    service.final_review(codex, task["id"], decision="accepted", findings=[], required_fixes=[])
    assert service.get_task(task["id"])["status"] == TaskStatus.TASK_CLOSED.value


def test_controller_owned_task_completion_cannot_bypass_existing_package(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    glm = _actor("glm", OrchestratorRole.GLM)
    project = service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {"unit": ["python", "-m", "pytest"]})
    service.register_agent(codex, project["id"], "junior", OrchestratorRole.WORKER_JUNIOR, [OrchestratorRole.WORKER_JUNIOR], mimo_model="xiaomi/mimo-v2.5")
    service.register_agent(codex, project["id"], "pro", OrchestratorRole.WORKER_PRO, [OrchestratorRole.WORKER_PRO], mimo_model="xiaomi/mimo-v2.5-pro")
    task = service.create_codex_task(codex, project["id"], "t", "o", "i", [], [], [], ["src/**"], [])
    service.plan_task(glm, task["id"])
    service.register_verification_plan(glm, task["id"], "deterministic", ["unit"])
    service.create_work_package(
        glm,
        task["id"],
        title="package",
        objective="bounded implementation",
        allowed_files=["src/**"],
        forbidden_files=["tests/**"],
        assigned_junior_agent="junior",
        assigned_pro_agent="pro",
        base_commit="a" * 40,
        **packet_kwargs(),
    )

    with pytest.raises(OrchestratorError, match="only allowed when the task has no work packages"):
        service.submit_controller_task_completion(
            codex,
            task["id"],
            summary="invalid bypass",
            evidence=SubmissionEvidence(
                base_commit="a" * 40,
                head_commit="b" * 40,
                diff="diff --git a/src/a.py b/src/a.py",
                diff_hash="b" * 64,
                files_changed=["src/a.py"],
            ),
            tests_run=[{"command_key": "unit", "exit_code": 0}],
            risk_notes="should fail",
            controller_report=worker_report(task_id=task["id"], package_id=0, files_changed=["src/a.py"]),
        )


def test_junior_cannot_be_registered_as_fallback_capable(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    project = service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {})

    with pytest.raises(OrchestratorError, match="Junior agents cannot be registered"):
        service.register_agent(
            codex,
            project["id"],
            "mimo-auto-junior",
            OrchestratorRole.WORKER_JUNIOR,
            [OrchestratorRole.WORKER_JUNIOR, OrchestratorRole.GLM],
        )


def test_pro_worker_can_plan_when_explicitly_delegated_for_glm_substitution(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    pro = _actor("mimo-2.5-pro", OrchestratorRole.WORKER_PRO)
    project = service.init_project(
        codex,
        "demo",
        tmp_path,
        tmp_path / "grace",
        "main",
        {"unit": ["python", "-m", "pytest"]},
    )
    service.register_agent(
        codex,
        project["id"],
        "glm-5.2",
        OrchestratorRole.GLM,
        [OrchestratorRole.GLM, OrchestratorRole.TEST_OWNER],
        availability="unavailable",
        mimo_model="zai-coding-plan/glm-5.2",
    )
    service.register_agent(
        codex,
        project["id"],
        pro.name,
        OrchestratorRole.WORKER_PRO,
        [OrchestratorRole.WORKER_PRO, OrchestratorRole.GLM, OrchestratorRole.TEST_OWNER],
        mimo_model="xiaomi/mimo-v2.5-pro",
    )
    task = service.create_codex_task(
        codex,
        project["id"],
        "pro substitution",
        "allow Pro to plan only under explicit GLM substitution",
        "delegation ledger owns authority",
        ["GLM unavailable"],
        ["silent role promotion"],
        ["delegated plan accepted"],
        ["src/**"],
        [],
    )
    service.delegate_role(
        codex,
        project["id"],
        task["id"],
        unavailable_role=OrchestratorRole.GLM,
        substitute_actor=pro.name,
        delegated_role=OrchestratorRole.GLM,
        reason="GLM unavailable; paid MiMo Pro is explicit substitute planner",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    service.delegate_role(
        codex,
        project["id"],
        task["id"],
        unavailable_role=OrchestratorRole.TEST_OWNER,
        substitute_actor=pro.name,
        delegated_role=OrchestratorRole.TEST_OWNER,
        reason="GLM unavailable; paid MiMo Pro is explicit substitute test owner",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )

    planned = service.plan_task(pro, task["id"])
    service.register_verification_plan(
        pro,
        task["id"],
        test_strategy="delegated Pro substitution",
        test_commands=["unit"],
    )

    assert planned["status"] == TaskStatus.GLM_GRACE_PLANNED.value
    assert service.get_task(task["id"])["status"] == TaskStatus.GLM_TESTS_PREPARED.value
    assert any(event["event_type"] == "role.delegated" for event in service.list_audit())


def test_final_review_before_package_acceptance_is_rejected(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    project = service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {})
    task = service.create_codex_task(codex, project["id"], "t", "o", "i", [], [], [], ["src/**"], [])
    with pytest.raises(OrchestratorError, match="requires GLM acceptance"):
        service.request_final_review(codex, task["id"])


def test_work_package_creation_requires_passing_contract_discovery(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    glm = _actor("glm", OrchestratorRole.GLM)
    project = service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {"unit": ["python", "-m", "pytest"]})
    service.register_agent(codex, project["id"], glm.name, OrchestratorRole.GLM, [OrchestratorRole.GLM])
    service.register_agent(codex, project["id"], "junior", OrchestratorRole.WORKER_JUNIOR, [OrchestratorRole.WORKER_JUNIOR], mimo_model="xiaomi/mimo-v2.5")
    service.register_agent(codex, project["id"], "pro", OrchestratorRole.WORKER_PRO, [OrchestratorRole.WORKER_PRO], mimo_model="xiaomi/mimo-v2.5-pro")
    task = service.create_codex_task(codex, project["id"], "t", "o", "i", [], [], [], ["src/**"], ["tests/**"])
    service.plan_task(glm, task["id"])
    service.register_verification_plan(glm, task["id"], "deterministic", ["unit"])

    with pytest.raises(OrchestratorError, match="Contract discovery gate"):
        service.create_work_package(
            glm,
            task["id"],
            "package",
            "bounded work",
            ["src/**"],
            [],
            "junior",
            "pro",
            "a" * 40,
            contract_discovery={"status": "blocked", "issues": ["missing contracts"]},
            test_surface=["unit"],
            rollback_boundary="revert package scope",
            compact_report_format=["task id"],
            module_id="M-ORCH-LEDGER",
            verification_id="V-M-ORCH-LEDGER",
            commands_allowed=["unit"],
            stop_conditions=["scope drift"],
        )


def test_submission_requires_worker_report_validation(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    glm = _actor("glm", OrchestratorRole.GLM)
    junior = _actor("junior", OrchestratorRole.WORKER_JUNIOR)
    project = service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {"unit": ["python", "-m", "pytest"]})
    service.register_agent(codex, project["id"], glm.name, OrchestratorRole.GLM, [OrchestratorRole.GLM])
    service.register_agent(codex, project["id"], junior.name, OrchestratorRole.WORKER_JUNIOR, [OrchestratorRole.WORKER_JUNIOR], mimo_model="xiaomi/mimo-v2.5")
    service.register_agent(codex, project["id"], "pro", OrchestratorRole.WORKER_PRO, [OrchestratorRole.WORKER_PRO], mimo_model="xiaomi/mimo-v2.5-pro")
    task = service.create_codex_task(codex, project["id"], "t", "o", "i", [], [], [], ["src/**"], ["tests/**"])
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
        "pro",
        "a" * 40,
        **packet_kwargs(),
    )
    service.assign_work_package(glm, package["id"])
    service.claim_work_package(junior, package["id"])

    with pytest.raises(OrchestratorError, match="Worker report validation"):
        service.submit_package(
            junior,
            package["id"],
            "worker result",
            SubmissionEvidence(
                base_commit="a" * 40,
                head_commit="b" * 40,
                diff="diff --git a/src/a.py b/src/a.py",
                diff_hash="f" * 64,
                files_changed=["src/a.py"],
            ),
            [{"command_key": "unit", "exit_code": 0}],
            "none",
            worker_report={"task_id": task["id"], "files_changed": ["src/a.py"]},
        )


def test_audit_log_rejects_mutation(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {})

    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        service.store.connection.execute("UPDATE audit_log SET event_type = 'tampered' WHERE id = 1")
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        service.store.connection.execute("DELETE FROM audit_log WHERE id = 1")


def test_fallback_rejects_an_incapable_or_unnecessary_substitute(tmp_path: Path) -> None:
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = _actor("codex", OrchestratorRole.CODEX)
    project = service.init_project(codex, "demo", tmp_path, tmp_path / "grace", "main", {})

    with pytest.raises(OrchestratorError, match="lacks capability glm"):
        service.delegate_role(
            codex,
            project["id"],
            None,
            unavailable_role=OrchestratorRole.GLM,
            substitute_actor="codex",
            delegated_role=OrchestratorRole.GLM,
            reason="bootstrap",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

    service.register_agent(
        codex,
        project["id"],
        name="glm-5.2",
        primary_role=OrchestratorRole.GLM,
        capabilities=[OrchestratorRole.GLM],
    )
    service.register_agent(
        codex,
        project["id"],
        name="codex",
        primary_role=OrchestratorRole.CODEX,
        capabilities=[OrchestratorRole.CODEX, OrchestratorRole.GLM],
    )
    with pytest.raises(OrchestratorError, match="assigned available agent"):
        service.delegate_role(
            codex,
            project["id"],
            None,
            unavailable_role=OrchestratorRole.GLM,
            substitute_actor="codex",
            delegated_role=OrchestratorRole.GLM,
            reason="bootstrap",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )
