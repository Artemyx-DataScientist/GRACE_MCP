from grace_orchestrator.policy import validate_worker_report
from conftest import worker_report


def test_worker_report_distinguishes_empty_required_field_from_missing() -> None:
    report = worker_report(task_id=1, package_id=2, files_changed=["src/a.py"])
    report["stop_conditions_encountered"] = []

    result = validate_worker_report(
        report,
        task_id=1,
        work_package_id=2,
        allowed_files=["src/**"],
        forbidden_files=["tests/**"],
        evidence_files=["src/a.py"],
    )

    assert result["status"] == "blocked"
    assert (
        "Worker report field is present but empty: stop conditions encountered."
        in result["issues"][0]
    )
    assert "missing required field: stop conditions encountered" not in result["issues"][0]
