import pytest

# FILE: tests/test_scope_validation.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Verify M-ORCH-REPO-BOUNDARY rejects out-of-scope and forbidden diff files.
#   SCOPE: Actual changed-file scope validation.
#   DEPENDS: M-ORCH-REPO-BOUNDARY
#   LINKS: M-ORCH-REPO-BOUNDARY, V-M-ORCH-REPO-BOUNDARY
#   ROLE: TEST
#   MAP_MODE: LOCALS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   test_submission_scope_checks_actual_changed_files - scope and forbidden-file assertions.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.1.0 - Added derived-diff scope checks.
# END_CHANGE_SUMMARY

from grace_orchestrator.models import OrchestratorError
from grace_orchestrator.repo import validate_scoped_files


def test_submission_scope_checks_actual_changed_files() -> None:
    validate_scoped_files(
        ["src/orchestrator.py", "src/models.py"],
        allowed_files=["src/**"],
        forbidden_files=["tests/**"],
    )

    with pytest.raises(OrchestratorError, match="outside allowed scope"):
        validate_scoped_files(
            ["src/orchestrator.py", "README.md"],
            allowed_files=["src/**"],
            forbidden_files=[],
        )

    with pytest.raises(OrchestratorError, match="forbidden file"):
        validate_scoped_files(
            ["src/orchestrator.py", "tests/test_orchestrator.py"],
            allowed_files=["**"],
            forbidden_files=["tests/**"],
        )
