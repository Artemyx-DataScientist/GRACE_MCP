import sys
from pathlib import Path
import subprocess

import pytest

# FILE: tests/test_repo_paths.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Verify root confinement and registered-test command rejection.
#   SCOPE: Path traversal and unknown test-key failures.
#   DEPENDS: M-ORCH-REPO-BOUNDARY
#   LINKS: M-ORCH-REPO-BOUNDARY, V-M-ORCH-REPO-BOUNDARY
#   ROLE: TEST
#   MAP_MODE: LOCALS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   test_* - project-root and command allowlist failure scenarios.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.1.0 - Added root-confinement and command-key coverage.
# END_CHANGE_SUMMARY

from grace_orchestrator.models import OrchestratorError
from grace_orchestrator.repo import RepositoryBoundary, resolve_within_root


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


def test_path_must_resolve_under_project_root(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    assert resolve_within_root(root, "logs/test.stdout") == root / "logs" / "test.stdout"

    with pytest.raises(OrchestratorError, match="outside project root"):
        resolve_within_root(root, "../escape.txt")


def test_unknown_test_command_is_rejected_before_execution(tmp_path: Path) -> None:
    boundary = RepositoryBoundary(tmp_path)
    with pytest.raises(OrchestratorError, match="not allowlisted"):
        boundary.run_allowed_test("unknown", {"safe": [sys.executable, "-c", "print('ok')"]})


def test_submission_evidence_is_derived_from_validated_git_commits(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init")
    _git(root, "config", "user.email", "orchestrator@example.invalid")
    _git(root, "config", "user.name", "Orchestrator Test")
    source = root / "src" / "worker.py"
    source.parent.mkdir()
    source.write_text("value = 1\n", encoding="utf-8")
    _git(root, "add", "src/worker.py")
    _git(root, "commit", "-m", "base")
    base = _git(root, "rev-parse", "HEAD")
    source.write_text("value = 2\n", encoding="utf-8")
    _git(root, "add", "src/worker.py")
    _git(root, "commit", "-m", "worker change")
    head = _git(root, "rev-parse", "HEAD")

    evidence = RepositoryBoundary(root).derive_submission(base, head)

    assert evidence.base_commit == base
    assert evidence.head_commit == head
    assert evidence.files_changed == ["src/worker.py"]
    assert "value = 2" in evidence.diff
    assert len(evidence.diff_hash) == 64
