"""Root-confined Git and test-command boundary for the orchestrator."""

# FILE: src/grace_orchestrator/repo.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Enforce M-ORCH-REPO-BOUNDARY project confinement, commit evidence, and command allowlists.
#   SCOPE: Path validation plus fixed-argv Git and registered test subprocesses.
#   DEPENDS: M-ORCH-DOMAIN
#   LINKS: M-ORCH-REPO-BOUNDARY, V-M-ORCH-REPO-BOUNDARY, type-RepositoryBoundary
#   ROLE: RUNTIME
#   MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   logger - stable repository-boundary telemetry sink.
#   resolve_within_root - rejects path escape before filesystem use.
#   validate_scoped_files - validates server-derived changed files against package scope.
#   RepositoryBoundary - fixed-argv Git/test boundary with shell disabled.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.2.0 - Added fixed-argv detached worktree provisioning for Mimo sessions.
# END_CHANGE_SUMMARY

from __future__ import annotations

from fnmatch import fnmatchcase
from hashlib import sha256
import logging
from pathlib import Path, PurePosixPath
import subprocess
from typing import Mapping, Sequence

from .models import OrchestratorError, SubmissionEvidence, TestRunResult

logger = logging.getLogger(__name__)


def resolve_within_root(project_root: Path, candidate: str | Path) -> Path:
    # START_CONTRACT: resolve_within_root
    #   PURPOSE: Resolve a path only when it remains under the registered project root.
    #   INPUTS: { project_root: Path, candidate: str|Path }
    #   OUTPUTS: { Path - normalized in-root path }
    #   SIDE_EFFECTS: Reads filesystem path resolution only.
    #   LINKS: M-ORCH-REPO-BOUNDARY, V-M-ORCH-REPO-BOUNDARY
    # END_CONTRACT: resolve_within_root
    # START_BLOCK_REJECT_PROJECT_PATH_ESCAPE
    root = project_root.resolve()
    raw_path = Path(candidate)
    resolved = raw_path.resolve() if raw_path.is_absolute() else (root / raw_path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise OrchestratorError(f"Path outside project root: {candidate}") from error
    logger.info("[GraceOrchestrator][repository][PROJECT_ROOT_VALIDATION] accepted path=%s", resolved)
    return resolved
    # END_BLOCK_REJECT_PROJECT_PATH_ESCAPE


def _matches(path: str, patterns: Sequence[str]) -> bool:
    return any(fnmatchcase(path, pattern) for pattern in patterns)


def validate_scoped_files(
    files_changed: Sequence[str],
    *,
    allowed_files: Sequence[str],
    forbidden_files: Sequence[str],
) -> None:
    # START_CONTRACT: validate_scoped_files
    #   PURPOSE: Reject derived submission files that escape allowed or forbidden package patterns.
    #   INPUTS: { files_changed: paths, allowed_files: patterns, forbidden_files: patterns }
    #   OUTPUTS: { None - returns only for allowed scope }
    #   SIDE_EFFECTS: Raises OrchestratorError on scope violation.
    #   LINKS: M-ORCH-REPO-BOUNDARY, V-M-ORCH-REPO-BOUNDARY
    # END_CONTRACT: validate_scoped_files
    # START_BLOCK_VALIDATE_DERIVED_SUBMISSION_SCOPE
    for raw_file in files_changed:
        path = PurePosixPath(raw_file)
        if path.is_absolute() or ".." in path.parts:
            raise OrchestratorError(f"Submission path is not project-relative: {raw_file}")
        normalized = path.as_posix()
        if _matches(normalized, forbidden_files):
            raise OrchestratorError(f"Submission changed forbidden file: {normalized}")
        if not _matches(normalized, allowed_files):
            raise OrchestratorError(f"Submission changed file outside allowed scope: {normalized}")
    # END_BLOCK_VALIDATE_DERIVED_SUBMISSION_SCOPE


class RepositoryBoundary:
    """Runs fixed argv vectors only; raw shell text never crosses this boundary."""

    def __init__(self, project_root: Path, log_root: Path | None = None) -> None:
        self.project_root = project_root.resolve()
        self.log_root = (log_root or self.project_root / ".grace-orchestrator" / "logs").resolve()

    def _run(self, argv: Sequence[str], timeout_seconds: float | None = None) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                list(argv),
                cwd=self.project_root,
                shell=False,
                check=False,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired as error:
            raise OrchestratorError(f"Command timed out after {timeout_seconds:g}s: {' '.join(argv)}") from error

    def _git(self, *args: str, timeout_seconds: float = 120.0) -> str:
        result = self._run(["git", *args], timeout_seconds=timeout_seconds)
        if result.returncode != 0:
            raise OrchestratorError(f"Git command failed: {' '.join(args)}: {result.stderr.strip()}")
        return result.stdout

    def derive_submission(self, base_commit: str, head_commit: str) -> SubmissionEvidence:
        # START_CONTRACT: RepositoryBoundary.derive_submission
        #   PURPOSE: Derive commit-pair evidence instead of trusting worker-provided diff text.
        #   INPUTS: { base_commit: str, head_commit: str }
        #   OUTPUTS: { SubmissionEvidence - files, diff, and content hash }
        #   SIDE_EFFECTS: Runs fixed Git argv under registered project root.
        #   LINKS: M-ORCH-REPO-BOUNDARY, V-M-ORCH-REPO-BOUNDARY
        # END_CONTRACT: RepositoryBoundary.derive_submission
        # START_BLOCK_DERIVE_COMMIT_PAIR_EVIDENCE
        self._git("rev-parse", "--verify", f"{base_commit}^{{commit}}")
        self._git("rev-parse", "--verify", f"{head_commit}^{{commit}}")
        ancestor = self._run(["git", "merge-base", "--is-ancestor", base_commit, head_commit])
        if ancestor.returncode != 0:
            raise OrchestratorError("Submission base commit must be an ancestor of head commit")
        changed_raw = self._git("diff", "--name-only", "-z", "--no-ext-diff", base_commit, head_commit)
        files_changed = [item for item in changed_raw.split("\0") if item]
        diff = self._git("diff", "--binary", "--no-ext-diff", base_commit, head_commit)
        logger.info("[GraceOrchestrator][repository][DERIVE_SUBMISSION_DIFF] derived submission", extra={"base_commit": base_commit, "head_commit": head_commit, "file_count": len(files_changed)})
        return SubmissionEvidence(
            base_commit=base_commit,
            head_commit=head_commit,
            diff=diff,
            diff_hash=sha256(diff.encode("utf-8")).hexdigest(),
            files_changed=files_changed,
        )
        # END_BLOCK_DERIVE_COMMIT_PAIR_EVIDENCE

    def create_detached_worktree(self, destination: Path, base_commit: str) -> Path:
        # START_CONTRACT: RepositoryBoundary.create_detached_worktree
        #   PURPOSE: Create one isolated package worktree from a validated base commit.
        #   INPUTS: { destination: Path - server-generated non-project directory, base_commit: Git commit }
        #   OUTPUTS: { Path - created detached worktree }
        #   SIDE_EFFECTS: Runs fixed Git worktree argv under registered project root.
        #   LINKS: M-ORCH-REPO-BOUNDARY, M-ORCH-MIMO-EXECUTOR, V-M-ORCH-MIMO-EXECUTOR
        # END_CONTRACT: RepositoryBoundary.create_detached_worktree
        # START_BLOCK_PROVISION_ISOLATED_MIMO_WORKTREE
        resolved = destination.resolve()
        try:
            resolved.relative_to(self.project_root)
        except ValueError:
            pass
        else:
            raise OrchestratorError("Mimo worktree must be outside the registered project root")
        if resolved.exists():
            raise OrchestratorError(f"Mimo worktree destination already exists: {resolved}")
        self._git("rev-parse", "--verify", f"{base_commit}^{{commit}}")
        resolved.parent.mkdir(parents=True, exist_ok=True)
        self._git("worktree", "add", "--detach", str(resolved), base_commit)
        logger.info(
            "[GraceOrchestrator][repository][MIMO_WORKTREE_PROVISION] created detached worktree",
            extra={"base_commit": base_commit, "workspace_path": str(resolved)},
        )
        return resolved
        # END_BLOCK_PROVISION_ISOLATED_MIMO_WORKTREE

    def status(self) -> dict[str, object]:
        raw = self._git("status", "--porcelain=v1", "--branch")
        lines = raw.splitlines()
        branch = lines[0].removeprefix("## ") if lines else ""
        changed = lines[1:]
        return {"branch": branch, "changes": changed}

    def diff(self, scope: str = "all", files: Sequence[str] | None = None) -> str:
        if scope == "all":
            return self._git("diff", "--binary", "--no-ext-diff")
        if scope == "staged":
            return self._git("diff", "--binary", "--cached", "--no-ext-diff")
        if scope == "file_list":
            if not files:
                raise OrchestratorError("file_list diff requires at least one project-relative file")
            normalized = [str(resolve_within_root(self.project_root, item).relative_to(self.project_root)) for item in files]
            return self._git("diff", "--binary", "--no-ext-diff", "--", *normalized)
        raise OrchestratorError(f"Unsupported diff scope: {scope}")

    def run_allowed_test(
        self,
        command_key: str,
        allowed_commands: Mapping[str, Sequence[str]],
    ) -> TestRunResult:
        # START_CONTRACT: RepositoryBoundary.run_allowed_test
        #   PURPOSE: Execute only a registered test argv vector with shell disabled.
        #   INPUTS: { command_key: str, allowed_commands: command registry }
        #   OUTPUTS: { TestRunResult - exit code and persisted stdout/stderr paths }
        #   SIDE_EFFECTS: Starts allowlisted child process and writes evidence logs.
        #   LINKS: M-ORCH-REPO-BOUNDARY, V-M-ORCH-REPO-BOUNDARY
        # END_CONTRACT: RepositoryBoundary.run_allowed_test
        # START_BLOCK_EXECUTE_REGISTERED_TEST_COMMAND
        argv = allowed_commands.get(command_key)
        if not argv:
            raise OrchestratorError(f"Test command {command_key!r} is not allowlisted")
        if any(not isinstance(part, str) or not part for part in argv):
            raise OrchestratorError(f"Test command {command_key!r} has invalid argv")
        logger.info("[GraceOrchestrator][repository][ALLOWLISTED_TEST_RUN] command approved", extra={"command_key": command_key})
        result = self._run(argv)
        self.log_root.mkdir(parents=True, exist_ok=True)
        run_hash = sha256("\0".join(argv).encode("utf-8")).hexdigest()[:16]
        stdout_path = self.log_root / f"{command_key}-{run_hash}.stdout.log"
        stderr_path = self.log_root / f"{command_key}-{run_hash}.stderr.log"
        stdout_path.write_text(result.stdout, encoding="utf-8")
        stderr_path.write_text(result.stderr, encoding="utf-8")
        return TestRunResult(command_key, result.returncode, stdout_path, stderr_path)
        # END_BLOCK_EXECUTE_REGISTERED_TEST_COMMAND
