"""Tests for OS Process Identity and Tri-State Liveness Detection Protocol."""

import os
from pathlib import Path
import sys

from grace_orchestrator.process_identity import (
    ProcessIdentity,
    ProcessMatchState,
    calculate_argv_hash,
    capture_process_identity,
    verify_process_liveness,
)


def test_argv_hash_stability():
    h1 = calculate_argv_hash(["python", "-m", "grace_orchestrator", "--data-dir", "./data"])
    h2 = calculate_argv_hash(["python", "-m", "grace_orchestrator", "--data-dir", "./data"])
    assert h1 == h2
    assert len(h1) == 16


def test_capture_and_verify_current_process():
    pid = os.getpid()
    identity = capture_process_identity(pid, sys.executable, sys.argv)

    assert identity.pid == pid
    assert identity.executable_path == str(Path(sys.executable).resolve())
    assert identity.argv_hash == calculate_argv_hash(sys.argv)

    # Liveness check on active running process should MATCH
    match_result = verify_process_liveness(identity)
    assert match_result == ProcessMatchState.MATCH


def test_liveness_detects_non_existent_pid():
    bogus_pid = 999_999
    identity = ProcessIdentity(
        pid=bogus_pid,
        process_started_at_os=12345678,
        executable_path="/usr/bin/python",
        argv_hash="abc123hash",
        launch_nonce="nonce123",
    )
    result = verify_process_liveness(identity)
    assert result == ProcessMatchState.NOT_FOUND_OR_REUSED


def test_liveness_detects_pid_reuse_time_mismatch():
    pid = os.getpid()

    # Capture identity with fake start time
    identity = ProcessIdentity(
        pid=pid,
        process_started_at_os=1111111111,
        executable_path=str(Path(sys.executable).resolve()),
        argv_hash=calculate_argv_hash(sys.argv),
        launch_nonce="nonce_fake",
    )

    result = verify_process_liveness(identity)
    assert result == ProcessMatchState.NOT_FOUND_OR_REUSED
