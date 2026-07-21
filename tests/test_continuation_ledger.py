"""Tests for continuation delivery ledger, ACK/RESOLVED tools, lease recovery, and attempt_id matching."""

import pytest

from grace_orchestrator.models import ActorIdentity, ConflictError, OrchestratorRole
from grace_orchestrator.service import OrchestratorService
from grace_orchestrator.host_continuation import HostContinuationConfig, HostContinuationSupervisor


def test_continuation_ack_and_resolve_lifecycle(tmp_path):
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = ActorIdentity(name="codex", primary_role=OrchestratorRole.CODEX)

    cont_id = "cont_test_123"
    source_evt = "evt_run1_0"
    attempt_id = "att_test_1"
    with service.store.transaction() as conn:
        conn.execute(
            """INSERT INTO continuation_deliveries (
                continuation_id, run_id, source_event_id, state, attempt_count, attempt_id, next_attempt_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (cont_id, "run1", source_evt, "CONTROLLER_STARTED", 1, attempt_id, "2026-06-20T00:00:00+00:00", "2026-06-20T00:00:00+00:00"),
        )

    record = service.get_continuation(cont_id)
    assert record["state"] == "CONTROLLER_STARTED"
    assert record["source_event_id"] == source_evt

    # Acknowledge continuation with matching attempt_id
    acked = service.ack_continuation(codex, cont_id, source_evt, attempt_id=attempt_id, controller_session_id="sess_1")
    assert acked["state"] == "ACKNOWLEDGED"
    assert acked["controller_session_id"] == "sess_1"

    # Idempotent ACK returns same record
    acked_again = service.ack_continuation(codex, cont_id, source_evt, attempt_id=attempt_id, controller_session_id="sess_1")
    assert acked_again["state"] == "ACKNOWLEDGED"

    # Resolve continuation
    resolved = service.resolve_continuation(codex, cont_id, source_evt, attempt_id=attempt_id, resolution_notes="Task completed cleanly")
    assert resolved["state"] == "RESOLVED"
    assert resolved["resolved_at"] is not None


def test_continuation_attempt_id_mismatch(tmp_path):
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = ActorIdentity(name="codex", primary_role=OrchestratorRole.CODEX)

    cont_id = "cont_test_att_mismatch"
    with service.store.transaction() as conn:
        conn.execute(
            """INSERT INTO continuation_deliveries (
                continuation_id, run_id, source_event_id, state, attempt_count, attempt_id, next_attempt_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (cont_id, "run1", "evt_1", "CONTROLLER_STARTED", 2, "att_current", "2026-06-20T00:00:00+00:00", "2026-06-20T00:00:00+00:00"),
        )

    # Late ACK from old attempt is rejected
    with pytest.raises(ConflictError, match="attempt_id mismatch"):
        service.ack_continuation(codex, cont_id, "evt_1", attempt_id="att_old_expired")


def test_continuation_lease_recovery(tmp_path):
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    supervisor = HostContinuationSupervisor(HostContinuationConfig(data_dir=tmp_path))

    # Insert an expired CLAIMED delivery (supervisor crashed before launch)
    with service.store.transaction() as conn:
        conn.execute(
            """INSERT INTO continuation_deliveries (
                continuation_id, run_id, source_event_id, state, attempt_count, attempt_id, lease_expires_at, next_attempt_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("cont_claimed_expired", "run1", "evt_c1", "CLAIMED", 1, "att_1", "2020-01-01T00:00:00+00:00", "2020-01-01T00:00:00+00:00", "2020-01-01T00:00:00+00:00"),
        )

    # Insert an expired CONTROLLER_STARTED delivery with dead PID
    with service.store.transaction() as conn:
        conn.execute(
            """INSERT INTO continuation_deliveries (
                continuation_id, run_id, source_event_id, state, attempt_count, attempt_id, controller_pid, lease_expires_at, next_attempt_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("cont_started_expired", "run1", "evt_s1", "CONTROLLER_STARTED", 1, "att_2", 999999, "2020-01-01T00:00:00+00:00", "2020-01-01T00:00:00+00:00", "2020-01-01T00:00:00+00:00"),
        )

    recovered = supervisor._recover_expired_leases(service.store)
    assert len(recovered) == 2

    r1 = service.get_continuation("cont_claimed_expired")
    assert r1["state"] == "RETRY_WAIT"

    r2 = service.get_continuation("cont_started_expired")
    assert r2["state"] == "RETRY_WAIT"


def test_deterministic_legacy_event_identity(tmp_path):
    runs_dir = tmp_path / "runs" / "run1"
    runs_dir.mkdir(parents=True)
    events_path = runs_dir / "events.ndjson"
    events_path.write_text('{"type": "WORKER_READY_FOR_REVIEW", "payload": {}}\n', encoding="utf-8")

    supervisor = HostContinuationSupervisor(HostContinuationConfig(data_dir=tmp_path))
    events = supervisor._read_events(events_path, after_index=0)
    assert len(events) == 1
    idx, evt = events[0]
    assert "legacy_evt_" in evt["event_id"]

    events2 = supervisor._read_events(events_path, after_index=0)
    assert events2[0][1]["event_id"] == evt["event_id"]
