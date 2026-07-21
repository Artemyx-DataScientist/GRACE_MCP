"""Tests for continuation delivery ledger, ACK/RESOLVED tools, and scan cursor separation."""

import pytest

from grace_orchestrator.models import ActorIdentity, OrchestratorError, OrchestratorRole
from grace_orchestrator.service import OrchestratorService


def test_continuation_ack_and_resolve_lifecycle(tmp_path):
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = ActorIdentity(name="codex", primary_role=OrchestratorRole.CODEX)

    # Insert test continuation delivery record directly
    cont_id = "cont_test_123"
    source_evt = "evt_run1_0"
    with service.store.transaction() as conn:
        conn.execute(
            """INSERT INTO continuation_deliveries (
                continuation_id, run_id, source_event_id, state, attempt_count, next_attempt_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (cont_id, "run1", source_evt, "PENDING", 1, "2026-06-20T00:00:00+00:00", "2026-06-20T00:00:00+00:00"),
        )

    # Fetch initial continuation
    record = service.get_continuation(cont_id)
    assert record["state"] == "PENDING"
    assert record["source_event_id"] == source_evt

    # Acknowledge continuation
    acked = service.ack_continuation(codex, cont_id, source_evt, controller_session_id="sess_1")
    assert acked["state"] == "ACKNOWLEDGED"
    assert acked["controller_session_id"] == "sess_1"

    # Idempotent ACK returns same record
    acked_again = service.ack_continuation(codex, cont_id, source_evt, controller_session_id="sess_1")
    assert acked_again["state"] == "ACKNOWLEDGED"

    # Resolve continuation
    resolved = service.resolve_continuation(codex, cont_id, source_evt, resolution_notes="Task completed cleanly")
    assert resolved["state"] == "RESOLVED"
    assert resolved["resolved_at"] is not None


def test_continuation_ack_rejects_source_event_mismatch(tmp_path):
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = ActorIdentity(name="codex", primary_role=OrchestratorRole.CODEX)

    cont_id = "cont_test_mismatch"
    with service.store.transaction() as conn:
        conn.execute(
            """INSERT INTO continuation_deliveries (
                continuation_id, run_id, source_event_id, state, attempt_count, next_attempt_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (cont_id, "run1", "evt_correct", "PENDING", 1, "2026-06-20T00:00:00+00:00", "2026-06-20T00:00:00+00:00"),
        )

    with pytest.raises(OrchestratorError, match="Continuation source event mismatch"):
        service.ack_continuation(codex, cont_id, "evt_WRONG")


def test_continuation_requeue_dead_letter(tmp_path):
    service = OrchestratorService(tmp_path / "ledger.sqlite3")
    codex = ActorIdentity(name="codex", primary_role=OrchestratorRole.CODEX)

    cont_id = "cont_test_dlq"
    with service.store.transaction() as conn:
        conn.execute(
            """INSERT INTO continuation_deliveries (
                continuation_id, run_id, source_event_id, state, attempt_count, next_attempt_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (cont_id, "run1", "evt_1", "DEAD_LETTER", 3, "2026-06-20T00:00:00+00:00", "2026-06-20T00:00:00+00:00"),
        )

    # Requeue requires descriptive reason
    with pytest.raises(OrchestratorError, match="descriptive reason"):
        service.requeue_dead_letter_continuation(codex, cont_id, reason="short")

    requeued = service.requeue_dead_letter_continuation(codex, cont_id, reason="Manually retrying after environment repair")
    assert requeued["state"] == "PENDING"
    assert requeued["attempt_count"] == 0
