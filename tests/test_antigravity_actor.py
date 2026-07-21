"""Tests for Antigravity external execution runtime, server-issued identity, and granted role boundaries."""

import pytest
from grace_orchestrator.models import (
    ActorIdentity,
    ExecutionRuntime,
    OrchestratorError,
    OrchestratorRole,
)
from grace_orchestrator.mimo import (
    canonical_runtime,
    is_external_antigravity_runtime,
)


def test_01_antigravity_runtime_gemini_flash_high(monkeypatch):
    monkeypatch.setenv("GRACE_ORCHESTRATOR_ACTOR_ID", "ag-001")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_ACTOR_NAME", "ag-gemini-flash-01")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_ACTOR_ROLE", "worker_pro")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_RUNTIME", "antigravity")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_PROVIDER", "google")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_MODEL", "gemini-3.5-flash")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_REASONING_PROFILE", "high")

    identity = ActorIdentity.from_environment()
    assert identity.actor_id == "ag-001"
    assert identity.name == "ag-gemini-flash-01"
    assert identity.granted_role == OrchestratorRole.WORKER_PRO
    assert identity.runtime == ExecutionRuntime.ANTIGRAVITY
    assert identity.provider == "google"
    assert identity.model == "gemini-3.5-flash"
    assert identity.reasoning_profile == "high"


def test_02_antigravity_runtime_gemini_pro_high(monkeypatch):
    monkeypatch.setenv("GRACE_ORCHESTRATOR_ACTOR_NAME", "ag-gemini-pro-01")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_ACTOR_ROLE", "worker_pro")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_RUNTIME", "antigravity")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_PROVIDER", "google")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_MODEL", "gemini-3.1-pro")

    identity = ActorIdentity.from_environment()
    assert identity.runtime == ExecutionRuntime.ANTIGRAVITY
    assert identity.model == "gemini-3.1-pro"


def test_03_antigravity_runtime_claude_sonnet_thinking(monkeypatch):
    monkeypatch.setenv("GRACE_ORCHESTRATOR_ACTOR_NAME", "ag-claude-sonnet")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_ACTOR_ROLE", "worker_pro")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_RUNTIME", "antigravity")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_PROVIDER", "anthropic")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_MODEL", "claude-sonnet-4.6")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_REASONING_PROFILE", "thinking")

    identity = ActorIdentity.from_environment()
    assert identity.runtime == ExecutionRuntime.ANTIGRAVITY
    assert identity.provider == "anthropic"
    assert identity.reasoning_profile == "thinking"


def test_04_antigravity_runtime_claude_opus_thinking(monkeypatch):
    monkeypatch.setenv("GRACE_ORCHESTRATOR_ACTOR_NAME", "ag-claude-opus")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_ACTOR_ROLE", "worker_pro")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_RUNTIME", "antigravity")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_PROVIDER", "anthropic")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_MODEL", "claude-opus-4.6")

    identity = ActorIdentity.from_environment()
    assert identity.model == "claude-opus-4.6"


def test_05_antigravity_runtime_gpt_oss_medium(monkeypatch):
    monkeypatch.setenv("GRACE_ORCHESTRATOR_ACTOR_NAME", "ag-gpt-oss")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_ACTOR_ROLE", "worker_junior")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_RUNTIME", "antigravity")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_PROVIDER", "openai")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_MODEL", "gpt-oss-120b")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_REASONING_PROFILE", "medium")

    identity = ActorIdentity.from_environment()
    assert identity.granted_role == OrchestratorRole.WORKER_JUNIOR
    assert identity.provider == "openai"


def test_06_same_runtime_different_providers_models(monkeypatch):
    monkeypatch.setenv("GRACE_ORCHESTRATOR_ACTOR_NAME", "ag-multi")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_ACTOR_ROLE", "worker_pro")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_RUNTIME", "antigravity")

    for provider, model in [("google", "gemini-3.5-flash"), ("anthropic", "claude-opus-4.6"), ("openai", "gpt-oss-120b")]:
        monkeypatch.setenv("GRACE_ORCHESTRATOR_PROVIDER", provider)
        monkeypatch.setenv("GRACE_ORCHESTRATOR_MODEL", model)
        identity = ActorIdentity.from_environment()
        assert identity.runtime == ExecutionRuntime.ANTIGRAVITY
        assert identity.provider == provider
        assert identity.model == model


def test_07_rejection_of_model_bound_runtime_aliases():
    with pytest.raises(OrchestratorError, match="Model-bound name"):
        canonical_runtime("gemini-3.5-flash")

    with pytest.raises(OrchestratorError, match="Model-bound name"):
        canonical_runtime("antigravity-flash")


def test_08_canonical_runtime_alias_normalization():
    assert canonical_runtime("google/antigravity") == ExecutionRuntime.ANTIGRAVITY
    assert canonical_runtime("antigravity") == ExecutionRuntime.ANTIGRAVITY
    assert canonical_runtime("openai/codex") == ExecutionRuntime.CODEX
    assert canonical_runtime("codex") == ExecutionRuntime.CODEX


def test_09_is_external_antigravity_runtime():
    assert is_external_antigravity_runtime("google/antigravity") is True
    assert is_external_antigravity_runtime("antigravity") is True
    assert is_external_antigravity_runtime("codex") is False


def test_10_missing_actor_name_raises(monkeypatch):
    monkeypatch.delenv("GRACE_ORCHESTRATOR_ACTOR_NAME", raising=False)
    monkeypatch.setenv("GRACE_ORCHESTRATOR_ACTOR_ROLE", "worker_pro")
    with pytest.raises(OrchestratorError, match="ACTOR_IDENTITY_UNCONFIGURED"):
        ActorIdentity.from_environment()


def test_11_invalid_role_raises(monkeypatch):
    monkeypatch.setenv("GRACE_ORCHESTRATOR_ACTOR_NAME", "ag-test")
    monkeypatch.setenv("GRACE_ORCHESTRATOR_ACTOR_ROLE", "invalid_role_name")
    with pytest.raises(OrchestratorError, match="Unknown configured actor role"):
        ActorIdentity.from_environment()


def test_12_antigravity_is_not_mimo_launchable():
    assert is_external_antigravity_runtime(ExecutionRuntime.ANTIGRAVITY) is True
