"""GRACE enforcement policy checks for MCP admission and acceptance gates."""

# FILE: src/grace_orchestrator/policy.py
# VERSION: 0.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Validate contract discovery, execution packets, worker reports, acceptance gates, and agent-infra files.
#   SCOPE: Deterministic local file reads and JSON-shape checks only; no model calls, shell commands, or workflow mutation.
#   DEPENDS: M-ORCH-DOMAIN, M-GRACE-ENFORCEMENT-LAYER
#   LINKS: M-GRACE-ENFORCEMENT-LAYER, V-M-GRACE-ENFORCEMENT-LAYER, M-ORCH-MCP-SERVER
#   ROLE: RUNTIME
#   MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
# START_MODULE_MAP
#   TRUTH_PRECEDENCE - fixed source-of-truth ordering shared by gate reports.
#   DEFAULT_REQUIRED_FILES - fallback required agent-infra file list.
#   DEFAULT_XML_ARTIFACTS - fallback required GRACE XML artifact list.
#   DEFAULT_PACKET_FIELDS - fallback execution packet required fields.
#   DEFAULT_WORKER_REPORT_FIELDS - fallback worker report required fields.
#   MODULE_REF_RE - M-* reference matcher used by contract discovery.
#   VERIFICATION_REF_RE - V-M-* reference matcher used by contract discovery.
#   RULE_REF_RE - @rule matcher used by contract discovery.
#   HTML_COMMENT_RE - hidden-comment matcher used by agent-infra lint.
#   require_gate_pass - raises a client-safe OrchestratorError on blocked gate results.
#   discover_contracts - builds the machine-readable contract discovery report.
#   validate_contract_discovery - validates a supplied discovery report shape.
#   validate_execution_packet - enforces dispatch packet shape before worker assignment.
#   validate_worker_report - enforces worker evidence shape before submission or acceptance.
#   lint_agent_infra - validates AGENTS/GRACE enforcement files without shell execution.
# END_MODULE_MAP
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v0.1.1 - Distinguish missing report fields from present-but-empty evidence.
# END_CHANGE_SUMMARY

from __future__ import annotations

from hashlib import sha256
import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import OrchestratorError
from .repo import validate_scoped_files


TRUTH_PRECEDENCE = [
    "code-adjacent contracts and semantic anchors",
    "interface schemas, OpenAPI, migrations, or typed protocol schemas",
    "protected tests and accepted test-owner fixtures",
    "canonical GRACE XML artifacts",
    "AGENTS.md and docs/grace routing rules",
    "SESSION_HANDOFF or MCP handoff events for current operational state",
    "MEMORY or long-term project memory",
    "ADR and decision logs",
    "historical session logs and archived reports",
]

DEFAULT_REQUIRED_FILES = [
    "AGENTS.md",
    "docs/grace/agent-enforcement-layer.md",
    "docs/grace/templates/task-packet-template.md",
    "docs/grace/templates/verification-packet-template.md",
    "docs/operational-packets.xml",
]

DEFAULT_XML_ARTIFACTS = [
    "docs/requirements.xml",
    "docs/technology.xml",
    "docs/development-plan.xml",
    "docs/verification-plan.xml",
    "docs/knowledge-graph.xml",
    "docs/operational-packets.xml",
]

DEFAULT_PACKET_FIELDS = [
    "operation id",
    "authority mode",
    "operation root",
    "codex required",
    "codex instance id",
    "glm instance id",
    "branch/worktree",
    "task id",
    "module id",
    "verification id",
    "goal",
    "assigned role",
    "orchestration stage",
    "substitution authority",
    "allowed files",
    "forbidden files",
    "worker runtime profile",
    "actual worker identity",
    "mimocode agent",
    "backend provider",
    "backend model",
    "launch mode",
    "trust flag",
    "model flag policy",
    "forbidden model flags",
    "pro/api assignment",
    "pro backend model",
    "claim identity",
    "glm scan/plan report",
    "required contracts read",
    "contract discovery report",
    "test surface",
    "commands allowed",
    "rollback boundary",
    "session routing",
    "operation isolation",
    "cache anchor",
    "retry budget",
    "stop conditions",
    "compact worker report format",
]

DEFAULT_WORKER_REPORT_FIELDS = [
    "operation id",
    "authority mode",
    "task id",
    "module id",
    "files read",
    "files changed",
    "contract delta",
    "commands run with exact results",
    "what is scaffolded",
    "what is wired",
    "what is verified",
    "unverified gaps",
    "stop conditions encountered",
    "protected-test and forbidden-scope statement",
    "graph delta proposal",
    "verification delta proposal",
    "next action",
]

MODULE_REF_RE = re.compile(r"\bM-[A-Z0-9]+(?:-[A-Z0-9]+)*\b")
VERIFICATION_REF_RE = re.compile(r"\bV-M-[A-Z0-9]+(?:-[A-Z0-9]+)*\b")
RULE_REF_RE = re.compile(r"@rule\s+id=\"([A-Z0-9-]+)\"")
HTML_COMMENT_RE = re.compile(r"<!--([\s\S]*?)-->")


def _canonical_key(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")


def _field_aliases(label: str) -> list[str]:
    key = _canonical_key(label)
    aliases = {
        "task_id": ["task_id", "task-id", "task id"],
        "operation_id": ["operation_id", "operation-id", "operation id"],
        "authority_mode": ["authority_mode", "authority-mode", "authority mode"],
        "operation_root": ["operation_root", "operation-root", "operation root"],
        "codex_required": ["codex_required", "codex-required", "codex required"],
        "codex_instance_id": ["codex_instance_id", "codex-instance-id", "codex instance id"],
        "glm_instance_id": ["glm_instance_id", "glm-instance-id", "glm instance id"],
        "branch_worktree": ["branch_worktree", "branch-worktree", "branch/worktree", "branch worktree"],
        "module_id": ["module_id", "module-id", "module id"],
        "verification_id": ["verification_id", "verification-id", "verification id"],
        "assigned_role": ["assigned_role", "assigned-role", "assigned role", "role_assigned"],
        "orchestration_stage": ["orchestration_stage", "orchestration-stage", "orchestration stage", "operation stage"],
        "substitution_authority": ["substitution_authority", "substitution-authority", "substitution authority"],
        "allowed_files": ["allowed_files", "allowed-files", "allowed files"],
        "forbidden_files": ["forbidden_files", "forbidden-files", "forbidden files"],
        "worker_runtime_profile": ["worker_runtime_profile", "worker-runtime-profile", "worker runtime profile"],
        "actual_worker_identity": ["actual_worker_identity", "actual-worker-identity", "actual worker identity"],
        "mimocode_agent": ["mimocode_agent", "mimocode-agent", "mimocode agent", "mimocode tui agent"],
        "backend_provider": ["backend_provider", "backend-provider", "backend provider", "provider"],
        "backend_model": ["backend_model", "backend-model", "backend model", "provider/model"],
        "launch_mode": ["launch_mode", "launch-mode", "launch mode"],
        "trust_flag": ["trust_flag", "trust-flag", "trust flag"],
        "model_flag_policy": ["model_flag_policy", "model-flag-policy", "model flag policy"],
        "forbidden_model_flags": ["forbidden_model_flags", "forbidden-model-flags", "forbidden model flags"],
        "pro_api_assignment": ["pro_api_assignment", "pro-api-assignment", "pro/api assignment"],
        "pro_backend_model": ["pro_backend_model", "pro-backend-model", "pro backend model"],
        "claim_identity": ["claim_identity", "claim-identity", "claim identity"],
        "glm_scan_plan_report": ["glm_scan_plan_report", "glm-scan-plan-report", "glm scan/plan report"],
        "required_contracts_read": ["required_contracts_read", "contracts_read", "required contracts read"],
        "contract_discovery_report": ["contract_discovery_report", "contract_discovery", "contract discovery report"],
        "test_surface": ["test_surface", "test-surface", "test surface"],
        "commands_allowed": ["commands_allowed", "commands-allowed", "commands allowed"],
        "rollback_boundary": ["rollback_boundary", "rollback-boundary", "rollback boundary"],
        "session_routing": ["session_routing", "session-routing", "session routing"],
        "operation_isolation": ["operation_isolation", "operation-isolation", "operation isolation"],
        "cache_anchor": ["cache_anchor", "cache-anchor", "cache anchor"],
        "retry_budget": ["retry_budget", "retry-budget", "retry budget"],
        "stop_conditions": ["stop_conditions", "stop-conditions", "stop conditions"],
        "compact_worker_report_format": [
            "compact_worker_report_format",
            "compact-worker-report-format",
            "compact worker report format",
            "expected_worker_report_fields",
        ],
        "files_read": ["files_read", "files-read", "files read"],
        "files_changed": ["files_changed", "files_touched", "files-changed", "files changed"],
        "contract_delta": ["contract_delta", "contract-delta", "contract delta"],
        "commands_run_with_exact_results": [
            "commands_run_with_exact_results",
            "commands-run-with-exact-results",
            "commands run with exact results",
            "commands_run",
            "exact_results",
        ],
        "what_is_scaffolded": ["what_is_scaffolded", "scaffolded", "what is scaffolded"],
        "what_is_wired": ["what_is_wired", "wired", "what is wired"],
        "what_is_verified": ["what_is_verified", "verified", "what is verified"],
        "unverified_gaps": ["unverified_gaps", "unverified-gaps", "unverified gaps"],
        "stop_conditions_encountered": [
            "stop_conditions_encountered",
            "stop-conditions-encountered",
            "stop conditions encountered",
        ],
        "protected_test_and_forbidden_scope_statement": [
            "protected_test_and_forbidden_scope_statement",
            "protected-test-and-forbidden-scope-statement",
            "protected-test and forbidden-scope statement",
            "protected_tests",
            "forbidden_scope_statement",
        ],
        "graph_delta_proposal": ["graph_delta_proposal", "graph-delta-proposal", "graph delta proposal"],
        "verification_delta_proposal": [
            "verification_delta_proposal",
            "verification-delta-proposal",
            "verification delta proposal",
        ],
        "next_action": ["next_action", "next-action", "next action"],
    }
    return aliases.get(key, [key, label])


def _get_field(payload: Mapping[str, Any], label: str) -> Any:
    for alias in _field_aliases(label):
        if alias in payload:
            return payload[alias]
    return None


def _has_field(payload: Mapping[str, Any], label: str) -> bool:
    return any(alias in payload for alias in _field_aliases(label))


def _is_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return len(value) > 0
    if isinstance(value, Mapping):
        return len(value) > 0
    return True


def _append_required_field_issue(
    issues: list[str],
    payload: Mapping[str, Any],
    label: str,
    *,
    subject: str,
) -> None:
    if not _has_field(payload, label):
        issues.append(f"{subject} missing required field: {label}")
        return
    if not _is_present(_get_field(payload, label)):
        issues.append(
            f"{subject} field is present but empty: {label}. "
            "Required: non-empty string, non-empty array, non-empty object, or scalar evidence."
        )


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [str(item) for item in value if str(item).strip()]
    return [str(value)]


def _load_policy(repo_root: Path) -> dict[str, Any]:
    policy_path = repo_root / ".agent-guards" / "agent-infra-policy.json"
    if not policy_path.is_file():
        return {
            "required_xml_artifacts": DEFAULT_XML_ARTIFACTS,
            "required_files": DEFAULT_REQUIRED_FILES,
            "packet_required_fields": DEFAULT_PACKET_FIELDS,
            "worker_report_required_fields": DEFAULT_WORKER_REPORT_FIELDS,
            "suspicious_html_comment_patterns": [],
        }
    return json.loads(policy_path.read_text(encoding="utf-8"))


def _read_repo_file(repo_root: Path, rel_path: str, issues: list[str]) -> str | None:
    path = repo_root / rel_path
    if not path.is_file():
        issues.append(f"Required file is missing: {rel_path}")
        return None
    return path.read_text(encoding="utf-8", errors="replace")


def _scope_fragments(scopes: Sequence[str]) -> list[str]:
    fragments: list[str] = []
    for scope in scopes:
        cleaned = scope.replace("\\", "/").strip()
        if not cleaned:
            continue
        head = cleaned.split("*", 1)[0].rstrip("/")
        if head:
            fragments.append(head.lower())
        if "/" in cleaned:
            fragments.append(cleaned.rsplit("/", 1)[0].lower())
    return list(dict.fromkeys(fragments))


def _matches_scope(text: str, rel_path: str, fragments: Sequence[str]) -> bool:
    lowered = text.lower()
    rel_lower = rel_path.lower()
    return any(fragment and (fragment in lowered or fragment in rel_lower) for fragment in fragments)


def _result(status: str, issues: list[str], warnings: list[str], **fields: Any) -> dict[str, Any]:
    return {"status": status, "issues": issues, "warnings": warnings, **fields}


def require_gate_pass(result: Mapping[str, Any], label: str) -> None:
    if result.get("status") not in {"pass", "verified"}:
        issues = result.get("issues")
        detail = "; ".join(str(item) for item in issues) if isinstance(issues, Sequence) else "gate did not pass"
        raise OrchestratorError(f"{label} failed: {detail}")


def discover_contracts(repo_root: Path, affected_files: Sequence[str]) -> dict[str, Any]:
    # START_CONTRACT: discover_contracts
    #   PURPOSE: Build the ContractDiscoveryReport used by MCP before dispatching a worker.
    #   INPUTS: { repo_root: Path, affected_files: path globs or files }
    #   OUTPUTS: { dict - pass/blocked result with contracts, graph refs, verification refs, and issues }
    #   SIDE_EFFECTS: Reads repository documentation and code-adjacent contracts only.
    #   LINKS: M-GRACE-ENFORCEMENT-LAYER, V-M-GRACE-ENFORCEMENT-LAYER
    # END_CONTRACT: discover_contracts
    root = repo_root.resolve()
    policy = _load_policy(root)
    issues: list[str] = []
    warnings: list[str] = []
    scopes = _string_list(affected_files)
    if not scopes:
        issues.append("Contract discovery requires at least one affected file or path glob")

    required_paths = list(
        dict.fromkeys(
            [
                *policy.get("required_xml_artifacts", DEFAULT_XML_ARTIFACTS),
                *policy.get("required_files", DEFAULT_REQUIRED_FILES),
            ]
        )
    )
    fragments = _scope_fragments(scopes)
    contracts_read: list[str] = []
    module_refs: set[str] = set()
    verification_refs: set[str] = set()
    rule_refs: set[str] = set()
    local_contract_files: list[str] = []

    for rel_path in required_paths:
        text = _read_repo_file(root, str(rel_path), issues)
        if text is None:
            continue
        contracts_read.append(str(rel_path))
        if _matches_scope(text, str(rel_path), fragments) or str(rel_path) in DEFAULT_REQUIRED_FILES:
            module_refs.update(MODULE_REF_RE.findall(text))
            verification_refs.update(VERIFICATION_REF_RE.findall(text))
            rule_refs.update(RULE_REF_RE.findall(text))

    for scope in scopes:
        if "*" in scope:
            continue
        rel = scope.replace("\\", "/").strip()
        path = root / rel
        if path.is_file():
            text = path.read_text(encoding="utf-8", errors="replace")
            if "START_MODULE_CONTRACT" in text or "START_CONTRACT:" in text:
                local_contract_files.append(rel)
                contracts_read.append(rel)
                module_refs.update(MODULE_REF_RE.findall(text))
                verification_refs.update(VERIFICATION_REF_RE.findall(text))

    if not module_refs:
        issues.append("No M-* module reference was discovered for the affected scope")
    if not verification_refs:
        issues.append("No V-M-* verification reference was discovered for the affected scope")

    status = "pass" if not issues else "blocked"
    return _result(
        status,
        issues,
        warnings,
        affected_files=scopes,
        contracts_read=sorted(set(contracts_read)),
        local_contract_files=sorted(set(local_contract_files)),
        module_refs=sorted(module_refs),
        verification_refs=sorted(verification_refs),
        rule_refs=sorted(rule_refs),
        missing_contracts=[item for item in issues if "No " in item or "missing" in item.lower()],
        conflicts=[],
        source_precedence=TRUTH_PRECEDENCE,
    )


def validate_contract_discovery(contract_discovery: Mapping[str, Any]) -> dict[str, Any]:
    issues: list[str] = []
    warnings: list[str] = []
    status = str(contract_discovery.get("status", "")).lower()
    if status not in {"pass", "verified"}:
        issues.append("Contract discovery result must be pass or verified")
    if not _is_present(contract_discovery.get("contracts_read")):
        issues.append("Contract discovery must list contracts_read")
    if not _is_present(contract_discovery.get("module_refs")):
        issues.append("Contract discovery must list at least one module_refs entry")
    if not _is_present(contract_discovery.get("verification_refs")):
        issues.append("Contract discovery must list at least one verification_refs entry")
    for field in ("missing_contracts", "conflicts"):
        value = contract_discovery.get(field)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) and value:
            issues.append(f"Contract discovery has unresolved {field}: {list(value)}")
    return _result("pass" if not issues else "blocked", issues, warnings)


def validate_execution_packet(
    packet: Mapping[str, Any],
    *,
    repo_root: Path | None = None,
    parent_allowed_files: Sequence[str] | None = None,
) -> dict[str, Any]:
    # START_CONTRACT: validate_execution_packet
    #   PURPOSE: Reject worker packets missing scope, contracts, verification surface, rollback boundary, or report format.
    #   INPUTS: { packet: mapping, repo_root: optional project root, parent_allowed_files: optional task scope }
    #   OUTPUTS: { dict - pass/blocked validation report }
    #   SIDE_EFFECTS: Reads policy file when repo_root is provided.
    #   LINKS: M-GRACE-ENFORCEMENT-LAYER, V-M-GRACE-ENFORCEMENT-LAYER
    # END_CONTRACT: validate_execution_packet
    policy = _load_policy(repo_root.resolve()) if repo_root is not None else {}
    required_fields = policy.get("packet_required_fields", DEFAULT_PACKET_FIELDS)
    worker_fields = policy.get("worker_report_required_fields", DEFAULT_WORKER_REPORT_FIELDS)
    issues: list[str] = []
    warnings: list[str] = []
    for field in required_fields:
        _append_required_field_issue(issues, packet, str(field), subject="Execution packet")

    allowed_files = _string_list(_get_field(packet, "allowed files"))
    if parent_allowed_files:
        for pattern in allowed_files:
            if not any(parent == "**" or parent == pattern or (parent.endswith("/**") and pattern.startswith(parent[:-3] + "/")) for parent in parent_allowed_files):
                issues.append(f"Execution packet allowed scope is outside parent task scope: {pattern}")

    discovery = _get_field(packet, "contract discovery report")
    if isinstance(discovery, Mapping):
        discovery_result = validate_contract_discovery(discovery)
        if discovery_result["status"] != "pass":
            issues.extend(f"Contract discovery: {issue}" for issue in discovery_result["issues"])
    else:
        issues.append("Execution packet contract discovery report must be a JSON object")

    compact_format = _string_list(_get_field(packet, "compact worker report format"))
    compact_keys = {_canonical_key(item) for item in compact_format}
    for field in worker_fields:
        if _canonical_key(str(field)) not in compact_keys:
            warnings.append(f"Compact report format does not explicitly name worker field: {field}")

    return _result("pass" if not issues else "blocked", issues, warnings)


def validate_worker_report(
    report: Mapping[str, Any],
    *,
    task_id: int,
    work_package_id: int,
    allowed_files: Sequence[str],
    forbidden_files: Sequence[str],
    evidence_files: Sequence[str] | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    # START_CONTRACT: validate_worker_report
    #   PURPOSE: Reject worker submissions that lack exact evidence, status split, contract delta, or scope proof.
    #   INPUTS: { report: mapping, task/package IDs, allowed/forbidden file scope, optional evidence files }
    #   OUTPUTS: { dict - pass/blocked validation report }
    #   SIDE_EFFECTS: Reads policy file when repo_root is provided.
    #   LINKS: M-GRACE-ENFORCEMENT-LAYER, V-M-GRACE-ENFORCEMENT-LAYER
    # END_CONTRACT: validate_worker_report
    policy = _load_policy(repo_root.resolve()) if repo_root is not None else {}
    required_fields = policy.get("worker_report_required_fields", DEFAULT_WORKER_REPORT_FIELDS)
    issues: list[str] = []
    warnings: list[str] = []
    for field in required_fields:
        # A verification-only package may honestly report no changed files.
        # The field must still be explicit so the reviewer can distinguish
        # "no changes" from omitted evidence.
        if _canonical_key(str(field)) == "files_changed":
            if not _has_field(report, str(field)):
                issues.append(f"Worker report missing required field: {field}")
            continue
        _append_required_field_issue(issues, report, str(field), subject="Worker report")

    reported_task = _get_field(report, "task id")
    reported_package = _get_field(report, "work package id") or report.get("work_package_id")
    if reported_task is not None and int(reported_task) != int(task_id):
        issues.append(f"Worker report task id mismatch: {reported_task} != {task_id}")
    if reported_package is not None and int(reported_package) != int(work_package_id):
        issues.append(f"Worker report package id mismatch: {reported_package} != {work_package_id}")

    changed_files = _string_list(_get_field(report, "files changed"))
    if evidence_files is not None:
        evidence_set = set(_string_list(evidence_files))
        reported_set = set(changed_files)
        if evidence_set and evidence_set != reported_set:
            issues.append(f"Worker report files changed do not match submission evidence: {sorted(reported_set)} != {sorted(evidence_set)}")
    if changed_files:
        try:
            validate_scoped_files(changed_files, allowed_files=allowed_files, forbidden_files=forbidden_files)
        except OrchestratorError as error:
            issues.append(str(error))

    verified = _get_field(report, "what is verified")
    gaps = _get_field(report, "unverified gaps")
    if not _is_present(verified) and not _is_present(gaps):
        issues.append("Worker report must explicitly state verified evidence or unverified gaps")

    return _result("pass" if not issues else "blocked", issues, warnings)


def lint_agent_infra(repo_root: Path) -> dict[str, Any]:
    # START_CONTRACT: lint_agent_infra
    #   PURPOSE: Validate the local agent-infra policy and hidden rule/comment hygiene without executing shell.
    #   INPUTS: { repo_root: Path }
    #   OUTPUTS: { dict - pass/blocked lint result }
    #   SIDE_EFFECTS: Reads project files only.
    #   LINKS: M-GRACE-ENFORCEMENT-LAYER, V-M-GRACE-ENFORCEMENT-LAYER
    # END_CONTRACT: lint_agent_infra
    root = repo_root.resolve()
    policy = _load_policy(root)
    issues: list[str] = []
    warnings: list[str] = []
    for rel_path in policy.get("required_xml_artifacts", DEFAULT_XML_ARTIFACTS):
        raw = _read_repo_file(root, str(rel_path), issues)
        if raw is not None and not raw.lstrip().startswith("<"):
            issues.append(f"XML artifact does not look like XML: {rel_path}")
    for rel_path in policy.get("required_files", DEFAULT_REQUIRED_FILES):
        _read_repo_file(root, str(rel_path), issues)

    for rule_set in policy.get("required_rules", []):
        raw = _read_repo_file(root, str(rule_set.get("file", "")), issues)
        if raw is None:
            continue
        for rule_id in rule_set.get("ids", []):
            if not re.search(rf"<!--\s*@rule\s+id=\"{re.escape(str(rule_id))}\"", raw):
                issues.append(f"Missing @rule id {rule_id} in {rule_set.get('file')}")
            if not re.search(rf"<!--\s*@rule-end\s+id=\"{re.escape(str(rule_id))}\"\s*-->", raw):
                issues.append(f"Missing @rule-end id {rule_id} in {rule_set.get('file')}")
        for needle in rule_set.get("contains", []):
            if str(needle).lower() not in raw.lower():
                issues.append(f"Missing required text in {rule_set.get('file')}: {needle}")

    for text_rule in policy.get("required_text", []):
        raw = _read_repo_file(root, str(text_rule.get("file", "")), issues)
        if raw is None:
            continue
        for needle in text_rule.get("contains", []):
            if str(needle).lower() not in raw.lower():
                issues.append(f"Missing required text in {text_rule.get('file')}: {needle}")

    operational = (root / "docs" / "operational-packets.xml").read_text(encoding="utf-8", errors="replace") if (root / "docs" / "operational-packets.xml").is_file() else ""
    for field in policy.get("packet_required_fields", DEFAULT_PACKET_FIELDS):
        if str(field).lower() not in operational.lower():
            issues.append(f"Missing packet field in docs/operational-packets.xml: {field}")
    for field in policy.get("worker_report_required_fields", DEFAULT_WORKER_REPORT_FIELDS):
        if str(field).lower() not in operational.lower():
            issues.append(f"Missing worker report field in docs/operational-packets.xml: {field}")

    suspicious = [str(item).lower() for item in policy.get("suspicious_html_comment_patterns", [])]
    for rel_path in [
        "docs/grace/agent-enforcement-layer.md",
        "docs/grace/templates/task-packet-template.md",
        "docs/grace/templates/verification-packet-template.md",
        "docs/operational-packets.xml",
    ]:
        path = root / rel_path
        if not path.is_file():
            continue
        raw = path.read_text(encoding="utf-8", errors="replace")
        for comment in HTML_COMMENT_RE.findall(raw):
            lowered = comment.lower()
            for pattern in suspicious:
                if pattern and pattern in lowered:
                    issues.append(f"Suspicious hidden-comment pattern in {rel_path}: {pattern}")

    return _result("pass" if not issues else "blocked", issues, warnings)


def calculate_rejection_fingerprint(rejection_reasons: Sequence[str]) -> str:
    """Produce a stable hash fingerprint for rejection reasons."""
    cleaned = [str(r).strip() for r in rejection_reasons if str(r).strip()]
    if not cleaned:
        return ""
    joined = "\n".join(sorted(cleaned))
    return sha256(joined.encode("utf-8")).hexdigest()[:16]


def create_compact_log_projection(
    full_log: str,
    artifact_ref: str = "",
    sha256_hash: str = "",
    head_lines: int = 20,
    tail_lines: int = 30,
) -> str:
    """Build a non-destructive compact LLM projection of a test or execution log."""
    if not full_log:
        return full_log

    lines = full_log.splitlines()
    total_lines = len(lines)

    if total_lines <= (head_lines + tail_lines):
        return full_log

    head = lines[:head_lines]
    tail = lines[-tail_lines:]
    middle = lines[head_lines:-tail_lines]

    diagnostic_patterns = ("traceback", "caused by", "error", "failed", "panic", "exception", "assert")
    matched_diagnostics = [
        line for line in middle
        if any(pat in line.lower() for pat in diagnostic_patterns)
    ]
    omitted_count = total_lines - (head_lines + tail_lines)

    parts = [
        f"--- BEGIN OUTPUT: first {head_lines} of {total_lines} lines ---",
        "\n".join(head),
        f"--- OMITTED {omitted_count} LINES ---",
    ]

    if matched_diagnostics:
        parts.extend([
            f"--- EXTRACTED DIAGNOSTICS ({len(matched_diagnostics)} matched lines) ---",
            "\n".join(matched_diagnostics[:50]),
        ])

    parts.extend([
        f"--- END OUTPUT: last {tail_lines} lines ---",
        "\n".join(tail),
    ])

    if artifact_ref:
        parts.append(f"--- FULL LOG ARTIFACT: {artifact_ref} (SHA-256: {sha256_hash}) ---")

    return "\n".join(parts)


def create_compact_diff_projection(
    files_changed: Sequence[str],
    diff_text: str,
    artifact_ref: str = "",
    sha256_hash: str = "",
) -> str:
    """Build a non-destructive compact LLM projection of a code diff."""
    adds = sum(1 for line in diff_text.splitlines() if line.startswith("+") and not line.startswith("+++"))
    dels = sum(1 for line in diff_text.splitlines() if line.startswith("-") and not line.startswith("---"))
    hunks = sum(1 for line in diff_text.splitlines() if line.startswith("@@"))

    summary_lines = [
        "--- COMPACT DIFF SUMMARY ---",
        f"Files Changed ({len(files_changed)}): " + ", ".join(files_changed),
        f"Stats: +{adds} / -{dels} lines across {hunks} hunks.",
    ]
    if artifact_ref:
        summary_lines.append(f"--- FULL DIFF ARTIFACT: {artifact_ref} (SHA-256: {sha256_hash}) ---")

    return "\n".join(summary_lines)


def compact_worker_report_for_context(report: Mapping[str, Any]) -> dict[str, Any]:
    """Pure projection helper returning a compact version of a worker report for LLM prompt context."""
    compacted = dict(report)
    commands_run = compacted.get("commands run with exact results")
    if isinstance(commands_run, Sequence) and not isinstance(commands_run, (str, bytes)):
        compacted_cmds = []
        for cmd in commands_run:
            if isinstance(cmd, Mapping):
                cmd_dict = dict(cmd)
                out = cmd_dict.get("stdout") or cmd_dict.get("output")
                if isinstance(out, str) and len(out.splitlines()) > 50:
                    cmd_dict["output"] = create_compact_log_projection(out)
                compacted_cmds.append(cmd_dict)
            else:
                compacted_cmds.append(cmd)
        compacted["commands run with exact results"] = compacted_cmds
    return compacted


ACTIVE_WORK_PACKAGE_STATUSES = frozenset({
    "CREATED",
    "ASSIGNED",
    "CLAIMED_JUNIOR",
    "CLAIMED_PRO",
    "SUBMITTED",
    "GLM_REVIEW_IN_PROGRESS",
    "REPAIR_REQUIRED",
})

BLOCKED_WORK_PACKAGE_STATUSES = frozenset({
    "HUMAN_INTERVENTION_REQUIRED",
    "BLOCKED",
})

TERMINAL_WORK_PACKAGE_STATUSES = frozenset({
    "GLM_ACCEPTED",
    "CANCELLED",
})


def project_next_action(
    task_status: str,
    packages: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, str]:
    """Pure classifier projecting the next recommended action for a task and its packages."""
    if task_status == "CODEX_TASK_CREATED":
        return {"role": "glm", "action": "task.plan"}
    if task_status == "GLM_GRACE_PLANNED":
        return {"role": "glm", "action": "verification.register_plan or submission.controller_task_completion"}
    if task_status == "GLM_TESTS_PREPARED":
        return {"role": "glm", "action": "workpackage.create or submission.controller_task_completion"}
    if task_status == "GLM_REJECTED_REPAIR_REQUIRED":
        return {"role": "glm", "action": "mimo.launch_package or submission.controller_repair"}

    pkgs = list(packages or [])
    for pkg in pkgs:
        st = str(pkg.get("status", ""))
        if st in BLOCKED_WORK_PACKAGE_STATUSES:
            return {"role": "user_or_codex", "action": f"workpackage.force_reset or task.force_transition for package #{pkg.get('id')}"}

    for pkg in pkgs:
        st = str(pkg.get("status", ""))
        if st == "CREATED":
            return {"role": "glm", "action": f"workpackage.assign for package #{pkg.get('id')}"}
        if st in {"ASSIGNED", "REPAIR_REQUIRED"}:
            return {"role": "worker_junior_or_pro", "action": f"workpackage.claim for package #{pkg.get('id')}"}
        if st in {"CLAIMED_JUNIOR", "CLAIMED_PRO"}:
            return {"role": "worker_junior_or_pro", "action": f"submission.create for package #{pkg.get('id')}"}
        if st in {"SUBMITTED", "GLM_REVIEW_IN_PROGRESS"}:
            return {"role": "glm", "action": f"review.glm_submit for package #{pkg.get('id')}"}

    if task_status in {"WORK_PACKAGES_CREATED", "WORK_PACKAGES_ASSIGNED"}:
        if pkgs and all(str(p.get("status", "")) == "GLM_ACCEPTED" for p in pkgs):
            return {"role": "codex", "action": "task.request_final_review"}
        return {"role": "glm", "action": "workpackage.assign or review.glm_submit"}

    if task_status == "CODEX_FINAL_REVIEW":
        return {"role": "codex", "action": "review.codex_submit"}
    if task_status == "CODEX_ACCEPTED":
        return {"role": "codex", "action": "task.close"}
    if task_status in {"CLOSED", "TASK_CLOSED"}:
        return {"role": "none", "action": "none"}

    return {"role": "codex", "action": "task.get"}

