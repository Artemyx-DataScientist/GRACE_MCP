# GRACE Orchestrator MCP

Local MCP control-plane for the Codex → GLM → worker → acceptance workflow.

This repository contains the reusable host/tooling layer. Product repositories
keep their own GRACE artifacts, policies, and runtime contracts; this server
only owns workflow ledger state, gate execution, handoff events, and host
continuation plumbing.

Mimo is the single local gateway for all non-Codex models. The server records
the workflow ledger, accepts only server-bound roles, derives Git evidence,
creates a detached worktree per dispatched Mimo session, and launches only
fixed Mimo CLI argv. A model launch is evidence, never a package submission or
acceptance decision.

## Local development

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python -m pip install -e ".[dev]"
.\.venv\Scripts\python -m pytest
```

Set `GRACE_MCP_HOME` in consuming projects to this checkout when docs or MCP
configuration need to refer to the external tool source.

## STDIO identity

Set the identity before starting the server. It is deliberately process-bound,
so no MCP tool can elevate itself by supplying a role argument.

```powershell
$env:GRACE_ORCHESTRATOR_ACTOR_NAME = "codex"
$env:GRACE_ORCHESTRATOR_ACTOR_ROLE = "codex"
$env:GRACE_ORCHESTRATOR_DATA_DIR = "D:\path\to\grace-orchestrator-state"
.\.venv\Scripts\grace-orchestrator-mcp
```

Logs use stderr; stdout is reserved for MCP JSON-RPC.

Keep `GRACE_ORCHESTRATOR_DATA_DIR` outside the repository: Mimo dispatches
create isolated Git worktrees below that directory.

## GUI-Neutral Inbox & Project Authorization

`inbox.next` and `inbox.list` provide passive, backend-neutral inbox queries returning actor-bound execution items:
- Queries are strictly bounded by process identity (`GRACE_ORCHESTRATOR_ACTOR_NAME` / `GRACE_ORCHESTRATOR_ACTOR_ROLE`).
- Inbox items contain only actions that the caller has active role or project capability/delegation authorization to perform.
- For GUI-backed environments (e.g. Antigravity IDE, VSCode, Codex GUI), active tab or context switching is performed manually by the human user.
- Calling `inbox.next` or `inbox.list` eliminates context transfer overhead, but does NOT launch, spawn, or wake up background Codex/ZCode/Antigravity instances.
- Project-scoped authorization is strictly enforced across both tool calls and FastMCP `@mcp.resource` endpoints. `GLM` and `TEST_OWNER` primary roles require explicit project capability registration or delegation to access project resources.

## Mimo setup

1. Register every non-Codex agent with its role capabilities and its exact
   Mimo `provider/model` identifier through `agent.register`.
2. As Codex, call `mimo.connection_profile` for each registered agent. Add the
   returned STDIO fields through Mimo's MCP-server setup. Do not reuse the
   Codex server process: every Mimo agent needs its own profile because actor
   identity is bound on process start.
3. GLM (or an explicit Codex fallback) assigns a work package, then calls
   `mimo.launch_package` in `tui` mode only. The server creates a separate
   worktree and briefing. TUI sessions open independently; multiple
   sessions do not share a checkout. Once a TUI window closes, GLM records that
   observation with `mimo.record_tui_closed`; this releases only the dispatch
   lock, never accepts the package.

The bridge intentionally does not write Mimo's global configuration, supply
credentials, bypass Mimo permission prompts, apply a patch, or auto-accept a
package. Run one human-approved non-production package after configuration to
establish live-provider evidence.

## Worker handoff and controller wait

Dispatch does not end controller responsibility.  Every launch writes
`WORKER_STARTED`; a valid `submission.create` writes a durable
`WORKER_READY_FOR_REVIEW` event and a controller-readable report.  Workers
must use `handoff.report_worker_event` for `WORKER_BLOCKED`,
`WORKER_NEEDS_CONTROLLER`, or `WORKER_FAILED`; controller review writes
`CONTROLLER_ACCEPTED` or `CONTROLLER_REWORK_REQUESTED`.

Events and reports live below the configured external data directory at
`runs/project-<id>/task-<id>/package-<id>/`, never in a product worktree.  A
live Codex controller reads the current count with `handoff.list_events` and
then blocks through `handoff.wait_for_event`.  The wait is a bounded,
cross-process Windows named-event wait rather than a blind polling timer; it
returns only when a new durable event appears or its explicit timeout expires.
The controller then reviews or escalates and renews the wait while the package
is still open.  A host that has completely ended a Codex conversation must
provide its own continuation/wakeup integration; the MCP server cannot revive
an already-terminated client process.

## Host-level continuation

`handoff.wait_for_event` is the live-controller wait path. It only works while
the controller session is alive and actively waiting inside an MCP tool call.
Host-level continuation is the external recovery path for the opposite case:
the runtime/host process watches durable files under
`GRACE_ORCHESTRATOR_DATA_DIR\runs\...` and starts controller review from those
files after a worker emits `WORKER_READY_FOR_REVIEW`, `WORKER_BLOCKED`,
`WORKER_FAILED`, or `WORKER_NEEDS_CONTROLLER`.

Start it from the repository root:

```powershell
$env:GRACE_ORCHESTRATOR_DATA_DIR = "D:\path\to\grace-orchestrator-state"
$env:GRACE_CODEX_START_COMMAND = 'codex "{prompt_file}"'
.\scripts\Start-GraceHostContinuation.ps1
```

Use `-Once` for a single scan during tests or manual recovery. The supervisor
keeps its cursor under
`GRACE_ORCHESTRATOR_DATA_DIR\host-continuation\cursor.json`, writes per-run
host events to `runs\project-<id>\task-<id>\package-<id>\host-events.ndjson`,
and uses per-run directory locks under `host-continuation\locks\` so two host
instances do not start duplicate controller continuation for the same run.

Continuation has two modes. If per-run controller metadata records a usable
`controller_session_id` and `GRACE_CODEX_RESUME_COMMAND` is configured, the
host attempts that resume command first. Resume syntax is Codex-version and
host-version dependent, so the command is deliberately configured instead of
hardcoded. If resume is unavailable or fails, the reliable baseline is logical
continuation: the host writes a compact controller prompt and launches
`GRACE_CODEX_START_COMMAND`. The configured command can use placeholders such
as `{prompt_file}`, `{data_dir}`, `{run_id}`, `{task_id}`,
`{work_package_id}`, `{report_path}`, `{worktree_path}`, and
`{controller_session_id}`. The prompt file path is also supplied through
`GRACE_CONTROLLER_CONTINUATION_PROMPT_FILE`.

The host emits these durable host events: `HOST_CONTINUATION_DETECTED`,
`HOST_CONTROLLER_RESUME_ATTEMPTED`, `HOST_CONTROLLER_RESUME_STARTED`,
`HOST_CONTROLLER_RESUME_FAILED`, and
`HOST_CONTROLLER_LOGICAL_CONTINUATION_STARTED`. These are workflow wakeup
evidence only. They do not accept a package, do not mutate product runtime
truth, and do not prove that a closed Codex session was revived. A closed
session is resumed only if the configured resume command really starts.

## Enforcement gates

The server is a gatekeeper, not only a ledger. It exposes:

- `gate.contract_discovery`: reads current GRACE/code-adjacent sources and
  returns contracts, M-* refs, V-M-* refs, rule refs, and blocking issues.
- `gate.validate_execution_packet`: rejects worker packets missing task,
  module, verification, scope, contracts-read, test surface, rollback, command,
  stop-condition, or compact-report fields.
- `gate.validate_worker_report`: rejects worker reports missing files read,
  files changed, contract delta, exact command results, scaffolded/wired/
  verified split, unverified gaps, protected-test scope statement, or delta
  proposals.
- `gate.agent_infra_lint`: validates AGENTS/GRACE enforcement files without
  executing a shell command.
- `gate.acceptance_review`: projects whether a task currently satisfies package
  acceptance, worker report, and agent-infra prerequisites.

`workpackage.create`, `submission.create`, GLM acceptance, and Codex final
acceptance call these validators internally. A client can preflight with the
`gate.*` tools, but cannot bypass the same checks by calling the lifecycle
tools directly.

## Transactional hooks

The server has a trusted in-process `HookRegistry`, not a public scripting
surface. It audits and revalidates the relevant scope after task, artifact,
package, submission, and review mutations. A rejected GLM package enables only
its assigned Pro repair path; the Codex final gate requires all canonical GRACE
artifacts; a Codex acceptance closes the task atomically.

`gate.promote` is an internal EventBus event and intentionally is not an MCP
tool. Hooks do not accept shell commands, scripts, or client-defined handlers.
