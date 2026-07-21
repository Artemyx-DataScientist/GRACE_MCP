"""CLI status dashboard for GRACE Orchestrator status read-model."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Sequence

from .service import OrchestratorService


def render_ascii_tree(snapshot: dict[str, Any], *, use_ascii: bool = False, failed_only: bool = False) -> str:
    lines = []
    lines.append(f"GRACE Orchestrator Status Snapshot [{snapshot.get('snapshot_timestamp', '')}]")
    lines.append(f"Total Projects: {snapshot.get('projects_count', 0)}")
    lines.append("=" * 60)

    branch_child = "|-- " if use_ascii else "├── "
    branch_last = "+-- " if use_ascii else "└── "
    pipe_space = "|   " if use_ascii else "│   "
    space_space = "    "

    projects = snapshot.get("projects", [])
    if not projects:
        lines.append("No registered projects found.")
        return "\n".join(lines)

    for p_idx, project in enumerate(projects):
        is_p_last = (p_idx == len(projects) - 1)
        p_prefix = branch_last if is_p_last else branch_child
        lines.append(f"{p_prefix}Project #{project['id']}: {project['name']} (Branch: {project['main_branch']})")

        p_indent = space_space if is_p_last else pipe_space
        tasks = project.get("tasks", [])

        if not tasks:
            lines.append(f"{p_indent}{branch_last}No tasks.")
            continue

        for t_idx, task in enumerate(tasks):
            is_t_last = (t_idx == len(tasks) - 1)
            t_prefix = branch_last if is_t_last else branch_child
            lines.append(f"{p_indent}{t_prefix}Task #{task['id']}: {task['title']} [{task['status']}]")

            t_indent = p_indent + (space_space if is_t_last else pipe_space)
            pkgs = task.get("work_packages", [])

            if failed_only:
                pkgs = [pkg for pkg in pkgs if pkg['status'] in {
                    "HUMAN_INTERVENTION_REQUIRED", "REPAIR_REQUIRED", "FAILED", "CANCELLED"
                }]

            if not pkgs:
                if not failed_only:
                    lines.append(f"{t_indent}{branch_last}No work packages.")
                continue

            for pkg_idx, pkg in enumerate(pkgs):
                is_pkg_last = (pkg_idx == len(pkgs) - 1)
                pkg_prefix = branch_last if is_pkg_last else branch_child
                claimed = f" (Claimed: {pkg['claimed_by_agent']})" if pkg.get("claimed_by_agent") else ""
                lines.append(f"{t_indent}{pkg_prefix}WP #{pkg['id']}: {pkg['title']} [{pkg['status']}]{claimed}")

                pkg_indent = t_indent + (space_space if is_pkg_last else pipe_space)
                sessions = pkg.get("mimo_sessions", [])

                for s_idx, sess in enumerate(sessions):
                    is_s_last = (s_idx == len(sessions) - 1)
                    s_prefix = branch_last if is_s_last else branch_child
                    pid_str = f" PID:{sess['pid']}" if sess.get("pid") else ""
                    lines.append(
                        f"{pkg_indent}{s_prefix}Mimo Session #{sess['id']}: {sess['assigned_agent']} ({sess['mode']}) [{sess['lifecycle_state']}{pid_str}]"
                    )

    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GRACE Orchestrator Status Dashboard")
    parser.add_argument("--data-dir", help="Path to GRACE_ORCHESTRATOR_DATA_DIR (or set env var)")
    parser.add_argument("--project", type=int, help="Filter status by specific project ID")
    parser.add_argument("--json", action="store_true", help="Output status snapshot in JSON format")
    parser.add_argument("--watch", action="store_true", help="Watch status continuously")
    parser.add_argument("--interval", type=float, default=2.0, help="Watch interval in seconds (default: 2.0)")
    parser.add_argument("--failed", action="store_true", help="Show only failed or paused work packages")
    parser.add_argument("--ascii", action="store_true", help="Use safe ASCII characters for tree rendering")

    args = parser.parse_args(argv)

    raw_data_dir = args.data_dir or os.environ.get("GRACE_ORCHESTRATOR_DATA_DIR", "./data")
    data_dir = Path(raw_data_dir).resolve()

    if not data_dir.exists():
        data_dir.mkdir(parents=True, exist_ok=True)

    service = OrchestratorService(data_dir)

    try:
        while True:
            snapshot = service.get_orchestrator_status_snapshot(project_id=args.project)

            if args.watch and not args.json and os.name == "nt":
                os.system("cls")
            elif args.watch and not args.json:
                os.system("clear")

            if args.json:
                print(json.dumps(snapshot, indent=2, ensure_ascii=args.ascii))
            else:
                print(render_ascii_tree(snapshot, use_ascii=args.ascii, failed_only=args.failed))

            if not args.watch:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
