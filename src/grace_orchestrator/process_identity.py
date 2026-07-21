"""OS Process Identity and Tri-State Liveness Detection Protocol."""

from __future__ import annotations

import ctypes
from ctypes import wintypes
from dataclasses import dataclass
from enum import Enum, auto
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Sequence


class ProcessMatchState(Enum):
    """Tri-state result for OS process liveness verification."""

    MATCH = auto()
    NOT_FOUND_OR_REUSED = auto()
    UNKNOWN = auto()


@dataclass(frozen=True, slots=True)
class ProcessIdentity:
    """Immutable identity bound to an OS process."""

    pid: int
    process_started_at_os: int | str
    executable_path: str
    argv_hash: str
    launch_nonce: str


def calculate_argv_hash(argv: Sequence[str]) -> str:
    """Calculate a stable hash of process command line arguments."""
    normalized = [str(arg) for arg in argv]
    serialized = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    return sha256(serialized.encode("utf-8")).hexdigest()[:16]


def get_process_start_time_windows(pid: int) -> int | None:
    """Get Windows process creation FILETIME ticks using native Win32 API."""
    if os.name != "nt":
        return None

    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    kernel32 = ctypes.windll.kernel32

    handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        return None

    try:
        creation_time = wintypes.FILETIME()
        exit_time = wintypes.FILETIME()
        kernel_time = wintypes.FILETIME()
        user_time = wintypes.FILETIME()

        res = kernel32.GetProcessTimes(
            handle,
            ctypes.byref(creation_time),
            ctypes.byref(exit_time),
            ctypes.byref(kernel_time),
            ctypes.byref(user_time),
        )
        if not res:
            return None

        # Convert FILETIME structure to 64-bit uint
        ticks = (creation_time.dwHighDateTime << 32) | creation_time.dwLowDateTime
        return ticks
    finally:
        kernel32.CloseHandle(handle)


def get_process_start_time_linux(pid: int) -> str | None:
    """Get Linux process starttime ticks from /proc/<pid>/stat."""
    proc_stat = Path(f"/proc/{pid}/stat")
    if not proc_stat.is_file():
        return None
    try:
        content = proc_stat.read_text(encoding="utf-8")
        # Field 22 (0-indexed: 21) is starttime
        parts = content.split()
        if len(parts) >= 22:
            return parts[21]
    except Exception:
        return None
    return None


def capture_process_identity(pid: int, executable_path: Path | str, argv: Sequence[str], launch_nonce: str = "") -> ProcessIdentity:
    """Capture ProcessIdentity from running OS process."""
    exec_str = str(Path(executable_path).resolve())
    argv_h = calculate_argv_hash(argv)

    start_time: int | str | None = None
    if os.name == "nt":
        start_time = get_process_start_time_windows(pid)
    else:
        start_time = get_process_start_time_linux(pid)

    start_val = start_time if start_time is not None else "UNKNOWN"
    nonce_val = launch_nonce or sha256(f"{pid}_{start_val}".encode()).hexdigest()[:16]

    return ProcessIdentity(
        pid=pid,
        process_started_at_os=start_val,
        executable_path=exec_str,
        argv_hash=argv_h,
        launch_nonce=nonce_val,
    )


def verify_process_liveness(identity: ProcessIdentity) -> ProcessMatchState:
    """Perform tri-state liveness check against running OS process."""
    pid = identity.pid

    # Check if PID exists
    if os.name == "nt":
        current_start_time = get_process_start_time_windows(pid)
        if current_start_time is None:
            # Check if open failed due to permission or non-existence
            # Try minimal access to see if process exists
            PROCESS_QUERY_INFORMATION = 0x0400
            h = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_INFORMATION, False, pid)
            if h:
                ctypes.windll.kernel32.CloseHandle(h)
                return ProcessMatchState.UNKNOWN
            return ProcessMatchState.NOT_FOUND_OR_REUSED

        if str(identity.process_started_at_os) == "UNKNOWN":
            return ProcessMatchState.UNKNOWN

        if str(current_start_time) == str(identity.process_started_at_os):
            return ProcessMatchState.MATCH
        else:
            return ProcessMatchState.NOT_FOUND_OR_REUSED
    else:
        proc_dir = Path(f"/proc/{pid}")
        if not proc_dir.exists():
            return ProcessMatchState.NOT_FOUND_OR_REUSED

        current_start_time_linux: str | None = get_process_start_time_linux(pid)
        if current_start_time_linux is None:
            return ProcessMatchState.UNKNOWN

        if str(identity.process_started_at_os) == "UNKNOWN":
            return ProcessMatchState.UNKNOWN

        if str(current_start_time_linux) == str(identity.process_started_at_os):
            return ProcessMatchState.MATCH
        else:
            return ProcessMatchState.NOT_FOUND_OR_REUSED
