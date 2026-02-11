"""
Stop the QBWC stack without admin rights.

What it does:
1) Stops Python processes running `execution/start_qbwc_service.py`.
2) Stops cloudflared processes running the `qbwc-190group` tunnel.

Usage:
  python execution/stop_qbwc_stack.py
  python execution/stop_qbwc_stack.py --quiet
"""

from __future__ import annotations

import argparse
import subprocess


def _log(message: str, quiet: bool) -> None:
    if not quiet:
        print(message)


def _list_pids_by_filter(process_name: str, commandline_glob: str) -> list[int]:
    script = (
        f"Get-CimInstance Win32_Process -Filter \"Name='{process_name}'\" "
        f"| Where-Object {{ $_.CommandLine -like '*{commandline_glob}*' }} "
        "| Select-Object -ExpandProperty ProcessId"
    )
    proc = subprocess.run(
        ["powershell", "-NoProfile", "-Command", script],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return []

    pids: list[int] = []
    for line in proc.stdout.splitlines():
        cleaned = line.strip()
        if cleaned.isdigit():
            pids.append(int(cleaned))
    return pids


def _stop_pid(pid: int) -> bool:
    proc = subprocess.run(
        ["taskkill", "/PID", str(pid), "/F"],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode == 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Stop QBWC middleware + cloudflared tunnel.")
    parser.add_argument("--quiet", action="store_true", help="Suppress normal status output.")
    args = parser.parse_args()

    middleware_pids = _list_pids_by_filter("python.exe", "start_qbwc_service.py")
    tunnel_pids = _list_pids_by_filter("cloudflared.exe", "qbwc-190group")

    stopped_middleware = sum(1 for pid in middleware_pids if _stop_pid(pid))
    stopped_tunnel = sum(1 for pid in tunnel_pids if _stop_pid(pid))

    _log(
        f"Stopped middleware processes: {stopped_middleware} (matched {len(middleware_pids)}).",
        args.quiet,
    )
    _log(
        f"Stopped tunnel processes: {stopped_tunnel} (matched {len(tunnel_pids)}).",
        args.quiet,
    )


if __name__ == "__main__":
    main()
