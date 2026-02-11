"""
Start the QBWC stack without admin rights.

What it does:
1) Ensures cloudflared tunnel is running.
2) Ensures QBWC middleware is listening on 127.0.0.1:8085.
3) Avoids duplicate starts by checking process/port first.

Usage:
  python execution/start_qbwc_stack.py
  python execution/start_qbwc_stack.py --quiet
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
TMP_DIR = PROJECT_ROOT / ".tmp" / "autostart"
CLOUDFLARED_EXE = Path(r"C:\Program Files (x86)\cloudflared\cloudflared.exe")
CLOUDFLARED_CONFIG = Path.home() / ".cloudflared" / "config.yml"
MIDDLEWARE_HOST = "127.0.0.1"
MIDDLEWARE_PORT = 8085
TUNNEL_NAME = "qbwc-190group"


def _log(message: str, quiet: bool) -> None:
    if not quiet:
        print(message)


def _is_port_open(host: str, port: int, timeout_seconds: float = 0.5) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout_seconds)
        return sock.connect_ex((host, port)) == 0


def _process_running(image_name: str) -> bool:
    proc = subprocess.run(
        ["tasklist", "/FI", f"IMAGENAME eq {image_name}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return False
    return image_name.lower() in proc.stdout.lower()


def _detached_flags() -> int:
    if os.name != "nt":
        return 0
    return (
        subprocess.CREATE_NEW_PROCESS_GROUP
        | subprocess.DETACHED_PROCESS
        | subprocess.CREATE_NO_WINDOW
    )


def _choose_python() -> Path:
    candidates = [
        PROJECT_ROOT / ".venv" / "Scripts" / "python.exe",
        Path(sys.executable),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return Path("python")


def _start_cloudflared(quiet: bool) -> None:
    if _process_running("cloudflared.exe"):
        _log("cloudflared already running.", quiet)
        return
    if not CLOUDFLARED_EXE.exists():
        raise RuntimeError(f"cloudflared executable not found at {CLOUDFLARED_EXE}")
    if not CLOUDFLARED_CONFIG.exists():
        raise RuntimeError(f"cloudflared config not found at {CLOUDFLARED_CONFIG}")

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    out_log = TMP_DIR / "cloudflared.out.log"
    err_log = TMP_DIR / "cloudflared.err.log"

    with out_log.open("a", encoding="utf-8") as out_handle, err_log.open("a", encoding="utf-8") as err_handle:
        subprocess.Popen(
            [
                str(CLOUDFLARED_EXE),
                "tunnel",
                "--config",
                str(CLOUDFLARED_CONFIG),
                "run",
                TUNNEL_NAME,
            ],
            cwd=str(PROJECT_ROOT),
            stdout=out_handle,
            stderr=err_handle,
            creationflags=_detached_flags(),
        )
    _log("Started cloudflared tunnel.", quiet)


def _start_middleware(quiet: bool) -> None:
    if _is_port_open(MIDDLEWARE_HOST, MIDDLEWARE_PORT):
        _log("QBWC middleware already listening on 127.0.0.1:8085.", quiet)
        return

    python_exe = _choose_python()
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    out_log = TMP_DIR / "qbwc_service.out.log"
    err_log = TMP_DIR / "qbwc_service.err.log"

    env = os.environ.copy()
    env.setdefault("CONVEX_RUN_PROD", "true")

    with out_log.open("a", encoding="utf-8") as out_handle, err_log.open("a", encoding="utf-8") as err_handle:
        subprocess.Popen(
            [str(python_exe), "execution/start_qbwc_service.py"],
            cwd=str(PROJECT_ROOT),
            env=env,
            stdout=out_handle,
            stderr=err_handle,
            creationflags=_detached_flags(),
        )
    _log("Started QBWC middleware.", quiet)


def main() -> None:
    parser = argparse.ArgumentParser(description="Start QBWC tunnel + middleware if not already running.")
    parser.add_argument("--quiet", action="store_true", help="Suppress normal status output.")
    args = parser.parse_args()

    _start_cloudflared(quiet=args.quiet)
    _start_middleware(quiet=args.quiet)

    deadline = time.time() + 12
    while time.time() < deadline:
        if _is_port_open(MIDDLEWARE_HOST, MIDDLEWARE_PORT):
            _log("QBWC stack is ready.", args.quiet)
            return
        time.sleep(0.4)
    raise SystemExit("QBWC middleware did not bind to 127.0.0.1:8085.")


if __name__ == "__main__":
    main()
