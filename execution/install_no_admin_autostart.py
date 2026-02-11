"""
Install or remove a no-admin autostart launcher for the QBWC stack.

Installs a Startup-folder .cmd that runs:
  python execution/start_qbwc_stack.py --quiet

Usage:
  python execution/install_no_admin_autostart.py
  python execution/install_no_admin_autostart.py --remove
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
STARTUP_DIR = Path(os.environ["APPDATA"]) / r"Microsoft\Windows\Start Menu\Programs\Startup"
LAUNCHER_NAME = "190Group_QBWC_Autostart.cmd"


def _choose_python() -> Path:
    candidate = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
    if candidate.exists():
        return candidate
    return Path("python")


def _launcher_contents(python_exe: Path) -> str:
    script = PROJECT_ROOT / "execution" / "start_qbwc_stack.py"
    return (
        "@echo off\n"
        "setlocal\n"
        f"cd /d \"{PROJECT_ROOT}\"\n"
        f"\"{python_exe}\" \"{script}\" --quiet\n"
        "endlocal\n"
    )


def install() -> Path:
    STARTUP_DIR.mkdir(parents=True, exist_ok=True)
    launcher_path = STARTUP_DIR / LAUNCHER_NAME
    launcher_path.write_text(_launcher_contents(_choose_python()), encoding="utf-8")
    return launcher_path


def remove() -> Path:
    launcher_path = STARTUP_DIR / LAUNCHER_NAME
    if launcher_path.exists():
        launcher_path.unlink()
    return launcher_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Install/remove no-admin QBWC autostart launcher.")
    parser.add_argument("--remove", action="store_true", help="Remove the Startup launcher.")
    args = parser.parse_args()

    if args.remove:
        path = remove()
        print(f"Removed startup launcher: {path}")
        return

    path = install()
    print(f"Installed startup launcher: {path}")


if __name__ == "__main__":
    main()
