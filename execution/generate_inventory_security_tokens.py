"""
Generate Batch 4 inventory API security tokens and .env snippet.

Usage:
  python execution/generate_inventory_security_tokens.py
  python execution/generate_inventory_security_tokens.py --length 64 --approval-threshold 10
  python execution/generate_inventory_security_tokens.py --no-write-file
"""

from __future__ import annotations

import argparse
import json
import secrets
import string
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / ".tmp" / "inventory_security_tokens.env"
TOKEN_ALPHABET = string.ascii_letters + string.digits


def generate_token(length: int) -> str:
    if length < 16:
        raise ValueError("Token length must be at least 16 characters.")
    return "".join(secrets.choice(TOKEN_ALPHABET) for _ in range(length))


def build_env_snippet(write_token: str, admin_token: str, approval_threshold: float) -> str:
    threshold_value = int(approval_threshold) if approval_threshold.is_integer() else approval_threshold
    lines = [
        f"# Generated: {datetime.now(timezone.utc).isoformat()}",
        f"INVENTORY_WRITE_TOKEN={write_token}",
        f"INVENTORY_ADMIN_TOKEN={admin_token}",
        "INVENTORY_REQUIRE_APPROVAL=true",
        f"INVENTORY_APPROVAL_QTY_THRESHOLD={threshold_value}",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate inventory API security tokens and env snippet.")
    parser.add_argument(
        "--length",
        type=int,
        default=48,
        help="Token length (minimum 16, default 48).",
    )
    parser.add_argument(
        "--approval-threshold",
        type=float,
        default=25,
        help="Default approval quantity threshold.",
    )
    parser.add_argument(
        "--write-token",
        default="",
        help="Optional explicit write token override.",
    )
    parser.add_argument(
        "--admin-token",
        default="",
        help="Optional explicit admin token override.",
    )
    parser.add_argument(
        "--output-path",
        default=str(DEFAULT_OUTPUT_PATH),
        help="Where to write env snippet.",
    )
    parser.add_argument(
        "--no-write-file",
        action="store_true",
        help="Skip writing file and print snippet only.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON summary in addition to snippet.",
    )
    args = parser.parse_args()

    if args.approval_threshold < 0:
        raise ValueError("--approval-threshold must be non-negative.")

    write_token = args.write_token.strip() or generate_token(args.length)
    admin_token = args.admin_token.strip() or generate_token(args.length)
    snippet = build_env_snippet(write_token, admin_token, args.approval_threshold)

    output_path = Path(args.output_path).resolve()
    if not args.no_write_file:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(snippet, encoding="utf-8")

    print(snippet, end="")

    if args.json:
        print(
            json.dumps(
                {
                    "outputPath": str(output_path),
                    "wroteFile": not args.no_write_file,
                    "length": args.length,
                    "approvalThreshold": args.approval_threshold,
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
