from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from typing import Any


@dataclass
class ConvexCliClient:
    env_file: str = ""
    run_prod: bool = False

    def _command(self, function_name: str, args_obj: dict[str, Any]) -> list[str]:
        base_cmd = [
            "npx",
            "convex",
            "run",
            "--typecheck",
            "disable",
            "--codegen",
            "disable",
        ]
        if self.env_file:
            base_cmd.extend(["--env-file", self.env_file])
        if self.run_prod:
            base_cmd.append("--prod")
        base_cmd.extend([function_name, json.dumps(args_obj)])

        if os.name == "nt":
            return ["cmd", "/c", *base_cmd]
        return base_cmd

    @staticmethod
    def _extract_json(stdout: str) -> dict[str, Any] | None:
        first_brace = stdout.find("{")
        last_brace = stdout.rfind("}")
        if first_brace == -1 or last_brace == -1 or last_brace <= first_brace:
            return None

        candidate = stdout[first_brace : last_brace + 1]
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            return None
        if not isinstance(parsed, dict):
            return None
        return parsed

    def run(self, function_name: str, args_obj: dict[str, Any]) -> dict[str, Any]:
        cmd = self._command(function_name, args_obj)
        proc = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
        )

        parsed = self._extract_json(proc.stdout)
        if parsed is None:
            raise RuntimeError(
                "Convex command did not return parseable JSON.\n"
                f"Command: {' '.join(cmd)}\n"
                f"Return code: {proc.returncode}\n"
                f"STDOUT:\n{proc.stdout}\n"
                f"STDERR:\n{proc.stderr}"
            )
        return parsed

    def get_next_pending_event(self, limit: int = 1) -> dict[str, Any]:
        return self.run(
            "qb_queue:getNextPendingQbEvent",
            {"limit": limit},
        )

    def mark_event_in_flight(self, event_id: str, ticket: str) -> dict[str, Any]:
        return self.run(
            "qb_queue:markEventInFlight",
            {"eventId": event_id, "ticket": ticket},
        )

    def apply_qb_result(
        self,
        event_id: str,
        ticket: str | None,
        *,
        success: bool,
        qb_txn_id: str | None = None,
        qb_txn_type: str | None = None,
        qb_error_code: str | None = None,
        qb_error_message: str | None = None,
        retryable: bool | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "eventId": event_id,
            "success": success,
        }
        if ticket is not None:
            payload["ticket"] = ticket
        if qb_txn_id is not None:
            payload["qbTxnId"] = qb_txn_id
        if qb_txn_type is not None:
            payload["qbTxnType"] = qb_txn_type
        if qb_error_code is not None:
            payload["qbErrorCode"] = qb_error_code
        if qb_error_message is not None:
            payload["qbErrorMessage"] = qb_error_message
        if retryable is not None:
            payload["retryable"] = retryable
        return self.run("qb_queue:applyQbResult", payload)
