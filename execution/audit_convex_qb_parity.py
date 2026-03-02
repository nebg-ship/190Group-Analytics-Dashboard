"""
Run an exact Convex vs QuickBooks parity audit for inventory parts.

Compares, by item:
- Quantity on hand (Convex total across all locations)
- Income account
- COGS account
- Asset account
- Purchase cost
- Sales price

Expected QB source is the detailed live cache export produced by QBWC middleware:
  .tmp/qb_items_live_detail_from_qbwc.csv

Usage:
  python execution/audit_convex_qb_parity.py --prod
  python execution/audit_convex_qb_parity.py --prod --qb-csv .tmp/qb_items_live_detail_from_qbwc.csv
  python execution/audit_convex_qb_parity.py --prod --allow-partial
  python execution/audit_convex_qb_parity.py --prod --push-first
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import re
import subprocess
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def normalize_header(value: str) -> str:
    return "".join(ch for ch in (value or "").lower() if ch.isalnum())


def normalize_item_key(value: str) -> str:
    return (value or "").strip().casefold()


def normalize_account(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    # Normalize common mojibake artifacts seen in QB account names.
    text = text.replace("Â·", "·")
    text = text.replace("â€™", "'")
    segments = [segment.strip() for segment in text.split(":") if segment.strip()]
    return ":".join(segments)


def _normalize_account_segment(segment: str) -> str:
    text = (segment or "").strip().lower()
    if not text:
        return ""

    text = text.replace("â€™", "'")
    text = text.replace("Â·", "·")
    text = text.replace("&", " and ")
    text = text.replace("cog's", "cogs")
    text = text.replace("new england bonsai gardens", "nebg")

    # Remove deterministic QB hash suffix added during truncation, e.g. "-6B1051".
    text = re.sub(r"-[0-9a-f]{6}$", "", text)

    # Remove common numeric prefix decorations, e.g. "12100 · Inventory Asset".
    text = re.sub(r"^\d+\s*[·\-]\s*", "", text)
    text = re.sub(r"\s+", " ", text).strip(" -·")

    if text == "inventory asset":
        return "inventory asset"
    return text


def canonical_account_path(value: Any) -> list[str]:
    clean = normalize_account(value)
    if not clean:
        return []
    return [
        normalized
        for normalized in (_normalize_account_segment(segment) for segment in clean.split(":"))
        if normalized
    ]


def _is_prefix_path(a: list[str], b: list[str]) -> bool:
    if not a or not b:
        return False
    if len(a) > len(b):
        return False
    return a == b[: len(a)]


def accounts_match(
    convex_value_raw: Any,
    qb_value_raw: Any,
    *,
    field_name: str,
    mode: str,
) -> tuple[bool, str, str]:
    convex_path = canonical_account_path(convex_value_raw)
    qb_path = canonical_account_path(qb_value_raw)

    convex_value = ":".join(convex_path)
    qb_value = ":".join(qb_path)

    if mode == "strict":
        return convex_value == qb_value, convex_value, qb_value

    # Base normalized mode.
    if convex_value == qb_value:
        return True, convex_value, qb_value

    if field_name == "asset_account":
        # Treat inventory asset label variants as equivalent.
        if convex_path and qb_path and convex_path[-1] == "inventory asset" and qb_path[-1] == "inventory asset":
            return True, convex_value, qb_value
        return False, convex_value, qb_value

    if mode == "normalized":
        return False, convex_value, qb_value

    # Hierarchy mode allows parent/child equivalence and stable root-level mapping.
    if _is_prefix_path(convex_path, qb_path) or _is_prefix_path(qb_path, convex_path):
        return True, convex_value, qb_value

    if field_name == "cogs_account":
        if convex_path and qb_path and convex_path[0] == "cogs" and qb_path[0] == "cogs":
            return True, convex_value, qb_value

    if field_name == "income_account":
        if len(convex_path) >= 2 and len(qb_path) >= 2 and convex_path[:2] == qb_path[:2]:
            return True, convex_value, qb_value

    return False, convex_value, qb_value


def parse_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    if text.startswith("$"):
        text = text[1:]
    try:
        parsed = float(text)
    except ValueError:
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def parse_last_json(stdout: str) -> Any:
    text = (stdout or "").strip()
    if not text:
        raise RuntimeError("Convex command returned empty stdout.")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    start = text.find("[")
    if start != -1:
        end = text.rfind("]")
        if end > start:
            try:
                return json.loads(text[start : end + 1])
            except json.JSONDecodeError:
                pass

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            pass

    raise RuntimeError("Convex command did not return parseable JSON.")


def convex_run(
    function_name: str,
    args_obj: dict[str, Any],
    *,
    env_file: str | None,
    run_prod: bool,
    push: bool,
) -> Any:
    cmd = [
        "node",
        str(PROJECT_ROOT / "node_modules" / "convex" / "bin" / "main.js"),
        "run",
        "--typecheck",
        "disable",
        "--codegen",
        "disable",
    ]
    if push:
        cmd.append("--push")
    if env_file:
        cmd.extend(["--env-file", env_file])
    if run_prod:
        cmd.append("--prod")
    cmd.extend([function_name, json.dumps(args_obj)])

    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    parsed = parse_last_json(proc.stdout)
    if proc.returncode != 0:
        raise RuntimeError(
            "Convex run command failed.\n"
            f"Command: {' '.join(cmd)}\n"
            f"Return code: {proc.returncode}\n"
            f"STDOUT:\n{proc.stdout}\n"
            f"STDERR:\n{proc.stderr}"
        )
    if proc.stderr.strip():
        print(
            "WARNING: Convex CLI returned stderr output.\n"
            f"CLI stderr:\n{proc.stderr.strip()}\n"
        )
    return parsed


def convex_data_inventory_parts(*, limit: int, env_file: str | None, run_prod: bool) -> list[dict[str, Any]]:
    cmd = [
        "node",
        str(PROJECT_ROOT / "node_modules" / "convex" / "bin" / "main.js"),
        "data",
        "inventory_parts",
        "--format",
        "json",
        "--limit",
        str(limit),
    ]
    if env_file:
        cmd.extend(["--env-file", env_file])
    if run_prod:
        cmd.append("--prod")

    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "Convex data command failed.\n"
            f"Command: {' '.join(cmd)}\n"
            f"Return code: {proc.returncode}\n"
            f"STDOUT:\n{proc.stdout}\n"
            f"STDERR:\n{proc.stderr}"
        )
    parsed = parse_last_json(proc.stdout)
    if not isinstance(parsed, list):
        raise RuntimeError(f"Expected JSON array from convex data, got: {type(parsed)}")
    rows: list[dict[str, Any]] = []
    for entry in parsed:
        if isinstance(entry, dict):
            rows.append(entry)
    return rows


def resolve_column(headers: list[str], candidates: list[str]) -> str | None:
    lookup = {normalize_header(header): header for header in headers}
    for candidate in candidates:
        resolved = lookup.get(normalize_header(candidate))
        if resolved:
            return resolved
    return None


def add_item_key_variants(keys: set[str], raw_value: str) -> None:
    clean = (raw_value or "").strip()
    if not clean:
        return
    keys.add(normalize_item_key(clean))
    if ":" in clean:
        leaf = clean.rsplit(":", 1)[1].strip()
        if leaf:
            keys.add(normalize_item_key(leaf))


def load_qb_rows(path: Path) -> tuple[list[dict[str, str]], dict[str, str | None], list[str]]:
    if not path.exists():
        raise RuntimeError(f"QB CSV not found: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as infile:
        reader = csv.DictReader(infile)
        headers = list(reader.fieldnames or [])
        if not headers:
            raise RuntimeError(f"QB CSV is missing headers: {path}")
        rows = [dict(row) for row in reader]

    column_map = {
        "item_full_name": resolve_column(
            headers,
            [
                "qbItemFullName",
                "Full Name",
                "Item Name/Number",
                "Sku",
                "SKU",
                "Name",
                "Item",
            ],
        ),
        "quantity": resolve_column(
            headers,
            [
                "quantityOnHand",
                "Quantity On Hand",
                "TOTAL",
            ],
        ),
        "income_account": resolve_column(
            headers,
            [
                "incomeAccountFullName",
                "Income Account",
                "Account",
            ],
        ),
        "cogs_account": resolve_column(
            headers,
            [
                "cogsAccountFullName",
                "COGS Account",
                "COGS_Account",
            ],
        ),
        "asset_account": resolve_column(
            headers,
            [
                "assetAccountFullName",
                "Asset Account",
                "Asset_Account",
            ],
        ),
        "purchase_cost": resolve_column(
            headers,
            [
                "purchaseCost",
                "Purchase Cost",
                "Cost",
            ],
        ),
        "sales_price": resolve_column(
            headers,
            [
                "salesPrice",
                "Sales Price",
                "Price",
            ],
        ),
    }
    return rows, column_map, headers


def build_qb_lookup(
    qb_rows: list[dict[str, str]],
    qb_columns: dict[str, str | None],
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    qb_records: list[dict[str, Any]] = []
    qb_by_key: dict[str, dict[str, Any]] = {}

    full_name_column = qb_columns["item_full_name"]
    if not full_name_column:
        raise RuntimeError("QB CSV is missing an item full name/SKU column.")

    for row in qb_rows:
        full_name = str(row.get(full_name_column, "") or "").strip()
        if not full_name:
            continue
        record = {
            "qbItemFullName": full_name,
            "quantityOnHand": parse_float(row.get(qb_columns["quantity"], "")) if qb_columns["quantity"] else None,
            "incomeAccountFullName": (
                str(row.get(qb_columns["income_account"], "") or "").strip()
                if qb_columns["income_account"]
                else ""
            ),
            "cogsAccountFullName": (
                str(row.get(qb_columns["cogs_account"], "") or "").strip()
                if qb_columns["cogs_account"]
                else ""
            ),
            "assetAccountFullName": (
                str(row.get(qb_columns["asset_account"], "") or "").strip()
                if qb_columns["asset_account"]
                else ""
            ),
            "purchaseCost": parse_float(row.get(qb_columns["purchase_cost"], "")) if qb_columns["purchase_cost"] else None,
            "salesPrice": parse_float(row.get(qb_columns["sales_price"], "")) if qb_columns["sales_price"] else None,
        }
        qb_records.append(record)

        key_variants: set[str] = set()
        add_item_key_variants(key_variants, full_name)
        for key in key_variants:
            qb_by_key.setdefault(key, record)

    return qb_by_key, qb_records


def values_match_number(a: float | None, b: float | None, tolerance: float = 1e-6) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) <= tolerance


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        # Always write headers for deterministic downstream reads.
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit Convex vs QuickBooks parity for inventory parts.")
    parser.add_argument(
        "--qb-csv",
        default=".tmp/qb_items_live_detail_from_qbwc.csv",
        help="QB CSV containing item detail fields (accounts/cost/price/quantity).",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional env file for Convex deployment selection.",
    )
    parser.add_argument(
        "--prod",
        action="store_true",
        help="Read Convex production deployment.",
    )
    parser.add_argument(
        "--push-first",
        action="store_true",
        help="Push Convex functions before running the audit.",
    )
    parser.add_argument(
        "--convex-limit",
        type=int,
        default=20000,
        help="Max inventory_parts rows to read from Convex.",
    )
    parser.add_argument(
        "--balances-limit",
        type=int,
        default=50000,
        help="Max on-hand total rows to accept from Convex.",
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Allow audit when required QB columns are missing (exact parity disabled).",
    )
    parser.add_argument(
        "--min-qb-rows",
        type=int,
        default=1000,
        help="Fail if QB dataset has fewer rows than this threshold (guards against smoke/test cache files).",
    )
    parser.add_argument(
        "--account-normalization",
        choices=["strict", "normalized", "hierarchy"],
        default="strict",
        help=(
            "Account comparison mode. "
            "'strict' requires exact account path equality, "
            "'normalized' applies naming/encoding normalization, "
            "'hierarchy' also treats parent/child account paths as equivalent."
        ),
    )
    args = parser.parse_args()

    if args.convex_limit < 1:
        raise RuntimeError("--convex-limit must be >= 1")
    if args.balances_limit < 1:
        raise RuntimeError("--balances-limit must be >= 1")
    if args.min_qb_rows < 1:
        raise RuntimeError("--min-qb-rows must be >= 1")

    qb_path = Path(args.qb_csv)
    qb_rows, qb_columns, qb_headers = load_qb_rows(qb_path)

    required_qb_fields = [
        "item_full_name",
        "quantity",
        "income_account",
        "cogs_account",
        "asset_account",
        "purchase_cost",
        "sales_price",
    ]
    missing_required = [field for field in required_qb_fields if not qb_columns.get(field)]
    if missing_required and not args.allow_partial:
        raise RuntimeError(
            "QB CSV is missing required columns for exact parity audit: "
            f"{missing_required}. Detected headers: {qb_headers}"
        )

    convex_rows = convex_data_inventory_parts(
        limit=args.convex_limit,
        env_file=args.env_file,
        run_prod=args.prod,
    )
    balances_payload = convex_run(
        "inventory:getOnHandTotalsBySku",
        {},
        env_file=args.env_file,
        run_prod=args.prod,
        push=args.push_first,
    )
    if not isinstance(balances_payload, dict):
        raise RuntimeError("Convex getOnHandTotalsBySku did not return an object.")
    balance_rows = balances_payload.get("rows")
    if not isinstance(balance_rows, list):
        raise RuntimeError("Convex getOnHandTotalsBySku did not return rows.")

    if len(balance_rows) >= args.balances_limit and not args.allow_partial:
        raise RuntimeError(
            "inventory_balances totals hit balances-limit; rerun with --allow-partial "
            "or increase --balances-limit."
        )

    on_hand_by_sku: dict[str, float] = {}
    on_hand_by_key: dict[str, float] = {}
    for balance in balance_rows:
        if not isinstance(balance, dict):
            continue
        sku = str(balance.get("sku") or "").strip()
        if not sku:
            continue
        qty = parse_float(balance.get("onHand"))
        if qty is None:
            continue
        on_hand_by_sku[sku] = qty
        on_hand_by_key[normalize_item_key(sku)] = qty

    qb_by_key, qb_records = build_qb_lookup(qb_rows, qb_columns)
    if len(qb_records) < args.min_qb_rows:
        raise RuntimeError(
            f"QB dataset is too small for a production parity audit: {len(qb_records)} rows "
            f"(minimum required: {args.min_qb_rows}). "
            "This usually means the live QB cache was not hydrated yet or a smoke/test file is being used."
        )

    field_specs = [
        ("quantity", "Quantity_On_Hand_2025", "quantityOnHand", "number"),
        ("income_account", "Account", "incomeAccountFullName", "account"),
        ("cogs_account", "COGS_Account", "cogsAccountFullName", "account"),
        ("asset_account", "Asset_Account", "assetAccountFullName", "account"),
        ("purchase_cost", "Cost", "purchaseCost", "number"),
        ("sales_price", "Price", "salesPrice", "number"),
    ]

    field_stats: dict[str, dict[str, int]] = {}
    unavailable_fields: list[str] = []
    for field_name, _, _, _ in field_specs:
        column_present = qb_columns.get(field_name if field_name != "quantity" else "quantity")
        if not column_present:
            unavailable_fields.append(field_name)
        field_stats[field_name] = {"compared": 0, "matched": 0, "mismatched": 0}

    missing_in_qb: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    matched_qb_full_names: set[str] = set()
    all_fields_match_count = 0
    matched_convex_count = 0

    for part in convex_rows:
        sku = str(part.get("Sku", "") or "").strip()
        qb_item_full_name = str(part.get("qbItemFullName", "") or "").strip()

        key_candidates: set[str] = set()
        add_item_key_variants(key_candidates, qb_item_full_name)
        add_item_key_variants(key_candidates, sku)

        qb_record = None
        for key in key_candidates:
            qb_record = qb_by_key.get(key)
            if qb_record is not None:
                break

        if qb_record is None:
            missing_in_qb.append(
                {
                    "sku": sku,
                    "qbItemFullName": qb_item_full_name,
                    "activeStatus": part.get("Active_Status"),
                    "isActive": part.get("isActive"),
                    "quantityOnHand2025": part.get("Quantity_On_Hand_2025"),
                    "account": part.get("Account"),
                    "cogsAccount": part.get("COGS_Account"),
                    "assetAccount": part.get("Asset_Account"),
                    "cost": part.get("Cost"),
                    "price": part.get("Price"),
                }
            )
            continue

        matched_convex_count += 1
        matched_qb_full_names.add(str(qb_record.get("qbItemFullName") or ""))
        part_all_fields_match = True

        for field_name, convex_key, qb_key, field_type in field_specs:
            if not qb_columns.get(field_name if field_name != "quantity" else "quantity"):
                continue

            field_stats[field_name]["compared"] += 1
            if field_name == "quantity":
                total_on_hand = on_hand_by_sku.get(sku)
                if total_on_hand is None:
                    total_on_hand = on_hand_by_key.get(normalize_item_key(sku), 0.0)
                convex_value_raw = total_on_hand
            else:
                convex_value_raw = part.get(convex_key)
            qb_value_raw = qb_record.get(qb_key)

            if field_type == "number":
                convex_value = parse_float(convex_value_raw)
                qb_value = parse_float(qb_value_raw)
                is_match = values_match_number(convex_value, qb_value)
                delta = None
                if convex_value is not None and qb_value is not None:
                    delta = qb_value - convex_value
            else:
                is_match, convex_value, qb_value = accounts_match(
                    convex_value_raw,
                    qb_value_raw,
                    field_name=field_name,
                    mode=args.account_normalization,
                )
                delta = None

            if is_match:
                field_stats[field_name]["matched"] += 1
            else:
                field_stats[field_name]["mismatched"] += 1
                part_all_fields_match = False
                mismatches.append(
                    {
                        "sku": sku,
                        "qbItemFullNameConvex": qb_item_full_name,
                        "qbItemFullNameQB": qb_record.get("qbItemFullName"),
                        "field": field_name,
                        "convexValue": convex_value,
                        "qbValue": qb_value,
                        "delta": delta,
                    }
                )

        if part_all_fields_match:
            all_fields_match_count += 1

    missing_in_convex: list[dict[str, Any]] = []
    for qb_record in qb_records:
        full_name = str(qb_record.get("qbItemFullName") or "")
        if full_name and full_name not in matched_qb_full_names:
            missing_in_convex.append(qb_record)

    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_path = PROJECT_ROOT / ".tmp" / f"qb_parity_audit_summary_{timestamp}.json"
    mismatch_path = PROJECT_ROOT / ".tmp" / f"qb_parity_mismatches_{timestamp}.csv"
    missing_in_qb_path = PROJECT_ROOT / ".tmp" / f"qb_parity_missing_in_qb_{timestamp}.csv"
    missing_in_convex_path = PROJECT_ROOT / ".tmp" / f"qb_parity_missing_in_convex_{timestamp}.csv"

    write_csv(mismatch_path, mismatches)
    write_csv(missing_in_qb_path, missing_in_qb)
    write_csv(missing_in_convex_path, missing_in_convex)

    summary = {
        "generatedAt": dt.datetime.now(dt.UTC).isoformat(),
        "qbCsv": str(qb_path),
        "accountNormalizationMode": args.account_normalization,
        "convexRows": len(convex_rows),
        "qbRows": len(qb_records),
        "matchedConvexRows": matched_convex_count,
        "allFieldsMatchCount": all_fields_match_count,
        "missingInQbCount": len(missing_in_qb),
        "missingInConvexCount": len(missing_in_convex),
        "mismatchCount": len(mismatches),
        "unavailableFields": unavailable_fields,
        "fieldStats": field_stats,
        "outputs": {
            "summaryJson": str(summary_path),
            "mismatchesCsv": str(mismatch_path),
            "missingInQbCsv": str(missing_in_qb_path),
            "missingInConvexCsv": str(missing_in_convex_path),
        },
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
