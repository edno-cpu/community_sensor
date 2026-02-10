#!/usr/bin/env python3
"""
Print sensor status based on the latest row in today's daily CSV.

Logic (file-based, no hardware probing):
- If required columns for a sensor are missing from the file header -> Not integrated
- Else:
    - For PMS sensors: use the pmsX_status column as the primary indicator
    - For SO2 (DFRobot): use so2_status / so2_error if present
    - For others: if latest row has "present" values for the sensor's columns -> recording
               else -> connected but not recording

Important detail:
- A value of "0" (zero) IS considered present/recording.
- Strings like "NODATA" / "BAD_FRAME" are treated as NOT present.
"""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import yaml

# --- ANSI styling (works over SSH terminals) ---
RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"


def fmt(color: str, text: str) -> str:
    return f"{BOLD}{color}{text}{RESET}"


# name, required value columns, optional status column (if present)
SENSORS: List[Tuple[str, List[str], Optional[str]]] = [
    ("PMS-1", ["pm1_atm_pms1", "pm25_atm_pms1", "pm10_atm_pms1"], "pms1_status"),
    ("PMS-2", ["pm1_atm_pms2", "pm25_atm_pms2", "pm10_atm_pms2"], "pms2_status"),
    ("BME688", ["temp_c", "rh_pct", "pressure_hpa"], "bme_status"),

    # OPC-N3 (leave here if columns exist in your DailyWriter; otherwise it'll show Not integrated)
    ("OPC-N3", ["pm1_atm_opc", "pm25_atm_opc", "pm10_atm_opc"], "opc_status"),

    # SO2 (DFRobot Gravity)
    # If you compute so2_ppm, include it. so2_status/so2_error help a lot.
    ("SO2", ["so2_ppm", "so2_raw", "so2_byte0", "so2_byte1"], "so2_status"),
]


def load_config(root: Path) -> Dict:
    cfg_path = root / "config" / "node.yaml"
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def today_local_datestr() -> str:
    return datetime.now().date().isoformat()


def newest_daily_file(daily_dir: Path, node_id: str) -> Optional[Path]:
    today = today_local_datestr()
    p = daily_dir / f"{node_id}_{today}.csv"
    if p.exists():
        return p

    candidates = sorted(
        daily_dir.glob(f"{node_id}_*.csv"),
        key=lambda x: x.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def read_header_and_last_row(path: Path) -> Tuple[List[str], Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        last: Optional[List[str]] = None
        for row in reader:
            if row:
                last = row

        if not header or last is None:
            return header, {}

        n = min(len(header), len(last))
        header = header[:n]
        last = last[:n]
        return header, {
            header[i]: (last[i].strip() if last[i] is not None else "")
            for i in range(n)
        }


# Strings that mean “not a real measurement”
NON_DATA_TOKENS = {
    "na", "nan", "none", "null",
    "nodata", "no_frame", "bad_frame", "rate_limit",
    "error", "unknown", "incomplete",
}


def is_present_value(v: str) -> bool:
    """
    True if the string value should count as "present/recording".

    - Empty string -> not present
    - NA-like values -> not present
    - NODATA/BAD_FRAME/etc -> not present
    - "0" / "0.0" / etc -> PRESENT (important!)
    """
    if v is None:
        return False
    s = str(v).strip()
    if s == "":
        return False

    sl = s.lower()

    # Treat tokens (including your pipeline codes) as “not present”
    if sl in NON_DATA_TOKENS:
        return False

    # Treat "error:whatever" or "exception:whatever" as not present
    if sl.startswith("error") or sl.startswith("exception") or sl.startswith("bad_") or sl.startswith("no_"):
        return False

    return True


def any_present(vals: Dict[str, str], cols: List[str]) -> bool:
    return any(is_present_value(vals.get(c, "")) for c in cols)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    cfg = load_config(root)
    node_id = cfg.get("node_id", "NodeX")

    daily_dir = root / "data" / "daily"
    path = newest_daily_file(daily_dir, node_id)

    if path is None:
        print("Sensor Status:")
        print(fmt(RED, "  No daily CSV found."))
        return

    header, last_vals = read_header_and_last_row(path)
    header_set = set(header)

    print(f"Sensor Status (file: {path.name})")

    if not header:
        print(fmt(YELLOW, "  Daily file has no header/rows yet."))
        return
    if not last_vals:
        print(fmt(YELLOW, "  Daily file has header but no data rows yet."))
        return

    for name, cols, status_col in SENSORS:
        # 1) Schema / integration check
        missing = [c for c in cols if c not in header_set]
        if missing:
            print(f"  {fmt(RED, f'{name}: Not integrated (missing columns)')}")
            continue

        # 2) Value + status extraction
        values_present = any_present(last_vals, cols)
        status_val = last_vals.get(status_col, "").strip() if status_col else ""
        err_val = last_vals.get("so2_error", "").strip() if name == "SO2" else ""

        # 3) PMS-specific: status-driven truth
        if name.startswith("PMS"):
            if status_val == "":
                print(f"  {fmt(RED, f'{name}: Not connected')}")
            elif status_val == "ok" and values_present:
                print(f"  {fmt(GREEN, f'{name}: Connected and recording')}")
            elif status_val.startswith("error"):
                print(f"  {fmt(RED, f'{name}: Error ({status_val})')}")
            elif status_val == "no_frame":
                print(f"  {fmt(RED, f'{name}: Not connected (no_frame)')}")
            else:
                if values_present:
                    print(f"  {fmt(GREEN, f'{name}: Connected (status={status_val})')}")
                else:
                    print(f"  {fmt(YELLOW, f'{name}: Not recording (status={status_val})')}")
            continue

        # 4) SO2: use status column if available
        if name == "SO2":
            # so2_status should be "ok" or "error"
            if status_val == "ok" and values_present:
                print(f"  {fmt(GREEN, 'SO2: Connected and recording')}")
            elif status_val == "ok" and not values_present:
                # This can happen if so2_status was left ok but values are NODATA
                msg = "SO2: Status ok but no valid data"
                if err_val:
                    msg += f" (so2_error={err_val})"
                print(f"  {fmt(YELLOW, msg)}")
            else:
                # error path
                msg = "SO2: Not recording"
                if status_val:
                    msg += f" (status={status_val})"
                if err_val:
                    msg += f" (so2_error={err_val})"
                print(f"  {fmt(RED, msg)}")
            continue

        # 5) Generic sensors
        if values_present:
            if status_val and status_val not in ("ok",):
                print(f"  {fmt(YELLOW, f'{name}: Recording but status={status_val}')}")
            else:
                print(f"  {fmt(GREEN, f'{name}: Connected and recording')}")
        else:
            if status_val:
                print(f"  {fmt(YELLOW, f'{name}: Connected but not recording (status={status_val})')}")
            else:
                print(f"  {fmt(YELLOW, f'{name}: Connected but not recording')}")


if __name__ == "__main__":
    main()