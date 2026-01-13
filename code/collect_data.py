#!/usr/bin/env python3
"""
Main data collection loop (daily CSV only).

Writes directly to:
    data/daily/<node_id>_YYYY-MM-DD.csv

Column order is defined by daily_writer.py (COLUMNS).
"""

from __future__ import annotations

import time
import logging
from collections import deque
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from sensors.pms import PMSReader
from sensors.bme import read_bme
from sensors.so2 import init_so2, read_so2
# from sensors.opc_n3 import OPCN3

from utils.timekeeping import now_utc, utc_to_local, isoformat_utc_z, isoformat_local
from daily_writer import DailyWriter


def load_config(root: Path) -> Dict[str, Any]:
    cfg_path = root / "config" / "node.yaml"
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def setup_logging(root: Path) -> logging.Logger:
    log_dir = root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "emis.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger("emis.collect")


# -----------------------
# PMS agreement logic (PurpleAir-style)
# -----------------------
BASELINE_N = 30      # rolling baseline length (samples)
MIN_PM = 1.0         # below this, don't overreact to mismatch
RPD_OK = 0.25        # <= 25% relative percent difference = OK


def rpd(a: float, b: float) -> Optional[float]:
    m = 0.5 * (a + b)
    if m <= 0:
        return None
    return abs(a - b) / m


def median(xs):
    xs = sorted(xs)
    n = len(xs)
    if n == 0:
        return None
    return xs[n // 2] if n % 2 else 0.5 * (xs[n // 2 - 1] + xs[n // 2])


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    log = setup_logging(root)

    cfg = load_config(root)
    node_id: str = cfg.get("node_id", "NodeX")
    tz_name: str = cfg.get("timezone", "UTC")
    tick_seconds: float = float(cfg.get("tick_seconds", 1.0))

    s_cfg: Dict[str, Any] = cfg.get("sensors", {}) or {}

    # Rolling baselines for PMS diagnostics
    pms1_hist = deque(maxlen=BASELINE_N)
    pms2_hist = deque(maxlen=BASELINE_N)

    # -----------------------
    # Initialise sensors
    # -----------------------
    pms1_reader: Optional[PMSReader] = None
    pms2_reader: Optional[PMSReader] = None
    # opc_reader: Optional[OPCN3] = None

    # PMS1
    p1 = s_cfg.get("pms1", {}) or {}
    if p1.get("enabled", False):
        port = p1.get("port")
        if port:
            pms1_reader = PMSReader(port)
            log.info(f"PMS1 enabled on {port}")
        else:
            log.warning("PMS1 enabled but no port provided; disabling")

    # PMS2
    p2 = s_cfg.get("pms2", {}) or {}
    if p2.get("enabled", False):
        port = p2.get("port")
        if port:
            pms2_reader = PMSReader(port)
            log.info(f"PMS2 enabled on {port}")
        else:
            log.warning("PMS2 enabled but no port provided; disabling")

    # BME688
    b_cfg = s_cfg.get("bme", {}) or {}
    bme_enabled = bool(b_cfg.get("enabled", False))
    bme_bus = int(b_cfg.get("i2c_bus", 1))
    bme_addr = b_cfg.get("address", 0x76)
    try:
        bme_addr = int(str(bme_addr), 0)
    except Exception:
        bme_addr = 0x76

    # SO2
    so_cfg = s_cfg.get("so2", {}) or {}
    so2_enabled = bool(so_cfg.get("enabled", False))
    so2_bus = int(so_cfg.get("i2c_bus", 1))
    so2_addr = so_cfg.get("address", 0x74)
    try:
        so2_addr = int(str(so2_addr), 0)
    except Exception:
        so2_addr = 0x74

    if so2_enabled:
        try:
            init_so2(bus=so2_bus, address=so2_addr)
            log.info(f"SO2 enabled on I2C bus {so2_bus}, addr 0x{so2_addr:02X}")
        except Exception as e:
            log.warning(f"Disabling SO2 after init failure: {e}")
            so2_enabled = False

    # Daily writer
    dw = DailyWriter(root_dir=root, node_id=node_id, tz_name=tz_name)
    log.info("Starting collection loop (daily CSV only)")

    try:
        while True:
            t_utc = now_utc()
            t_local = utc_to_local(t_utc, tz_name)

            row: Dict[str, Any] = {
                "timestamp_utc": isoformat_utc_z(t_utc),
                "timestamp_local": isoformat_local(t_local),
                "node_id": node_id,
            }

            # ---- BME ----
            if bme_enabled:
                try:
                    b = read_bme(bus=bme_bus, address=bme_addr)
                    if b:
                        row["temp_c"] = b.get("temp_c")
                        row["rh_pct"] = b.get("rh_pct")
                        row["pressure_hpa"] = b.get("pressure_hpa")
                        row["voc_ohm"] = b.get("voc_ohm")
                        row["bme_status"] = "ok"
                    else:
                        row["bme_status"] = "no_data"
                except Exception as e:
                    row["bme_status"] = f"error:{e}"
                    log.warning(f"BME read error: {e}")

            # ---- PMS1 ----
            if pms1_reader is not None:
                try:
                    s1 = pms1_reader.read()
                    if s1:
                        row["pm1_atm_pms1"] = s1.get("pm1")
                        row["pm25_atm_pms1"] = s1.get("pm25")
                        row["pm10_atm_pms1"] = s1.get("pm10")
                        row["pms1_status"] = "ok"
                    else:
                        row["pms1_status"] = "no_frame"
                except Exception as e:
                    row["pms1_status"] = f"error:{e}"
                    log.warning(f"PMS1 read error: {e}")

            # ---- PMS2 ----
            if pms2_reader is not None:
                try:
                    s2 = pms2_reader.read()
                    if s2:
                        row["pm1_atm_pms2"] = s2.get("pm1")
                        row["pm25_atm_pms2"] = s2.get("pm25")
                        row["pm10_atm_pms2"] = s2.get("pm10")
                        row["pms2_status"] = "ok"
                    else:
                        row["pms2_status"] = "no_frame"
                except Exception as e:
                    row["pms2_status"] = f"error:{e}"
                    log.warning(f"PMS2 read error: {e}")

            # ---- PMS pair diagnostics (PM2.5) ----
            row["pm25_pms_mean"] = "NODATA"
            row["pm25_pms_rpd"] = "NODATA"
            row["pm25_pair_flag"] = "NODATA"
            row["pm25_suspect_sensor"] = "NODATA"   # will be set to OK if nothing is suspect

            pm1 = row.get("pm25_atm_pms1")
            pm2 = row.get("pm25_atm_pms2")
            st1 = row.get("pms1_status", "")
            st2 = row.get("pms2_status", "")

            # Update rolling baselines from "ok" readings
            if st1 == "ok" and pm1 is not None:
                try:
                    pms1_hist.append(float(pm1))
                except Exception:
                    pass

            if st2 == "ok" and pm2 is not None:
                try:
                    pms2_hist.append(float(pm2))
                except Exception:
                    pass

            # Status-first logic
            if st1 != "ok" and st2 != "ok":
                row["pm25_pair_flag"] = "BOTH_BAD"
                row["pm25_suspect_sensor"] = "BOTH"
            elif st1 != "ok":
                row["pm25_pair_flag"] = "PMS1_BAD"
                row["pm25_suspect_sensor"] = "PMS1"
            elif st2 != "ok":
                row["pm25_pair_flag"] = "PMS2_BAD"
                row["pm25_suspect_sensor"] = "PMS2"
            elif pm1 is not None and pm2 is not None:
                try:
                    pm1f = float(pm1)
                    pm2f = float(pm2)

                    mean_pm = 0.5 * (pm1f + pm2f)
                    row["pm25_pms_mean"] = mean_pm

                    if mean_pm >= MIN_PM:
                        d = rpd(pm1f, pm2f)
                        row["pm25_pms_rpd"] = d

                        if d is not None and d <= RPD_OK:
                            row["pm25_pair_flag"] = "OK"
                        else:
                            b1 = median(list(pms1_hist))
                            b2 = median(list(pms2_hist))

                            dev1 = abs(pm1f - b1) / max(b1, MIN_PM) if b1 is not None else 0.0
                            dev2 = abs(pm2f - b2) / max(b2, MIN_PM) if b2 is not None else 0.0

                            row["pm25_pair_flag"] = "MISMATCH"
                            if dev1 > dev2 * 1.5:
                                row["pm25_suspect_sensor"] = "PMS1"
                            elif dev2 > dev1 * 1.5:
                                row["pm25_suspect_sensor"] = "PMS2"
                            else:
                                row["pm25_suspect_sensor"] = "BOTH"
                    else:
                        row["pm25_pair_flag"] = "LOW_PM_OK"

                except Exception:
                    row["pm25_pair_flag"] = "ERROR"
                    row["pm25_suspect_sensor"] = "UNKNOWN"
            else:
                # Not enough data to compare (e.g., one PM missing but status ok)
                row["pm25_pair_flag"] = "INCOMPLETE"
                row["pm25_suspect_sensor"] = "UNKNOWN"

            # If nothing ended up being "suspect", explicitly mark OK.
            if row.get("pm25_suspect_sensor", "") == "" and row.get("pm25_pair_flag", "") in ("OK", "LOW_PM_OK"):
                row["pm25_suspect_sensor"] = "OK"
            # ---- SO2 ----
            if so2_enabled:
                try:
                    v = read_so2()
                    row["so2_ppm"]   = v.get("so2_ppm")
                    row["so2_raw"]   = v.get("so2_raw")
                    row["so2_byte0"] = v.get("so2_byte0")
                    row["so2_byte1"] = v.get("so2_byte1")
                    row["so2_error"] = v.get("so2_error")     # "OK" if fine
                    row["so2_status"] = v.get("so2_status")   # "ok" or "error"
                except Exception as e:
                    row["so2_ppm"] = "NODATA"
                    row["so2_error"] = f"exception:{e}"
                    row["so2_status"] = "error"
                    log.warning(f"SO2 read error: {e}")


            # Write
            dw.write_sample(row=row, sample_time_utc=t_utc)
            time.sleep(tick_seconds)

    except KeyboardInterrupt:
        log.info("Stopping collection loop (KeyboardInterrupt)")

    finally:
        dw.close()
        if pms1_reader is not None:
            pms1_reader.close()
        if pms2_reader is not None:
            pms2_reader.close()
        log.info("Shutdown complete")


if __name__ == "__main__":
    main()