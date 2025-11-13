#!/usr/bin/env python3
"""
Main data collection loop.

- Loads config/node.yaml
- Sets up PMS1, PMS2, BME, SO2 (SO2 still stubbed)
- Every tick:
    - reads sensors
    - builds a row dict
    - hands it to ChunkManager to write into 5-minute chunk file
"""

from __future__ import annotations

import time
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import yaml  # pip install pyyaml

from sensors.pms import PMSReader
from sensors.bme import read_bme
from sensors.so2 import read_so2
from utils.timekeeping import (
    TimeConfig,
    now_utc,
    utc_to_local,
    isoformat_utc_z,
    isoformat_local,
)
from manage_5min import ChunkManager


def load_config(root: Path) -> Dict[str, Any]:
    cfg_path = root / "config" / "node.yaml"
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging(root: Path) -> None:
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


def main() -> None:
    root = Path(__file__).resolve().parents[1]  # project root
    setup_logging(root)
    log = logging.getLogger("emis.collect")

    cfg = load_config(root)
    node_id: str = cfg.get("node_id", "NodeX")
    tz_name: str = cfg.get("timezone", "UTC")
    tick_seconds: float = float(cfg.get("tick_seconds", 1.0))

    chunk_cfg_raw = cfg.get("chunks", {})
    time_cfg = TimeConfig(
        timezone_name=tz_name,
        window_seconds=int(chunk_cfg_raw.get("window_seconds", 300)),
        use_utc_filenames=bool(chunk_cfg_raw.get("use_utc_filenames", True)),
    )

    # Sensor config
    s_cfg = cfg.get("sensors", {})

    pms1_reader: Optional[PMSReader] = None
    pms2_reader: Optional[PMSReader] = None

    if s_cfg.get("pms1", {}).get("enabled", False):
        p1_port = s_cfg["pms1"]["port"]
        pms1_reader = PMSReader(p1_port)
        log.info(f"PMS1 enabled on {p1_port}")

    if s_cfg.get("pms2", {}).get("enabled", False):
        p2_port = s_cfg["pms2"]["port"]
        pms2_reader = PMSReader(p2_port)
        log.info(f"PMS2 enabled on {p2_port}")

    bme_enabled = s_cfg.get("bme", {}).get("enabled", False)
    bme_bus = s_cfg.get("bme", {}).get("i2c_bus", 1)
    bme_addr = s_cfg.get("bme", {}).get("address", 0x76)

    so2_enabled = s_cfg.get("so2", {}).get("enabled", False)
    so2_bus = s_cfg.get("so2", {}).get("i2c_bus", 1)
    so2_addr = s_cfg.get("so2", {}).get("address", 0x75)

    cm = ChunkManager(root_dir=root, node_id=node_id, time_cfg=time_cfg)

    log.info("Starting main collection loop")

    try:
        while True:
            t_utc = now_utc()
            t_local = utc_to_local(t_utc, tz_name)

            row: Dict[str, Any] = {
                "timestamp_utc": isoformat_utc_z(t_utc),
                "timestamp_local": isoformat_local(t_local),
                "node_id": node_id,
            }

            # BME
            if bme_enabled:
                try:
                    b = read_bme(bus=bme_bus, address=bme_addr)
                    if b:
                        row["temp_c"] = b.get("temp_c")
                        row["rh_pct"] = b.get("rh_pct")
                        row["pressure_hpa"] = b.get("pressure_hpa")
                        row["voc_ohm"] = b.get("voc_ohm")
                except Exception as e:
                    log.warning(f"BME read error: {e}")

            # PMS1
            if pms1_reader is not None:
                try:
                    s1 = pms1_reader.read()
                    if s1:
                        # Treat pm1/pm25/pm10 from PMSReader as atmospheric mass
                        row["pm1_atm_pms1"] = s1["pm1"]
                        row["pm25_atm_pms1"] = s1["pm25"]
                        row["pm10_atm_pms1"] = s1["pm10"]
                        row["pms1_status"] = "ok"
                    else:
                        row["pms1_status"] = "no_frame"
                except Exception as e:
                    row["pms1_status"] = f"error:{e}"
                    log.warning(f"PMS1 read error: {e}")

            # PMS2
            if pms2_reader is not None:
                try:
                    s2 = pms2_reader.read()
                    if s2:
                        row["pm1_atm_pms2"] = s2["pm1"]
                        row["pm25_atm_pms2"] = s2["pm25"]
                        row["pm10_atm_pms2"] = s2["pm10"]
                        row["pms2_status"] = "ok"
                    else:
                        row["pms2_status"] = "no_frame"
                except Exception as e:
                    row["pms2_status"] = f"error:{e}"
                    log.warning(f"PMS2 read error: {e}")

            # SO2
            if so2_enabled:
                try:
                    v = read_so2(bus=so2_bus, address=so2_addr)
                    if v is not None:
                        row["so2_ppm"] = v
                except Exception as e:
                    log.warning(f"SO2 read error: {e}")

            cm.write_sample(row=row, sample_time_utc=t_utc)

            time.sleep(tick_seconds)

    except KeyboardInterrupt:
        log.info("Stopping collection loop (KeyboardInterrupt)")

    finally:
        cm.close()
        if pms1_reader is not None:
            pms1_reader.close()
        if pms2_reader is not None:
            pms2_reader.close()
        log.info("Shutdown complete")


if __name__ == "__main__":
    main()
