#!/usr/bin/env python3
"""
Manage rolling 5-minute CSV files.

- Keeps track of current window start
- Writes rows to .csv.part in data/5minute/
- On window change, renames .csv.part -> .csv atomically
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, List

from utils.timekeeping import TimeConfig, floor_to_window, chunk_filename
from utils.atomic import atomic_rename

COLUMNS: List[str] = [
    "timestamp_utc",
    "timestamp_local",
    "node_id",

    "temp_c",
    "rh_pct",
    "pressure_hpa",
    "voc_ohm",

    "pm1_cf1_pms1",
    "pm25_cf1_pms1",
    "pm10_cf1_pms1",
    "pm1_atm_pms1",
    "pm25_atm_pms1",
    "pm10_atm_pms1",
    "n_0_3_pms1",
    "n_0_5_pms1",
    "n_1_0_pms1",
    "n_2_5_pms1",
    "n_5_0_pms1",
    "n_10_pms1",
    "pms1_status",

    "pm1_cf1_pms2",
    "pm25_cf1_pms2",
    "pm10_cf1_pms2",
    "pm1_atm_pms2",
    "pm25_atm_pms2",
    "pm10_atm_pms2",
    "n_0_3_pms2",
    "n_0_5_pms2",
    "n_1_0_pms2",
    "n_2_5_pms2",
    "n_5_0_pms2",
    "n_10_pms2",
    "pms2_status",

    "so2_ppm",
]


@dataclass
class ChunkManager:
    root_dir: Path
    node_id: str
    time_cfg: TimeConfig

    data_dir: Path = field(init=False)
    current_window_start: Optional[datetime] = field(default=None, init=False)
    current_path_part: Optional[Path] = field(default=None, init=False)
    _file: Optional[object] = field(default=None, init=False)
    _writer: Optional[csv.writer] = field(default=None, init=False)

    def __post_init__(self):
        self.data_dir = self.root_dir / "data" / "5minute"
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _open_new_file(self, window_start: datetime) -> None:
        """Open a new .csv.part for the given window_start."""
        self.close()

        filename = chunk_filename(self.node_id, window_start, self.time_cfg)
        part_path = self.data_dir / (filename + ".part")
        is_new = not part_path.exists()

        self._file = part_path.open("a", encoding="utf-8", newline="")
        self._writer = csv.writer(self._file)

        if is_new:
            self._writer.writerow(COLUMNS)
            self._file.flush()

        self.current_window_start = window_start
        self.current_path_part = part_path

    def _finalize_current_file(self) -> None:
        """Close current .part file and rename to .csv."""
        if self._file is None or self.current_path_part is None:
            return

        self._file.flush()
        self._file.close()

        part_path = self.current_path_part
        final_path = part_path.with_suffix("")  # drop .part
        atomic_rename(part_path, final_path)

        self._file = None
        self._writer = None
        self.current_path_part = None
        self.current_window_start = None

    def _roll_if_needed(self, sample_time_utc: datetime) -> None:
        """Roll to a new 5-minute file if the window has changed."""
        if sample_time_utc.tzinfo is None:
            sample_time_utc = sample_time_utc.replace(tzinfo=timezone.utc)

        window_start = floor_to_window(sample_time_utc, self.time_cfg.window_seconds)

        if self.current_window_start is None:
            self._open_new_file(window_start)
            return

        if window_start != self.current_window_start:
            self._finalize_current_file()
            self._open_new_file(window_start)

    def write_sample(self, row: Dict[str, object], sample_time_utc: datetime) -> None:
        """
        Write one sample row to the current 5-minute file, rolling if needed.
        """
        self._roll_if_needed(sample_time_utc)

        if self._file is None or self._writer is None:
            self._open_new_file(
                floor_to_window(sample_time_utc, self.time_cfg.window_seconds)
            )

        out = []
        for col in COLUMNS:
            val = row.get(col, "")
            if val is None:
                val = ""
            out.append(val)

        self._writer.writerow(out)
        self._file.flush()

    def close(self) -> None:
        """Close current file without renaming (e.g., on shutdown)."""
        if self._file is not None:
            self._file.flush()
            self._file.close()
        self._file = None
        self._writer = None
        self.current_path_part = None
        self.current_window_start = None
