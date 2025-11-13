#!/usr/bin/env python3
"""
Timekeeping utilities for the ÉMIS node.

- now_utc(): timezone-aware UTC now
- utc_to_local(): convert UTC -> local zone
- floor_to_window(): floor a datetime to a fixed-size window
- chunk_filename(): build the 5-minute chunk filename
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo  # Python 3.9+


@dataclass(frozen=True)
class TimeConfig:
    timezone_name: str
    window_seconds: int
    use_utc_filenames: bool = True


def now_utc() -> datetime:
    """Return current UTC time (timezone-aware)."""
    return datetime.now(timezone.utc)


def utc_to_local(dt_utc: datetime, tz_name: str) -> datetime:
    """Convert UTC datetime to local timezone given by tz_name."""
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    tz = ZoneInfo(tz_name)
    return dt_utc.astimezone(tz)


def floor_to_window(dt: datetime, window_seconds: int) -> datetime:
    """
    Floor a datetime to the start of the window of length window_seconds.

    Example: 14:07 with 300-second windows (5 min) → 14:05.
    Operates in the datetime's own timezone.
    """
    epoch = dt.replace(minute=0, second=0, microsecond=0)
    delta = dt - epoch
    windows = int(delta.total_seconds() // window_seconds)
    return epoch + timedelta(seconds=windows * window_seconds)


def isoformat_utc_z(dt_utc: datetime) -> str:
    """
    ISO8601 string for UTC with trailing 'Z', e.g. 2025-11-16T14:05:03.123Z
    """
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    s = dt_utc.astimezone(timezone.utc).isoformat(timespec="milliseconds")
    return s.replace("+00:00", "Z")


def isoformat_local(dt: datetime) -> str:
    """ISO8601 string for a local datetime (keep offset)."""
    return dt.isoformat(timespec="milliseconds")


def chunk_filename(node_id: str, window_start: datetime, cfg: TimeConfig) -> str:
    """
    Build the filename for a 5-minute chunk.

    Pattern: NodeID_YYYY-MM-DD_HH-MM.csv
    window_start is expected as UTC if use_utc_filenames=True.
    """
    if cfg.use_utc_filenames:
        dt = window_start.astimezone(timezone.utc)
    else:
        dt = window_start

    date_part = dt.strftime("%Y-%m-%d")
    time_part = dt.strftime("%H-%M")  # window start minute
    return f"{node_id}_{date_part}_{time_part}.csv"
