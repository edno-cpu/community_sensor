#!/usr/bin/env python3
"""
SO2 sensor reader (DFRobot Gravity calibrated SO2, I2C, address 0x74)

Stable output keys (match your daily CSV columns):
  - so2_ppm
  - so2_raw
  - so2_byte0
  - so2_byte1
  - so2_error   ("OK" if no error; otherwise error code/message)
  - so2_status  ("ok" or "error")

This device is command/response over I2C (not register-mapped).
We send:  FF 01 86 00 00 00 00 00 CS   and read 8 bytes back.

Response (typical):
  [0]=0xFF
  [1]=0x86
  [2]=high
  [3]=low
  [4]=gas_type
  [5]=decimals   (0=>1, 1=>0.1, 2=>0.01)
"""

from __future__ import annotations

import logging
import time
from typing import Dict, Any, Optional, List

try:
    import smbus2
    from smbus2 import i2c_msg
except ImportError as e:
    raise SystemExit("smbus2 is required. Install with: pip install smbus2") from e


I2C_BUS = 1
DEFAULT_ADDR = 0x74

START = 0xFF
DEV_ADDR_BYTE = 0x01

CMD_READ_GAS = 0x86
CMD_SET_MODE = 0x78
MODE_PASSIVE = 0x04

# Safety knobs
MIN_READ_INTERVAL_S = 2.0        # don't hammer I2C
MODE_SET_INTERVAL_S = 3600.0     # at most once/hour

_last_read_monotonic = 0.0
_last_mode_set_monotonic = 0.0

_bus: Optional[smbus2.SMBus] = None
_addr: int = DEFAULT_ADDR


def _close_bus() -> None:
    global _bus
    if _bus is not None:
        try:
            _bus.close()
        except Exception:
            pass
    _bus = None


def init_so2(bus: int = I2C_BUS, address: int = DEFAULT_ADDR) -> None:
    global _bus, _addr
    _addr = address
    if _bus is None:
        _bus = smbus2.SMBus(bus)


def _checksum(frame9: List[int]) -> int:
    s = sum(frame9[1:8]) & 0xFF
    return ((~s + 1) & 0xFF)


def _xfer(out_bytes: List[int], read_len: int) -> List[int]:
    global _bus, _addr
    if _bus is None:
        init_so2()
    assert _bus is not None
    w = i2c_msg.write(_addr, out_bytes)
    r = i2c_msg.read(_addr, read_len)
    _bus.i2c_rdwr(w, r)
    return list(r)


def _maybe_set_passive_mode() -> None:
    global _last_mode_set_monotonic
    now = time.monotonic()
    if (now - _last_mode_set_monotonic) < MODE_SET_INTERVAL_S:
        return

    frame = [START, DEV_ADDR_BYTE, CMD_SET_MODE, MODE_PASSIVE, 0, 0, 0, 0, 0]
    frame[8] = _checksum(frame)
    try:
        _ = _xfer(frame, 8)
        _last_mode_set_monotonic = now
    except Exception:
        # Not fatal; skip
        pass


def _read_gas_frame() -> Optional[List[int]]:
    frame = [START, DEV_ADDR_BYTE, CMD_READ_GAS, 0, 0, 0, 0, 0, 0]
    frame[8] = _checksum(frame)
    try:
        resp = _xfer(frame, 8)
        return resp if resp and len(resp) == 8 else None
    except Exception:
        return None


def _decode(resp: List[int]) -> Optional[Dict[str, Any]]:
    if len(resp) < 6:
        return None
    if resp[0] != 0xFF:
        return None
    if resp[1] != 0x86:
        return None

    b0 = resp[2]
    b1 = resp[3]
    raw = (b0 << 8) | b1

    dec = resp[5]
    scale = {0: 1.0, 1: 0.1, 2: 0.01}.get(dec, 1.0)
    ppm = float(raw) * scale

    return {
        "so2_ppm": ppm,     # 0.0 is valid!
        "so2_raw": raw,
        "so2_byte0": b0,
        "so2_byte1": b1,
    }


def read_so2() -> Dict[str, Any]:
    global _last_read_monotonic

    result: Dict[str, Any] = {
        "so2_ppm": "NODATA",
        "so2_raw": "NODATA",
        "so2_byte0": "NODATA",
        "so2_byte1": "NODATA",
        "so2_error": "OK",
        "so2_status": "ok",
    }

    now = time.monotonic()
    if (now - _last_read_monotonic) < MIN_READ_INTERVAL_S:
        result["so2_status"] = "error"
        result["so2_error"] = "RATE_LIMIT"
        return result
    _last_read_monotonic = now

    try:
        # Don’t do this every loop, just occasionally
        _maybe_set_passive_mode()

        resp = _read_gas_frame()
        if not resp:
            result["so2_status"] = "error"
            result["so2_error"] = "NO_FRAME"
            return result

        decoded = _decode(resp)
        if decoded is None:
            result["so2_status"] = "error"
            result["so2_error"] = "BAD_FRAME"
            return result

        result.update(decoded)
        result["so2_error"] = "OK"
        result["so2_status"] = "ok"
        return result

    except OSError as e:
        # Common I2C issues: errno 5, remote I/O, etc.
        logging.exception("I2C OSError reading SO2")
        result["so2_status"] = "error"
        result["so2_error"] = f"OSError:{getattr(e, 'errno', '')}:{e}"
        _close_bus()  # allow recovery next call
        return result

    except Exception as e:
        logging.exception("Error reading SO2")
        result["so2_status"] = "error"
        result["so2_error"] = str(e)
        return result