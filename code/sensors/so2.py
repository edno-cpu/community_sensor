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

Frame formats observed on your bus:
  - FF 86 ...   (common DFRobot gas read frame style)
  - FF 78 ...   (you also saw cmd=0x78 responses)

This reader:
  1) tries to "poke" the device with a command (0x86 then 0x78),
  2) reads 8 bytes from register 0x00,
  3) parses FF 86 / FF 78 frames:
       raw = (byte2<<8) | byte3
       ppm = float(raw)   # placeholder scaling; adjust if your sensor uses /10, /100, etc.
"""

import logging
from typing import Dict, Any, Optional, List

try:
    import smbus2 as smbus
except ImportError:
    import smbus  # type: ignore

I2C_BUS = 1
DEFAULT_ADDR = 0x74

_bus = None
_addr = DEFAULT_ADDR


def init_so2(bus: int = I2C_BUS, address: int = DEFAULT_ADDR) -> None:
    """Initialize the I2C bus and remember the SO2 address. Safe to call multiple times."""
    global _bus, _addr
    _addr = address
    if _bus is None:
        _bus = smbus.SMBus(bus)


def _read8_from_reg0() -> Optional[List[int]]:
    """Read 8 bytes from register 0x00; return list of ints or None."""
    global _bus, _addr
    if _bus is None:
        init_so2()
    try:
        data = _bus.read_i2c_block_data(_addr, 0x00, 8)
        if data and len(data) == 8:
            return data
        return None
    except Exception:
        return None


def _poke_then_read(cmd: int) -> Optional[List[int]]:
    """
    Try to "poke" the device with a command byte, then read from reg 0x00.
    Many DFRobot I2C firmwares behave like this.
    """
    global _bus, _addr
    if _bus is None:
        init_so2()

    # Some firmwares accept just a command byte written; others ignore it.
    # We try the simplest safe write: write_byte (falls back to SMBus "quick command" style).
    try:
        _bus.write_byte(_addr, cmd)
    except Exception:
        # If write fails, still try read — sometimes sensor just streams latest value.
        pass

    return _read8_from_reg0()


def _parse_frame(data: List[int]) -> Optional[Dict[str, Any]]:
    """
    Parse either FF 86 or FF 78 style frames.
    Returns dict with so2_ppm/so2_raw/so2_byte0/so2_byte1 if recognized, else None.
    """
    if not data or len(data) < 4:
        return None

    if data[0] != 0xFF:
        return None

    if data[1] not in (0x86, 0x78):
        return None

    b0 = data[2]
    b1 = data[3]
    raw = (b0 << 8) | b1

    # Placeholder: treat raw as ppm directly (as you saw: raw=256 -> ppm=256.0).
    # If later you learn the scaling (e.g., raw/10), change only this line.
    ppm = float(raw)

    return {
        "so2_ppm": ppm,
        "so2_raw": raw,
        "so2_byte0": b0,
        "so2_byte1": b1,
    }


def read_so2() -> Dict[str, Any]:
    """
    Read SO2 and return stable column dict.

    - so2_error is ALWAYS populated ("OK" if fine)
    - so2_ppm is NEVER blank:
        - numeric when parsed
        - "NODATA" when no frame
    """
    result: Dict[str, Any] = {
        "so2_ppm": "NODATA",
        "so2_raw": "NODATA",
        "so2_byte0": "NODATA",
        "so2_byte1": "NODATA",
        "so2_error": "OK",
        "so2_status": "ok",
    }

    try:
        # Try command + read patterns in the order you observed
        for cmd in (0x86, 0x78):
            data = _poke_then_read(cmd)
            if not data:
                continue

            frame = _parse_frame(data)
            if frame:
                # Fill outputs; allow zeros as valid data
                result.update(frame)
                result["so2_error"] = "OK"
                result["so2_status"] = "ok"
                return result

        # If we got here: no recognized frame this cycle
        result["so2_status"] = "error"
        result["so2_error"] = "NO_FRAME"
        return result

    except Exception as e:
        logging.exception("Error reading SO2 sensor")
        result["so2_status"] = "error"
        result["so2_error"] = str(e)
        return result


def _pretty_print_reading() -> None:
    """Helper for standalone testing from the command line."""
    from time import sleep

    print(f"Testing SO2 on I2C bus {I2C_BUS}, address 0x{DEFAULT_ADDR:02X}")
    init_so2()

    try:
        while True:
            r = read_so2()
            print(
                f"ppm={r['so2_ppm']} raw={r['so2_raw']} "
                f"b0={r['so2_byte0']} b1={r['so2_byte1']} "
                f"status={r['so2_status']} err={r['so2_error']}"
            )
            sleep(1.0)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    _pretty_print_reading()