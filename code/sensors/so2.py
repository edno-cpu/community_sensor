#!/usr/bin/env python3
"""
SO2 sensor reader (DFRobot Gravity calibrated SO2, I2C, address 0x74)

Outputs a dict with these columns (keep stable with your daily CSV):
  - so2_ppm
  - so2_raw
  - so2_byte0
  - so2_byte1
  - so2_error   (NOW: "OK" if no error)
  - so2_status  ("ok" or "error")

Notes:
- You observed responses like: FF 86 00 00 2B 01 00 00
  This script uses the FF 86 frame format:
    [0]=0xFF, [1]=0x86, [2]=high byte, [3]=low byte, rest ignored here.
  ppm = (high<<8 | low)
- If your module uses a different command/frame, we can adjust.
"""

import logging
from typing import Dict, Any, Optional

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


def _read_frame_ff86() -> Optional[Dict[str, Any]]:
    """
    Attempt to read an 8-byte FF 86 frame from the device.

    Returns dict with ppm/raw/byte0/byte1 if successful, else None.
    """
    global _bus, _addr
    if _bus is None:
        init_so2()

    # Some DFRobot I2C firmwares respond to a "gas read" command 0x86 or 0x78.
    # We will try a couple patterns, but still keep outputs minimal.
    # Pattern A: read 8 bytes starting at 0x00 (many I2C adapters map to a register space)
    candidates = []
    try:
        candidates.append(_bus.read_i2c_block_data(_addr, 0x00, 8))
    except Exception:
        pass

    # Pattern B: read 8 bytes from a command register 0x86
    try:
        candidates.append(_bus.read_i2c_block_data(_addr, 0x86, 8))
    except Exception:
        pass

    # Pattern C: read 8 bytes from 0x78 (you saw cmd=0x78 responses too)
    try:
        candidates.append(_bus.read_i2c_block_data(_addr, 0x78, 8))
    except Exception:
        pass

    for data in candidates:
        if not data or len(data) < 4:
            continue

        # Look for FF 86 header
        if data[0] == 0xFF and data[1] == 0x86:
            b0 = data[2]
            b1 = data[3]
            raw = (b0 << 8) | b1
            ppm = float(raw)

            return {
                "so2_ppm": ppm,
                "so2_raw": raw,
                "so2_byte0": b0,
                "so2_byte1": b1,
            }

    return None


def read_so2() -> Dict[str, Any]:
    """
    Read SO2 value and return stable column dict.

    so2_error is ALWAYS populated:
      - "OK" if no error
      - error string if something went wrong
    """
    result: Dict[str, Any] = {
        "so2_ppm": "",
        "so2_raw": "",
        "so2_byte0": "",
        "so2_byte1": "",
        "so2_error": "OK",
        "so2_status": "ok",
    }

    try:
        frame = _read_frame_ff86()
        if frame is None:
            # No valid frame found this cycle
            result["so2_status"] = "error"
            result["so2_error"] = "NO_FRAME"
            return result

        # Fill outputs (note: 0 is valid)
        result.update(frame)
        result["so2_error"] = "OK"
        result["so2_status"] = "ok"
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