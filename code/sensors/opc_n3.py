#!/usr/bin/env python3
"""
OPC-N3 sensor reader via SPI.

Provides an OPCN3 class that reads PM1/PM2.5/PM10 using SPI commands
and returns them as a dict with the same style as PMSReader.
"""

import time
import struct
from typing import Optional, Dict
import spidev


BUSY = 0x31
READY = 0xF3
CMD_PM = 0x32
CMD_INFO = 0x3F


class OPCN3:
    def __init__(
        self,
        bus: int = 0,
        device: int = 0,          # CE0 pin
        spi_mode: int = 0b01,     # Mode 1 per datasheet
        max_speed_hz: int = 500_000
    ):
        self.bus = bus
        self.device = device
        self.spi_mode = spi_mode
        self.max_speed_hz = max_speed_hz
        self._spi: Optional[spidev.SpiDev] = None

    # ---------------------------------------------------
    # Low-level initialization
    # ---------------------------------------------------
    def open(self) -> None:
        if self._spi is None:
            spi = spidev.SpiDev()
            spi.open(self.bus, self.device)
            spi.mode = self.spi_mode
            spi.max_speed_hz = self.max_speed_hz
            self._spi = spi

            # Allow OPC to boot after power-up
            time.sleep(2.0)

    def close(self) -> None:
        if self._spi is not None:
            try:
                self._spi.close()
            except Exception:
                pass
            self._spi = None

    # ---------------------------------------------------
    # Internal helper
    # ---------------------------------------------------
    def _wait_ready(self, cmd: int, timeout: float = 2.0):
        """
        Send a command byte and poll until the OPC returns READY (0xF3).
        """
        spi = self._spi
        if spi is None:
            raise RuntimeError("SPI not initialized. Call open().")

        start = time.time()
        resp = spi.xfer2([cmd])[0]

        while resp == BUSY:
            if time.time() - start > timeout:
                raise TimeoutError("OPC-N3 did not become ready")
            time.sleep(0.01)
            resp = spi.xfer2([cmd])[0]

        if resp != READY:
            raise RuntimeError(f"Unexpected OPC response 0x{resp:02X}")

    # ---------------------------------------------------
    # Public API — same style as PMSReader.read()
    # ---------------------------------------------------
    def read(self) -> Optional[Dict[str, float]]:
        """
        Read PM1, PM2.5, PM10 (µg/m³) from the OPC-N3.
        Returns a dict or None on failure.
        """
        try:
            self.open()
            spi = self._spi

            # 1. Command the OPC to prepare PM data
            self._wait_ready(CMD_PM)

            # 2. Read 14 bytes = PM1, PM2.5, PM10 (floats) + checksum
            raw = spi.xfer2([CMD_PM] * 14)
            raw_bytes = bytes(raw)

            pm1  = struct.unpack_from("<f", raw_bytes, 0)[0]
            pm25 = struct.unpack_from("<f", raw_bytes, 4)[0]
            pm10 = struct.unpack_from("<f", raw_bytes, 8)[0]
            csum = raw[12] | (raw[13] << 8)

            return {
                "pm1": pm1,
                "pm25": pm25,
                "pm10": pm10,
                "csum": csum,
            }

        except Exception as e:
            print(f"[OPCN3] Error reading: {e}")
            return None

    # ---------------------------------------------------
    # Optional: same pattern as PMSReader to support:
    # with OPCN3() as opc:
    #     data = opc.read()
    # ---------------------------------------------------
    def __enter__(self):
        self.open()
        return me

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()