#!/usr/bin/env python3
"""
BME688 reader using the bme680 Python library.

Exposes read_bme(bus, address) -> dict or None.

Returns (if successful):
    {
        "temp_c": float,
        "rh_pct": float,
        "pressure_hpa": float,
        "voc_ohm": float or None,
    }
"""

from __future__ import annotations

from typing import Optional, Dict

import bme680

# We keep a single global sensor instance so we don't re-init on every call.
_sensor: Optional[bme680.BME680] = None


def _ensure_sensor(address: int = 0x76) -> bme680.BME680:
    """
    Initialize the BME680/BME688 sensor if needed and return it.
    """
    global _sensor
    if _sensor is None:
        # i2c_addr uses /dev/i2c-1 by default on a Pi
        sensor = bme680.BME680(i2c_addr=address)

        # Oversampling and filter settings – moderate smoothing
        sensor.set_humidity_oversample(bme680.OS_2X)
        sensor.set_pressure_oversample(bme680.OS_4X)
        sensor.set_temperature_oversample(bme680.OS_8X)
        sensor.set_filter(bme680.FILTER_SIZE_3)

        # Enable gas sensor; for now we just report gas resistance as "voc_ohm"
        sensor.set_gas_status(bme680.ENABLE_GAS_MEAS)
        sensor.set_gas_heater_temperature(320)
        sensor.set_gas_heater_duration(150)
        sensor.select_gas_heater_profile(0)

        _sensor = sensor

    return _sensor


def read_bme(bus: int = 1, address: int = 0x76) -> Optional[Dict[str, float]]:
    """
    Read temperature, relative humidity, pressure, and gas resistance.

    Args:
        bus: I2C bus number (ignored here; we always use /dev/i2c-1).
        address: I2C address (0x76 or 0x77).

    Returns:
        dict with keys: temp_c, rh_pct, pressure_hpa, voc_ohm
        or None if no new data is available.
    """
    try:
        sensor = _ensure_sensor(address=address)

        if sensor.get_sensor_data():
            data = sensor.data
            temp_c = data.temperature
            rh_pct = data.humidity
            pressure_hpa = data.pressure

            # Record VOC resistance even if not heat-stable yet
            voc_ohm = data.gas_resistance

            return {
                "temp_c": temp_c,
                "rh_pct": rh_pct,
                "pressure_hpa": pressure_hpa,
                "voc_ohm": voc_ohm,
            }

        # No new sample at this instant
        return None

    except Exception as e:
        # In production we'll log this; for now we just print.
        print(f"[BME] Error reading sensor at 0x{address:02x}: {e}")
        return None
