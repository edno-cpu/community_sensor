#!/usr/bin/env python3
"""
DFRobot Gravity (calibrated gas sensor) SO2 over I2C test.

This device is NOT register-based. You must send a command frame then read a response frame.

Protocol highlights (from datasheet):
- Passive/Q&A mode: send command 0x86 to read gas concentration
- Response includes: high byte, low byte, gas type, decimal places
- Concentration = (high*256 + low) * resolution, where resolution depends on decimal places
"""

from time import sleep

try:
    import smbus2
    from smbus2 import i2c_msg
except ImportError:
    raise SystemExit("Please install smbus2:  pip install smbus2")

I2C_BUS = 1
I2C_ADDR = 0x74

# Frame constants (datasheet uses 0xFF start byte and an internal addr byte 0x01)
START = 0xFF
DEV_ADDR_BYTE = 0x01  # internal address byte used in frames
CMD_READ_GAS = 0x86   # read gas concentration in passive mode
CMD_SET_MODE = 0x78   # switch active/passive mode (optional)

MODE_ACTIVE = 0x03
MODE_PASSIVE = 0x04


def checksum(frame9):
    """
    Datasheet checksum:
    check = (invert(byte1 + byte2 + ... + byte7) + 1)  (8-bit)
    Where byte0 is START and byte8 is checksum.
    """
    s = sum(frame9[1:8]) & 0xFF
    return ((~s + 1) & 0xFF)


def xfer(bus, out_bytes, read_len):
    """
    Raw I2C write then read using i2c_rdwr (avoids SMBus "register" semantics).
    """
    w = i2c_msg.write(I2C_ADDR, out_bytes)
    r = i2c_msg.read(I2C_ADDR, read_len)
    bus.i2c_rdwr(w, r)
    return list(r)


def set_mode(bus, mode):
    frame = [START, DEV_ADDR_BYTE, CMD_SET_MODE, mode, 0x00, 0x00, 0x00, 0x00, 0x00]
    frame[8] = checksum(frame)
    resp = xfer(bus, frame, 9)
    return resp


def read_gas(bus):
    # Command frame: FF 01 86 00 00 00 00 00 CS
    frame = [START, DEV_ADDR_BYTE, CMD_READ_GAS, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
    frame[8] = checksum(frame)

    resp = xfer(bus, frame, 8)  # datasheet shows 8-byte return for gas read
    return resp


def decode_gas(resp):
    """
    Expected response (8 bytes) in passive mode:
    [0]=0xFF
    [1]=0x86 (command echo)
    [2]=conc_hi
    [3]=conc_lo
    [4]=gas_type
    [5]=decimal_places
    [6]=--
    [7]=-- (sometimes checksum/unused depending on firmware)
    """
    if len(resp) < 6:
        return None

    if resp[0] != 0xFF:
        return None

    cmd = resp[1]
    conc_hi = resp[2]
    conc_lo = resp[3]
    gas_type = resp[4]
    dec = resp[5]

    raw = conc_hi * 256 + conc_lo

    # Resolution rules from datasheet:
    # dec=0 -> 1
    # dec=1 -> 0.1
    # dec=2 -> 0.01
    res = {0: 1.0, 1: 0.1, 2: 0.01}.get(dec, None)
    ppm = raw * res if res is not None else None

    return {
        "cmd": cmd,
        "raw": raw,
        "gas_type": gas_type,
        "decimals": dec,
        "ppm": ppm,
    }


def main():
    print(f"DFRobot SO2 test on I2C bus {I2C_BUS}, addr 0x{I2C_ADDR:02X}")
    with smbus2.SMBus(I2C_BUS) as bus:
        # Optional: force passive mode
        resp_mode = set_mode(bus, MODE_PASSIVE)
        print("Set passive mode response:", " ".join(f"{b:02X}" for b in resp_mode))

        for i in range(10):
            resp = read_gas(bus)
            decoded = decode_gas(resp)
            print("Gas resp:", " ".join(f"{b:02X}" for b in resp))
            if decoded:
                print(
                    f"  cmd=0x{decoded['cmd']:02X} raw={decoded['raw']} "
                    f"gas_type=0x{decoded['gas_type']:02X} dec={decoded['decimals']} ppm={decoded['ppm']}"
                )
            else:
                print("  Could not decode response (unexpected format).")
            sleep(1)


if __name__ == "__main__":
    main()