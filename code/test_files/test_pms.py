#!/usr/bin/env python3

"""
Simple PMS test script.

Reads once from PMS1 (/dev/ttyS0) and PMS2 (/dev/ttyAMA0)
and prints the results.
"""

from sensors.pms import PMSReader


def test_port(port: str, label: str):
    print(f"\n=== Testing {label} on {port} ===")
    p = PMSReader(port)
    sample = p.read()
    print("Sample:", sample)


def main():
    test_port("/dev/ttyS0", "PMS1")
    test_port("/dev/ttyAMA0", "PMS2")


if __name__ == "__main__":
    main()
