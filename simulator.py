"""
Simulates the racecar's OBD source POSTing decoded readings to the server's
ingestion route, so you can see the dashboard move without real hardware.

Usage:
    python simulator.py                       # 2 Hz, forever, http://localhost:8000
    python simulator.py --hz 5 --url http://192.168.1.50:8000
    python simulator.py --laps 3              # also auto-creates a driver stint
"""
import argparse
import math
import time

import requests

FIELD_NAMES = [
    "calculated_engine_load",
    "engine_rpm",
    "vehicle_speed",
    "throttle_position",
    "coolant_temp",
    "intake_air_temperature",
]


def sample(t: float) -> dict:
    """Rough lap-like oscillation so the charts show believable shapes."""
    lap_phase = (t % 90) / 90.0  # pretend a 90s lap
    throttle = max(0.0, math.sin(lap_phase * math.pi * 2) * 60 + 40)
    rpm = 1200 + throttle * 68 + math.sin(t * 3) * 150
    speed = max(0.0, throttle * 2.2 + math.sin(t * 0.7) * 8)
    load = min(100.0, throttle * 0.9 + 10)
    coolant = 85 + math.sin(t / 40) * 4
    intake = 28 + math.sin(t / 55) * 3
    return {
        "calculated_engine_load": round(load, 1),
        "engine_rpm": round(rpm, 0),
        "vehicle_speed": round(speed, 1),
        "throttle_position": round(min(100.0, throttle), 1),
        "coolant_temp": round(coolant, 1),
        "intake_air_temperature": round(intake, 1),
        "source_ts": time.time(),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--hz", type=float, default=2.0, help="samples per second")
    ap.add_argument("--driver", default=None, help="e.g. 'Alex' to also open a stint as driver 1")
    args = ap.parse_args()

    if args.driver:
        requests.post(
            f"{args.url}/api/stints/start",
            json={"driver_number": 1, "driver_name": args.driver},
            timeout=5,
        )
        print(f"Started stint for {args.driver} (driver 1)")

    period = 1.0 / args.hz
    t0 = time.time()
    print(f"Posting to {args.url}/api/ingest at {args.hz} Hz. Ctrl+C to stop.")
    try:
        while True:
            loop_start = time.time()
            payload = sample(loop_start - t0)
            try:
                r = requests.post(f"{args.url}/api/ingest", json=payload, timeout=5)
                r.raise_for_status()
            except requests.RequestException as e:
                print(f"ingest failed: {e}")
            elapsed = time.time() - loop_start
            time.sleep(max(0.0, period - elapsed))
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
