"""
Field schema derived from ecu-obd-telemetry/obd_dictionary.py
(https://github.com/a-dejesus/ecu-obd-telemetry).

That module maps raw OBD-II PIDs to a decode formula run on the ECU/OBD
reader side. By the time a reading reaches this server it is already a
decoded numeric value, so we only need the resulting field names + units
to define the ingestion schema, DB columns, and chart axis grouping.

If the source's PID list changes, update this single dict — it drives the
DB schema (db.py), the ingest validation (main.py), and the frontend's
metric picker (GET /api/fields).
"""

# name -> (unit, axis_group). axis_group lets the frontend auto-scale
# series that share a physical unit onto the same y-axis when overlaid,
# without preventing the user from overlaying across groups too.
OBD_FIELDS: dict[str, dict] = {
    "calculated_engine_load": {"unit": "%", "pid": "0104", "axis_group": "percent", "color": "#ff6b35"},
    "engine_rpm":             {"unit": "RPM", "pid": "010C", "axis_group": "rpm", "color": "#e63946"},
    "vehicle_speed":          {"unit": "km/h", "pid": "010D", "axis_group": "speed", "color": "#2a9d8f"},
    "throttle_position":      {"unit": "%", "pid": "0111", "axis_group": "percent", "color": "#f4a261"},
    "coolant_temp":           {"unit": "°C", "pid": "0105", "axis_group": "temp", "color": "#4361ee"},
    "intake_air_temperature": {"unit": "°C", "pid": "010F", "axis_group": "temp", "color": "#7209b7"},
}

FIELD_NAMES: list[str] = list(OBD_FIELDS.keys())
