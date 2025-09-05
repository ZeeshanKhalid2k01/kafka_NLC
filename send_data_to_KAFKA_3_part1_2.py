# fetch_new_alerts_psycopg2.py
import json
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP

import psycopg2
import psycopg2.extras as extras
from typing import Optional # Optional for Python < 3.10

# ── PostgreSQL connection params (from you) ───────────────────────────────
DB_PARAMS = {
    "host":     "nlc-db.c1yckugsel2y.ap-south-1.rds.amazonaws.com",
    "port":     5432,
    "dbname":   "VTS",
    "user":     "postgres",
    "password": "admin123",
}

# ── helpers ───────────────────────────────────────────────────────────────
def iso8601_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    # 2025-08-15T07:56:49.762Z
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

def as_str_latlon(val, places=6):
    if val is None:
        return None
    q = Decimal(str(val)).quantize(Decimal("1." + "0"*places), rounding=ROUND_HALF_UP)
    return format(q, "f")
try:
    def compute_wait_window_until(severity: int | None, created_at_utc: datetime) -> datetime:
        """Policy: sev1 +6h, sev2 +2h, sev3 +1h, else +30m."""
        if severity == 1:
            delta = timedelta(hours=6)
        elif severity == 2:
            delta = timedelta(hours=2)
        elif severity == 3:
            delta = timedelta(hours=1)
        else:
            delta = timedelta(minutes=30)
        base = created_at_utc if created_at_utc.tzinfo else created_at_utc.replace(tzinfo=timezone.utc)
        return base + delta
except:
    print("python is less than 3.10")
    def compute_wait_window_until(severity: Optional[int], created_at_utc: datetime) -> datetime:
        """Policy: sev1 +6h, sev2 +2h, sev3 +1h, else +30m."""
        if severity == 1:
            delta = timedelta(hours=6)
        elif severity == 2:
            delta = timedelta(hours=2)
        elif severity == 3:
            delta = timedelta(hours=1)
        else:
            delta = timedelta(minutes=30)
        base = created_at_utc if created_at_utc.tzinfo else created_at_utc.replace(tzinfo=timezone.utc)
        return base + delta


def get_conn():
    return psycopg2.connect(**DB_PARAMS)

# ── bootstrap our own tables (we never modify alerts/call_logs) ───────────
DDL_CREATE = """
CREATE TABLE IF NOT EXISTS alerts_mgmt (
  alert_id                   uuid PRIMARY KEY,
  assignment_group_id        uuid NOT NULL,
  severity                   int,
  alert_created_at           timestamptz NOT NULL,
  wait_window_until          timestamptz,
  status                     text NOT NULL DEFAULT 'pending',   -- pending|enriched|expired|closed

  -- polling bookkeeping (Part 4)
  check_attempts             int NOT NULL DEFAULT 0,
  next_check_at              timestamptz,
  last_checked_at            timestamptz,

  -- enrichment snapshot (for call-logs)
  last_call_log_id           uuid,
  last_call_log_created_at   timestamptz,

  -- send bookkeeping (Part 3)
  needs_send                 boolean NOT NULL DEFAULT true,
  last_sent_at               timestamptz,
  payload_hash               text,
  attempts                   int NOT NULL DEFAULT 0,
  last_error                 text,

  created_at                 timestamptz NOT NULL DEFAULT now(),
  updated_at                 timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS alerts_mgmt_status_idx
  ON alerts_mgmt (status);

CREATE INDEX IF NOT EXISTS alerts_mgmt_status_next_idx
  ON alerts_mgmt (status, next_check_at);

CREATE INDEX IF NOT EXISTS alerts_mgmt_send_idx
  ON alerts_mgmt (needs_send, alert_created_at);

-- Optional companion table for future call-log tracking (not used today)
CREATE TABLE IF NOT EXISTS call_logs_mgmt (
  id                         bigserial PRIMARY KEY,
  alert_id                   uuid NOT NULL,
  last_call_log_id           uuid,
  last_call_created_at       timestamptz,
  updated_at                 timestamptz NOT NULL DEFAULT now()
);
"""

# Keep updated_at fresh (optional)
TOUCH_TRIGGER = """
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'fn_alerts_mgmt_touch_update') THEN
    CREATE OR REPLACE FUNCTION fn_alerts_mgmt_touch_update() RETURNS trigger AS $F$
    BEGIN
      NEW.updated_at := now();
      RETURN NEW;
    END
    $F$ LANGUAGE plpgsql;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_alerts_mgmt_touch_update') THEN
    CREATE TRIGGER trg_alerts_mgmt_touch_update
      BEFORE UPDATE ON alerts_mgmt
      FOR EACH ROW EXECUTE FUNCTION fn_alerts_mgmt_touch_update();
  END IF;
END $$;
"""

# In case the table existed from an older version, make sure all needed columns/indexes exist.
SCHEMA_ENSURE = """
ALTER TABLE alerts_mgmt
  ADD COLUMN IF NOT EXISTS severity                   int,
  ADD COLUMN IF NOT EXISTS alert_created_at           timestamptz,
  ADD COLUMN IF NOT EXISTS wait_window_until          timestamptz,
  ADD COLUMN IF NOT EXISTS status                     text DEFAULT 'pending',
  ADD COLUMN IF NOT EXISTS check_attempts             int  DEFAULT 0,
  ADD COLUMN IF NOT EXISTS next_check_at              timestamptz,
  ADD COLUMN IF NOT EXISTS last_checked_at            timestamptz,
  ADD COLUMN IF NOT EXISTS last_call_log_id           uuid,
  ADD COLUMN IF NOT EXISTS last_call_log_created_at   timestamptz,
  ADD COLUMN IF NOT EXISTS needs_send                 boolean DEFAULT true,
  ADD COLUMN IF NOT EXISTS last_sent_at               timestamptz,
  ADD COLUMN IF NOT EXISTS payload_hash               text,
  ADD COLUMN IF NOT EXISTS attempts                   int  DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_error                 text;

CREATE INDEX IF NOT EXISTS alerts_mgmt_status_next_idx
  ON alerts_mgmt (status, next_check_at);

CREATE INDEX IF NOT EXISTS alerts_mgmt_send_idx
  ON alerts_mgmt (needs_send, alert_created_at);
"""

# ── core SELECT: all alerts NOT already tracked in alerts_mgmt ────────────
CORE_SQL_NEW_ALERTS = """
SELECT
  a.id,
  a.created_at,
  a.type,
  a.trip_log_id,
  a.lat,
  a.lon,
  a.message,
  a.severity,
  a.assignment_group_id,

  v.registration_number,
  d.name  AS driver_name,
  d.phone AS driver_number,
  pmd.device_id AS pmd_device_id,
  csd.device_id AS csd_device_id,
  c.plate_number AS container_plate

FROM alerts a
LEFT JOIN trip_logs    tl   ON tl.id = a.trip_log_id
LEFT JOIN vehicles     v    ON v.id = tl.vehicle_id
LEFT JOIN drivers      d    ON d.id = tl.driver_id
LEFT JOIN pmd_devices  pmd  ON pmd.id = tl.pmd_id
LEFT JOIN csd_devices  csd  ON csd.id = tl.csd_id
LEFT JOIN containers   c    ON c.id = csd.container_id

WHERE NOT EXISTS (
  SELECT 1 FROM alerts_mgmt m WHERE m.alert_id = a.id
)

ORDER BY a.created_at ASC; -- oldest first so registration is monotonic
"""

def build_payload_from_row(r: dict) -> dict:
    """Map DB row -> your locked schema (call logs intentionally ignored)."""
    return {
        "timestamp":                   iso8601_utc(r["created_at"]),
        "alert_id":                    str(r["id"]),
        "alert_name":                  r["type"],
        "vehicle_registration_number": r.get("registration_number"),
        "container_id":                r.get("container_plate"),
        "trip_id":                     str(r["trip_log_id"]) if r.get("trip_log_id") else None,
        "latitude":                    as_str_latlon(r["lat"]),
        "longitude":                   as_str_latlon(r["lon"]),
        "remarks":                     r.get("message"),
        "device_id":                   r.get("pmd_device_id"),
        "device_type":                 "GPS",
        "flag":                        str(r.get("severity")) if r.get("severity") is not None else None,
        "driver_name":                 r.get("driver_name"),
        "driver_number":               r.get("driver_number"),
        "designation":                 "Driver",
        "csd_no":                      r.get("csd_device_id"),
    }

def fetch_all_new_alert_payloads():
    """
    - First run: alerts_mgmt is empty -> fetch ALL alerts.
    - Next runs: fetch ONLY alerts whose id is NOT in alerts_mgmt.
    - Registers each fetched alert in alerts_mgmt so it won't be fetched again.
    Returns list[dict] payloads.
    """
    payloads = []
    rows_to_register = []

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(DDL_CREATE)     # create tables if missing
                cur.execute(TOUCH_TRIGGER)  # ensure touch trigger exists
                cur.execute(SCHEMA_ENSURE)  # bring existing table up to date

            # Stream unseen alerts
            with conn.cursor(name="alerts_stream", cursor_factory=extras.RealDictCursor) as cur:
                cur.itersize = 1000
                cur.execute(CORE_SQL_NEW_ALERTS)
                for r in cur:
                    payloads.append(build_payload_from_row(r))
                    wait_until = compute_wait_window_until(r.get("severity"), r["created_at"])
                    rows_to_register.append((
                        r["id"],                    # alert_id
                        r["assignment_group_id"],   # assignment_group_id
                        r.get("severity"),          # severity
                        r["created_at"],            # alert_created_at
                        wait_until                  # wait_window_until
                    ))

            # Register newly seen alerts in mgmt (initialize poll/send fields too)
            if rows_to_register:
                with conn.cursor() as cur:
                    extras.execute_batch(
                        cur,
                        """
                        INSERT INTO alerts_mgmt (
                          alert_id,
                          assignment_group_id,
                          severity,
                          alert_created_at,
                          wait_window_until,
                          status,
                          check_attempts,
                          next_check_at,
                          needs_send
                        )
                        VALUES (%s, %s, %s, %s, %s, 'pending', 0, now(), true)
                        ON CONFLICT (alert_id) DO NOTHING;
                        """,
                        rows_to_register,
                        page_size=1000
                    )
        return payloads
    finally:
        conn.close()

# ── demo run ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    data = fetch_all_new_alert_payloads()
    print(f"new alerts fetched this run: {len(data)}")
    print(json.dumps(data[:5], indent=2, ensure_ascii=False))
