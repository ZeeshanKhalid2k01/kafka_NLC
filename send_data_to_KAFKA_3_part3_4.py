# sender_poller_psycopg2.py
import os, json, hashlib, time
from datetime import timedelta, datetime, timezone
from collections import deque
import psycopg2
import psycopg2.extras as extras

from confluent_kafka import Producer, KafkaException, KafkaError
from typing import Optional #if python <3.10
from send_data_to_KAFKA_3_part1_2 import (
    DB_PARAMS, get_conn, iso8601_utc, as_str_latlon, compute_wait_window_until
)

from dotenv import load_dotenv
from pathlib import Path
import os

# Load .env.events file
load_dotenv(dotenv_path=Path(".env.alerts"), override=True)
# ───────── Kafka config + single Producer instance ─────────
KAFKA_CONF = {
    "bootstrap.servers":       os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol":       os.environ.get("KAFKA_SECURITY_PROTOCOL"),
    "sasl.mechanisms":         os.environ.get("KAFKA_SASL_MECHANISMS"),
    "sasl.username":           os.environ.get("KAFKA_SASL_USERNAME"),
    "sasl.password":           os.environ.get("KAFKA_SASL_PASSWORD"),
    "enable.idempotence":      True,
    "acks":                    "all",
    # quiet the warning you saw:
    "retry.backoff.ms":        2000,
    "retry.backoff.max.ms":    2000,
    "socket.keepalive.enable": True,
}
# ALERTS_TOPIC = "nl_alerts"
ALERTS_TOPIC = os.getenv("EVENT_DATA_TOPIC", "nl_alerts")

# ALERTS_TOPIC = "nl_Alerts_Data"

try:
  _PRODUCER: Producer | None = None
except:
    print("python is less than 3.10")
    _PRODUCER: Optional[Producer] = None
    
# Keep last N delivery outcomes in memory (for ad-hoc debugging if needed)
RECENT_KAFKA_STATUS = deque(maxlen=200)
    
def _delivery_cb(err, msg):
    key = None
    try:
        key = msg.key().decode("utf-8") if msg.key() else None
    except Exception:
        pass
    status = {
        "ts": iso8601_utc(datetime.now(timezone.utc)),
        "topic": msg.topic(),
        "key": key,
        "partition": msg.partition(),
        "offset": msg.offset(),
        "error": str(err) if err else None,
    }
    RECENT_KAFKA_STATUS.append(status)
    if err:
        print(f"[KAFKA-ERROR ] topic={msg.topic()} key={key} err={err}")
    else:
        print(f"[KAFKA-SENT  ] topic={msg.topic()} key={key} part={msg.partition()} off={msg.offset()}")

def _get_producer() -> Producer:
    global _PRODUCER
    if _PRODUCER is None:
        _PRODUCER = Producer(KAFKA_CONF)
    return _PRODUCER

def _flush_producer(timeout_sec: float = 3.0):
    try:
        if _PRODUCER is not None:
            _PRODUCER.flush(timeout_sec)
    except Exception:
        pass

# ───────── Local JSONL debug ─────────
# Load KAFKA_LOG_PATH from .env, fallback to default if not set
KAFKA_LOG_PATH = os.getenv("KAFKA_LOG_PATH")
if not KAFKA_LOG_PATH:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    KAFKA_LOG_PATH = os.path.join(BASE_DIR, "kafka_logs.jsonl")


MAX_LOG_LINES = os.getenv("MAX_LOG_LINES")
if MAX_LOG_LINES is None:
    MAX_LOG_LINES = 10000

# def append_kafka_log(key: str, payload: dict, payload_hash: str, sent_ok: bool, error: str | None):
# it will run on even if python < 3.10
def append_kafka_log(key: str, payload: dict, payload_hash: str, sent_ok: bool, error: Optional[str]):
    rec = {
        "logged_at": iso8601_utc(datetime.now(timezone.utc)),
        "key": key,
        "payload_hash": payload_hash,
        "sent_ok": sent_ok,
        "error": error,
        "payload": payload,
    }
    # Append the new log entry
    with open(KAFKA_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    # Trim the file to the last MAX_LOG_LINES lines
    try:
        with open(KAFKA_LOG_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) > MAX_LOG_LINES:
            with open(KAFKA_LOG_PATH, "w", encoding="utf-8") as f:
                f.writelines(lines[-MAX_LOG_LINES:])
    except Exception:
        pass

def payload_hash(payload: dict) -> str:
    # Create a copy to avoid modifying the original
    payload_for_hash = payload.copy()
    
    # Remove timestamps that change on every build
    if "data_sent_timestamp" in payload_for_hash:
        payload_for_hash.pop("data_sent_timestamp")
    
    canon = json.dumps(payload_for_hash, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canon).hexdigest()

# ───────── Schema guard (idempotent) ─────────
SCHEMA_ENSURE = """
ALTER TABLE alerts_mgmt
  ADD COLUMN IF NOT EXISTS check_attempts             int DEFAULT 0,
  ADD COLUMN IF NOT EXISTS next_check_at              timestamptz,
  ADD COLUMN IF NOT EXISTS last_checked_at            timestamptz,
  ADD COLUMN IF NOT EXISTS severity                   int,
  ADD COLUMN IF NOT EXISTS alert_created_at           timestamptz,
  ADD COLUMN IF NOT EXISTS wait_window_until          timestamptz,
  ADD COLUMN IF NOT EXISTS last_call_log_id           uuid,
  ADD COLUMN IF NOT EXISTS last_call_log_created_at   timestamptz,
  ADD COLUMN IF NOT EXISTS status                     text DEFAULT 'pending',
  ADD COLUMN IF NOT EXISTS needs_send                 boolean DEFAULT true,
  ADD COLUMN IF NOT EXISTS last_sent_at               timestamptz,
  ADD COLUMN IF NOT EXISTS payload_hash               text,
  ADD COLUMN IF NOT EXISTS attempts                   int DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_error                 text;

CREATE INDEX IF NOT EXISTS alerts_mgmt_status_next_idx
  ON alerts_mgmt (status, next_check_at);

CREATE INDEX IF NOT EXISTS alerts_mgmt_send_idx
  ON alerts_mgmt (needs_send, alert_created_at);

CREATE TABLE IF NOT EXISTS call_logs_mgmt (
  id                      bigserial PRIMARY KEY,
  alert_id                uuid NOT NULL,
  last_call_log_id        uuid,
  last_call_created_at    timestamptz,
  updated_at              timestamptz NOT NULL DEFAULT now()
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname = ANY (current_schemas(true))
      AND indexname = 'call_logs_mgmt_alert_id_key'
  ) THEN
    BEGIN
      ALTER TABLE call_logs_mgmt
        ADD CONSTRAINT call_logs_mgmt_alert_id_key UNIQUE (alert_id);
    EXCEPTION WHEN duplicate_object THEN
      NULL;
    END;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS call_logs_mgmt_last_ts_idx
  ON call_logs_mgmt (last_call_created_at);
"""

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_ENSURE)

# ───────── Payload builder ─────────
SQL_ALERT_ROW = """
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
  c.plate_number AS container_plate,

  m.status,
  m.alert_created_at,
  m.last_call_log_id,
  m.last_call_log_created_at,

  cl.outcome AS clog_outcome,
  cl.who     AS clog_who,
  cl.step    AS clog_step

FROM alerts a
LEFT JOIN alerts_mgmt m     ON m.alert_id = a.id
LEFT JOIN trip_logs    tl   ON tl.id = a.trip_log_id
LEFT JOIN vehicles     v    ON v.id = tl.vehicle_id
LEFT JOIN drivers      d    ON d.id = tl.driver_id
LEFT JOIN pmd_devices  pmd  ON pmd.id = tl.pmd_id
LEFT JOIN csd_devices  csd  ON csd.id = tl.csd_id
LEFT JOIN containers   c    ON c.id = csd.container_id
LEFT JOIN call_logs    cl   ON cl.id = m.last_call_log_id
WHERE a.id = %s
LIMIT 1;
"""

# def _action_detail(status: str | None, has_call: bool) -> str:
# it will run even if python < 3.10
def _action_detail(status: Optional[str], has_call: bool) -> str:
    s = (status or "").lower()
    if has_call:        return "Call log recorded"
    if s == "expired":  return "No call log within window"
    if s == "closed":   return "Closed by ops"
    return "Awaiting call log"

# def build_payload_for_alert_id(conn, alert_id: str) -> dict | None:
# it will run on even if python < 3.10
def build_payload_for_alert_id(conn, alert_id: str) -> Optional[dict]:
    with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
        cur.execute(SQL_ALERT_ROW, (alert_id,))
        r = cur.fetchone()
        if not r:
            return None

        has_call     = r.get("last_call_log_id") is not None
        status       = r.get("status") or "pending"
        last_clog_ts = r.get("last_call_log_created_at")
        action_time  = last_clog_ts or r.get("alert_created_at") or r.get("created_at")

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

            "action_detail":         _action_detail(status, has_call),
            "action_time":           iso8601_utc(action_time) if action_time else None,
            "action_by":             "system",
            "is_combination_alert":  "NO",
            "combination_parent_id": "",
            "is_closed":             "YES" if (status or "").lower() == "closed" else "NO",
            "source":                "source-1",
            "data_sent_timestamp":   iso8601_utc(datetime.now(timezone.utc)),
            "reason_by_driver":      (r.get("clog_outcome") or ""),
            "last_contacted_name":   r.get("driver_name"),
            "last_contacted_number": r.get("driver_number"),
        }

# ───────── Part 3: Sender ─────────
SQL_CLAIM_SEND = """
SELECT alert_id, payload_hash
FROM alerts_mgmt
WHERE needs_send = true
ORDER BY alert_created_at ASC
FOR UPDATE SKIP LOCKED
LIMIT %s;
"""

def send_to_kafka(key: str, payload: dict) -> None:
    p = _get_producer()
    b = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    print(f"[KAFKA-QUEUED] topic={ALERTS_TOPIC} key={key} bytes={len(b)}")
    # attach delivery callback so you'll see SENT/ERROR lines
    p.produce(ALERTS_TOPIC, key=str(key), value=b, on_delivery=_delivery_cb)
    p.poll(0)

# def process_send_batch(batch_size: int = 200, time_budget_sec: float | None = None) -> int:
# if less then 3.10, then still work
def process_send_batch(batch_size: int = 200, time_budget_sec: Optional[float] = None) -> int:    
    start = time.monotonic()
    processed = 0
    conn = get_conn()
    try:
        with conn:
            ensure_schema(conn)
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute(SQL_CLAIM_SEND, (batch_size,))
                rows = cur.fetchall()

                for row in rows:
                    aid = str(row["alert_id"])
                    prev_hash = row["payload_hash"]

                    payload = build_payload_for_alert_id(conn, aid)
                    if not payload:
                        cur.execute(
                            "UPDATE alerts_mgmt SET needs_send=false, last_error=%s WHERE alert_id=%s",
                            ("missing alert row", aid),
                        )
                        processed += 1
                        if time_budget_sec and (time.monotonic() - start) >= time_budget_sec:
                            break
                        continue

                    phash = payload_hash(payload)
                    if prev_hash == phash:
                        cur.execute(
                            "UPDATE alerts_mgmt SET needs_send=false, last_sent_at=now() WHERE alert_id=%s",
                            (aid,),
                        )
                        append_kafka_log(aid, payload, phash, sent_ok=False, error="duplicate-payload")
                        processed += 1
                        if time_budget_sec and (time.monotonic() - start) >= time_budget_sec:
                            break
                        continue

                    try:
                        send_to_kafka(key=aid, payload=payload)
                        cur.execute(
                            """
                            UPDATE alerts_mgmt
                               SET needs_send=false,
                                   payload_hash=%s,
                                   last_sent_at=now(),
                                   attempts=0,
                                   last_error=NULL
                             WHERE alert_id=%s
                            """,
                            (phash, aid),
                        )
                        append_kafka_log(aid, payload, phash, sent_ok=True, error=None)
                    except (KafkaException, BufferError, Exception) as e:
                        cur.execute(
                            """
                            UPDATE alerts_mgmt
                               SET attempts=attempts+1,
                                   last_error=%s
                             WHERE alert_id=%s
                            """,
                            (str(e), aid),
                        )
                        append_kafka_log(aid, payload, phash, sent_ok=False, error=str(e))

                    processed += 1
                    if time_budget_sec and (time.monotonic() - start) >= time_budget_sec:
                        break
    finally:
        conn.close()
    return processed

# ───────── Part 4: Poller (Option B pointer table) ─────────
SQL_CLAIM_PENDING = """
SELECT alert_id,
       assignment_group_id,
       check_attempts,
       wait_window_until,
       severity,
       alert_created_at,
       last_call_log_id,
       last_call_log_created_at
FROM alerts_mgmt
WHERE status='pending'
  AND (last_checked_at IS NULL OR now() >= next_check_at)
ORDER BY COALESCE(next_check_at, now()) ASC
FOR UPDATE SKIP LOCKED
LIMIT %s;
"""

SQL_GET_POINTER = """
SELECT last_call_log_id, last_call_created_at
FROM call_logs_mgmt
WHERE alert_id = %s;
"""

SQL_UPSERT_POINTER = """
INSERT INTO call_logs_mgmt (alert_id, last_call_log_id, last_call_created_at, updated_at)
VALUES (%s, %s, %s, now())
ON CONFLICT (alert_id)
DO UPDATE SET last_call_log_id      = EXCLUDED.last_call_log_id,
              last_call_created_at  = EXCLUDED.last_call_created_at,
              updated_at            = now();
"""

SQL_FIND_NEXT_CALL_LOG_TUPLE = """
SELECT id, created_at, outcome, who, step
FROM call_logs
WHERE group_id = %s
  AND (created_at, id) >
      (COALESCE(%s, 'epoch'::timestamptz),
       COALESCE(%s::uuid, '00000000-0000-0000-0000-000000000000'::uuid))
ORDER BY created_at ASC, id ASC
LIMIT 1;
"""

def next_check_after(attempts: int) -> timedelta:
    if attempts <= 0: return timedelta(minutes=1)
    if attempts == 1: return timedelta(minutes=2)
    if attempts <= 3: return timedelta(minutes=5)
    if attempts <= 5: return timedelta(minutes=10)
    return timedelta(minutes=15)

# def process_enrichment_batch(batch_size: int = 200, time_budget_sec: float | None = None) -> int:
# if less than 3.10, then still work
def process_enrichment_batch(batch_size: int = 200, time_budget_sec: Optional[float] = None) -> int:
    start = time.monotonic()
    updated = 0
    conn = get_conn()
    try:
        with conn:
            ensure_schema(conn)
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute(SQL_CLAIM_PENDING, (batch_size,))
                pendings = cur.fetchall()

                for p in pendings:
                    aid   = str(p["alert_id"])
                    gid   = str(p["assignment_group_id"])
                    atts  = int(p["check_attempts"])
                    until = p["wait_window_until"]

                    if until is None and p["alert_created_at"] is not None:
                        until = compute_wait_window_until(p["severity"], p["alert_created_at"])
                        cur.execute(
                            "UPDATE alerts_mgmt SET wait_window_until=%s, next_check_at=now() WHERE alert_id=%s",
                            (until, aid)
                        )

                    cur.execute(SQL_GET_POINTER, (aid,))
                    ptr = cur.fetchone()
                    last_id_ptr = ptr["last_call_log_id"] if (ptr and ptr["last_call_log_id"]) else p["last_call_log_id"]
                    last_ts_ptr = ptr["last_call_created_at"] if (ptr and ptr["last_call_created_at"]) else p["last_call_log_created_at"]

                    cur.execute(SQL_FIND_NEXT_CALL_LOG_TUPLE, (gid, last_ts_ptr, last_id_ptr))
                    clog = cur.fetchone()

                    if clog:
                        cur.execute(SQL_UPSERT_POINTER, (aid, clog["id"], clog["created_at"]))
                        cur.execute(
                            """
                            UPDATE alerts_mgmt
                               SET last_call_log_id=%s,
                                   last_call_log_created_at=%s,
                                   needs_send=true,
                                   last_checked_at=now(),
                                   next_check_at = now()
                             WHERE alert_id=%s
                            """,
                            (clog["id"], clog["created_at"], aid),
                        )
                        updated += 1
                        if time_budget_sec and (time.monotonic() - start) >= time_budget_sec:
                            break
                        continue

                    if until is not None:
                        cur.execute("SELECT (now() >= %s) AS over;", (until,))
                        if bool(cur.fetchone()["over"]):
                            cur.execute(
                                """
                                UPDATE alerts_mgmt
                                   SET status='expired',
                                       needs_send=true,
                                       last_checked_at=now(),
                                       next_check_at=NULL
                                 WHERE alert_id=%s
                                """,
                                (aid,),
                            )
                            updated += 1
                            if time_budget_sec and (time.monotonic() - start) >= time_budget_sec:
                                break
                            continue

                    wait = next_check_after(attempts=atts)
                    cur.execute(
                        """
                        UPDATE alerts_mgmt
                           SET check_attempts = check_attempts + 1,
                               last_checked_at = now(),
                               next_check_at   = now() + %s::interval
                         WHERE alert_id=%s
                        """,
                        (f"{int(wait.total_seconds())} seconds", aid),
                    )

                    if time_budget_sec and (time.monotonic() - start) >= time_budget_sec:
                        break
    finally:
        conn.close()
    return updated

# ───────── Runner ─────────
def run_pipeline_until_drained(max_cycles: int = 10):
    sent0 = process_send_batch(batch_size=500, time_budget_sec=None)
    if sent0:
        print(f"[KAFKA] initial sends queued: {sent0}")

    for _ in range(max_cycles):
        updates = process_enrichment_batch(batch_size=500, time_budget_sec=2.0)
        if updates <= 0:
            break
        sent = process_send_batch(batch_size=500, time_budget_sec=2.0)
        print(f"[KAFKA] poll updates={updates}, sends queued={sent}")
        if sent <= 0:
            break

    _flush_producer(3.0)
    print("[KAFKA] flush complete")

if __name__ == "__main__":
    run_pipeline_until_drained()
