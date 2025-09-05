import os
import sys
import json
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Tuple, Dict, List

import psycopg2
from psycopg2.extras import RealDictCursor
from confluent_kafka import Producer, KafkaException

# ──────────────────────────────────────────────────────────────────────────────
# Kafka
KAFKA_CONF = {
    "bootstrap.servers":       os.getenv("KAFKA_BOOTSTRAP", "cmcr-q.deliverydevs.com:9091"),
    "security.protocol":       os.getenv("KAFKA_SEC_PROTO", "SASL_PLAINTEXT"),
    "sasl.mechanisms":         os.getenv("KAFKA_SASL_MECH", "PLAIN"),
    "sasl.username":           os.getenv("KAFKA_USERNAME", "nlc"),
    "sasl.password":           os.getenv("KAFKA_PASSWORD", "KDuw41kSeO8INXbT20"),
    "enable.idempotence":      True,
    "retries":                 5,
    "retry.backoff.ms":        1000,  # Adjusted to avoid warning with retry.backoff.max.ms
    "socket.keepalive.enable": True,
    "acks":                    "all",
}
DEVICE_LOCATION_TOPIC = os.getenv("DEVICE_LOCATION_TOPIC", "nl_device_location")

# PostgreSQL
DB_PARAMS = {
    "host":     os.getenv("PGHOST",     "nlc-db.c1yckugsel2y.ap-south-1.rds.amazonaws.com"),
    "port":     int(os.getenv("PGPORT", "5432")),
    "dbname":   os.getenv("PGDATABASE", "VTS"),
    "user":     os.getenv("PGUSER",     "postgres"),
    "password": os.getenv("PGPASSWORD", "admin123"),
}

# Files & misc
LOG_PATH_DEVICE = Path(os.getenv("DEVICE_LOG_FILE", "payload_log.jsonl"))
MAX_LOG_LINES   = int(os.getenv("MAX_LOG_LINES", "1000"))
TRIP_LIMIT      = int(os.getenv("TRIP_LIMIT", "200"))
SLEEP_SECS_IDLE = float(os.getenv("SLEEP_SECS_IDLE", "2"))

# Timezone helper
KHI_TZ = timezone(timedelta(hours=5))  # Asia/Karachi, simple tz w/out pytz

# Packet table autodetect
CSD_PACKET_SCHEMA = os.getenv("CSD_PACKET_SCHEMA", "public")
CSD_PACKET_TABLE_CANDIDATES = [s.strip() for s in os.getenv(
    "CSD_PACKET_TABLE_CANDIDATES",
    "csd_packet,csd_packets,csd_data,csd_raw,csd_device_packet,device_packets,device_packet,packets"
).split(",")]
CSD_PACKET_TIME_CANDIDATES = [s.strip() for s in os.getenv(
    "CSD_PACKET_TIME_CANDIDATES",
    "packet_time,created_at,server_time,recv_time,ts,inserted_at,id"
).split(",")]
CSD_PACKET_TYPE_COL_CANDIDATES = [s.strip() for s in os.getenv(
    "CSD_PACKET_TYPE_COL_CANDIDATES",
    "packet_type,pkt_type,msg_type,type,ptype,codec"
).split(",")]
CSD_PACKET_TYPES = [s.strip().upper() for s in os.getenv("CSD_PACKET_TYPES", "A3,82,83").split(",")]

# ──────────────────────────────────────────────────────────────────────────────
# Trip SELECT (to get device context for location data)
TRIPS_SQL = f"""
SELECT
  tl.id                           AS trip_log_id,
  tl.log_time,
  tl.departure_time,
  tl.expected_arrival,
  tl.trip_id,
  tl.status                       AS trip_status,

  v.registration_number           AS vehicle_reg_no,
  r.name                          AS route_name,

  d.name                          AS driver_name,
  d.cnic_number                   AS driver_cnic,
  d.phone                         AS driver_contact_number,

  cd.id                           AS csd_id,
  cd.imei                         AS csd_imei,
  cd.vts_id                       AS csd_vts_id_hex,   -- HEX in csd_devices

  di_csd.device_id                AS csd_device_id,    -- decimal fallback from inventory
  c.plate_number                  AS container_id

FROM public.trip_logs AS tl
LEFT JOIN public.vehicles         AS v       ON v.id = tl.vehicle_id
LEFT JOIN public.routes           AS r       ON r.id = tl.route_id
LEFT JOIN public.drivers          AS d       ON d.id = tl.driver_id
LEFT JOIN public.csd_devices      AS cd      ON cd.id = tl.csd_id
LEFT JOIN public.device_inventory AS di_csd  ON di_csd.imei = cd.imei AND di_csd.device_type = 'CSD'
LEFT JOIN public.containers       AS c       ON cd.container_id = c.id
ORDER BY tl.log_time DESC
LIMIT {TRIP_LIMIT};
"""

# ──────────────────────────────────────────────────────────────────────────────
# Rolling JSONL logger
def append_to_log(path: Path, line: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
    with path.open("r+", encoding="utf-8") as f:
        lines = deque(f, MAX_LOG_LINES)
        f.seek(0); f.truncate()
        f.writelines(lines)

def dict_json_safe(d: dict) -> str:
    return json.dumps(d, default=str, ensure_ascii=False)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def normalize_hex(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = str(s).strip()
    if s.lower().startswith("0x"):
        s = s[2:]
    return s.upper()

def decimal_to_hex(value, width: int = 0) -> Optional[str]:
    if value is None:
        return None
    try:
        n = int(value, 10) if not isinstance(value, int) else value
    except (TypeError, ValueError):
        return None
    h = format(n, "x").upper()
    return h.zfill(width) if width else h

# ──────────────────────────────────────────────────────────────────────────────
# InfoSchema helpers to auto-detect packets table
def _list_columns(conn, schema: str, table: str) -> set[str]:
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return {r[0] for r in cur.fetchall()}

def _table_exists(conn, schema: str, table: str) -> bool:
    sql = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return cur.fetchone() is not None

def _find_any_table_with_device_id(conn, schema: str) -> List[str]:
    sql = """
        SELECT DISTINCT table_name
        FROM information_schema.columns
        WHERE table_schema = %s AND column_name = 'device_id'
        ORDER BY table_name
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema,))
        return [r[0] for r in cur.fetchall()]

def _get_col_data_type(conn, schema: str, table: str, column: str) -> Optional[str]:
    sql = """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table, column))
        row = cur.fetchone()
    return row[0] if row else None

def resolve_packet_table(conn) -> Optional[Tuple[str, str, str]]:
    # 1) Try candidates
    for t in CSD_PACKET_TABLE_CANDIDATES:
        if t and _table_exists(conn, CSD_PACKET_SCHEMA, t):
            cols = _list_columns(conn, CSD_PACKET_SCHEMA, t)
            if "device_id" in cols:
                for c in CSD_PACKET_TIME_CANDIDATES:
                    if c in cols:
                        return (CSD_PACKET_SCHEMA, t, c)
                if "id" in cols:
                    return (CSD_PACKET_SCHEMA, t, "id")
                return (CSD_PACKET_SCHEMA, t, "device_id")
    # 2) Any table with device_id
    for t in _find_any_table_with_device_id(conn, CSD_PACKET_SCHEMA):
        cols = _list_columns(conn, CSD_PACKET_SCHEMA, t)
        for c in CSD_PACKET_TIME_CANDIDATES:
            if c in cols:
                return (CSD_PACKET_SCHEMA, t, c)
        if "id" in cols:
            return (CSD_PACKET_SCHEMA, t, "id")
        return (CSD_PACKET_SCHEMA, t, "device_id")
    return None

def _resolve_type_col(conn, schema: str, table: str) -> Optional[str]:
    cols = _list_columns(conn, schema, table)
    for c in CSD_PACKET_TYPE_COL_CANDIDATES:
        if c in cols:
            return c
    return None

def _build_type_filter_expr(conn, schema: str, table: str, type_col: str) -> Tuple[str, tuple]:
    data_type = (_get_col_data_type(conn, schema, table, type_col) or "").lower()
    numeric_kinds = {"integer", "bigint", "smallint", "numeric", "double precision", "real", "decimal"}
    if data_type in numeric_kinds:
        values = []
        for t in CSD_PACKET_TYPES:
            try: values.append(int(t, 16))
            except ValueError: values.append(int(t))
        return (f"{type_col} = ANY(%s)", (values,))
    else:
        norm_values = [t.upper().removeprefix("0X") for t in CSD_PACKET_TYPES]
        return (f"UPPER(regexp_replace({type_col}::text, '^0x', '', 'i')) = ANY(%s)", (norm_values,))

def fetch_latest_packets_any_and_by_type(conn, device_ids_hex: List[str]) -> Tuple[Dict, Dict]:
    want_ids = [s.upper() for s in device_ids_hex if s]
    if not want_ids:
        return {}, {}
    resolved = resolve_packet_table(conn)
    if not resolved:
        return {}, {}
    schema, table, order_col = resolved
    type_col = _resolve_type_col(conn, schema, table)

    # No type column: latest per device only
    if not type_col:
        q = f"""
            SELECT DISTINCT ON (UPPER(device_id)) *
            FROM {schema}.{table}
            WHERE UPPER(device_id) = ANY(%s)
            ORDER BY UPPER(device_id), {order_col} DESC
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(q, (want_ids,))
            rows = cur.fetchall()
        latest_any = {(r.get("device_id") or "").upper(): r for r in rows if r.get("device_id")}
        return latest_any, {}

    # Latest among allowed types
    latest_any = {}
    type_expr, type_params = _build_type_filter_expr(conn, schema, table, type_col)
    q_any = f"""
        SELECT DISTINCT ON (UPPER(device_id)) *
        FROM {schema}.{table}
        WHERE UPPER(device_id) = ANY(%s) AND {type_expr}
        ORDER BY UPPER(device_id), {order_col} DESC
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q_any, (want_ids, *type_params))
        rows = cur.fetchall()
        latest_any = {(r.get("device_id") or "").upper(): r for r in rows if r.get("device_id")}

    # Latest per specific type
    latest_by_type: Dict[str, Dict] = {}
    for t in CSD_PACKET_TYPES:
        data_type = (_get_col_data_type(conn, schema, table, type_col) or "").lower()
        if data_type in {"integer","bigint","smallint","numeric","double precision","real","decimal"}:
            try: val = int(t, 16)
            except ValueError: val = int(t)
            expr, params = f"{type_col} = %s", (val,)
        else:
            val = t.upper().removeprefix("0X")
            expr, params = f"UPPER(regexp_replace({type_col}::text, '^0x', '', 'i')) = %s", (val,)
        q_one = f"""
            SELECT DISTINCT ON (UPPER(device_id)) *
            FROM {schema}.{table}
            WHERE UPPER(device_id) = ANY(%s) AND {expr}
            ORDER BY UPPER(device_id), {order_col} DESC
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(q_one, (want_ids, *params))
            rows = cur.fetchall()
        for r in rows:
            dev = (r.get("device_id") or "").upper()
            if not dev: continue
            latest_by_type.setdefault(dev, {})
            latest_by_type[dev][t.upper()] = r

    return latest_any, latest_by_type

# ──────────────────────────────────────────────────────────────────────────────
# Helpers for building the device-location payload
def _parse_raw_json(pkt: Optional[dict]) -> dict:
    if not pkt:
        return {}
    raw = pkt.get("raw_json")
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw or "{}")
    except Exception:
        return {}

def _choose_base_pkt(row: dict) -> Optional[dict]:
    # For coordinates/time, prefer A3, else 82, else 83, else latest_any
    return row.get("csd_packet_A3") or row.get("csd_packet_82") or row.get("csd_packet_83") or row.get("csd_packet_latest")

def _event_epoch_from_pkt(pkt: dict, raw: dict) -> int:
    # Prefer numeric unix
    if isinstance(raw.get("date_time_unix"), (int, float, str)):
        try:
            return int(raw["date_time_unix"])
        except Exception:
            pass
    # Else Pakistan local → epoch
    dtp = raw.get("date_time_pakistan") or pkt.get("date_time_pakistan")
    if dtp:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                dt = datetime.strptime(dtp, fmt).replace(tzinfo=KHI_TZ)
                return int(dt.timestamp())
            except Exception:
                continue
    # Else created_at → epoch
    ca = pkt.get("created_at")
    if ca:
        try:
            return int(datetime.fromisoformat(str(ca)).timestamp())
        except Exception:
            pass
    # Now
    return int(datetime.now(timezone.utc).timestamp())

def _ts_from_pkt(pkt: dict, raw: dict) -> str:
    # Emits ISO Z string using same precedence
    if "date_time_unix" in raw:
        try:
            return iso_z(datetime.fromtimestamp(int(raw["date_time_unix"]), tz=timezone.utc))
        except Exception:
            pass
    dtp = raw.get("date_time_pakistan") or pkt.get("date_time_pakistan")
    if dtp:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return iso_z(datetime.strptime(dtp, fmt).replace(tzinfo=KHI_TZ))
            except Exception:
                continue
    ca = pkt.get("created_at")
    if ca:
        try:
            return iso_z(datetime.fromisoformat(str(ca)))
        except Exception:
            pass
    return iso_z(datetime.now(timezone.utc))

def _ignition_from_raw(raw: dict) -> str:
    acc = (((raw.get("status") or {}).get("St1") or {}).get("ACC") or "").lower()
    if acc in {"on","off"}:
        return "ON" if acc == "on" else "OFF"
    acc2 = ((raw.get("alarm_params") or {}).get("acc_status") or "").upper()
    return "ON" if acc2 == "ON" else "OFF"

def _battery_level_from_row(row: dict) -> Optional[int]:
    for k in ("csd_packet_82", "csd_packet_83"):
        pkt = row.get(k)
        if not pkt: continue
        if "battery_level" in pkt and pkt["battery_level"] is not None:
            return pkt["battery_level"]
        raw = _parse_raw_json(pkt)
        if "battery_level" in raw:
            return raw["battery_level"]
    return None

def _battery_status_from_raw(raw: dict) -> str:
    st4 = ((raw.get("status") or {}).get("St4") or {})
    if isinstance(st4.get("On charging"), str) and st4.get("On charging").strip().lower() == "on charging":
        return "Charging"
    if (raw.get("alarm_params") or {}).get("charging_alarm"):
        return "Charging"
    return "Unknown"

def _door_status_from_82_83_only(row: dict) -> str:
    # Only 82/83; prefer 82
    for k in ("csd_packet_82", "csd_packet_83"):
        pkt = row.get(k)
        if not pkt: continue
        raw = _parse_raw_json(pkt)
        ds = (raw.get("alarm_params") or {}).get("door_status")
        if isinstance(ds, str) and ds.strip():
            v = ds.strip().lower()
            if v.startswith("open"):   return "OPENED"
            if v.startswith("close"):  return "CLOSED"
        cs = raw.get("custom_special") or {}
        if isinstance(cs, str):
            try: cs = json.loads(cs)
            except Exception: cs = {}
        d = (cs.get("door") or "").strip().lower()
        if d in {"unlock","open","opened"}: return "OPENED"
        if d in {"lock","locked","close","closed"}: return "CLOSED"
    return "UNKNOWN"

def build_location_payload(row: dict) -> Tuple[dict, int]:
    """
    Returns (payload, event_epoch).
    """
    pkt = _choose_base_pkt(row)
    if not pkt:
        return {}, 0
    raw = _parse_raw_json(pkt)

    dev_hex = normalize_hex(row.get("csd_vts_id_hex")) \
              or normalize_hex(decimal_to_hex(row.get("csd_device_id"), width=8)) \
              or normalize_hex(pkt.get("device_id"))

    # Coords
    lat = pkt.get("latitude_dd"); lon = pkt.get("longitude_dd")
    try:
        lat = float(lat) if lat is not None else None
        lon = float(lon) if lon is not None else None
    except Exception:
        lat, lon = None, None

    event_epoch = _event_epoch_from_pkt(pkt, raw)

    payload = {
        "timestamp":                   _ts_from_pkt(pkt, raw),
        "device_id":                   dev_hex or "",
        "latitude":                    lat,
        "longitude":                   lon,
        "speed":                       int(raw.get("speed", 0)) if isinstance(raw.get("speed"), (int,float)) else (raw.get("speed") or 0),
        "angle":                       int(raw.get("direction", 0)) if isinstance(raw.get("direction"), (int,float)) else (raw.get("direction") or 0),
        "ignition_status":             _ignition_from_raw(raw),
        "door_status":                 _door_status_from_82_83_only(row),  # rule
        "data_sent_timestamp":         iso_z(datetime.fromisoformat(str(pkt["created_at"])) if pkt.get("created_at") else datetime.now(timezone.utc)),

        "altitude":                    None,
        "satelite_count":              None,
        "battery_level":               _battery_level_from_row(row),
        "battery_status":              _battery_status_from_raw(raw),
        "device_voltage":              None,
        "battery_voltage":             None,

        "vehicle_registration_number": row.get("vehicle_reg_no") or "",
        "device_number":               dev_hex or "",   # rule
        "container_id":                row.get("container_id") or "",
        "tracking_company_name":       "NLCSS",
        "driver_name":                 row.get("driver_name") or "",
        "driver_cnic":                 row.get("driver_cnic") or "",
        "driver_contact_number":       row.get("driver_contact_number") or "",
        "trip_id":                     row.get("trip_id") or "",
        "bonded_carrier":              "NLC",
        "attached":                    "YES" if (((raw.get("status") or {}).get("St1") or {}).get("Detach/light sensor","").lower() == "normal") else "NO",
        "sync":                        "YES",
        "is_event":                    "NO",
        "source":                      "source-1",
    }
    return payload, event_epoch

# ──────────────────────────────────────────────────────────────────────────────
# Location offsets
def ensure_location_offset_table(conn):
    with conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS device_location_offsets (
              device_hex       text PRIMARY KEY,
              last_event_unix  bigint NOT NULL DEFAULT 0
            )
        """)

def fetch_location_offsets(conn, device_hexes: List[str]) -> Dict[str, int]:
    offsets: Dict[str, int] = {}
    if not device_hexes:
        return offsets
    with conn.cursor() as cur:
        cur.execute("""
            SELECT device_hex, last_event_unix
            FROM device_location_offsets
            WHERE device_hex = ANY(%s)
        """, (device_hexes,))
        for dev, lastu in cur.fetchall():
            offsets[dev] = int(lastu)
    return offsets

def upsert_location_offsets(conn, updates: Dict[str, int]):
    if not updates:
        return
    rows = [(k, v) for k, v in updates.items()]
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO device_location_offsets(device_hex, last_event_unix)
            VALUES (%s, %s)
            ON CONFLICT (device_hex)
            DO UPDATE SET last_event_unix = GREATEST(device_location_offsets.last_event_unix, EXCLUDED.last_event_unix)
        """, rows)

# ──────────────────────────────────────────────────────────────────────────────
# Location sender using latest A3/82/83 + per-device offsets
def ship_location_once(conn, producer) -> int:
    sent = 0
    with conn:
        # 1) Fetch recent trips (device & context)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(TRIPS_SQL)
            trip_rows = cur.fetchall()
        if not trip_rows:
            return 0

        # 2) Normalize device hex & collect device list
        device_hexes: List[str] = []
        for row in trip_rows:
            norm_hex = normalize_hex(row.get("csd_vts_id_hex")) \
                       or normalize_hex(decimal_to_hex(row.get("csd_device_id"), width=8))
            row["csd_vts_id_hex"] = norm_hex
            if norm_hex:
                device_hexes.append(norm_hex)
        device_hexes = sorted(list({h for h in device_hexes if h}))

        if not device_hexes:
            return 0

        # 3) Enrich with latest packets
        latest_any, latest_by_type = fetch_latest_packets_any_and_by_type(conn, device_hexes)

        # 4) Load per-device offsets
        last_offsets = fetch_location_offsets(conn, device_hexes)

        # 5) Build & send if newer than last_event_unix
        updates: Dict[str, int] = {}

        for row in trip_rows:
            dev_hex = row.get("csd_vts_id_hex")
            if not dev_hex:
                continue

            # Attach latest packets for this device
            row["csd_packet_latest"] = latest_any.get(dev_hex)
            bytype = latest_by_type.get(dev_hex, {})
            row["csd_packet_A3"] = bytype.get("A3")
            row["csd_packet_82"] = bytype.get("82")
            row["csd_packet_83"] = bytype.get("83")

            payload, event_epoch = build_location_payload(row)
            if not payload or not event_epoch:
                continue

            last_epoch = int(last_offsets.get(dev_hex, 0))
            if event_epoch <= last_epoch:
                # No newer telemetry for this device
                continue

            # Produce. Use a stable key; include epoch to help partitioning/debug
            key = f"{dev_hex}:{event_epoch}"
            value = dict_json_safe(payload)
            producer.produce(DEVICE_LOCATION_TOPIC, key=key, value=value.encode("utf-8"))
            append_to_log(LOG_PATH_DEVICE, value)
            updates[dev_hex] = event_epoch
            sent += 1

        if sent:
            producer.flush()
            upsert_location_offsets(conn, updates)
            print(f"Location batch sent: {sent} device payload(s)")

    return sent

# ──────────────────────────────────────────────────────────────────────────────
def get_db_conn():
    return psycopg2.connect(**DB_PARAMS)

def main():
    print("Location Shipper starting… topic:", DEVICE_LOCATION_TOPIC)
    producer = Producer(KAFKA_CONF)
    conn     = get_db_conn()

    ensure_location_offset_table(conn)

    try:
        while True:
            try:
                sent_loc = ship_location_once(conn, producer)
                if sent_loc == 0:
                    time.sleep(SLEEP_SECS_IDLE)
            except (psycopg2.InterfaceError, psycopg2.OperationalError):
                print("DB connection lost – reconnecting…")
                try: conn.close()
                except Exception: pass
                time.sleep(2)
                conn = get_db_conn()
            except KafkaException as ke:
                print(f"Kafka error {ke} – retrying in 5 s")
                time.sleep(5)
    except KeyboardInterrupt:
        print("Stopping…")
    finally:
        try: producer.flush(5)
        except Exception: pass
        try: conn.close()
        except Exception: pass
        print("Goodbye!")

if __name__ == "__main__":
    sys.exit(main())