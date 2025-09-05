#!/usr/bin/env python3
"""
Extended shipper to send both A3 device-location packets and trip data
  * A3 → DEVICE_LOCATION_TOPIC  (exactly once via kafka_logs.id offset)
  * trip_logs → TRIP_DATA_TOPIC (exactly once via trip_kafka_logs.log_time offset)
  * Rolling logs of last 1 000 payloads each
  * Resilient auto-reconnect for Postgres & Kafka
"""

import json
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from confluent_kafka import Producer, KafkaException


from dotenv import load_dotenv
from pathlib import Path
import os

# Load .env.trip file
load_dotenv(dotenv_path=Path(".env.trip"))


# ── Kafka connection ─────────────────────────────────────────────────────
KAFKA_CONF = {
    "bootstrap.servers":       os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol":       os.environ.get("KAFKA_SECURITY_PROTOCOL"),
    "sasl.mechanisms":         os.environ.get("KAFKA_SASL_MECHANISMS"),
    "sasl.username":           os.environ.get("KAFKA_SASL_USERNAME"),
    "sasl.password":           os.environ.get("KAFKA_SASL_PASSWORD"),
    "enable.idempotence":      True,
    "retries":                 5,
    "retry.backoff.ms":        2000,
    "socket.keepalive.enable": True,
    "acks":                    "all",
}


DEVICE_LOCATION_TOPIC = os.getenv("DEVICE_LOCATION_TOPIC", "nl_device_location")
TRIP_DATA_TOPIC       = os.getenv("TRIP_DATA_TOPIC", "nl_trip")

# ── PostgreSQL connection params ──────────────────────────────────────────
DB_PARAMS = {
    "host":     os.environ.get("DB_HOST"),
    "port":     int(os.environ.get("DB_PORT")),
    "dbname":   os.environ.get("DB_NAME"),
    "user":     os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
}

# ── Rolling payload logs (last 1k lines each) ────────────────────────────
LOG_PATH_DEVICE = Path(os.environ.get("LOG_PATH_DEVICE", "payload_log.jsonl"))
LOG_PATH_TRIP   = Path(os.environ.get("LOG_PATH_TRIP", "trip_payload_log.jsonl"))
MAX_LOG_LINES   = int(os.environ.get("MAX_LOG_LINES", 1000))

def append_to_log(path: Path, line: str):
    path.parent.mkdir(exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
    # trim to last MAX_LOG_LINES
    with path.open("r+", encoding="utf-8") as f:
        lines = deque(f, MAX_LOG_LINES)
        f.seek(0); f.truncate()
        f.writelines(lines)

def iso_z(dt: datetime) -> str:
    """Convert aware UTC datetime to ISO-8601 with Z suffix."""
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


# ── [UNCHANGED] A3-packet offset table & shipper ──────────────────────────

def ensure_offset_row(conn):
    with conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS kafka_logs (
              packet_type text PRIMARY KEY,
              last_row_id bigint NOT NULL
            )
        """)
        cur.execute("""
            INSERT INTO kafka_logs(packet_type, last_row_id)
            VALUES ('a3', 0)
            ON CONFLICT DO NOTHING
        """)

# [STATIC_META, DEVICE_META, normalise_door, build_device_location,
#  ship_once] are copied verbatim from your original A3 shipper—

STATIC_META = {
    "device_number":         "PMD-25-0044",
    "tracking_company_name": "NLCSS",
    "driver_name":           "",
    "driver_contact_number":"",
    "trip_id":               "SS-t01",
    "bonded_carrier":        "NLC",
    "attached":              "YES",
    "sync":                  "YES",
    "is_event":              "NO",
    "source":                "source_1",
    "altitude":              0,
    "satelite_count":        0,
    "battery_level":         90,
    "battery_status":        "Unknown",
    "device_voltage":        0,
    "battery_voltage":       0,
}
DEVICE_META = {
    "448fa433": {"vehicle_registration_number":"TLA-900","container_id":"YMMU6165052"},
    "448fa444": {"vehicle_registration_number":"E-2435","container_id":"CAAU8382526"},
}

def normalise_door(raw: str) -> str:
    return "OPENED" if (raw or "").lower() in {"unlock","open","opened"} else "CLOSED"

def build_device_location(pkt: dict, created_at: datetime) -> dict:
    doc = {
        "timestamp":           iso_z(datetime.fromtimestamp(pkt["date_time_unix"], tz=timezone.utc)),
        "device_id":           pkt["device_id"],
        "latitude":            str(pkt["latitude_dd"]),
        "longitude":           str(pkt["longitude_dd"]),
        "speed":               pkt["speed"],
        "angle":               pkt["direction"],
        "ignition_status":     "ON" if pkt["status"]["St1"]["ACC"]=="on" else "OFF",
        "door_status":         normalise_door(pkt["custom_special"].get("door")),
        "data_sent_timestamp": iso_z(created_at),
        "altitude":            pkt.get("altitude",0),
        "satelite_count":      pkt.get("satelite_count",0),
        "battery_level":       pkt.get("battery_level",0),
        "battery_status":      pkt.get("battery_status",""),
        "device_voltage":      pkt.get("device_voltage",0),
        "battery_voltage":     pkt.get("battery_voltage",0),
        # placeholders
        "vehicle_registration_number":"",
        "device_number":"",
        "container_id":"",
        "tracking_company_name":"",
        "driver_name":"",
        "driver_cnic":"",
        "driver_contact_number":"",
        "trip_id":"",
        "bonded_carrier":"",
        "attached":"",
        "sync":"",
        "is_event":"",
        "source":"",
    }
    for k,v in STATIC_META.items():
        if not doc[k]:
            doc[k] = v
    doc.update(DEVICE_META.get(pkt["device_id"], {}))
    return doc

def ship_once(conn, producer):
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT last_row_id FROM kafka_logs WHERE packet_type='a3' FOR UPDATE")
            last_id = cur.fetchone()["last_row_id"]

            cur.execute("""
                SELECT id, raw_json, created_at
                FROM   csd_packets
                WHERE  packet_type='a3' AND id > %s
                ORDER BY id
                LIMIT 200
            """, (last_id,))
            rows = cur.fetchall()
            if not rows:
                return 0

            for r in rows:
                pkt     = json.loads(r["raw_json"])
                payload = json.dumps(build_device_location(pkt, r["created_at"]))
                # print("DEVICE PAYLOAD:", payload)  # ← Add this line
                producer.produce(DEVICE_LOCATION_TOPIC,
                                 key=str(r["id"]),
                                 value=payload.encode("utf-8"))
                append_to_log(LOG_PATH_DEVICE, payload)

            producer.flush()
            new_last = rows[-1]["id"]
            cur.execute("UPDATE kafka_logs SET last_row_id=%s WHERE packet_type='a3'",
                        (new_last,))
            print(f"Batch sent: {len(rows)} rows (ids {rows[0]['id']}-{new_last})")
            return len(rows)


# ── [NEW] Trip-data offset table & shipper by timestamp ───────────────────

def ensure_trip_offset_table(conn):
    with conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trip_kafka_logs (
              last_log_time timestamptz NOT NULL
            )
        """)
        # insert initial sentinel: long ago
        cur.execute("""
            INSERT INTO trip_kafka_logs(last_log_time)
            VALUES ('1970-01-01T00:00:00Z')
            ON CONFLICT DO NOTHING
        """)

def get_last_trip_time(conn) -> datetime:
    with conn.cursor() as cur:
        cur.execute("SELECT last_log_time FROM trip_kafka_logs")
        return cur.fetchone()[0]

def update_last_trip_time(conn, new_time: datetime):
    # inside the existing "with conn:" block, so just open a cursor
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE trip_kafka_logs SET last_log_time = %s",
            (new_time,)
        )

def build_trip_payload(row: dict) -> dict:
    evt = row["log_time"].astimezone(timezone.utc)
    now = datetime.now(timezone.utc)

    dep = row["departure_time"].astimezone(timezone.utc)
    arr = row["expected_arrival"].astimezone(timezone.utc)
    if     now < dep: status = "IN_PROGRESS"
    elif dep <= now <= arr: status = "IN_PROGRESS"
    else:                 status = "IN_PROGRESS"

    # Use the business trip_id directly
    trip_id = str(row['trip_id'])

    return {
        "timestamp":              iso_z(evt),
        "trip_id":                trip_id,
        "device_id":              row["pmd_device_id"],
        "start_datetime":         iso_z(dep),
        "end_datetime":           iso_z(arr),
        "departure":              row["origin"],
        "destination":            row["destination"],
        "vehicle_id":             row["vehicle_registration_number"],
        "container_1_id":         row["container_1_id"],
        "container_2_id":         None,
        "seal_1_id":              row["seal_1_id"],
        "seal_2_id":              None,
        "seal_1_status":          row["seal_1_status"],
        "seal_2_status":          None,
        "gd_number_1":            None,
        "gd_number_2":            None,
        "status":                 status,
        "bonded_carrier":         None,
        "consignee":              None,
        "prime_over_device_field":None,
        "device_type":            None,
        "drivers_info":           row["drivers_info"],
        "engine_no":              row["engine_number"],
        "chassis_no":             row["chassis_number"],
        "phone_number":           row["phone_number"],
        "clearing_agent":         None,
        "importer_name":          None,
        "source":                 "kafka",
        "data_sent_timestamp":    iso_z(now),
        "trip_type":              None,
        "vir_number":             None,
        "trader_ntn":             None,
        "trader_name":            None,
        "check_post":             None,
        "cargo_type":             None,
        "csd_number":             row["csd_number"]
    }

def ship_trip_once(conn, producer):
    with conn:
        last_time = get_last_trip_time(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                  tl.id,
                  tl.trip_id,
                  tl.log_time,
                  tl.departure_time,
                  tl.expected_arrival,
                  pd.device_id               AS pmd_device_id,
                  r.origin,
                  r.destination,
                  v.registration_number      AS vehicle_registration_number,
                  c.plate_number             AS container_1_id,
                  cd.device_id               AS csd_number,
                  ed.imei                    AS seal_1_id,
                  tl.status                  AS seal_1_status,
                  CONCAT(d.name, ', ', d.cnic_number) AS drivers_info,
                  v.engine_number,
                  v.chassis_number,
                  d.phone                    AS phone_number
                FROM trip_logs tl
                LEFT JOIN pmd_devices pd   ON tl.pmd_id   = pd.id
                LEFT JOIN routes r         ON tl.route_id = r.id
                LEFT JOIN vehicles v       ON tl.vehicle_id = v.id
                LEFT JOIN csd_devices cd   ON tl.csd_id   = cd.id
                LEFT JOIN containers c     ON cd.container_id = c.id
                LEFT JOIN eseal_devices ed ON tl.eseal_id = ed.id
                LEFT JOIN drivers d        ON tl.driver_id = d.id
                WHERE tl.log_time > %s
                AND tl.status = 'started'
                ORDER BY tl.log_time
                LIMIT 200
            """, (last_time,))
            rows = cur.fetchall()
            if not rows:
                return 0

            for r in rows:
                payload = json.dumps(build_trip_payload(r))
                producer.produce(TRIP_DATA_TOPIC,
                                 key=str(r["trip_id"]),
                                 value=payload.encode("utf-8"))
                append_to_log(LOG_PATH_TRIP, payload)

            producer.flush()
            new_time = rows[-1]["log_time"]
            update_last_trip_time(conn, new_time)
            print(f"Trip batch sent: {len(rows)} rows (times {rows[0]['log_time']}→{new_time}) [status=started only]")
            return len(rows)


# ── Main loop ─────────────────────────────────────────────────────────────

def get_db_conn():
    return psycopg2.connect(**DB_PARAMS)

def main():
    producer = Producer(KAFKA_CONF)
    conn     = get_db_conn()

    # ensure both offset records exist
    ensure_offset_row(conn)
    ensure_trip_offset_table(conn)

    print("Shipper started → topics", TRIP_DATA_TOPIC, " Ctrl-C to stop")

    try:
        while True:
            try:
                # sent_dev  = ship_once(conn, producer)
                sent_dev=0
                sent_trip = ship_trip_once(conn, producer)
                if sent_dev + sent_trip == 0:
                    time.sleep(2)
            except (psycopg2.InterfaceError, psycopg2.OperationalError):
                print("DB connection lost – reconnecting…")
                conn.close()
                time.sleep(2)
                conn = get_db_conn()
            except KafkaException as ke:
                print(f"Kafka error {ke} – retrying in 5 s")
                time.sleep(5)
    except KeyboardInterrupt:
        print("Stopping…")
    finally:
        producer.flush(5)
        conn.close()
        print("Goodbye!")

if __name__ == "__main__":
    main()