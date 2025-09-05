#!/usr/bin/env python3
import time
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import KafkaException

# --- Part 1–2: fetch/register new alerts (pick the module name you actually use)
# If your file is named send_data_to_KAFKA_3_part1_2.py, use this:
from send_data_to_KAFKA_3_part1_2 import fetch_all_new_alert_payloads
# If it's named fetch_new_alerts_psycopg2.py instead, comment the above and use:
# from fetch_new_alerts_psycopg2 import fetch_all_new_alert_payloads

# --- Part 3–4: sender + poller (already produces to Kafka + logs to JSONL)
from send_data_to_KAFKA_3_part3_4 import run_pipeline_until_drained

TICK_SECS = 5           # base loop sleep when there was work
IDLE_BACKOFF_SECS = 15  # sleep when there was no work
MAX_PIPELINE_CYCLES = 6 # how many poll->send rounds per tick

def nowz():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def main():
    idle_streak = 0
    print(f"[RUNNER] started at {nowz()} — Ctrl+C to stop")

    try:
        while True:
            try:
                # 1) Part 1–2: discover new alerts and register them (sets needs_send=true).
                new_payloads = fetch_all_new_alert_payloads()
                n_new = len(new_payloads or [])
                if n_new:
                    print(f"[RUNNER] registered {n_new} new alerts")

                # 2) Part 3–4: send initial payloads + drain call-log updates/expiries to Kafka.
                #    (This function already flushes the Kafka producer at the end of the run.)
                run_pipeline_until_drained(max_cycles=MAX_PIPELINE_CYCLES)

                # 3) Sleep with a small backoff if idle
                if n_new == 0:
                    idle_streak += 1
                    sleep_for = IDLE_BACKOFF_SECS if idle_streak > 2 else TICK_SECS
                else:
                    idle_streak = 0
                    sleep_for = TICK_SECS

                time.sleep(sleep_for)

            except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
                # transient DB issues; short retry
                print(f"[RUNNER] DB error: {e}. Retrying in 5s…")
                time.sleep(5)

            except KafkaException as ke:
                # transient Kafka issues; short retry
                print(f"[RUNNER] Kafka error: {ke}. Retrying in 5s…")
                time.sleep(5)

            except Exception as ex:
                # don’t crash the daemon — log and retry
                print(f"[RUNNER] Unexpected error: {ex}. Retrying in 5s…")
                time.sleep(5)

    except KeyboardInterrupt:
        print("[RUNNER] stopping…")

if __name__ == "__main__":
    main()
