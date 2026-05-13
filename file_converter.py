import base64
import hashlib
import json
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


source_path = Path("./corrupted_events.json")
output_path = Path("./cleaned_events.parquet")

decoder = json.JSONDecoder()
text = source_path.read_bytes().decode("latin-1")
index = 0
hex_pattern = re.compile(r"^[0-9a-fA-F]+$")

event_ids = []
payload_hexes = []
original_payloads = []


def normalize_payload(payload: str) -> str | None:
    if payload == "":
        return None
    if len(payload) % 2 == 0 and hex_pattern.fullmatch(payload):
        return payload.lower()
    try:
        return base64.b64decode(payload, validate=True).hex()
    except Exception:
        return None


def selector_field(event_id: int) -> str:
    digest = hashlib.sha256(f"{event_id}:payload-selector".encode("ascii")).digest()
    return "payload_a" if digest[0] % 2 == 0 else "payload_b"


def select_payload(record: dict) -> tuple[str, str]:
    normalized_a = normalize_payload(record["payload_a"])
    normalized_b = normalize_payload(record["payload_b"])
    if normalized_a is not None and normalized_b is not None:
        field = selector_field(int(record["event_id"]))
        return record[field], normalized_a if field == "payload_a" else normalized_b
    for field, normalized_payload in (("payload_a", normalized_a), ("payload_b", normalized_b)):
        if normalized_payload is not None:
            return record[field], normalized_payload
    raise ValueError(f"No valid payload candidates for event_id={record['event_id']}")

while index < len(text):
    start = text.find("{", index)
    if start < 0:
        break
    try:
        record, next_index = decoder.raw_decode(text, start)
    except json.JSONDecodeError:
        index = start + 1
        continue
    if not isinstance(record, dict) or "event_id" not in record or "payload_a" not in record or "payload_b" not in record:
        index = start + 1
        continue
    selected_payload, normalized_payload = select_payload(record)
    event_ids.append(int(record["event_id"]))
    payload_hexes.append(normalized_payload)
    original_payloads.append(selected_payload)
    index = next_index

schema = pa.schema(
    [
        pa.field("event_id", pa.int64()),
        pa.field("payload_hex", pa.string()),
        pa.field("payload", pa.string()),
    ]
)

table = pa.Table.from_arrays(
    [
        pa.array(event_ids, type=pa.int64()),
        pa.array(payload_hexes, type=pa.string()),
        pa.array(original_payloads, type=pa.string()),
    ],
    schema=schema,
)

pq.write_table(table, output_path)