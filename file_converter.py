import base64
import json
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


source_path = Path("./corrupted_events.json")
output_path = Path("./cleaned_events.parquet")

decoder = json.JSONDecoder()
text = source_path.read_text(encoding="utf-8")
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
    return base64.b64decode(payload, validate=True).hex()

while index < len(text):
    while index < len(text) and text[index].isspace():
        index += 1
    if index >= len(text):
        break

    record, next_index = decoder.raw_decode(text, index)
    normalized_payload = normalize_payload(record["payload"])
    if normalized_payload is not None:
        event_ids.append(int(record["event_id"]))
        payload_hexes.append(normalized_payload)
        original_payloads.append(record["payload"])
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