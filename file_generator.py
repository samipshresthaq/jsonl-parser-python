import base64
import hashlib
import json
import random
import re
from pathlib import Path

random.seed(42)

output_path = Path("./corrupted_events.json")
sources = ["edge-gw-1", "edge-gw-2", "collector-a", "collector-b"]
event_types = ["ingest", "forward", "retry", "archive"]
hex_pattern = re.compile(r"^[0-9a-fA-F]+$")


def encode_base64_payload(data: bytes) -> str:
    suffix = 0
    while True:
        candidate = data + bytes([suffix]) if suffix else data
        payload = base64.b64encode(candidate).decode("ascii")
        if not (len(payload) % 2 == 0 and hex_pattern.fullmatch(payload)):
            return payload
        suffix += 1

records = []
for event_id in range(1, 50_001):
    span = 6 + (event_id % 23)
    digest = hashlib.sha256(f"event:{event_id}:telemetry".encode("ascii")).digest()[:span]
    if event_id % 53 == 0:
        payload = ""
    elif event_id % 37 == 0:
        payload = digest.hex().upper()
    else:
        payload = encode_base64_payload(digest)
    records.append(
        {
            "event_id": event_id,
            "payload": payload,
            "source": random.choice(sources),
            "event_type": random.choice(event_types),
            "attempt": 1 + (event_id % 4),
            "received_at": f"2026-01-{1 + (event_id % 28):02d}T{event_id % 24:02d}:{(event_id * 7) % 60:02d}:{(event_id * 13) % 60:02d}Z",
        }
    )

with output_path.open("w", encoding="utf-8") as handle:
    index = 0
    while index < len(records):
        group_size = random.randint(1, 6)
        chunk = records[index : index + group_size]
        handle.write("".join(json.dumps(record, separators=(",", ":")) for record in chunk))
        handle.write("\n")
        index += group_size