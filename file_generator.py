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

def encode_malformed_payload(event_id: int, label: str, data: bytes) -> str:
    return f"{data.hex()[:10]}!{label}~{event_id}"


records = []
for event_id in range(1, 50_001):
    span = 6 + (event_id % 23)
    digest_a = hashlib.sha256(f"event:{event_id}:server-a".encode("ascii")).digest()[:span]
    digest_b = hashlib.sha256(f"event:{event_id}:server-b".encode("ascii")).digest()[: span + 1]
    if event_id % 61 == 0:
        payload_a = encode_base64_payload(digest_a)
        payload_b = digest_b.hex().upper()
    elif event_id % 47 == 0:
        payload_a = digest_a.hex().upper()
        payload_b = encode_base64_payload(digest_b)
    elif event_id % 41 == 0:
        payload_a = encode_malformed_payload(event_id, "a", digest_a)
        payload_b = encode_base64_payload(digest_b)
    elif event_id % 31 == 0:
        payload_a = ""
        payload_b = digest_b.hex().upper()
    elif event_id % 29 == 0:
        payload_a = encode_base64_payload(digest_a)
        payload_b = encode_malformed_payload(event_id, "b", digest_b)
    elif event_id % 23 == 0:
        payload_a = digest_a.hex().upper()
        payload_b = ""
    elif event_id % 7 == 0:
        payload_a = digest_a.hex().upper()
        payload_b = ""
    else:
        payload_a = encode_base64_payload(digest_a)
        payload_b = encode_malformed_payload(event_id, "b", digest_b) if event_id % 5 == 0 else ""
    records.append(
        {
            "event_id": event_id,
            "payload_a": payload_a,
            "payload_b": payload_b,
            "source": random.choice(sources),
            "event_type": random.choice(event_types),
            "attempt": 1 + (event_id % 4),
            "received_at": f"2026-01-{1 + (event_id % 28):02d}T{event_id % 24:02d}:{(event_id * 7) % 60:02d}:{(event_id * 13) % 60:02d}Z",
        }
    )

record_blobs = [json.dumps(record, separators=(",", ":")).encode("utf-8") for record in records]
noise_chunks = [
    b"\x00\xff\x1e",
    b"\x81\xfe\x80",
    b"\x00BROKEN\xff",
    b"\x7f\x00\xffTRACE",
]


def truncated_fragment(blob: bytes) -> bytes:
    if len(blob) < 18:
        return blob[: max(1, len(blob) - 1)]
    cut = random.randint(12, len(blob) - 2)
    return blob[:cut]


inserted_noise = 0
inserted_fragments = 0
buffer = bytearray()
index = 0
while index < len(record_blobs):
    group_size = random.randint(1, 6)
    chunk = record_blobs[index : index + group_size]
    for offset, blob in enumerate(chunk, start=index):
        event_id = offset + 1
        if event_id % 11 == 0:
            buffer.extend(random.choice(noise_chunks))
            inserted_noise += 1
        if event_id % 17 == 0:
            fragment_source = record_blobs[(offset + 137) % len(record_blobs)]
            buffer.extend(truncated_fragment(fragment_source))
            inserted_fragments += 1
        buffer.extend(blob)
        if event_id % 19 == 0:
            buffer.extend(b"}")
            inserted_fragments += 1
        if event_id % 23 == 0:
            buffer.extend(random.choice(noise_chunks))
            inserted_noise += 1
    if index % 9 == 0:
        buffer.extend(random.choice(noise_chunks))
        inserted_noise += 1
    buffer.extend(b"\n")
    index += group_size

assert inserted_noise > 0
assert inserted_fragments > 0
output_path.write_bytes(bytes(buffer))