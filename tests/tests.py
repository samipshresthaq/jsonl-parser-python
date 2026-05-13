import base64
import hashlib
import json
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


SOURCE_PATH = Path("./corrupted_events.json")
OUTPUT_PATH = Path("./cleaned_events.parquet")
HEX_PATTERN = re.compile(r"^[0-9a-fA-F]+$")


def parse_corrupted_records(path: Path) -> tuple[list[dict], dict[str, int]]:
    decoder = json.JSONDecoder()
    raw = path.read_bytes()
    text = raw.decode("latin-1")
    index = 0
    records: list[dict] = []
    failed_starts = 0

    while index < len(text):
        start = text.find("{", index)
        if start < 0:
            break
        try:
            record, next_index = decoder.raw_decode(text, start)
        except json.JSONDecodeError:
            failed_starts += 1
            index = start + 1
            continue
        if not isinstance(record, dict) or "event_id" not in record or "payload_a" not in record or "payload_b" not in record:
            failed_starts += 1
            index = start + 1
            continue
        records.append(record)
        index = next_index

    return records, {
        "failed_starts": failed_starts,
        "non_ascii_bytes": sum(1 for value in raw if value >= 0x80),
        "control_bytes": sum(1 for value in raw if value < 0x20 and value not in (0x09, 0x0A, 0x0D)),
    }


def normalize_payload(payload: str) -> str | None:
    if payload == "":
        return None
    if len(payload) % 2 == 0 and HEX_PATTERN.fullmatch(payload):
        return payload.lower()
    try:
        return base64.b64decode(payload, validate=True).hex()
    except Exception:
        return None


def selector_field(event_id: int) -> str:
    digest = hashlib.sha256(f"{event_id}:payload-selector".encode("ascii")).digest()
    return "payload_a" if digest[0] % 2 == 0 else "payload_b"


def select_payload(record: dict) -> tuple[str, str, str]:
    payload_a = record["payload_a"]
    payload_b = record["payload_b"]

    normalized_a = normalize_payload(payload_a)
    normalized_b = normalize_payload(payload_b)
    if normalized_a is not None and normalized_b is not None:
        selected_field = selector_field(int(record["event_id"]))
        selected_payload = record[selected_field]
        selected_hex = normalized_a if selected_field == "payload_a" else normalized_b
        return selected_field, selected_payload, selected_hex
    if normalized_a is not None:
        return "payload_a", payload_a, normalized_a

    if normalized_b is not None:
        return "payload_b", payload_b, normalized_b

    raise AssertionError(f"Seed data must provide at least one valid payload candidate for event_id={record['event_id']}")


def build_expectations() -> dict[str, object]:
    rows: list[dict] = []
    invalid_candidates = 0
    empty_candidates = 0
    payload_a_selected = 0
    payload_b_selected = 0
    both_valid_records: list[dict] = []
    fallback_records: list[dict] = []
    records, source_stats = parse_corrupted_records(SOURCE_PATH)
    both_valid_a_selected = 0
    both_valid_b_selected = 0
    for record in records:
        normalized_a = normalize_payload(record["payload_a"])
        normalized_b = normalize_payload(record["payload_b"])

        for payload in (record["payload_a"], record["payload_b"]):
            normalized_payload = normalize_payload(payload)
            if normalized_payload is None:
                if payload == "":
                    empty_candidates += 1
                else:
                    invalid_candidates += 1

        selected_field, selected_payload, selected_hex = select_payload(record)
        if selected_field == "payload_a":
            payload_a_selected += 1
        else:
            payload_b_selected += 1
        if normalized_a is None and normalized_b is not None:
            fallback_records.append(
                {
                    "event_id": int(record["event_id"]),
                    "payload": selected_payload,
                }
            )

        if normalized_a is not None and normalized_b is not None:
            both_valid_records.append(
                {
                    "event_id": int(record["event_id"]),
                    "payload_a": record["payload_a"],
                    "payload_b": record["payload_b"],
                    "selected_field": selected_field,
                }
            )
            if selected_field == "payload_a":
                both_valid_a_selected += 1
            else:
                both_valid_b_selected += 1

        rows.append(
            {
                "event_id": int(record["event_id"]),
                "payload_hex": selected_hex,
                "payload": selected_payload,
            }
        )
    return {
        "rows": rows,
        "source_stats": source_stats,
        "invalid_candidates": invalid_candidates,
        "empty_candidates": empty_candidates,
        "payload_a_selected": payload_a_selected,
        "payload_b_selected": payload_b_selected,
        "both_valid_records": both_valid_records,
        "both_valid_a_selected": both_valid_a_selected,
        "both_valid_b_selected": both_valid_b_selected,
        "fallback_records": fallback_records,
    }


def load_output_table() -> pa.Table:
    return pq.read_table(OUTPUT_PATH)


def classify_output_columns(table: pa.Table) -> tuple[int, list[int]]:
    int_indexes = [index for index, field in enumerate(table.schema) if field.type == pa.int64()]
    string_indexes = [index for index, field in enumerate(table.schema) if field.type == pa.string()]
    assert len(int_indexes) == 1
    assert len(string_indexes) == 2
    return int_indexes[0], string_indexes


def test_parquet_file_exists():
    # Spec bullet 1: output Parquet file exists.
    assert OUTPUT_PATH.exists(), f"Expected output file at {OUTPUT_PATH}"


def test_parquet_contains_all_non_empty_payload_rows():
    # Spec bullet 2: the output contains one row per recoverable source record despite noise in the stream.
    expectations = build_expectations()
    source_records = expectations["rows"]
    source_stats = expectations["source_stats"]
    expected = expectations["rows"]
    invalid_candidates = expectations["invalid_candidates"]
    empty_candidates = expectations["empty_candidates"]
    payload_a_selected = expectations["payload_a_selected"]
    payload_b_selected = expectations["payload_b_selected"]
    both_valid_records = expectations["both_valid_records"]
    assert len(source_records) == 50_000, "Seed data must contain exactly 50,000 recoverable JSON objects"
    assert empty_candidates > 0, "Seed data must include empty payload candidates"
    assert invalid_candidates > 0, "Seed data must include malformed non-empty payload candidates"
    assert payload_a_selected > 0, "Seed data must include rows that select payload_a"
    assert payload_b_selected > 0, "Seed data must include rows that select payload_b"
    assert both_valid_records, "Seed data must include records where both candidates are valid"
    assert source_stats["failed_starts"] > 0, "Seed data must include truncated fragments or garbage starts"
    assert source_stats["non_ascii_bytes"] > 0, "Seed data must include inserted non-ASCII bytes"
    assert source_stats["control_bytes"] > 0, "Seed data must include inserted control bytes"
    assert len(expected) == len(source_records)

    table = load_output_table()
    assert table.num_rows == len(expected)


def test_parquet_contains_required_column_types():
    # Spec bullet 3: output must contain one INT64 identifier column and two UTF8 string columns.
    table = load_output_table()
    int_index, string_indexes = classify_output_columns(table)
    assert int_index >= 0
    assert len(string_indexes) == 2
    assert len(table.schema) == 3

def test_parquet_contains_specific_columns():
    # Spec bullet 4: output must contain payload_hex and payload columns.
    table = load_output_table()
    assert 'payload_hex' in table.schema.names
    assert 'payload' in table.schema.names


def test_one_string_column_preserves_original_payloads():
    # Spec bullet 5: the payload column must preserve the selected original valid non-empty payload values.
    expected = build_expectations()["rows"]
    table = load_output_table()
    identifier_values = table.column(table.schema.get_field_index("event_id")).to_pylist()
    payload_values = table.column(table.schema.get_field_index("payload")).to_pylist()

    expected_sorted = sorted(expected, key=lambda row: row["event_id"])
    expected_ids = [row["event_id"] for row in expected_sorted]
    expected_original_payloads = [row["payload"] for row in expected_sorted]

    rows_by_id = sorted(zip(identifier_values, payload_values), key=lambda row: row[0])
    actual_ids = [row[0] for row in rows_by_id]
    payload_sorted = [row[1] for row in rows_by_id]

    assert actual_ids == expected_ids
    assert payload_sorted == expected_original_payloads


def test_one_string_column_contains_normalized_hex_payloads():
    # Spec bullet 6: the payload_hex column must contain normalized hexadecimal payloads for the selected candidate.
    expected = build_expectations()["rows"]
    table = load_output_table()
    identifier_values = table.column(table.schema.get_field_index("event_id")).to_pylist()
    payload_hex_values = table.column(table.schema.get_field_index("payload_hex")).to_pylist()

    expected_sorted = sorted(expected, key=lambda row: row["event_id"])
    expected_ids = [row["event_id"] for row in expected_sorted]
    expected_hex_values = [row["payload_hex"] for row in expected_sorted]

    rows_by_id = sorted(zip(identifier_values, payload_hex_values), key=lambda row: row[0])
    actual_ids = [row[0] for row in rows_by_id]
    payload_hex_sorted = [row[1] for row in rows_by_id]

    assert actual_ids == expected_ids
    assert payload_hex_sorted == expected_hex_values

    for payload_hex in payload_hex_sorted:
        assert isinstance(payload_hex, str)
        assert payload_hex
        bytes.fromhex(payload_hex)


def test_payload_b_is_used_when_payload_a_is_invalid_or_empty():
    # Spec bullet 7: payload_b is selected when payload_a is invalid or empty.
    expectations = build_expectations()
    fallback_records = expectations["fallback_records"]
    assert fallback_records, "Seed data must include payload_b fallback rows for this check"

    table = load_output_table()
    rows = sorted(
        zip(
            table.column(table.schema.get_field_index("event_id")).to_pylist(),
            table.column(table.schema.get_field_index("payload")).to_pylist(),
        ),
        key=lambda row: row[0],
    )
    actual_payload_by_id = {event_id: payload for event_id, payload in rows}

    for record in fallback_records:
        assert actual_payload_by_id[record["event_id"]] == record["payload"]


def test_both_valid_records_follow_selector_rule():
    # Spec bullet 8: both-valid rows follow the deterministic selector derived from event_id.
    expectations = build_expectations()
    both_valid_records = expectations["both_valid_records"]
    both_valid_a_selected = expectations["both_valid_a_selected"]
    both_valid_b_selected = expectations["both_valid_b_selected"]
    assert both_valid_records, "Seed data must include both-valid records for this check"
    assert both_valid_a_selected > 0, "Seed data must include both-valid rows that choose payload_a"
    assert both_valid_b_selected > 0, "Seed data must include both-valid rows that choose payload_b"

    table = load_output_table()
    rows = sorted(
        zip(
            table.column(table.schema.get_field_index("event_id")).to_pylist(),
            table.column(table.schema.get_field_index("payload")).to_pylist(),
        ),
        key=lambda row: row[0],
    )
    actual_payload_by_id = {event_id: payload for event_id, payload in rows}

    for record in both_valid_records:
        expected_payload = record[record["selected_field"]]
        rejected_payload = record["payload_b"] if record["selected_field"] == "payload_a" else record["payload_a"]
        assert actual_payload_by_id[record["event_id"]] == expected_payload
        assert actual_payload_by_id[record["event_id"]] != rejected_payload
