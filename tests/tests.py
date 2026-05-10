import base64
import json
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


SOURCE_PATH = Path("./corrupted_events.json")
OUTPUT_PATH = Path("./cleaned_events.parquet")
HEX_PATTERN = re.compile(r"^[0-9a-fA-F]+$")


def parse_corrupted_records(path: Path) -> list[dict]:
    decoder = json.JSONDecoder()
    text = path.read_text(encoding="utf-8")
    index = 0
    records: list[dict] = []

    while index < len(text):
        while index < len(text) and text[index].isspace():
            index += 1
        if index >= len(text):
            break
        record, next_index = decoder.raw_decode(text, index)
        records.append(record)
        index = next_index

    return records


def normalize_payload(payload: str) -> str | None:
    if payload == "":
        return None
    if len(payload) % 2 == 0 and HEX_PATTERN.fullmatch(payload):
        return payload.lower()
    return base64.b64decode(payload, validate=True).hex()


def expected_rows() -> tuple[list[dict], int, int]:
    rows: list[dict] = []
    empty_payloads = 0
    hex_payloads = 0
    for record in parse_corrupted_records(SOURCE_PATH):
        normalized_payload = normalize_payload(record["payload"])
        if normalized_payload is None:
            empty_payloads += 1
            continue
        if len(record["payload"]) % 2 == 0 and HEX_PATTERN.fullmatch(record["payload"]):
            hex_payloads += 1
        rows.append(
            {
                "event_id": int(record["event_id"]),
                "payload_hex": normalized_payload,
                "payload": record["payload"],
            }
        )
    return rows, empty_payloads, hex_payloads


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
    # Spec bullet 2: the seeded dataset yields exactly 49,057 valid output rows after excluding empty payloads.
    source_records = parse_corrupted_records(SOURCE_PATH)
    expected, empty_payloads, hex_payloads = expected_rows()
    assert len(source_records) == 50_000, "Seed data must contain exactly 50,000 logical JSON objects"
    assert empty_payloads > 0, "Seed data must include empty payloads"
    assert hex_payloads > 0, "Seed data must include already-hex payloads"
    assert len(expected) == len(source_records) - empty_payloads

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
    # Spec bullet 5: one string column must preserve the original non-empty payload values.
    expected, _, _ = expected_rows()
    table = load_output_table()
    int_index, string_indexes = classify_output_columns(table)

    identifier_values = table.column(int_index).to_pylist()
    first_string_values = table.column(string_indexes[0]).to_pylist()
    second_string_values = table.column(string_indexes[1]).to_pylist()

    expected_sorted = sorted(expected, key=lambda row: row["event_id"])
    expected_ids = [row["event_id"] for row in expected_sorted]
    expected_original_payloads = [row["payload"] for row in expected_sorted]

    rows_by_id = sorted(zip(identifier_values, first_string_values, second_string_values), key=lambda row: row[0])
    actual_ids = [row[0] for row in rows_by_id]
    first_sorted = [row[1] for row in rows_by_id]
    second_sorted = [row[2] for row in rows_by_id]

    assert actual_ids == expected_ids
    assert first_sorted == expected_original_payloads or second_sorted == expected_original_payloads


def test_one_string_column_contains_normalized_hex_payloads():
    # Spec bullet 6: one string column must contain normalized hexadecimal payloads.
    expected, _, _ = expected_rows()
    table = load_output_table()
    int_index, string_indexes = classify_output_columns(table)

    identifier_values = table.column(int_index).to_pylist()
    first_string_values = table.column(string_indexes[0]).to_pylist()
    second_string_values = table.column(string_indexes[1]).to_pylist()

    expected_sorted = sorted(expected, key=lambda row: row["event_id"])
    expected_ids = [row["event_id"] for row in expected_sorted]
    expected_hex_values = [row["payload_hex"] for row in expected_sorted]

    rows_by_id = sorted(zip(identifier_values, first_string_values, second_string_values), key=lambda row: row[0])
    actual_ids = [row[0] for row in rows_by_id]
    first_sorted = [row[1] for row in rows_by_id]
    second_sorted = [row[2] for row in rows_by_id]

    assert actual_ids == expected_ids
    assert first_sorted == expected_hex_values or second_sorted == expected_hex_values

    normalized_column = first_sorted if first_sorted == expected_hex_values else second_sorted
    for payload_hex in normalized_column:
        assert isinstance(payload_hex, str)
        assert payload_hex
        bytes.fromhex(payload_hex)
