# jsonl-parser-python
JsonL parser using Python to Parquet with Tests

# Steps
 ## file_generator.py
 First run `python file_generator.py` to generate an initial `.jsonl` file that contains a base64 payload

 ## file_converter.py
 Second run `python file_converter.py` to convert the generated file from above into `.parquet` file and convert the base64 payload to hex

 ## tests.py
 Finally, run `pytest ./tests/tests.py` to test that the generated file is valid


# Environment Description
- A corrupted log stream `./corrupted_events.json` containing exactly 50,000 recoverable JSON objects. Many objects are fused together on the same line, and the byte stream also includes truncated JSON fragments and arbitrary inserted bytes between intact objects.
- Each JSON object contains two candidate payload fields, `payload_a` and `payload_b`, which may be base64-encoded, already hexadecimal, empty, or malformed non-empty text that is neither valid base64 nor hexadecimal.
- At least one of `payload_a` or `payload_b` is valid and non-empty in every source record. Some records have both candidates valid, and in those cases the selected candidate is determined pseudo-randomly from `event_id`: compute SHA-256 of the ASCII string `{event_id}:payload-selector` and use `payload_a` when the first digest byte is even, otherwise `payload_b`.
- The source record identifier is `event_id`, and the target Parquet output contains three fields: `event_id` (integer), a normalized hexadecimal string column `payload_hex`, and the selected original valid non-empty payload string `payload`.
- Python with `pandas` and `pyarrow` is available in the environment.

# Test Description
1. A Parquet file exists at `./cleaned_events.parquet`.
2. The Parquet file contains exactly one output row for every recoverable source record, even though the source stream also contains truncated fragments and inserted bytes.
3. The Parquet file contains one INT64 identifier column and two UTF8 string columns.
4. The Parquet file contains `payload_hex` and `payload` columns.
5. The `payload` column preserves the selected original valid non-empty payload value from the source record.
6. The `payload_hex` column contains valid hexadecimal representations of the selected source data, whether the selected payload was base64 or already hexadecimal.
7. Records where `payload_a` is invalid or empty correctly fall back to `payload_b`.
8. Records where both `payload_a` and `payload_b` are valid follow the deterministic pseudo-random selector derived from `event_id`: SHA-256 of `{event_id}:payload-selector`, with `payload_a` chosen when the first digest byte is even and `payload_b` otherwise.
