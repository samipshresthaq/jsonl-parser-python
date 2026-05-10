# jsonl-parser-python
JsonL parser using Python to Parquet with Tests

# Steps
 ## file_generator.py
 First run `python file_generator.py` to generate an initial `.jsonl` file that contains a base64 payload

 ## file_converter.py
 Second run `python file_converter.py` to convert the generated file from above into `.parquet` file and convert the base64 payload to hex

 ## tests.py
 Finally, run `pytest ./tests/tests.py` to test that the generated file is valid
