from pathlib import Path

from src.services.faker_service import generate_data

OUTPUT_DIR = "tests/output"
OUTPUT_FILE = "data.csv"


def test_generate_data() -> None:
    generate_data(size=1)
    assert Path(f"{OUTPUT_DIR}/{OUTPUT_FILE}").exists()
