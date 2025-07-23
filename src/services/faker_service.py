import polars as pl
from faker import Faker
from random import randint
from pathlib import Path
from src.utils.logging import Logger

fake = Faker()
logger = Logger(__name__)
OUTPUT_DIR = "output"
OUTPUT_FILE = "data.csv"


def generate_data(size=1000):
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    data = [
        {
            "id": randint(1, 100),
            "name": fake.name(),
            "address": fake.address(),
            "latitude": fake.latitude(),
            "longitude": fake.longitude(),
        }
        for _ in range(size)
    ]
    df = pl.DataFrame(data)
    df.write_csv(f"{OUTPUT_DIR}/{OUTPUT_FILE}")
    logger.info(f"{size} fake data generated")
