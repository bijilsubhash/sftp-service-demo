import os
import paramiko
from typing import Generator
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

from services.faker_service import FakerService
from services.sftp_service import SFTPClient
from utils.logging import Logger

load_dotenv()

logger = Logger(__name__)

sftp_client = SFTPClient(
    hostName=os.getenv("hostName", ""),
    port=int(os.getenv("port", 0)),
    userName=os.getenv("userName", ""),
    password=os.getenv("password", ""),
)


@contextmanager
def sftp_connection() -> Generator[SFTPClient, None, None]:
    try:
        sftp_client.connect()
        yield sftp_client
    except paramiko.AuthenticationException as e:
        logger.error(f"Authentication failed: {e}")
    except Exception as e:
        logger.error(f"Uncaught exception: {e}")
    finally:
        sftp_client.close()


def clear_sftp_data() -> None:
    """Clear all data in the SFTP input directory"""
    with sftp_connection() as client:
        try:
            client.clear_directory("input")
            logger.info("Cleared all data from SFTP input directory")
        except Exception as e:
            logger.warning(f"Could not clear SFTP directory: {e}")


def upload_data(generated_dirs: list[str]) -> None:
    with sftp_connection() as client:
        for dir_path in generated_dirs:
            local_dir = Path(dir_path)
            date_folder = local_dir.name

            for csv_file in ["customer.csv", "product.csv", "order.csv"]:
                local_file = local_dir / csv_file
                if local_file.exists():
                    remote_path = f"input/{date_folder}/{csv_file}"
                    client.put(str(local_file), remote_path)
                    logger.info(f"Uploaded {local_file} to {remote_path}")
                else:
                    logger.warning(f"File not found: {local_file}")


def generate_multi_day_data(size: int = 1000) -> list[str]:
    """Generate data for yesterday, today, and tomorrow"""
    current_date = datetime.now()
    dates = [
        current_date - timedelta(days=1),
        current_date,
        current_date + timedelta(days=1),
    ]

    generated_dirs = []
    for date in dates:
        date_service = FakerService(date)
        date_service.generate_data(size)
        generated_dirs.append(str(date_service.output_dir))

    return generated_dirs


if __name__ == "__main__":
    # Clear existing data from SFTP first
    clear_sftp_data()

    # Generate data for yesterday, today, and tomorrow
    generated_dirs = generate_multi_day_data(size=1000)

    # Upload all generated data to SFTP
    upload_data(generated_dirs)

    logger.info(f"Generated and uploaded data for {len(generated_dirs)} days")
