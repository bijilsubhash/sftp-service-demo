import os
import paramiko
from typing import Generator
from contextlib import contextmanager

from dotenv import load_dotenv

from src.services.faker_service import generate_data
from src.services.sftp_service import SFTPClient
from src.utils.logging import Logger

load_dotenv()

logger = Logger(__name__)

sftp_client = SFTPClient(
    hostName=os.getenv("hostName", ""),
    port=int(os.getenv("port", 0)),
    userName=os.getenv("userName", ""),
    password=os.getenv("password", ""),
)


@contextmanager
def sftp_connection() -> Generator[SFTPClient]:
    try:
        sftp_client.connect()
        yield sftp_client
    except paramiko.AuthenticationException as e:
        logger.error(f"Authentication failed: {e}")
    except Exception as e:
        logger.error(f"Uncaught exception: {e}")
    finally:
        sftp_client.close()


def upload_data() -> None:
    with sftp_connection() as client:
        client.put("output/data.csv", "input/data.csv")
        logger.info("Fake data uploaded")


if __name__ == "__main__":
    generate_data()
    upload_data()
