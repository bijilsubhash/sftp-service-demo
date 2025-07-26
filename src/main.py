import sys
import re
import paramiko

from pathlib import Path
from typing import Generator
from contextlib import contextmanager

from common.utils.logging_util import Logger
from sftp.models.config import SFTPConfig
from sftp.services.faker_service import FakerService
from sftp.services.sftp_service import SFTPClient

sftp_config = SFTPConfig()

logger = Logger(__name__)

sftp_client = SFTPClient(
    hostName=sftp_config.hostName,
    port=sftp_config.port,
    userName=sftp_config.userName,
    password=sftp_config.password,
)


@contextmanager
def sftp_connection() -> Generator[SFTPClient, None, None]:
    try:
        sftp_client.connect()
        yield sftp_client.SSH_Client.open_sftp()
    except paramiko.AuthenticationException as e:
        logger.error(f"Authentication failed: {e}")
    except Exception as e:
        logger.error(f"Uncaught exception: {e}")
    finally:
        sftp_client.close()


def upload_data(data_dir: Path) -> None:
    with sftp_connection() as conn:
        for csv_file in [
            "customer.csv",
            "product.csv",
            "order.csv",
        ]:
            local_file = data_dir / csv_file
            if local_file.exists():
                remote_path = f"input/{data_dir.name}/{csv_file}"
                sftp_client.put(conn, str(local_file), remote_path)
            else:
                logger.warning(f"File not found: {local_file}")
        logger.info(f"Uploaded data for {data_dir.name}")


if __name__ == "__main__":
    args = sys.argv[1:]

    if not args:
        raise ValueError("No date provided")

    if len(args) != 1:
        raise ValueError("Only one date is allowed")

    if not re.match(r"^\d{2}-\d{2}-\d{4}$", args[0]):
        raise ValueError("Invalid date format")

    faker_service = FakerService(args[0])
    faker_service.generate_data(size=1000)
    upload_data(faker_service.output_dir)
