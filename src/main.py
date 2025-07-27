import sys
import re

from pathlib import Path

from common.utils.logging_util import Logger
from sftp.services.faker_service import FakerService
from sftp.services.sftp_service import SFTPClient

import dagster as dg

logger = Logger(__name__)


sftp_client = SFTPClient(
    hostName=dg.EnvVar("hostName").get_value(),
    port=dg.EnvVar("port").get_value(),
    userName=dg.EnvVar("userName").get_value(),
    password=dg.EnvVar("password").get_value(),
)


def upload_data(csv_file: str, data_dir: Path | None = None) -> None:
    try:
        sftp_conn = sftp_client.connect()

        if csv_file in ["customer.csv", "product.csv"]:
            local_file = Path("data") / csv_file
        elif data_dir:
            local_file = data_dir / csv_file

        if local_file.exists():
            remote_path = f"input/{csv_file}"
            sftp_client.put(sftp_conn, str(local_file), remote_path)
            logger.info(f"Uploaded {csv_file} from {local_file}")
        else:
            logger.warning(f"File not found: {local_file}")
    finally:
        sftp_client.close()


# used for local testing
if __name__ == "__main__":
    args = sys.argv[1:]

    if not args:
        raise ValueError(
            "No argument provided. Please provide a date in format DD-MM-YYYY"
        )

    if len(args) != 1:
        raise ValueError("Only one argument is allowed")

    if not re.match(r"^\d{2}-\d{2}-\d{4}$", args[0]):
        raise ValueError("Invalid date format")

    faker_service = FakerService(args[0])
    faker_service.generate_data(size=1000)
    upload_data("order.csv", faker_service.output_dir)
