import paramiko
from common.utils.logging_util import Logger

logger = Logger(__name__)


class SFTPClient:
    def __init__(self, hostName: str, port: int, userName: str, password: str):
        self.hostName = hostName
        self.port = port
        self.userName = userName
        self.password = password

    def connect(self) -> None:
        self.SSH_Client = paramiko.SSHClient()
        self.SSH_Client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self.SSH_Client.connect(
                hostname=self.hostName,
                port=self.port,
                username=self.userName,
                password=self.password,
                look_for_keys=False,
            )
        except paramiko.AuthenticationException as e:
            logger.error(f"Authentication failed: {e}")
        except paramiko.SSHException as e:
            logger.error(f"SSH error: {e}")
        except Exception as e:
            logger.error(f"An error occurred: {e}")

    def close(self) -> None:
        self.SSH_Client.close()

    def get(
        self, sftp_client: paramiko.SFTPClient, remoteFilePath: str, localFilePath: str
    ) -> None:
        try:
            sftp_client.get(remoteFilePath, localFilePath)
        except Exception as e:
            logger.error(f"An error occurred: {e}")

    def put(
        self, sftp_client: paramiko.SFTPClient, localFilePath: str, remoteFilePath: str
    ) -> None:
        try:
            sftp_client.put(localFilePath, remoteFilePath)
        except Exception as e:
            logger.error(f"An error occurred: {e}")
