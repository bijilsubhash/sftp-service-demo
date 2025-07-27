import paramiko
from common.utils.logging_util import Logger

logger = Logger(__name__)


class SFTPClient:
    def __init__(self, hostName: str, port: int, userName: str, password: str):
        self.hostName = hostName
        self.port = port
        self.userName = userName
        self.password = password

    def connect(self) -> paramiko.SFTPClient:
        self.SSH_Client = paramiko.SSHClient()
        self.SSH_Client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            logger.info(f"Connecting to {self.hostName}:{self.port} as {self.userName}")
            self.SSH_Client.connect(
                hostname=self.hostName,
                port=self.port,
                username=self.userName,
                password=self.password,
                look_for_keys=False,
                allow_agent=False,
                timeout=30,
                banner_timeout=30,
                auth_timeout=30,
            )
            logger.info(f"SSH connection established to {self.hostName}")
            sftp_client = self.SSH_Client.open_sftp()
            logger.info("SFTP channel opened successfully")
            return sftp_client
        except paramiko.AuthenticationException as e:
            logger.error(
                f"Authentication failed for {self.userName}@{self.hostName}: {e}"
            )
            raise
        except paramiko.SSHException as e:
            logger.error(f"SSH error connecting to {self.hostName}: {e}")
            raise
        except Exception as e:
            logger.error(f"Connection error to {self.hostName}: {e}")
            raise

    def close(self) -> None:
        self.SSH_Client.close()

    def put(
        self, sftp_client: paramiko.SFTPClient, localFilePath: str, remoteFilePath: str
    ) -> None:
        try:
            sftp_client.put(localFilePath, remoteFilePath)
            logger.info(f"Uploaded {localFilePath} to {remoteFilePath}")
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            raise
