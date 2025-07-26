import paramiko
from utils.logging import Logger

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

    def get(self, remoteFilePath: str, localFilePath: str) -> None:
        self.sftp_client = self.SSH_Client.open_sftp()
        try:
            self.sftp_client.get(remoteFilePath, localFilePath)
        except Exception as e:
            logger.error(f"An error occurred: {e}")
        finally:
            self.sftp_client.close()

    def put(self, localFilePath: str, remoteFilePath: str) -> None:
        self.sftp_client = self.SSH_Client.open_sftp()
        try:
            self.sftp_client.put(localFilePath, remoteFilePath)
        except Exception as e:
            logger.error(f"An error occurred: {e}")
        finally:
            self.sftp_client.close()

    def list(self, remoteFilePath: str) -> list[str]:
        self.sftp_client = self.SSH_Client.open_sftp()
        try:
            return self.sftp_client.listdir(remoteFilePath)
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            return []
        finally:
            self.sftp_client.close()

    def remove_file(self, remoteFilePath: str) -> None:
        self.sftp_client = self.SSH_Client.open_sftp()
        try:
            self.sftp_client.remove(remoteFilePath)
            logger.info(f"Removed file: {remoteFilePath}")
        except Exception as e:
            logger.error(f"Error removing file {remoteFilePath}: {e}")
        finally:
            self.sftp_client.close()

    def remove_dir(self, remoteDirPath: str) -> None:
        self.sftp_client = self.SSH_Client.open_sftp()
        try:
            self.sftp_client.rmdir(remoteDirPath)
            logger.info(f"Removed directory: {remoteDirPath}")
        except Exception as e:
            logger.error(f"Error removing directory {remoteDirPath}: {e}")
        finally:
            self.sftp_client.close()

    def clear_directory(self, remoteDirPath: str) -> None:
        """Recursively delete all files and subdirectories in a directory"""
        self.sftp_client = self.SSH_Client.open_sftp()
        try:
            # List all items in the directory
            items = self.sftp_client.listdir_attr(remoteDirPath)

            for item in items:
                item_path = f"{remoteDirPath}/{item.filename}"

                # Check if it's a directory or file
                if item.st_mode and item.st_mode & 0o040000:  # Directory
                    self.clear_directory(item_path)  # Recursive call
                    self.sftp_client.rmdir(item_path)
                    logger.info(f"Removed directory: {item_path}")
                else:  # File
                    self.sftp_client.remove(item_path)
                    logger.info(f"Removed file: {item_path}")

        except Exception as e:
            logger.error(f"Error clearing directory {remoteDirPath}: {e}")
        finally:
            self.sftp_client.close()
