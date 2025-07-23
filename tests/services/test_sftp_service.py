import pytest
import paramiko
from src.services.sftp_service import SFTPClient
from unittest import mock


@pytest.fixture
def sftp_client() -> SFTPClient:
    return SFTPClient(hostName="localhost", port=22, userName="test", password="test")


@mock.patch("src.services.sftp_service.SFTPClient.connect")
def test_connect(mock_connect, sftp_client) -> None:
    sftp_client.connect()
    mock_connect.assert_called_once()


@mock.patch("src.services.sftp_service.SFTPClient.connect")
def test_connect_with_invalid_credentials(mock_connect, sftp_client) -> None:
    sftp_client.hostName = "invalid"
    mock_connect.side_effect = paramiko.AuthenticationException("Invalid credentials")
    with pytest.raises(paramiko.AuthenticationException):
        sftp_client.connect()
    mock_connect.side_effect = paramiko.SSHException("SSH error")
    with pytest.raises(paramiko.SSHException):
        sftp_client.connect()
    mock_connect.side_effect = Exception("An error occurred")
    with pytest.raises(Exception):
        sftp_client.connect()


@mock.patch("src.services.sftp_service.SFTPClient.close")
def test_close(mock_close, sftp_client) -> None:
    sftp_client.close()
    mock_close.assert_called_once()


@mock.patch("src.services.sftp_service.SFTPClient.get")
def test_get(mock_get, sftp_client) -> None:
    sftp_client.get("test.csv", "test.csv")
    mock_get.assert_called_once()


@mock.patch("src.services.sftp_service.SFTPClient.get")
def test_get_with_error(mock_get, sftp_client) -> None:
    mock_get.side_effect = Exception("An error occurred")
    with pytest.raises(Exception):
        sftp_client.get("test.csv", "test.csv")
    mock_get.assert_called_once()


@mock.patch("src.services.sftp_service.SFTPClient.put")
def test_put_with_error(mock_put, sftp_client) -> None:
    mock_put.side_effect = Exception("An error occurred")
    with pytest.raises(Exception):
        sftp_client.put("test.csv", "test.csv")
    mock_put.assert_called_once()


@mock.patch("src.services.sftp_service.SFTPClient.put")
def test_put(mock_put, sftp_client) -> None:
    sftp_client.put("test.csv", "test.csv")
    mock_put.assert_called_once()


@mock.patch("src.services.sftp_service.SFTPClient.list")
def test_list(mock_list, sftp_client) -> None:
    sftp_client.list("test.csv")
    mock_list.assert_called_once()


@mock.patch("src.services.sftp_service.SFTPClient.list")
def test_list_with_error(mock_list, sftp_client) -> None:
    mock_list.side_effect = Exception("An error occurred")
    with pytest.raises(Exception):
        sftp_client.list("test.csv")
    mock_list.assert_called_once()
