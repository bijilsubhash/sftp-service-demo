from pydantic_settings import BaseSettings, SettingsConfigDict


class SFTPConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    hostName: str
    port: int
    userName: str
    password: str
