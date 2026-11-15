from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE = f"{Path(__file__).parent}/.env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=ENV_FILE,
    )

    db_database: str = "postgres"
    db_user: str = "admin"
    db_password: str = "admin"
    db_port: int = 5433
    db_host: str = "localhost"
    db_database_dwh: str = "dwh"


db_settings = Settings()
