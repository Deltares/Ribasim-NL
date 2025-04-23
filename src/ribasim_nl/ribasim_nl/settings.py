from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    ribasim_exe: Path = Path("ribasim")
    ribasim_nl_cloud_pass: str = ""
    ribasim_nl_data_dir: Path = Path("data")

    model_config = SettingsConfigDict(env_file=(".env"))


settings = Settings()
