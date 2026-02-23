from pathlib import Path

from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict


class Settings(BaseSettings):
    ribasim_exe: Path = Path("ribasim")
    ribasim_nl_cloud_pass: str = ""
    ribasim_nl_data_dir: Path = Path("data")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Let .env file override environment variables.
        return (
            init_settings,
            dotenv_settings,
            env_settings,
            file_secret_settings,
        )

    model_config = SettingsConfigDict(env_file=(".env"))


settings = Settings()
