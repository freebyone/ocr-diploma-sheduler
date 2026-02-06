from pydantic_settings import BaseSettings,SettingsConfigDict 
from pydantic import Field


class Settings(BaseSettings):
    port: int 
    host: str

    model_config = SettingsConfigDict(
        env_file=".env"
    )

global_settings = Settings()
# print(Settings().model_dump())