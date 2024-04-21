from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DB_URI: str = "mongodb://localhost:27017/?authSource=admin"
    DB_NAME: str = "sampleDB"
    BOT_TOKEN: str = ""

    model_config = SettingsConfigDict(env_prefix='AMB_')  # Aggregation Mongo Bot  # Aggregation Mongo Bot


settings = Settings()
