from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DB_URI: str = "mongodb://localhost:27017/?authSource=admin"
    DB_NAME: str = "sampleDB"


settings = Settings()
