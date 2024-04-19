from motor.motor_asyncio import AsyncIOMotorClient
from core.config import settings


class MongoDbFactory:

    def __init__(self, uri: str):
        self.uri = uri
        self.client = AsyncIOMotorClient(self.uri)

    def get_db(self, db_name):
        return self.client.get_database(db_name)


db_factory = MongoDbFactory(uri=settings.DB_URI)

db = db_factory.get_db(settings.DB_NAME)

