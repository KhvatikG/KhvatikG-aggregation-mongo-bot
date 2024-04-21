from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from core.config import settings


class MongoDbFactory:

    def __init__(self, uri: str):
        self.uri = uri
        self.client = AsyncIOMotorClient(self.uri)

    def get_db(self, db_name) -> AsyncIOMotorDatabase:
        """
        Получение объекта базы данных.

        :param db_name: Имя базы данных.
        :return: Экземпляр базы данных.
        """
        return self.client.get_database(db_name)


db_factory = MongoDbFactory(uri=settings.DB_URI)

db = db_factory.get_db(db_name=settings.DB_NAME)
