import asyncio

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from pprint import pprint

uri = "mongodb://localhost:27017/?authSource=admin"
client = AsyncIOMotorClient(uri)
db = client.get_database("sampleDB")


async def hello_mongo(db_: AsyncIOMotorDatabase):
    sample_collection = db_.get_collection("sample_collection")

    doc = await sample_collection.find_one({"value": 800})

    pprint((doc))

if __name__ == '__main__':
    asyncio.run(hello_mongo(db))
