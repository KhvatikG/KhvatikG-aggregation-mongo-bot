from datetime import datetime
from typing import Literal

from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger

from utils.dataframe_skip_filling import dt_range, fill_blanks


async def aggregate_sum_from_date(
        db_: AsyncIOMotorDatabase,
        dt_from: str,
        dt_upto: str,
        group_type: Literal['hour', 'day', 'month']):

    if not (group_type == 'hour' or group_type == 'day' or group_type == 'month'):
        e = f"group_type value is {group_type} but this value should be only ('hour' | 'day' | 'month')"
        logger.warning(e)
        return e

    collection = db_.get_collection("sample_collection")

    date_format = None
    date_group = None

    if group_type == "month":
        date_format = "%Y-%m-%dT00:00:00"
        date_group = {
            "year": {"$year": "$dt"},
            "month": {"$month": "$dt"},
            "day": 1
        }
    elif group_type == "day":
        date_format = "%Y-%m-%dT00:00:00"
        date_group = {
            "year": {"$year": "$dt"},
            "month": {"$month": "$dt"},
            "day": {"$dayOfMonth": "$dt"}
        }
    elif group_type == "hour":
        date_format = "%Y-%m-%dT%H:00:00"
        date_group = {
            "year": {"$year": "$dt"},
            "month": {"$month": "$dt"},
            "day": {"$dayOfMonth": "$dt"},
            "hour": {"$hour": "$dt"}
        }

    query = [
        {"$match": {
            "dt": {
                "$gte": datetime.fromisoformat(dt_from),
                "$lte": datetime.fromisoformat(dt_upto)
            }
        }},
        {"$project": {
            "value": 1,
            "formatted_date": {
                "$dateToString": {
                    "format": date_format,
                    "date": {"$dateFromParts": date_group}
                }
            }
        }},
        {"$group": {
            "_id": {"formatted_date": "$formatted_date"},
            "totalValue": {"$sum": "$value"}
        }},
        {"$sort": {"_id.formatted_date": 1}},
        {"$project": {
            "_id": 0,
            "date": "$_id.formatted_date",
            "totalValue": "$totalValue"
        }},
        {"$group": {
            "_id": None,
            "data": {
                "$push": {"k": "$date", "v": "$totalValue"}
            }
        }},
        {"$project": {
            "_id": 0,
            "data": {"$arrayToObject": "$data"}
        }}
    ]

    data = await collection.aggregate(query).to_list(None)
    data = data[0]

    dates = dt_range(date_from=dt_from, date_to=dt_upto, step=group_type)

    result = fill_blanks(date_list=dates, data_df=data)

    return result
