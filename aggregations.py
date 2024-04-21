from datetime import datetime
from typing import Literal

from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger

from utils.dataframe_skip_filling import dt_range, fill_blanks


async def aggregate_sum_from_date(
        db_: AsyncIOMotorDatabase,
        dt_from: datetime,
        dt_upto: datetime,
        group_type: Literal['hour', 'day', 'month']) -> dict | str:
    """
    Агрегирует суммы данных из базы между двумя датами
    с заданным интервалом группировки.

    Данная асинхронная функция выполняет агрегацию данных из коллекции MongoDB,
    суммируя значения в указанные временные периоды (часы, дни, месяцы) между двумя датами.

    :param db_: Экземпляр базы данных MongoDB.
    :param dt_from: Начальная дата и время для агрегации.
    :param dt_upto: Конечная дата и время для агрегации.
    :param group_type: Тип группировки
            для агрегации. Допустимые значения: 'hour', 'day', 'month'.
    :return: Словарь вида {'dataset': list[int], 'labels': list[str]},
             с датами и агрегированными значениями. Даты, для которых не найдены данные,
             будут включены с нулевым значением суммы.

             Если group_type не один из ожидаемых значений,
             функция возвращает строку с ошибкой и логирует предупреждение.
    """

    if not (group_type == 'hour' or group_type == 'day' or group_type == 'month'):
        e = f"group_type must be either 'hour', 'day', or 'month'. Received: {group_type}"
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
                "$gte": dt_from,
                "$lte": dt_upto
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
