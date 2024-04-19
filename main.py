import asyncio

from aggregations import aggregate_sum_from_date
from core.mongo_db import db



if __name__ == '__main__':
    asyncio.run(aggregate_sum_from_date(db_=db,
                                        dt_from="2022-10-01T00:00:00",
                                        dt_upto="2022-11-30T23:59:00",
                                        group_type="day"))
