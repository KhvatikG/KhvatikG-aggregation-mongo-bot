import asyncio

from core.message_validation import ValidIncomingMessage
from tests.tasks_for_tests import task1, task2, task3
from aggregations import aggregate_sum_from_date

import pytest

pytestmark = pytest.mark.asyncio(scope="module")
loop: asyncio.AbstractEventLoop


async def test_task1():
    db_ = task1.db_
    data = ValidIncomingMessage.model_validate_json(task1.task)
    answer = await aggregate_sum_from_date(
        db_=db_,
        dt_from=data.dt_from,
        dt_upto=data.dt_upto,
        group_type=data.group_type
    )
    assert answer == task1.answer_for_task


async def test_task2():
    db_ = task2.db_
    data = ValidIncomingMessage.model_validate_json(task2.task)
    answer = await aggregate_sum_from_date(
        db_=db_,
        dt_from=data.dt_from,
        dt_upto=data.dt_upto,
        group_type=data.group_type
    )
    assert answer == task2.answer_for_task


async def test_task3():
    db_ = task2.db_
    data = ValidIncomingMessage.model_validate_json(task3.task)
    answer = await aggregate_sum_from_date(
        db_=db_,
        dt_from=data.dt_from,
        dt_upto=data.dt_upto,
        group_type=data.group_type
    )
    assert answer == task3.answer_for_task
