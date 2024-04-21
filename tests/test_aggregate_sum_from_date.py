import asyncio

import pytest

from tests.tasks_for_tests import task1, task2, task3
from aggregations import aggregate_sum_from_date

pytestmark = pytest.mark.asyncio(scope="module")
loop: asyncio.AbstractEventLoop


async def test_task1():
    data = await aggregate_sum_from_date(**task1.task)
    assert data == task1.answer_for_task


async def test_task2():
    data = await aggregate_sum_from_date(**task2.task)
    assert data == task2.answer_for_task


async def test_task3():
    data = await aggregate_sum_from_date(**task3.task)
    assert data == task3.answer_for_task
