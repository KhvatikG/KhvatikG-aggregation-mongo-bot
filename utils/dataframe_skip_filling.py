from datetime import datetime, timedelta
from typing import Literal

from dateutil.relativedelta import relativedelta
from loguru import logger


def dt_range(date_from: datetime, date_to: datetime, step: Literal['hour', 'day', 'month']) -> list[str] | None:
    """
    Возвращает список дат в ISO от date_from до date_to включительно, с шагом step.
    :param date_from: Первая дата диапазона.
    :param date_to: Последняя дата диапазона.
    :param step: Шаг изменения даты.
    :return: Список дат в ISO от date_from до date_to включительно, с шагом step.
    """
    current = date_from
    date_to = date_to
    result = []

    match step:

        case "hour":

            while current <= date_to:
                result.append(current.isoformat())
                current += timedelta(hours=1)

        case "day":

            while current <= date_to:
                result.append(current.isoformat())
                current += timedelta(days=1)

        case "month":

            while current <= date_to:
                result.append(current.isoformat())
                current += relativedelta(months=1)

        case _:
            logger.error(f"step value should be only 'hour' | 'day' | 'month' !, but step is {step}")
            return

    return result


def fill_blanks(date_list: list, data_df: dict[str: int]) -> dict:
    """
    Заполняет пропуски и формирует датафрейм вида {'dataset': list[int], 'labels': list[str]},
    в случае если в db небыло данных за определенный "label", добавляет отсутствующий "label"
    и соответствующее ему значение "dataset" равное нулю.
    :param date_list: Список всех дат в ISO которые должны быть в итоговом датафрейме.
    :param data_df: Данные в виде {'data': {str(date): int(value)}}.
    :return: Датафрейм с заполненными пропусками вида {'dataset': list[int], 'labels': list[str]}.
    """
    result_data = []
    data_dict = data_df['data']

    for date_ in date_list:
        if value := data_dict.get(date_):
            result_data.append(value)
        else:
            result_data.append(0)

    df = {'dataset': result_data, 'labels': date_list}

    return df
