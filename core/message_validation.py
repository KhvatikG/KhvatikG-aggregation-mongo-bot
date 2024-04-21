from datetime import datetime
from typing import Literal

from pydantic import BaseModel, field_validator


class ValidIncomingMessage(BaseModel):
    """
    Модель для валидации входящих сообщений.

    :param dt_from: Дата и время начала периода.
    :param dt_upto: Дата и время окончания периода.
    :param group_type: Тип группировки данных, может принимать значения 'month', 'day', 'hour'.

    .. seealso:: check_dates - убеждается, что dt_upto больше, чем dt_from.
                check_group_type - проверяет корректность значения group_type.
    """
    dt_from: datetime
    dt_upto: datetime
    group_type: Literal['month', 'day', 'hour']

    @field_validator('dt_upto')
    def check_dates(cls, v, values, **kwargs):
        if 'dt_from' in values.data and v <= values.data['dt_from']:
            raise ValueError(f"'dt_upto' must be greater than 'dt_from'")
        return v

    @field_validator('group_type')
    def check_group_type(cls, v):
        if v not in ['month', 'day', 'hour']:
            raise ValueError("group_type must be one of 'month', 'day', 'hour'")
        return v