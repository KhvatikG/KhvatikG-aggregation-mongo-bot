from datetime import datetime
from typing import Literal

from pydantic import BaseModel, field_validator, validator


class ValidIncomingMessage(BaseModel):
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