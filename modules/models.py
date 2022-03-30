from dataclasses import dataclass
from typing import List
import logging

import pandas as pd
import numpy as np
import typing

from modules.custom_exeptions import ValidationError, MissingDecorator, MissingArguments


class StringColumn(pd.Series):
    def __init__(self, **kwargs):
        super().__init__(dtype='object')
        self.str_type = "object"
        self.np_type = np.str_
        self.kwargs = kwargs


class IntegerColumn(pd.Series):
    def __init__(self, **kwargs):
        super().__init__(dtype='int64')
        self.str_type = "int64"
        self.np_type = np.int64
        self.kwargs = kwargs


class FloatColumn(pd.Series):
    def __init__(self, **kwargs):
        super().__init__(dtype='float64')
        self.str_type = "float64"
        self.np_type = np.float64
        self.kwargs = kwargs


class DateColumn(pd.Series):
    def __init__(self, **kwargs):
        super().__init__(dtype='datetime64[ns]')
        self.str_type = 'datetime64[ns]'
        self.np_type = np.datetime64
        self.kwargs = kwargs


class BoolColumn(pd.Series):
    def __init__(self, **kwargs):
        super().__init__(dtype='bool')
        self.str_type = "bool"
        self.np_type = np.bool_
        self.kwargs = kwargs

        self.true_or_false = None
        if kwargs.get('true') is not None and kwargs.get('false') is not None:
            self.true_or_false = {kwargs.get('true'): True, kwargs.get('false'): False}


class DataFrame(pd.DataFrame):

    def __init__(self, df: pd.DataFrame = None):
        super().__init__()
        self._data_types: typing.Optional[list] = None
        self.sql: typing.Optional[dict] = None
        self.__is_valide = False

        if df is not None:
            for col_name in df.columns:
                self[col_name] = df[col_name]

    def is_valid(self) -> bool:
        if self._data_types is None:
            self.__is_valide = True
            return self.__is_valide
        try:
            for data_type in self._data_types:
                if data_type.col_obj_series.dtype != self[data_type.name].dtype:
                    raise ValidationError(
                        f"The column {data_type.name} is not of type {data_type.col_obj_series.dtype}")
            self.__is_valide = True
            return self.__is_valide
        except ValidationError as ve:
            logging.warning(ve.msg)
            return False

    def validate(self):
        for data_type in self._data_types:
            if data_type.str_type == 'datetime64[ns]':
                self[data_type.name] = pd.to_datetime(self[data_type.name],
                                                      format=data_type.col_obj_series.kwargs['format'])
            else:
                self[data_type.name] = self[data_type.name].astype(data_type.str_type)
        self.is_valid()

    def save(self, *args, **kwargs) -> int:
        if self.__is_valide:
            self.is_sql_decorator_missing()
            kwargs['name'] = self.sql.get('table')
            kwargs['con'] = self.sql.get('con')
            return self.to_sql(*args, **kwargs)
        else:
            raise ValidationError(f"You need to validate the dataframe before saving by calling the is_valid() method")

    @classmethod
    def read_csv(cls, *args, **kwargs) -> pd.DataFrame:
        return cls(pd.read_csv(*args, **kwargs))

    @classmethod
    def read_sql_query(cls, *args, **kwargs) -> pd.DataFrame:
        return cls(pd.read_sql_query(*args, **kwargs))

    def is_sql_decorator_missing(self) -> None:
        if self.sql is None:
            raise MissingDecorator("You have to decorate your class with models.sql")
        for key in self.sql.keys():
            if self.sql.get(key) is None:
                raise MissingArguments("Missing arguments on models.sql decorator")


@dataclass
class DataTypes:
    name: str
    str_type: str
    np_type: np.generic
    col_obj_series: pd.Series


class Data:
    def __init__(self, decorated_class):
        self.decorated_class = decorated_class

        self.decorated_inst = self.decorated_class()

        self.data_types: List[DataTypes] = [
            DataTypes(
                name=attr_key,
                str_type=attr_val.str_type,
                np_type=attr_val.np_type,
                col_obj_series=getattr(self.decorated_class, attr_key)
            )
            for attr_key, attr_val in self.decorated_class.__dict__.items()
            if not attr_key.startswith('__') and not attr_key.endswith('__')]

        self.df = DataFrame()
        self.df._data_types = self.data_types

    def __call__(self, *args, **kwargs) -> pd.DataFrame:

        if hasattr(self, 'sql'):
            self.df.sql = self.sql

        if kwargs.get('from_csv') is not None:
            return self._validate_from_csv_kwarg(kwargs=kwargs)
        if kwargs.get('from_sql_query') is not None:
            self.df.is_sql_decorator_missing()
            kwargs['con'] = self.df.sql.get('con')
            return self._validate_from_sql_query_kwarg(kwargs=kwargs)
        for data_type in self.data_types:
            self.df[data_type.name] = data_type.col_obj_series
        return self.df

    def _validate_from_csv_kwarg(self, kwargs: dict) -> pd.DataFrame:
        kwargs['filepath_or_buffer'] = kwargs.pop('from_csv')
        return self._validate(kwargs, self.df.read_csv)

    def _validate_from_sql_query_kwarg(self, kwargs: dict) -> pd.DataFrame:
        kwargs['sql'] = kwargs.pop('from_sql_query')
        return self._validate(kwargs, self.df.read_sql_query)

    def _validate(self, kwargs: dict, func):
        col_type = {}
        bool_validator = {}
        for index, data_type in enumerate(self.data_types):

            if data_type.str_type == 'datetime64[ns]':
                if 'parse_dates' in kwargs.keys():
                    kwargs['parse_dates'].append(data_type.name)
                else:
                    kwargs['parse_dates'] = [data_type.name]
                continue

            if data_type.str_type == 'bool' and data_type.col_obj_series.true_or_false is not None:
                bool_validator[data_type.name] = data_type.col_obj_series.true_or_false

            col_type[data_type.name] = data_type.np_type
        df = func(**kwargs)
        for col_name, bool_val_dict in bool_validator.items():
            df[col_name] = df[col_name].map(bool_val_dict)

        for col_name in df.columns:
            self.df[col_name] = df[col_name]

        return self.df


def sql(*args, **kwargs):
    def wrapper(func):
        func.__setattr__('sql', kwargs)
        return func
    return wrapper
