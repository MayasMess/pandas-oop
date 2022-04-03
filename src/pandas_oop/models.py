from dataclasses import dataclass
from typing import List
import logging

import pandas as pd
from pangres import upsert
import numpy as np
import typing

from sqlalchemy import create_engine

from ._decorators import _decorate_all_methods, _return_custom_df_on_call, sql
from .custom_exceptions import ValidationError, MissingDecorator, MissingArguments, MissingUniqueField


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


@dataclass
class DataFrameState:
    data_types: typing.Optional[list] = None
    index_list: typing.Optional[list] = None
    sql: typing.Optional[dict] = None
    class_name: typing.Optional = None
    decorated_class: typing.Optional = None


@_decorate_all_methods(_return_custom_df_on_call)
class DataFrame(pd.DataFrame):

    def __init__(self, from_df: pd.DataFrame = None, from_csv=None, from_sql_query=None):
        super().__init__()
        self._dataframe_state = DataFrameState()
        self.__is_valide = False

    def is_valid(self) -> bool:
        if self._dataframe_state.data_types is None:
            self.__is_valide = True
            return self.__is_valide
        try:
            for data_type in self._dataframe_state.data_types:
                if data_type.col_obj_series.dtype != self[data_type.name].dtype:
                    raise ValidationError(
                        f"The column {data_type.name} is not of type {data_type.col_obj_series.dtype}")
            self.__is_valide = True
            return self.__is_valide
        except ValidationError as ve:
            logging.warning(ve.msg)
            return False

    def validate(self, from_class=None) -> 'DataFrame':
        if from_class is not None:
            self._dataframe_state = from_class().dataframe_state
        for data_type in self._dataframe_state.data_types:
            if data_type.str_type == 'datetime64[ns]':
                self[data_type.name] = pd.to_datetime(self[data_type.name],
                                                      format=data_type.col_obj_series.kwargs['format'])
            else:
                self[data_type.name] = self[data_type.name].astype(data_type.str_type)
        self.is_valid()
        return self

    def save(self, *args, **kwargs) -> int:
        self.is_valid()
        self.is_sql_decorator_missing()
        if kwargs.get("if_row_exists") is not None:
            if self._dataframe_state.index_list is None or not self._dataframe_state.index_list:
                raise MissingUniqueField(
                    'Your class must contain one or multiple fields with the parameter "unique=True"')
            return upsert(df=self.set_index(self._dataframe_state.index_list),
                          con=self.sql_engine,
                          table_name=self.sql_table, **kwargs)
        return self.normal_save(*args, **kwargs)

    def normal_save(self, *args, **kwargs) -> int:
        kwargs['name'] = self.sql_table
        with self.sql_engine.connect() as con:
            kwargs['con'] = con
            if kwargs.get('if_exists') is None:
                kwargs['if_exists'] = 'append'
            elif kwargs.get('if_exists') == 'replace':
                raise ValueError(f'got an unexpected value "if_exists=replace". Please use a normal pandas dataframe '
                                 f'to access this functionality')
            if kwargs.get('index') is None:
                kwargs['index'] = False
            elif kwargs.get('index') is True:
                return self.set_index(self._dataframe_state.index_list).to_sql(*args, **kwargs)
            return self.to_sql(*args, **kwargs)

    def is_sql_decorator_missing(self) -> None:
        if self._dataframe_state.sql is None:
            raise MissingDecorator("You have to decorate your class with models.sql")
        for key in self._dataframe_state.sql.keys():
            if self._dataframe_state.sql.get(key) is None:
                raise MissingArguments("Missing arguments on models.sql decorator")

    @classmethod
    def read_csv(cls, *args, **kwargs) -> pd.DataFrame:
        return cls(pd.read_csv(*args, **kwargs))

    @classmethod
    def read_sql_query(cls, *args, **kwargs) -> pd.DataFrame:
        return cls(pd.read_sql_query(*args, **kwargs))

    @classmethod
    def generic_overrider(cls, df: pd.DataFrame, ct_df: 'DataFrame') -> 'DataFrame':
        new_custom_df = cls()
        new_custom_df._dataframe_state = ct_df.dataframe_state
        for col_name in df.columns:
            new_custom_df[col_name] = df[col_name]
        return new_custom_df

    @property
    def dataframe_state(self):
        return self._dataframe_state

    @property
    def sql_engine(self):
        return self._dataframe_state.sql.get('con').sql_engine

    @property
    def sql_table(self):
        return self._dataframe_state.sql.get('table')

    def __str__(self):
        return self._dataframe_state.class_name


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
        self.df: typing.Optional[DataFrame] = None
        self.index_list: typing.Optional[list] = None

        self.data_types: List[DataTypes] = [
            DataTypes(
                name=attr_key,
                str_type=attr_val.str_type,
                np_type=attr_val.np_type,
                col_obj_series=getattr(self.decorated_class, attr_key)
            )
            for attr_key, attr_val in self.decorated_class.__dict__.items()
            if not attr_key.startswith('__') and not attr_key.endswith('__')]

    def __call__(self, *args, **kwargs) -> pd.DataFrame:

        self.init_new_custom_df()

        if kwargs.get('from_df') is not None:
            return self._validate_kwargs(kwargs=kwargs)
        if kwargs.get('from_csv') is not None:
            return self._validate_from_csv_kwarg(kwargs=kwargs)
        if kwargs.get('from_sql_query') is not None:
            self.df.is_sql_decorator_missing()
            with self.df.sql_engine.connect() as con:
                kwargs['con'] = con
                return self._validate_from_sql_query_kwarg(kwargs=kwargs)
        for data_type in self.data_types:
            self.df[data_type.name] = data_type.col_obj_series
        return self.df

    def _validate_from_csv_kwarg(self, kwargs: dict) -> DataFrame:
        kwargs['filepath_or_buffer'] = kwargs.pop('from_csv')
        return self._validate_kwargs(kwargs=kwargs, func=pd.read_csv)

    def _validate_from_sql_query_kwarg(self, kwargs: dict) -> DataFrame:
        kwargs['sql'] = kwargs.pop('from_sql_query')
        return self._validate_kwargs(kwargs=kwargs, func=pd.read_sql_query)

    def _validate_kwargs(self, kwargs: dict, func=None):
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
        if kwargs.get('from_df') is not None:
            df = kwargs.get('from_df')
        else:
            df = func(**kwargs)
        for col_name, bool_val_dict in bool_validator.items():
            df[col_name] = df[col_name].map(bool_val_dict)

        for col_name in df.columns:
            self.df[col_name] = df[col_name]

        return self.df

    def validate(self):
        self.init_new_custom_df()

    def init_new_custom_df(self):
        self.df = DataFrame()
        self.df.dataframe_state.decorated_class = self.decorated_class
        self.df.dataframe_state.class_name = self.decorated_class.__name__
        self.df.dataframe_state.data_types = self.data_types
        self.index_list = [data_type.name
                           for data_type in self.data_types
                           if data_type.col_obj_series.kwargs.get('unique') is True]
        self.df.dataframe_state.index_list = self.index_list
        if hasattr(self, 'sql'):
            self.df.dataframe_state.sql = self.sql


class Connection:
    def __init__(self, con_string):
        self.sql_engine = create_engine(con_string)


_trust = sql
