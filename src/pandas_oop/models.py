from dataclasses import dataclass
from typing import List
import logging

import pandas as pd
from pandas._typing import NDFrameT
from pandas.io.parsers.readers import TextFileReader
from pangres import upsert
import numpy as np
import typing

from sqlalchemy import create_engine

from ._decorators import _decorate_all_methods, _return_custom_df_on_call, sql
from .custom_exceptions import ValidationError, MissingDecorator, MissingArguments, MissingUniqueField
from . import Base


@dataclass
class DataFrameState:
    data_types: typing.Optional[list] = None
    index_list: typing.Optional[list] = None
    sql: typing.Optional[dict] = None
    class_name: typing.Optional = None
    decorated_class: typing.Optional = None
    sqlalchemy_class: Base = None


@_decorate_all_methods(_return_custom_df_on_call)
class DataFrame(pd.DataFrame):

    def __init__(self, from_df: pd.DataFrame = None, from_csv=None, from_sql_query=None, from_iterator=None, chunksize=None):
        super().__init__()
        self._dataframe_state = DataFrameState()
        self.__is_valide = False

    def is_valid(self) -> bool:
        if self._dataframe_state.data_types is None:
            self.__is_valide = True
            return self.__is_valide
        try:
            for data_type in self._dataframe_state.data_types:
                if data_type.base_type not in self[data_type.name].dtype.name:
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
                                                      format=data_type.col_obj_series.kwargs.get('format'))
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
                raise TypeError(f'got an unexpected value "if_exists=replace". Please use a normal pandas dataframe '
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

    def _take_with_is_copy(self, indices, axis=0) -> NDFrameT:
        """
        Internal version of the `take` method that sets the `_is_copy`
        attribute to keep track of the parent dataframe (using in indexing
        for the SettingWithCopyWarning).

        See the docstring of `take` for full explanation of the parameters.
        """
        result = self.generic_overrider(self.take(indices=indices, axis=axis), self)
        # Maybe set copy if we didn't actually change the index.
        if not result._get_axis(axis).equals(self._get_axis(axis)):
            result._set_is_copy(self)
        return result

    def _slice(self, slobj: slice, axis=0) -> NDFrameT:
        """
        Construct a slice of this container.

        Slicing with this method is *always* positional.
        """
        assert isinstance(slobj, slice), type(slobj)
        axis = self._get_block_manager_axis(axis)
        result = self._constructor(self._mgr.get_slice(slobj, axis=axis))
        result = result.__finalize__(self)

        # this could be a view
        # but only in a single-dtyped view sliceable case
        is_copy = axis != 0 or result._is_view
        result._set_is_copy(self, copy=is_copy)
        return self.generic_overrider(result, self)

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
    base_type: str
    str_type: str
    np_type: np.generic
    col_obj_series: pd.Series
    target_name: str


class Data:
    def __init__(self, decorated_class):
        """
        This init function is called in the class definition
        """
        self.decorated_class = decorated_class
        self.decorated_inst = self.decorated_class()
        self.df: typing.Optional[DataFrame] = None
        self.index_list: typing.Optional[list] = None
        self.sqlalchemy_class = None
        self.data_types: List[DataTypes] = [
            DataTypes(
                name=attr_key,
                base_type=attr_val.base_type,
                str_type=attr_val.str_type,
                np_type=attr_val.np_type,
                col_obj_series=getattr(self.decorated_class, attr_key),
                target_name=attr_val.kwargs.get('target_name') if attr_val.kwargs.get('target_name') is not None else attr_key
            )
            for attr_key, attr_val in self.decorated_class.__dict__.items()
            if not attr_key.startswith('__') and not attr_key.endswith('__')]

    """
    Between them is called the sql decorator in the _decorators.py file
    """
    def __call__(self, *args, **kwargs) -> DataFrame:
        """
        This call function is called in the class instantiation
        """
        self.init_new_custom_df()

        if kwargs.get('from_df') is not None:
            return self._validate_kwargs(**kwargs)
        if kwargs.get('from_csv') is not None:
            return self._validate_from_csv_kwarg(**kwargs)
        if kwargs.get('from_iterator') is not None:
            return self._validate_from_iterator_kwarg(**kwargs)
        if kwargs.get('from_sql_query') is not None:
            self.df.is_sql_decorator_missing()
            with self.df.sql_engine.connect() as con:
                kwargs['con'] = con
                return self._validate_from_sql_query_kwarg(**kwargs)
        for data_type in self.data_types:
            self.df[data_type.name] = data_type.col_obj_series
        return self.df

    def _validate_from_csv_kwarg(self, **kwargs) -> DataFrame:
        kwargs['filepath_or_buffer'] = kwargs.pop('from_csv')
        return self._validate_kwargs(func=pd.read_csv, **kwargs)

    def _validate_from_sql_query_kwarg(self, **kwargs) -> DataFrame:
        kwargs['sql'] = kwargs.pop('from_sql_query')
        return self._validate_kwargs(func=pd.read_sql_query, **kwargs)

    def _validate_from_iterator_kwarg(self, **kwargs) -> DataFrame:
        data = []
        for row in kwargs.get('from_iterator')():
            data.append(row)
        kwargs['columns'] = [data_type.name for data_type in self.data_types]
        kwargs['data'] = data
        kwargs.pop('from_iterator')
        return self._validate_kwargs(func=self.create_df_from_data_and_columns, **kwargs)

    def _validate_kwargs(self, func=None, **kwargs):
        col_type = {}
        bool_validator = {}
        for index, data_type in enumerate(self.data_types):

            if data_type.str_type == 'datetime64[ns]':
                if 'parse_dates' in kwargs.keys():
                    kwargs['parse_dates'].append(data_type.target_name)
                else:
                    kwargs['parse_dates'] = [data_type.target_name]
                continue

            if data_type.str_type == 'bool' and data_type.col_obj_series.true_or_false is not None:
                bool_validator[data_type.name] = data_type.col_obj_series.true_or_false

            col_type[data_type.name] = data_type.np_type
        if kwargs.get('from_df') is not None:
            df = kwargs.get('from_df')
        else:
            df = func(**kwargs)
        if isinstance(df, TextFileReader):
            return self.df_generator(df, bool_validator)
        self.build_custom_df(df, bool_validator)
        return self.df

    def df_generator(self, df, bool_validator):
        for chunk in df:
            self.init_new_custom_df()
            self.build_custom_df(chunk, bool_validator)
            yield self.df

    def build_custom_df(self, df, bool_validator):
        # Convert bool values
        for col_name, bool_val_dict in bool_validator.items():
            df[col_name] = df[col_name].map(bool_val_dict)

        for data_type in self.data_types:
            self.df[data_type.name] = df[data_type.target_name]

    def init_new_custom_df(self):
        self.df = DataFrame()
        self.df.dataframe_state.decorated_class = self.decorated_class
        self.df.dataframe_state.class_name = self.decorated_class.__name__
        self.df.dataframe_state.data_types = self.data_types
        self.df.dataframe_state.index_list = self.index_list
        if hasattr(self, 'sql'):
            self.df.dataframe_state.sql = self.sql
            self.df.dataframe_state.sql['table'] = self.sql.get('table')
            self.df.dataframe_state.sqlalchemy_class = self.sqlalchemy_class

    @staticmethod
    def create_df_from_data_and_columns(**kwargs) -> pd.DataFrame:
        return pd.DataFrame(data=kwargs.get('data'), columns=kwargs.get('columns'))


class Connection:
    def __init__(self, con_string):
        self.sql_engine = create_engine(con_string)


_trust = sql
