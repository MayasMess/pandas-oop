from copy import copy

import pandas as pd
import numpy as np
from sqlalchemy import Column, Text, Integer, Float, Date, Boolean


class StringColumn(pd.Series):
    def __init__(self, **kwargs):
        super().__init__(dtype='object')
        self.str_type = "object"
        self.np_type = np.str_
        self.kwargs = copy(kwargs)
        kwargs['primary_key'] = kwargs.pop('unique', None)
        kwargs.pop('target_name', None)
        self.sqlalchemy_column = Column(Text, **kwargs)


class IntegerColumn(pd.Series):
    def __init__(self, **kwargs):
        super().__init__(dtype='int64')
        self.str_type = "int64"
        self.np_type = np.int64
        self.kwargs = copy(kwargs)
        kwargs['primary_key'] = kwargs.pop('unique', None)
        kwargs.pop('target_name', None)
        self.sqlalchemy_column = Column(Integer, **kwargs)


class FloatColumn(pd.Series):
    def __init__(self, **kwargs):
        super().__init__(dtype='float64')
        self.str_type = "float64"
        self.np_type = np.float64
        self.kwargs = copy(kwargs)
        kwargs['primary_key'] = kwargs.pop('unique', None)
        kwargs.pop('target_name', None)
        self.sqlalchemy_column = Column(Float, **kwargs)


class DateColumn(pd.Series):
    def __init__(self, **kwargs):
        super().__init__(dtype='datetime64[ns]')
        self.str_type = 'datetime64[ns]'
        self.np_type = np.datetime64
        self.kwargs = copy(kwargs)
        if kwargs.get('format') is not None:
            del kwargs['format']
        kwargs['primary_key'] = kwargs.pop('unique', None)
        kwargs.pop('target_name', None)
        self.sqlalchemy_column = Column(Date, **kwargs)


class BoolColumn(pd.Series):
    def __init__(self, **kwargs):
        super().__init__(dtype='bool')
        self.str_type = "bool"
        self.np_type = np.bool_
        self.true_or_false = None
        self.kwargs = copy(kwargs)
        if kwargs.get('true') is not None and kwargs.get('false') is not None:
            self.true_or_false = {kwargs.get('true'): True, kwargs.get('false'): False}
            del kwargs['true']
            del kwargs['false']
        kwargs['primary_key'] = kwargs.pop('unique', None)
        kwargs.pop('target_name', None)
        self.sqlalchemy_column = Column(Boolean, **kwargs)
