from copy import copy

import pandas as pd
import numpy as np
from sqlalchemy import Column, Text, Integer, Float, Date, Boolean


class BaseColumn(pd.Series):
    def __init__(self, base_type, dtype, np_type, **kwargs):
        super().__init__(dtype=dtype)
        self.str_type = dtype
        self.np_type = np_type
        self.base_type = base_type
        self.kwargs = copy(kwargs)

    @staticmethod
    def init_sqlalchemy_column(sqlalchemy_col_type, **kwargs):
        kwargs['primary_key'] = kwargs.pop('unique', None)
        kwargs.pop('target_name', None)
        return Column(sqlalchemy_col_type, **kwargs)


class StringColumn(BaseColumn):
    def __init__(self, **kwargs):
        super().__init__(base_type='object', dtype='object', np_type=np.str_, **kwargs)
        self.sqlalchemy_column = self.init_sqlalchemy_column(Text, **kwargs)


class IntegerColumn(BaseColumn):
    def __init__(self, **kwargs):
        super().__init__(base_type='int', dtype='int64', np_type=np.int64, **kwargs)
        self.sqlalchemy_column = self.init_sqlalchemy_column(Integer, **kwargs)


class FloatColumn(BaseColumn):
    def __init__(self, **kwargs):
        super().__init__(base_type='float', dtype='float64', np_type=np.float64, **kwargs)
        self.sqlalchemy_column = self.init_sqlalchemy_column(Float, **kwargs)


class DateColumn(BaseColumn):
    def __init__(self, **kwargs):
        super().__init__(base_type='datetime', dtype='datetime64[ns]', np_type=np.datetime64, **kwargs)
        if kwargs.get('format') is not None:
            del kwargs['format']
        self.sqlalchemy_column = self.init_sqlalchemy_column(Date, **kwargs)


class BoolColumn(BaseColumn):
    def __init__(self, **kwargs):
        super().__init__(base_type='bool', dtype='bool', np_type=np.bool_, **kwargs)
        self.true_or_false = None
        if kwargs.get('true') is not None and kwargs.get('false') is not None:
            self.true_or_false = {kwargs.get('true'): True, kwargs.get('false'): False}
            del kwargs['true']
            del kwargs['false']
        self.sqlalchemy_column = self.init_sqlalchemy_column(Boolean, **kwargs)
