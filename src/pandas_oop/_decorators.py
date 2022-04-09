from functools import wraps
from pandas.core.generic import NDFrame
from pandas.core.frame import DataFrame

from sqlalchemy import Column, Integer

from . import Base


# this methods will return a pandas_oop.models.DataFrame
METHODS_TO_OVERRIDE = [
    'isnull',
    'head',
    'abs',
    'merge',
]


def _decorate_all_methods(method_decorator):
    def decorator(cls):
        _classes = [DataFrame, NDFrame]
        method_to_override = {}
        for _class in _classes:
            for name, obj in vars(_class).items():
                if callable(obj) and name in METHODS_TO_OVERRIDE:
                    method_to_override[name] = obj.__annotations__
                    setattr(cls, name, method_decorator(obj, cls))
        return cls
    return decorator


def _return_custom_df_on_call(func, cls=None):
    @wraps(func)
    def wrapper(*args, **kwargs):
        res = func(*args, **kwargs)
        return cls.generic_overrider(res, args[0])
    return wrapper


def init_sqlalchemy_class(func):
    # Init sqlalchemy class. (this is used for migration detection)
    attr_sqlalchemy_dict = {data_type.name: data_type.col_obj_series.sqlalchemy_column
                            for data_type in func.data_types}
    attr_sqlalchemy_dict['__tablename__'] = func.sql.get('table')
    func.index_list = [data_type.name
                       for data_type in func.data_types
                       if data_type.col_obj_series.kwargs.get('unique') is True]
    if not func.index_list:
        attr_sqlalchemy_dict['id'] = Column(Integer, primary_key=True)
    func.sqlalchemy_class = type(func.decorated_class.__name__,
                                 (Base,),
                                 attr_sqlalchemy_dict)
    return func


def sql(**kwargs):
    """
    Sql Decorator => just used to get arguments and init the sqlalchemy class to enable db migrations
    """
    def wrapper(func):
        func.__setattr__('sql', kwargs)
        func = init_sqlalchemy_class(func=func)
        return func
    return wrapper
