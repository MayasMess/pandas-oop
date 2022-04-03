from functools import wraps
from pandas.core.generic import NDFrame
from pandas.core.frame import DataFrame

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


def sql(**kwargs):
    """
    Sql Decorator => just used to get arguments
    """
    def wrapper(func):
        func.__setattr__('sql', kwargs)
        return func
    return wrapper
