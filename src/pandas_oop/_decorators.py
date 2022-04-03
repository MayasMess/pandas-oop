from functools import wraps
from pandas.core.generic import NDFrame


def _decorate_all_methods(method_decorator):
    def decorator(cls):
        _classes = [cls.__bases__[0], NDFrame]
        method_to_override = {}
        for _class in _classes:
            for name, obj in vars(_class).items():
                if callable(obj) and obj.__annotations__.get('return') is not None:
                    if 'DataFrame' in obj.__annotations__.get('return'):
                        # Todo: Getting max recursion error =>  or 'NDFrameT' in obj.__annotations__.get('return'):
                        method_to_override[name] = obj.__annotations__
                        setattr(cls, name, method_decorator(obj, cls=cls))
        return cls
    return decorator


def _return_custom_df_on_call(func, cls):
    @wraps(func)
    def wrapper(*args, **kwargs):
        res = func(*args, **kwargs)
        return cls.generic_overrider(res)
    return wrapper


def sql(**kwargs):
    """
    Sql Decorator => just used to get arguments
    """
    def wrapper(func):
        func.__setattr__('sql', kwargs)
        return func
    return wrapper
