import logging
from inspect import isfunction, isbuiltin

logger = logging.getLogger("redis_models")


class RedisModelsException(Exception):
    pass


def process_prefix(prefix) -> str:
    """Return prefix that models can use"""
    if not type(prefix) == str:
        logger.warning(f'Prefix {prefix} is type of {type(prefix)} not allowed. using default prefix "redis_test"')
        return "redis_test"
    if ':' in prefix:
        logger.warning(f'Prefix can not contain colon (:), replacing with spaces')
        return prefix.replace(':', '')
    return prefix


def validate_type(value, allowed_types):
    if not value or isinstance(value, allowed_types):
        return value
    if isinstance(allowed_types, tuple):
        allowed_types_text = ", ".join([str(allowed_type.__name__) for allowed_type in allowed_types])
    else:
        allowed_types_text = str(allowed_types)
    raise Exception(f'{value} has type: {value.__class__.__name__}. Allowed only: {allowed_types_text}')

def exec_if_callable(value):
    return value() if isfunction(value) or isbuiltin(value) else value
