import datetime
import decimal
import json
from typing import Set

from redis_models.utils import exec_if_callable, RedisModelsException, validate_type, logger


def validate_model(model):
    from .core import RedisModel
    if model is None or not issubclass(model, RedisModel):
        raise RedisModelsException(f'{model.__name__} class is not RedisModel')


def get_ids_from_untyped_data(instances):
    from .core import RedisModel
    if isinstance(instances, RedisModel):
        return [instances.id, ]
    elif isinstance(instances, (list, tuple, set)) and isinstance(instances[0], RedisModel):
        return [i.id for i in instances]
    raise RedisModelsException(f"Can't get ids from {instances}")


class RedisField:

    def __init__(self, default=None, choices=None, null=True):
        default = default
        choices = validate_type(exec_if_callable(choices), dict)
        null = validate_type(exec_if_callable(null), bool)
        self.default = default
        self.value = None
        self.choices = choices
        self.null = null

    def _copy_params(self) -> Set[str]:
        return {'default', 'choices', 'null'}

    def copy(self):
        kwargs = {k: getattr(self, k) for k in self._copy_params()}
        return self.__class__(**kwargs)

    def check_value(self):
        if self.value is None:
            self.value = exec_if_callable(self.default)
        if self.value is None and not self.null:
            raise RedisModelsException('null is not allowed')
        elif self.value and self.choices and self.value not in self.choices.keys():
            raise RedisModelsException(
                f'{self.value} is not allowed. Allowed values: {", ".join(list(self.choices.keys()))}')
        return self.value

    def clean(self):
        self.value = self.check_value()
        return self.value

    def deserialize_value(self, value, ignore_deserialization_errors):
        if value is None and not self.null:
            logger.warning(f'{value} can not be deserialized like {self.__class__.__name__}, ignoring')
            if not ignore_deserialization_errors:
                raise RedisModelsException(f'{value} can not be deserialized like {self.__class__.__name__}')
        return value


class RedisString(RedisField):

    def clean(self):
        self.value = super().clean()
        if self.value is not None:
            self.value = f'{self.value}'
        return self.value

    def deserialize_value(self, value, ignore_deserialization_errors):
        value = super().deserialize_value(value, ignore_deserialization_errors)
        if value is not None:
            value = f'{value}'
        return value


class RedisNumber(RedisField):
    def clean(self):
        self.value = super().clean()
        return validate_type(self.value, (int, float))

    def deserialize_value(self, value, ignore_errors):
        value = super().deserialize_value(value, ignore_errors)
        if isinstance(value, str):
            value = float(value) if '.' in value else int(value)
        return validate_type(value, (int, float))


class RedisId(RedisNumber):
    def __init__(self, *args, **kwargs):
        kwargs['null'] = False
        super().__init__(*args, **kwargs)


class RedisBool(RedisNumber):
    def __init__(self, *args, **kwargs):
        kwargs['choices'] = {True: 'Yes', False: 'No'}
        super().__init__(*args, **kwargs)

    def clean(self):
        self.value = super().clean()
        if self.value is not None:
            self.value = int(validate_type(self.value, bool))
        return self.value

    def deserialize_value(self, value, ignore_errors):
        value = super().deserialize_value(value, ignore_errors)
        if value is not None:
            value = bool(validate_type(value, int))
        return value


class RedisDecimal(RedisField):

    def clean(self):
        self.value = super().clean()
        if isinstance(self.value,decimal.Decimal):
            self.value = float(self.value)
        return validate_type(self.value, (int, float, decimal.Decimal))

    def deserialize_value(self, value, ignore_deserialization_errors):
        value = super().deserialize_value(value, ignore_deserialization_errors)
        if value is not None:
            value = decimal.Decimal(value)
        return value


class RedisJson(RedisField):
    JSON_TYPES = (dict, list,)

    def __init__(self, *args, **kwargs):
        self.allowed_types = kwargs.pop('allowed_types', RedisJson.JSON_TYPES)
        super().__init__(*args, **kwargs)

    def clean(self):
        self.value = super().clean()
        if self.value is not None:
            validate_type(self.value, self.allowed_types)
            json_string = json.dumps(self.value)
            self.value = json_string
        return self.value

    def deserialize_value(self, value, ignore_deserialization_errors):
        value = super(RedisJson, self).deserialize_value(value, ignore_deserialization_errors)
        if value is not None:
            validate_type(value, str)
            value = json.loads(value)
            validate_type(value, self.allowed_types)
        return value


class RedisDict(RedisJson):
    def __init__(self, *args, **kwargs):
        kwargs['allowed_types'] = (dict,)
        super(RedisDict, self).__init__(*args, **kwargs)


class RedisList(RedisJson):
    def __init__(self, *args, **kwargs):
        kwargs['allowed_types'] = (list,)
        super(RedisList, self).__init__(*args, **kwargs)


class RedisDateTime(RedisField):

    def clean(self):
        self.value = super().clean()
        if self.value is not None:
            validate_type(self.value, datetime.datetime)
            string_datetime = self.value.replace(tzinfo=datetime.timezone.utc).strftime('%Y.%m.%d-%H:%M:%S+%Z')
            self.value = string_datetime
        return self.value

    def deserialize_value(self, value, ignore_deserialization_errors):
        value = super().deserialize_value(value, ignore_deserialization_errors)
        if value is not None:
            validate_type(value, str)
            value = datetime.datetime.strptime(value, '%Y.%m.%d-%H:%M:%S+%Z').replace(tzinfo=datetime.timezone.utc)
        return value


class RedisDate(RedisField):

    def clean(self):
        self.value = super().clean()
        if self.value is not None:
            validate_type(self.value, datetime.date)
            string_date = self.value.strftime('%Y.%m.%d')
            self.value = string_date
        return self.value

    def deserialize_value(self, value, ignore_deserialization_errors):
        value = super().deserialize_value(value, ignore_deserialization_errors)
        if value is not None:
            validate_type(value, str)
            value = datetime.datetime.strptime(value, '%Y.%m.%d').date()
        return value


class RedisForeignKey(RedisField):

    def __init__(self, model=None, *args, **kwargs):
        self.model = exec_if_callable(model)
        validate_model(self.model)
        args = [exec_if_callable(v) for v in args]
        kwargs = {k: exec_if_callable(v) for k, v in kwargs.items()}
        super().__init__(*args, **kwargs)

    def _copy_params(self) -> Set[str]:
        return super(RedisForeignKey, self)._copy_params().union({'model', })

    def clean(self):
        if self.value is not None:
            self.value = self.value.id
        self.value = super().clean()
        return self.value

    def deserialize_value(self, value, ignore_deserialization_errors):
        value = super(RedisForeignKey, self).deserialize_value(value, ignore_deserialization_errors)
        if value is None:
            return None
        validate_type(value, int)
        value = self.model.objects.query(id=value)
        if len(value) != 1:
            raise RedisModelsException(f'Found more than one {self.model.model_name} with id {value}')
        return value[0]


class RedisManyToMany(RedisList):
    def __init__(self, model: 'RedisModel' = None, *args, **kwargs):
        self.model = exec_if_callable(model)
        validate_model(self.model)
        args = list(map(exec_if_callable, *args)) if args else args
        super(RedisManyToMany, self).__init__(*args, **kwargs)

    def _copy_params(self) -> Set[str]:
        return super(RedisManyToMany, self)._copy_params().union({'model', })

    def clean(self):
        if self.value is not None:
            self.value = get_ids_from_untyped_data(self.value)
        self.value = super().clean()
        return self.value

    def deserialize_value(self, value, ignore_deserialization_errors):
        value = super(RedisManyToMany, self).deserialize_value(value, ignore_deserialization_errors)
        if value is not None:
            instances = self.model.objects.query(id__in=value)
            value = instances
        return value
