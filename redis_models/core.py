import asyncio
import datetime
import json
from typing import List, Type, Dict

import redis

from .fields import RedisField, get_ids_from_untyped_data, RedisId
from .utils import validate_type, exec_if_callable, RedisModelsException, logger, process_prefix


class RedisQuerySet:
    def __init__(self, model: Type['RedisModel'], results: List[Type['RedisModel']]):
        self.model = model
        self.results = results

    def order_by(self, field_name):
        reverse = False
        if field_name.startswith('-'):
            reverse = True
            field_name = field_name[1:]

        self.results = sorted(self.results, key=(lambda instance: instance[field_name]), reverse=reverse)
        return self

    def count(self):
        return len(self.results)

    def __len__(self):
        return len(self.results)

    def __iter__(self):
        return iter(self.results)

    def __getitem__(self, idx):
        return self.results[idx]

    def as_dict(self) -> Dict[int, Type['RedisModel']]:
        return {item.id: item for item in self.results}

    def as_list(self):
        return self.results

    def values(self, *fields):
        field_names = self.model.get_class_fields().keys()
        for field in fields:
            if field not in field_names:
                raise RedisModelsException(f'model {self.model.model_name} does not have a field {field}')
        return list(map(lambda i: {field: getattr(i, field) for field in fields}, self.results))


class BaseRedisManager:
    FILTER_METHODS = {
        'exact': lambda x, y: x == y,
        'iexact': lambda x, y: x.lower() == y.lower(),
        'contains': lambda x, y: x in y,
        'in': lambda x, y: x in y,
        'gt': lambda x, y: x > y,
        'gte': lambda x, y: x >= y,
        'lt': lambda x, y: x < y,
        'lte': lambda x, y: x <= y,
        'startswith': lambda x, y: x.startswith(y),
        'endswith': lambda x, y: x.endswith(y),
        'istartswith': lambda x, y: x.lower().startswith(y.lower()),
        'iendswith': lambda x, y: x.lower().endswith(y.lower()),
        'range': lambda x, y: x in range(y),
        'isnull': lambda x, y: (x is None) == y,
    }
    __registered_models = []
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(BaseRedisManager, cls).__new__(cls, *args, **kwargs)
            cls._instance._initialized = False
        return cls._instance

    def __init__(
            self,
            connection_pool=None,
            prefix='redis_test',
            ignore_deserialization_errors=True,
            use_keys=True,
            non_blocking=False,
    ):
        """Initialize a RedisManager manager.

        :param connection_pool:
        :param prefix: Prefix to use for storing data in redis.
        :param ignore_deserialization_errors:
        :param use_keys: Whether to use redis `keys` command or `scan` command.
        :param non_blocking: Async ORM methods
        """
        self.connection_pool = self._get_connection_pool(exec_if_callable(connection_pool))
        self.prefix = process_prefix(exec_if_callable(prefix))
        self.ignore_deserialization_errors = validate_type(exec_if_callable(ignore_deserialization_errors), bool)
        self.use_keys = validate_type(exec_if_callable(use_keys), bool)
        self.non_blocking = validate_type(exec_if_callable(non_blocking), bool)
        self._lock = False
        self.__lock_key = f'__lock__:{self.prefix}'
        self.release_lock()
        if self._initialized:
            return

        self._initialized = True

    @property
    def redis_instance(self):
        redis_instance = redis.Redis(connection_pool=self.connection_pool)
        return redis_instance

    def _get_connection_pool(self, connection_pool):
        if not isinstance(connection_pool, redis.ConnectionPool):
            logger.warning('No connection_pool provided, trying default config (redis://localhost:6389/0).')
            try:
                connection_pool = redis.ConnectionPool(
                    decode_responses=True,
                    host='localhost',
                    port=6379,
                    db=0,
                )
                self.connection_pool = connection_pool
            except Exception as ex:
                raise RedisModelsException(
                    'Default config (localhost:6379, db=0) failed, please provide connection_pool to BaseRedisManager')
        connection_pool.connection_kwargs['decode_responses'] = True
        self.connection_pool = connection_pool
        return self.connection_pool

    def register_models(self, models_list: List[Type['RedisModel']]):

        if not isinstance(models_list, list):
            models_list = [models_list, ]
        for model in models_list:
            if not issubclass(model, RedisModel):
                raise RedisModelsException(f'{model.__name__} class is not RedisModel')
            if model not in BaseRedisManager.__registered_models:
                BaseRedisManager.__registered_models.append(model)
                model.objects = RedisModelManager(self, model)

    def is_locked(self):
        return bool(int(self.redis_instance.get(self.__lock_key)))

    def release_lock(self):
        self.redis_instance.set(self.__lock_key, int(False))

    def wait_for_lock(self):
        while self.is_locked():
            pass
        self.redis_instance.set(self.__lock_key, int(True))

    # Fetch items from redis

    def query(self, model, **filters) -> RedisQuerySet:
        instances = self._get_model_instances(model, filters)
        return RedisQuerySet(model, instances)

    def _get_model_instances(self, model, filters):

        keys = self.get_keys(f'{self.prefix}:{model.__name__}:*')
        values = self.redis_instance.mget(keys)
        raw_instances = dict(zip(keys, values))

        res = list()
        for instance_key, fields_json in raw_instances.items():
            prefix, model_name, instance_id = instance_key.split(':')
            model = self._get_registered_model_by_name(model_name)
            _ = int(instance_id)
            fields_dict = json.loads(fields_json)
            # Deserialize fields for instance, and perform filter.
            tmp_instance = dict()
            allowed = True
            for field_name, raw_value in fields_dict.items():
                value = self._deserialize_instance_field(model, field_name, raw_value)
                allowed = allowed and self._filter_field_name(field_name, value, filters)
                tmp_instance[field_name] = value
                if not allowed:
                    break
            if allowed:
                res.append(model(**tmp_instance))
        return res

    def _deserialize_instance_field(self, model, field_name, raw_value):
        value = raw_value
        redis_field_val = getattr(model, field_name)
        if issubclass(redis_field_val.__class__, RedisField):
            value = redis_field_val.deserialize_value(raw_value, self.ignore_deserialization_errors)
        return value

    def _get_registered_model_by_name(self, model_name):
        matching = filter(lambda mod: mod.__name__ == model_name, BaseRedisManager.__registered_models)
        model = next(matching, None)
        if model is None:
            logger.warning(f'{model_name} not found in registered models, ignoring')
            if not self.ignore_deserialization_errors:
                raise RedisModelsException(f'{model_name} not found in registered models')
            model = model_name
        return model

    def _split_filtering(self, filter_param):
        filter_field_name, filter_type = filter_param, 'exact'

        filter_param_split = filter_param.split('__')
        if filter_param_split[-1] not in self.FILTER_METHODS.keys():
            return filter_param_split, filter_type
        fields_to_filter = filter_param_split[:-1]
        filter_type = filter_param_split[-1]
        return fields_to_filter, filter_type

    def _filter_value(self, value, filter_type, filter_by):
        if filter_type not in self.FILTER_METHODS:
            raise RedisModelsException(f'Filter {filter_type} not supported')
        if isinstance(filter_by, datetime.datetime):
            filter_by = filter_by.replace(tzinfo=datetime.timezone.utc)
        return self.FILTER_METHODS[filter_type](value, filter_by)

    def _filter_field_name(self, field_name, value, raw_filters):
        allowed_list = [True]
        for filter_param in raw_filters.keys():
            filter_by = raw_filters[filter_param]
            fields_to_filter, filter_type = self._split_filtering(filter_param)
            if field_name == fields_to_filter[0]:
                fields_to_filter = fields_to_filter[1:]
                allowed_list.append(self._filter(value, fields_to_filter, filter_type, filter_by))
        allowed = all(allowed_list)
        return allowed

    def _filter(self, value, nested_field_names, filter_type, filter_by):
        for field_name in nested_field_names:
            if value is None:
                continue
            if not isinstance(value, RedisModel) or not hasattr(value, field_name):
                raise RedisModelsException(f'{value.__class__.__name__} has no field {field_name}')
            value = value[field_name]
        if isinstance(value, datetime.datetime) and isinstance(filter_by, datetime.datetime):
            value = value.replace(tzinfo=datetime.timezone.utc)
            filter_by = filter_by.replace(tzinfo=datetime.timezone.utc)
        allowed = self._filter_value(value, filter_type, filter_by)
        return allowed

    def update(self, model, instances=None, **fields_to_update):
        if instances is not None and isinstance(instances, RedisModel):
            instances = [instances, ]
        if instances is not None and all([isinstance(item, model) for item in instances]):
            updated_instances = instances
        elif instances is not None:
            ids_to_update = get_ids_from_untyped_data(instances)
            updated_instances = list(self.query(model, id__in=ids_to_update))
        else:
            updated_instances = list(self.query(model, ))
        collected_data_to_update = {}
        for idx, instance in enumerate(updated_instances):
            instance_key = instance.instance_key()
            fields_to_write = self._update_serialize_fields(instance_key, model, fields_to_update)
            collected_data_to_update[instance_key] = json.dumps(fields_to_write)
            updated_instances[idx] = fields_to_write

        if self.non_blocking:
            asyncio.run(self._update_async(collected_data_to_update))
            return updated_instances
        else:
            self._update_sync(collected_data_to_update)
            return updated_instances

    def _update_serialize_fields(self, instance_key, model, fields_to_update):
        instance_data_json = self.redis_instance.get(instance_key)
        instance_data = json.loads(instance_data_json)
        serialized_data = {}
        for field_name, field_data in instance_data.items():
            saved_field_instance = getattr(model, field_name)
            if field_name in fields_to_update.keys():
                saved_field_instance.value = fields_to_update[field_name]
                cleaned_value = saved_field_instance.clean()
            else:
                cleaned_value = field_data
            serialized_data[field_name] = cleaned_value
        return serialized_data

    def _update_sync(self, data_to_update):
        self.redis_instance.mset(data_to_update)

    async def _update_async(self, data_to_update):
        self._update_sync(data_to_update)

    def delete(self, model, instances=None):
        model_name = model.__name__
        if self.non_blocking:
            asyncio.run(self._delete_async(model_name, instances))
        else:
            self._delete_sync(model_name, instances)

    def _delete_sync(self, model_name, instances):
        keys = []
        if instances is None:
            keys = self.get_keys(f'{self.prefix}:{model_name}:*')
        else:
            ids_to_delete = get_ids_from_untyped_data(instances)
            keys.extend([f'{self.prefix}:{model_name}:{instance_id}' for instance_id in ids_to_delete])
        if keys:
            self.redis_instance.delete(*keys)

    async def _delete_async(self, model_name, instances):
        self._delete_sync(model_name, instances)

    def create(self, model, **params):
        model_attrs = model.get_class_fields()
        allowed_params = {
            param_name: params[param_name]
            for param_name in params.keys()
            if param_name in model_attrs.keys()
        }
        redis_instance = model(**allowed_params).save()
        return redis_instance

    def get_keys(self, pattern):
        if self.use_keys:
            return self.redis_instance.keys(pattern)
        else:
            return list(self.redis_instance.scan_iter(pattern))


class RedisModelManager:
    def __init__(self, base_mgr: BaseRedisManager, model: 'RedisModel'):
        self.base_mgr = base_mgr
        self.model = model
        self.max_model_id = 0

    def query(self, **filters) -> RedisQuerySet:
        return self.base_mgr.query(self.model, **filters)

    def create(self, **params):
        return self.base_mgr.create(self.model, **params)

    def delete(self, instances=None):
        return self.base_mgr.delete(self.model, instances)

    def update(self, instances=None, **fields_to_update):
        return self.base_mgr.update(self.model, instances, **fields_to_update)

    def next_id(self, model):
        self.base_mgr.wait_for_lock()
        stored_max_id = self.base_mgr.redis_instance.get(model.max_id_key())
        new_id = int(stored_max_id or 0) + 1
        self.base_mgr.redis_instance.set(model.max_id_key(), new_id)
        self.base_mgr.release_lock()
        return new_id

    @property
    def prefix(self):
        return self.base_mgr.prefix

    @property
    def non_blocking(self):
        return self.base_mgr.non_blocking

    @property
    def redis_instance(self):
        return self.base_mgr.redis_instance


class RedisModelWatcher(type):
    base_manager: 'BaseRedisManager' = None

    def __init__(cls, name, bases, clsdict):
        if len(cls.mro()) > 2:
            print("was subclassed by " + name)
            cls.model_name = name
            if RedisModelWatcher.base_manager is None:
                RedisModelWatcher.base_manager = BaseRedisManager()
            RedisModelWatcher.base_manager.register_models([cls, ])
        super(RedisModelWatcher, cls).__init__(name, bases, clsdict)


class RedisModel(metaclass=RedisModelWatcher):
    id = RedisId()
    model_name: str = None
    objects: RedisModelManager = None

    def __init__(self, **kwargs):
        self.__fields__ = {}
        self.__model_data__ = {
            'meta': {},
        }
        self.__fields__ = {k: v.copy() for k, v in self.__class__.get_class_fields().items()}
        self._fill_fields_values(kwargs)

    @classmethod
    def get_class_fields(cls):
        field_names = dir(cls)
        fields = {}
        for field_name in field_names:
            field_value = getattr(cls, field_name)
            if isinstance(field_value, RedisField):
                fields[field_name] = field_value
        return fields

    def __setattr__(self, key, value):
        super(RedisModel, self).__setattr__(key, value)
        if key != '__fields__' and key in self.__fields__:
            self.__fields__[key].value = value

    def __contains__(self, field_name):
        return field_name in self.__fields__

    def __getitem__(self, field_name):
        return getattr(self, field_name)

    def __eq__(self, other):
        fields = self.__fields__
        return all((getattr(self, k) == getattr(other, k)) for k in fields)

    def _fill_fields_values(self, values_dict):
        fields = self.__fields__
        for name, value in values_dict.items():
            if name not in fields.keys():
                raise RedisModelsException(f'{self.__class__.__name__} has no field {name}')
            setattr(self, name, value)

    def save(self):
        instance_key, fields_dict, deserialized_fields = self._serialize_data()
        if self.objects.non_blocking:
            asyncio.run(self._async_save(instance_key, fields_dict, deserialized_fields))
        else:
            self._save(instance_key, fields_dict, deserialized_fields)
        return self

    def _serialize_data(self):
        fields = self.__fields__
        if self.id is None or fields['id'].value is None:
            self.id = self.objects.next_id(self.__class__)
        instance_key = self.instance_key()
        deserialized_fields = {}
        cleaned_fields = {}
        for field_name, field in fields.items():
            try:
                cleaned_value = field.clean()
                cleaned_fields[field_name] = cleaned_value
                deserialized_value = self.objects.base_mgr._deserialize_instance_field(self, field_name, cleaned_value)
                deserialized_fields[field_name] = deserialized_value
            except RedisModelsException as ex:
                raise RedisModelsException(f'{ex} ({self.model_name} -> {field_name})')
        return instance_key, cleaned_fields, deserialized_fields

    def _save(self, instance_key, fields_dict, deserialized_fields):
        prefix, model_name, instance_id = instance_key.split(':')
        int(instance_id)
        fields_data = json.dumps(fields_dict)
        self.objects.redis_instance.set(instance_key, fields_data)
        for k, v in deserialized_fields.items():
            setattr(self, k, v)

    async def _async_save(self, instance_key, fields_dict, deserialized_fields):
        self._save(instance_key, fields_dict, deserialized_fields)

    def instance_key(self):
        curr_id = self.__fields__['id'].value
        return f'{self.objects.prefix}:{self.model_name}:{curr_id}'

    @classmethod
    def max_id_key(cls):
        return f'max_id:{cls.objects.prefix}:{cls.model_name}'
