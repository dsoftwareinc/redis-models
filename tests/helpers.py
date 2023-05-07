import os
import time
import unittest
import uuid

import fakeredis

from redis_models.core import *
from redis_models.fields import *

REDIS_MANAGER_PARAMS = [
    ('non_blocking', 'ignore_deserialization_errors', 'use_keys'),
    [
        (False, False, False),
        (False, False, True),
        (False, True, False),
        (False, True, True),
        (True, False, False),
        (True, False, True),
        (True, True, False),
        (True, True, True),
    ]
]


class BotSession(RedisModel):
    session_token = RedisString(default=uuid.uuid4)
    created = RedisDateTime(default=datetime.datetime.now)


class TestModel(RedisModel):
    STATUS_CHOICES = {
        'in_work': 'Working',
        'completed': 'Completed',
        'failed_bot': 'Failed - bot',
        'failed_task_creator': 'Failed - task creator',
    }
    bot_session = RedisForeignKey(model=BotSession)
    task_id = RedisNumber(default=0, null=False)
    status = RedisString(default='in_work', choices=STATUS_CHOICES, null=False)
    account_checks_count = RedisNumber(default=0)
    target_date = RedisDate(default=datetime.datetime.today)
    created = RedisDateTime(default=datetime.datetime.now)
    dec_val = RedisDecimal(default=1.1)


class DictCheckModel(RedisModel):
    redis_dict = RedisDict()


class ListCheckModel(RedisModel):
    redis_list = RedisList()


class ForeignKeyCheckModel(RedisModel):
    task_challenge = RedisForeignKey(model=TestModel)


class NestedForeignKeyCheckModel(RedisModel):
    nested = RedisForeignKey(model=ForeignKeyCheckModel)


class ManyToManyCheckModel(RedisModel):
    task_challenges = RedisManyToMany(model=TestModel)


class ModelWithOverriddenSave(RedisModel):
    multiplied_max_field = RedisNumber()

    def save(self):
        new_value = 1
        all_instances = self.objects.query()
        if all_instances:
            max_value = max(map(lambda instance: instance.multiplied_max_field, all_instances))
            new_value = max_value * 2
        self.multiplied_max_field = new_value
        return super().save()


class SomeAbstractModel(RedisModel):
    abstract_field = RedisString(default='hello')


class InheritanceTestModel(SomeAbstractModel):
    some_field = RedisBool(default=True)


def get_connection_pool():
    host = os.getenv('REDIS_HOST', 'localhost')
    port = os.getenv('REDIS_PORT', '6379')
    if host is None:
        redis_conn = fakeredis.FakeRedis()
        return redis_conn.connection_pool
    else:
        return redis.ConnectionPool(host=host, port=port, db=0, decode_responses=True, )


def class_name_func(cls, num, params_dict):
    default_params = params_dict
    default_params_str = ','.join(f'{k}={v}' for k, v in default_params.items())
    return f"{cls.__name__}{num}({default_params_str})"


class AbstractRedisTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(AbstractRedisTest, cls).setUpClass()
        cls.connection_pool = get_connection_pool()

    def _setup_base_mgr(self, **kwargs):
        params = dict(
            ignore_deserialization_errors=getattr(self, 'ignore_deserialization_errors', True),
            use_keys=getattr(self, 'use_keys', True),
            non_blocking=getattr(self, 'non_blocking', False),
        )
        params.update(kwargs)
        self.redis_mgr = BaseRedisManager(
            prefix=self.__class__.__name__[:10],
            connection_pool=self.connection_pool,
            **params
        )

    def setUp(self) -> None:
        super(AbstractRedisTest, self).setUp()
        self._setup_base_mgr()
        self.start_time = time.time()

    def tearDown(self) -> None:
        run_time = int((time.time() - self.start_time) * 1000)
        logger.info(f'Test {self._testMethodName} finished in {run_time}ms')
        super(AbstractRedisTest, self).tearDown()
        self._cleanup()

    def _cleanup(self):
        redis_instance = self.redis_mgr.redis_instance
        keys_to_delete = {
            *list(redis_instance.keys(f'{self.redis_mgr.prefix}*')),
            *list(redis_instance.keys(f'*{self.redis_mgr.prefix}')),
            *list(redis_instance.keys(f'*{self.redis_mgr.prefix}*')),
            *list(redis_instance.keys(f'max_id:{self.redis_mgr.prefix}:*')),
        }
        if keys_to_delete:
            redis_instance.delete(*keys_to_delete)
