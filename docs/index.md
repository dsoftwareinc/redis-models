# redis-models

Redis ORM library that gives redis easy-to-use objects with fields and speeds a development up,
inspired by Django ORM.

## Install

Install using pip:

```shell
pip install redis-models
```

## Configure

You can configure the way redis-models work using the `BaseRedisManager` class.

```python
BaseRedisManager(
    # redis.ConnectionPool - will try to connect to localhost:6389/0 if none is provided
    connection_pool=None,

    # Default prefix for all keys stored in redis.
    prefix='redis_test',

    # Should deserialization errors raise an exception?
    ignore_deserialization_errors=True,

    # Whether KEYS or SCAN should be used for getting all instances matching a pattern from redis.
    # When the database size is relatively small, KEYS is significantly faster, however, when
    # the database is getting bigger, SCAN has better performance since it does not require to load all
    # keys at once
    use_keys=True,

    # Perform actions in a non-blocking manner (do not wait for ack from redis).
    non_blocking=False,
)
```

## Usage

This package has Django-like architecture for `RedisModel` classes.
A `RedisModel` has a `RedisModelManager` class which can be used to create, query, update, delete
existing instances in redis.

## Supported field types

`RedisField` - base class for nesting all fields, support default value, set of choices,
and whether field is nullable (empty).

- `RedisString` - string
- `RedisNumber` - int or float
- `RedisId` - instances IDs
- `RedisBool` - bool
- `RedisDecimal` - working accurately with numbers via decimal
- `RedisJson` - for data, that can be used with `json.loads`/`json.dumps`.
- `RedisList` - list
- `RedisDict` - dict
- `RedisDateTime` - for work with date and time, via python datetime.datetime
- `RedisDate` - for work with date, via python datetime.data
- `RedisForeignKey` - for link to other instance
- `RedisManyToMany` - for links to other instances

## Filtering

Using your model manager, you can query and filter instances of the model.
For example, for the model:

```python
from redis_models import (RedisModel, RedisString, RedisDateTime, )


class BotSession(RedisModel):
    session_token = RedisString(default='session_token_value')
    created = RedisDateTime(default=datetime.datetime.now)
```

It is possible to query:

```python
BotSession.objects.query(session_token='session_token_value')
# Will return the instances with session_token='session_token_value'

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
BotSession.objects.query(created__gte=yesterday)
# Will return all instances that have created >= yesterday.
```

Supported filtering:

- `exact` - equality
- `iexact` - case-independent equality
- `contains` - is filter string in the value string
- `icontains` - is filter string case-independent in the value string
- `in` - is value in the provided list
- `gt` - is value greater
- `gte` - is value greater or equals
- `lt` - is value less
- `lte` - is value less or equals
- `startswith` - is string starts with
- `istartswith` - is string case-independent starts with
- `endswith` - is string ends with
- `iendswith` - is string case-independent ends wth
- `range` - is value in provided range
- `isnull` - is value in ["null", None]
