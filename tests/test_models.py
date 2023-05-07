import datetime
import random
import time
import uuid

from parameterized import parameterized_class

from redis_models import RedisForeignKey, RedisManyToMany, RedisModelsException
from redis_models.core import BaseRedisManager
from tests.helpers import (AbstractRedisTest, TestModel, BotSession, ListCheckModel,
                           DictCheckModel, ForeignKeyCheckModel, ModelWithOverriddenSave,
                           InheritanceTestModel, ManyToManyCheckModel, REDIS_MANAGER_PARAMS,
                           class_name_func, NestedForeignKeyCheckModel)


@parameterized_class(*REDIS_MANAGER_PARAMS, class_name_func=class_name_func)
class RedisModelTest(AbstractRedisTest):
    def test_basic(self):
        tasks = []
        count = 5
        for i in range(count):
            task = TestModel(status='in_work', )
            task.save()
            tasks.append(task)
        as_list = list(TestModel.objects.query().order_by('id'))

        self.assertEquals(len(as_list), count)
        for i in range(count):
            self.assertEquals(tasks[i], as_list[i])

    def test_save_again(self):
        task = TestModel(status='in_work', )
        task.save()
        qs = TestModel.objects.query()
        self.assertEquals(1, qs.count())
        self.assertEquals(0, qs[0].account_checks_count)
        task.account_checks_count = 5
        task.save()
        qs = TestModel.objects.query()
        self.assertEquals(1, qs.count())
        self.assertEquals(5, qs[0].account_checks_count)

    def test_no_connection_pool(self):
        self.redis_mgr = BaseRedisManager(ignore_deserialization_errors=True, )
        item = TestModel(status='in_work', )
        item.save()
        item_list = TestModel.objects.query()
        self.assertEquals(len(item_list), 1)

    def test_bad_choice_value(self):
        item = TestModel(status='bad-choice', )
        with self.assertRaises(RedisModelsException):
            item.save()

    def test_order_test(self):
        for i in range(3):
            TestModel().save()
        order_asc = TestModel.objects.query().order_by('id')
        order_desc = TestModel.objects.query().order_by('-id')
        self.assertEquals(len(order_desc), len(order_asc))
        ids_asc = list(map(lambda x: x['id'], order_asc))
        self.assertTrue(all(ids_asc[i] <= ids_asc[i + 1] for i in range(len(ids_asc) - 1)))
        ids_desc = list(map(lambda x: x['id'], order_desc))
        self.assertTrue(all(ids_desc[i] >= ids_desc[i + 1] for i in range(len(ids_desc) - 1)))

    def test_filter(self):
        same_tokens_count = 2
        random_tokens_count = 8
        same_token = uuid.uuid4().hex
        random_tokens = [uuid.uuid4().hex for _ in range(random_tokens_count)]
        for i in range(same_tokens_count):
            BotSession(session_token=same_token).save()
        for random_token in random_tokens:
            BotSession(session_token=random_token).save()
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        item_list_with_same_token = BotSession.objects.query(
            session_token=same_token, created__gte=yesterday)
        self.assertEquals(len(item_list_with_same_token), same_tokens_count)

    def test_bad_filter(self):
        BotSession(session_token=uuid.uuid4().hex).save()
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        with self.assertRaises(RedisModelsException):
            BotSession.objects.query(created__bad=yesterday)

    def test_update_some(self):
        item = BotSession(session_token='123').save()
        item_id = item['id']
        BotSession.objects.update(item, session_token='234')
        items_filtered = BotSession.objects.query(id=item_id)
        self.assertEquals(len(items_filtered), 1)
        self.assertIn('session_token', items_filtered[0])
        self.assertEquals(items_filtered[0]['session_token'], '234')

    def test_update_all(self):
        item = BotSession(session_token='123').save()
        BotSession.objects.update(session_token='234')
        items_filtered = BotSession.objects.query()
        self.assertEquals(len(items_filtered), 1)
        self.assertIn('session_token', items_filtered[0])
        self.assertEquals(items_filtered[0]['session_token'], '234')

    def test_delete(self):
        bot = BotSession().save()
        task = TestModel(bot_session=bot).save()
        BotSession.objects.delete(bot)
        TestModel.objects.delete(task)

        bot_sessions = BotSession.objects.query()
        task_list = BotSession.objects.query()

        self.assertEquals(len(bot_sessions), 0)
        self.assertEquals(len(task_list), 0)

    def test_delete_all(self):
        bot = BotSession().save()
        task = TestModel(bot_session=bot).save()
        BotSession.objects.delete()
        TestModel.objects.delete()

        bot_sessions = BotSession.objects.query()
        task_list = TestModel.objects.query()

        self.assertEquals(len(bot_sessions), 0)
        self.assertEquals(len(task_list), 0)

    def test_list_field(self):
        some_list = [5, 9, 's', 4.5, False]
        ListCheckModel(redis_list=some_list).save()
        item = ListCheckModel.objects.query()[0]
        self.assertIn('redis_list', item)
        self.assertEquals(item['redis_list'], some_list)
        self.assertEquals(item.redis_list, some_list)

    def test_dict_field(self):
        some_dict = {'numprop': 19, 'boolprop': True}
        DictCheckModel(redis_dict=some_dict).save()
        item = DictCheckModel.objects.query()[0]
        self.assertEquals(item.redis_dict, some_dict)

    def test_default_value_as_method(self):
        item = BotSession().save()
        time.sleep(1)
        item2 = BotSession().save()
        self.assertTrue(isinstance(item.session_token, str))
        self.assertTrue(isinstance(item2.session_token, str))
        self.assertNotEquals(item.session_token, item2.session_token)
        self.assertTrue(isinstance(item.created, datetime.datetime))
        self.assertTrue(isinstance(item2.created, datetime.datetime))
        self.assertLess(item.created, item2.created)

    def test_foreignkey_field(self):
        task_id = 12345
        task_challenge = TestModel(task_id=task_id).save()
        ForeignKeyCheckModel.objects.create(task_challenge=task_challenge)
        task_challenge_qs = TestModel.objects.query(task_id=task_id)
        self.assertEquals(len(task_challenge_qs), 1)
        fk_check_instances = ForeignKeyCheckModel.objects.query(task_challenge=task_challenge)
        self.assertEquals(len(fk_check_instances), 1)

    def test_nested_foreignkey_field(self):
        task_id = 12345
        task_challenge = TestModel(task_id=task_id).save()
        fk_instance = ForeignKeyCheckModel.objects.create(task_challenge=task_challenge)
        NestedForeignKeyCheckModel.objects.create(nested=fk_instance)

        task_challenge_qs = TestModel.objects.query(task_id=task_id)
        self.assertEquals(len(task_challenge_qs), 1)
        fk_check_instances = NestedForeignKeyCheckModel.objects.query(nested__task_challenge=task_challenge)
        self.assertEquals(len(fk_check_instances), 1)

    def test_save_override(self):
        instance_1 = ModelWithOverriddenSave.objects.create()
        instance_2 = ModelWithOverriddenSave.objects.create()
        self.assertEquals(instance_1.multiplied_max_field * 2, instance_2.multiplied_max_field)

    def test_inheritance(self):
        InheritanceTestModel.objects.create()
        InheritanceTestModel.objects.create(abstract_field='nice')
        all_instances = InheritanceTestModel.objects.query()
        filtered_instances = InheritanceTestModel.objects.query(abstract_field='nice')

        self.assertEquals(len(all_instances), 2)
        self.assertEquals(len(filtered_instances), 1)

    def test_manytomany_field(self):
        tasks_ids = set([random.randrange(0, 100) for _ in range(10)])
        item_list = [
            TestModel(task_id=task_id).save()
            for task_id in tasks_ids
        ]
        ManyToManyCheckModel.objects.create(task_challenges=item_list)
        m2m_qs = ManyToManyCheckModel.objects.query()
        self.assertEquals(len(m2m_qs), 1)
        self.assertEquals({i.id for i in m2m_qs[0].task_challenges}, {i.id for i in item_list})

    def test_queryset_count(self):
        count = 10
        for i in range(count):
            TestModel().save()
        qs_count = TestModel.objects.query().count()
        self.assertEquals(count, qs_count)

    def test_queryset_values__green(self):
        count = 1
        for i in range(count):
            TestModel().save()
        qs_values = TestModel.objects.query().values('bot_session', 'status')
        self.assertEquals(count, len(qs_values))
        item = qs_values[0]
        self.assertEquals(2, len(item.keys()))
        self.assertTrue('bot_session' in item.keys())
        self.assertTrue('status' in item.keys())

    def test_queryset_values__bad_field_name(self):
        count = 1
        for i in range(count):
            TestModel().save()
        qs = TestModel.objects.query()
        with self.assertRaises(RedisModelsException):
            qs.values('no_such_field', 'status')

    def test_as_dict(self):
        bot = BotSession().save()
        qs_values = BotSession.objects.query().as_dict()
        self.assertEquals(1, len(qs_values))
        item = qs_values[bot.id]
        self.assertEquals(bot, item)


class ExceptionsTests(AbstractRedisTest):
    def test_register_bad_model(self):
        with self.assertRaises(RedisModelsException):
            self.redis_mgr.register_models(self.__class__)

    def test_set_field_outside(self):
        with self.assertRaises(RedisModelsException):
            TestModel(x=1)

    def test_non_existing_model_foreign_key(self):
        with self.assertRaises(RedisModelsException):
            RedisForeignKey(model=self.__class__)

    def test_non_existing_model_m2m(self):
        with self.assertRaises(RedisModelsException):
            RedisManyToMany(model=self.__class__)

#
# class TestComparisons(AbstractRedisTest):
#     def test_compare_performance_using_keys_vs_scan(self):
#         tests_count = 1000
#
#         print(f'Starting SCAN part with {tests_count} items')
#         self._cleanup()
#         self._setup_base_mgr(use_keys=False)
#         start = time.time()
#         for i in range(tests_count):
#             item = TestModel(status='in_work', ).save()
#             qs = TestModel.objects.query(id=item.id)
#             TestModel.objects.update(qs.as_list(), account_checks_count=1)
#         scan_time = (time.time() - start) * 1000
#
#         print(f'Starting KEYS part with {tests_count} items')
#         self._cleanup()
#         self._setup_base_mgr(use_keys=True)
#         start = time.time()
#         for i in range(tests_count):
#             item = TestModel(status='in_work', ).save()
#             qs = TestModel.objects.query(id=item.id)
#             TestModel.objects.update(qs.as_list(), account_checks_count=1)
#         keys_time = ((time.time() - start) * 1000)  # in ms
#
#         keys_percent = round((scan_time / keys_time) * 100, 2)
#         print(f'Using `SCAN` took {keys_percent}% of the time using `KEYS` ({keys_time:2f}ms)')
#         print(f'`KEYS` ({keys_time:2f}ms), `SCAN` ({scan_time:2f}ms)')
