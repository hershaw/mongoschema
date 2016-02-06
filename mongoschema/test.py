import unittest
import os
import re
import json

from bson.objectid import ObjectId
import pymongo
import requests

from mongoschema.base import (
    MongoSchema, MongoDoc, MongoField as MF, ValidationError, flaskprep,
    set_api_prefix,
)

WITH_PROFILE = False


conn = pymongo.MongoClient()
TEST_DB_NAME = os.environ['TEST_DB_NAME']
db = conn[TEST_DB_NAME]


class Referencer(MongoSchema):
    collection = db.referencer
    schema = {
        'referenced': MF('test.Referenced'),
    }


class Referenced(MongoSchema):
    collection = db.referenced
    schema = {
        'nothing': MF(str),
        'referencer': MF('test.Referencer', required=False),
    }


class SchemaWithDict(MongoSchema):
    collection = db.user
    schema = {
        'data': MF(dict),
        'list_data': [MF(dict)],
    }


def sayhi():
    return 'hi'


class UserDoc(MongoDoc):

    def get_username(self):
        return self.username

    def get_with_params(self, param1, param2):
        """
        Used for testing in flask_test_server.py
        """
        return {'param1': param1, 'param2': param2}

    def set_username(self, username=None):
        self.username = username
        return self.save()

    @flaskprep(keyword_arg=sayhi)
    def useless_function(self, keyword_arg=None):
        assert keyword_arg is not None
        return 'hello world', keyword_arg


class User(MongoSchema):
    collection = db.user
    schema = {
        'username': MF(str),
    }
    indexes = [
        ['username', {'unique': True}],
    ]
    doc_class = UserDoc

    @classmethod
    def custom_create(cls, **kwargs):
        kwargs['username'] += '_custom'
        return cls.create(**kwargs)

    @classmethod
    @flaskprep(keyword_arg=sayhi)
    def useless_function(cls, keyword_arg=None):
        assert keyword_arg is not None
        return 'completely useless', keyword_arg


class UserWithCacheDisabled(MongoSchema):
    collection = db.user
    schema = {
        'username': MF(str),
    }
    indexes = [
        ['username', {'unique': True}],
    ]
    doc_class = UserDoc
    cache_enabled = False


class UserAfterChanges(MongoSchema):
    collection = db.user_after_changes
    schema = {
        'username': MF(str),
        # this field was added so there will be some users in the database
        # without it. test that the fields are filled in correctly.
        'lang': MF(str, default='en'),
    }
    indexes = [
        ['username', {'unique': True}],
    ]


class Farmer(User):
    collection = db.farmer
    schema = {
        'farm_name': MF(str)
    }
    schema.update(User.schema)

    indexes = User.indexes + [
        'farm_name'
    ]


class UserWithOptionalReference(MongoSchema):
    collection = db.user
    schema = {
        'farmer': MF(Farmer, required=False),
    }


class Email(MongoSchema):
    collection = db.email
    schema = {
        'user': MF(User),
        'subject': MF(str),
        'body': MF(str),
    }
    indexes = [
        'user',
        [(('subject', -1), ('body', -1)), {}],    # compound index
    ]


class EmbedDoc(MongoSchema):
    collection = db.embeded_doc
    schema = {
        'data': MF(dict)
    }


"""
class EmbedDocWithValidation(MongoSchema):
    collection = db.embeded_doc
    schema = {
        'data': {
            'height': MF(float),
            'weight': MF(float),
        }
    }

"""


class SchemaWithList(MongoSchema):
    collection = db.with_list
    schema = {
        'users': [MF(User)],
        'numbers': [MF(int)],
    }
    indexes = [
        'users'
    ]


class EmailEntry(MongoSchema):
    collection = db.email_entry
    schema = {
        'email': MF(str, validate_regexp=re.compile('[^@]+@[^@]+\.[^@]+'))
    }

    @classmethod
    def custom_static(cls):
        return 'this is a custom static function for testing flask auth'


class WithOptionalField(MongoSchema):
    collection = db.with_optional_field
    schema = {
        'field': MF(str, required=False),
    }


# bullshit that I must call these directly, can't share the code between
# the various test classes without all tests running any time one of
# the classes are used

def _setUp():
    conn.drop_database(TEST_DB_NAME)
    MongoSchema.clear_cache_and_init()


def _tearDown():
    conn.drop_database(TEST_DB_NAME)


def _create_user(username='this is a test'):
    return User.create(username=username)


class MongoSchemaBaseTestCase(unittest.TestCase):

    def setUp(self):
        _setUp()

    def tearDown(self):
        _tearDown()

    def _create_email(self, user):
        return Email.create(user=user, subject='subject', body='body')

    def _get_field_from_db(self, doc, field):
        raw_doc = doc.ms.collection.find_one({'_id': doc.id})
        return raw_doc[field]

    def testtest_pkey_generation(self):
        user = _create_user()
        self.assertTrue(isinstance(user.id, ObjectId))

    def test_creation_cache(self):
        user = _create_user()
        self.assertTrue(user.id in User.cache)
        User.clear_cache_and_init()
        self.assertTrue(user.id not in User.cache)
        User.get(id=user.id)
        self.assertTrue(user.id in User.cache)

    def test_basic_validation_errors(self):
        with self.assertRaises(ValidationError):
            # should fail a type error
            User.create(username=1)

        with self.assertRaises(ValidationError):
            # testing with key not in schema
            User.create(username='asdf', blah='nothing')

    def test_update(self):
        user = _create_user()
        newname = 'anothername'
        user.username = newname
        user.save()
        self.assertEqual(user.username, newname)
        self._compare_with_db(user, 'username')

    def test_compound_index(self):
        self._create_email(_create_user())
        index_info = Email.collection.index_information()
        self.assertTrue('subject_-1_body_-1' in index_info)

    def test_set_mongoschema(self):
        user = _create_user()
        email = self._create_email(user)
        self.assertTrue(user is email.user)
        other_user = _create_user(username='another')
        email.user = other_user
        email.save()
        self.assertTrue(email.user is other_user)

    def test_reference(self):
        user = _create_user()
        email = self._create_email(user)
        # actually compare memory addresses so we know the cache
        # is working
        self.assertTrue(email.user is user)
        email.user.username = 'a new name'
        self.assertEqual(email.user.username, user.username)
        email.user.save()
        self._compare_with_db(user, 'username')

    def test_reference_query(self):
        user = _create_user()
        email = self._create_email(user)
        MongoSchema.clear_cache_and_init()
        email_from_query = Email.get(user=user)
        self.assertTrue(email.id == email_from_query.id)
        MongoSchema.clear_cache_and_init()
        email_from_find = [x for x in Email.find(user=user)][0]
        self.assertTrue(email.id == email_from_find.id)

    def test_schemaless_embeded_doc(self):
        data = {'testing': 'nothing', 'hellow': 'world'}
        doc = EmbedDoc.create(data=data)
        raw_doc = doc.ms.collection.find_one({'_id': doc.id})
        self.assertEqual(MongoSchema._unfix_dict_keys(raw_doc['data']), data)

    """
    def test_embeded_doc_validation(self):
        EmbedDocWithValidation.create(
            data={
                'height': 100.0,
                'weight': 20.0,
            }
        )
        with self.assertRaises(ValidationError):
            EmbedDocWithValidation.create(
                data={
                    'height': 100.0,
                    'weight': 20,
                }
            )
            self.assertTrue(False)
    """

    def test_list_definition(self):
        user1 = User.create(username='bob')
        user2 = User.create(username='sally')
        with_list = SchemaWithList.create(
            users=[user1, user2],
            numbers=[1, 2, 3]
        )
        # test that following list references behaves correctly
        # just need to make sure this doesn't crash. should be it's own test
        SchemaWithList.todict_follow_references = True
        with_list.to_dict()
        SchemaWithList.todict_follow_references = True
        self.assertTrue(with_list.users[0] is user1)
        self.assertTrue(with_list.users[1] is user2)
        with_list.users.pop(0)
        with_list.save()
        q = {'_id': with_list.id}
        with_list_raw_doc = db.with_list.find_one(q)
        self.assertEqual(len(with_list_raw_doc['users']), 1)
        self.assertEqual(with_list_raw_doc['_id'], with_list.id)
        self.assertTrue(user2 in with_list.users)
        self.assertTrue(user2.id in with_list_raw_doc['users'])
        # now test with remove
        with_list.users.remove(user2)
        self.assertEqual(len(with_list.users), 0)
        with_list.save()
        with_list_raw_doc = db.with_list.find_one(q)
        self.assertEqual(len(with_list_raw_doc['users']), 0)
        with_list_2 = SchemaWithList.create(
            users=[],
            numbers=[]
        )
        with_list_2.users.append(user1)
        with_list_2.save()
        self.assertEqual(len(with_list_2.users), 1)
        users_raw_list = self._get_field_from_db(with_list_2, 'users')
        self.assertEqual(len(users_raw_list), 1)
        self.assertTrue(user1.id in users_raw_list)
        self.assertTrue(user1 in with_list_2.users)
        with_list_2.numbers.append(1)
        with_list_2.save()
        numbers_raw_list = self._get_field_from_db(with_list_2, 'numbers')
        self.assertEqual(len(numbers_raw_list), len(with_list_2.numbers))
        self.assertEqual(numbers_raw_list, with_list_2.numbers)
        # test that the reference is the same
        self.assertTrue(with_list_2.numbers is with_list_2.doc['numbers'])
        with self.assertRaises(ValidationError):
            with_list_2.numbers.append('blah')
            with_list_2.save()

    def test_doc_inheritence(self):
        pass

    def _create_farmer(self):
        farmer = Farmer.create(
            username='farmer_john',
            farm_name='farm name 1'
        )
        return farmer

    def test_schema_inheritence(self):
        farmer = self._create_farmer()
        farmer.farm_name = 'farm name 2'
        farmer.save()
        self._compare_with_db(farmer, 'farm_name')

    def _compare_indexes(self, ms):
        mongo_indexes = ms.collection.index_information()
        defined_indexes = ms.indexes
        # need to add one because you always have _id
        self.assertTrue(len(mongo_indexes) == len(defined_indexes) + 1)

    def test_ensure_indexes(self):
        """
        With inheritence as well
        """
        _create_user()
        self._compare_indexes(User)

        self._create_farmer()
        self._compare_indexes(Farmer)

    def test_regexp_validate(self):
        # should be able to create new ones with no problem
        EmailEntry.create(email='sam@gmail.com')
        with self.assertRaises(ValidationError):
            EmailEntry.create(email='nobody_at_gmail.com')

    def test_doc_class(self):
        user = _create_user()
        self.assertEqual(user.username, user.get_username())

    def test_query_by_non_pkey(self):
        user = _create_user()
        user_by_name = User.get(username=user.username)
        self.assertTrue(user is user_by_name)

    def test_query_not_found(self):
        self.assertIsNone(User.get(username='asdfasfdasfdsdf'))

    def test_schema_change(self):
        username = 'test'
        UserAfterChanges.collection.insert_one({'username': username})
        user = UserAfterChanges.get(username=username)
        self.assertTrue(user.lang, 'pt-PT')

    def test_mongodoc_update(self):
        user = _create_user()
        new_username = 'new'
        update_dict = {'username': new_username}
        user.update(update_dict)
        self.assertTrue(user.username == new_username)
        raw_user = User.collection.find_one({'_id': user.id})
        self.assertTrue(raw_user['username'] == new_username)

    def _compare_with_db(self, doc, field):
        raw_doc = doc.ms.collection.find_one({'_id': doc.id})
        self.assertEqual(raw_doc[field], getattr(doc, field))
        return raw_doc

    def test_list(self):
        users = []
        for i in range(0, 10):
            users.append(_create_user(username='%s' % i))
        MongoSchema.clear_cache_and_init()
        users_from_db = User.list()
        for user in users_from_db:
            self.assertTrue(user in users)

    def test_remove(self):
        users = []
        for i in range(0, 10):
            users.append(_create_user(username='%s' % i))
        for user in users:
            user.remove()
        self.assertTrue(len(User.list()) == 0)

    def test_disabled_cache(self):
        MongoSchema.disable_cache()
        user = _create_user()
        user_again = User.get(id=user.id)
        self.assertTrue(user is not user_again)
        MongoSchema.enable_cache()

    def test_todict(self):
        email = self._create_email(_create_user())
        email_dict = email.to_dict()
        self.assertTrue(type(email_dict['user']) is ObjectId)
        Email.todict_follow_references = True
        email_dict = email.to_dict()
        self.assertTrue(type(email_dict['user']) is dict)
        # now set it back for the rest of the tests
        Email.todict_follow_references = False

    def test_optional_reference(self):
        user = UserWithOptionalReference.create()
        self.assertFalse(user.farmer)
        farmer = self._create_farmer()
        user.farmer = farmer
        user.save()
        self.assertTrue(user.farmer is farmer)

    def test_reload(self):
        user = _create_user()
        new_name = 'filmore'
        db.user.update_one({'_id': user.id}, {'$set': {'username': new_name}})
        self.assertFalse(new_name == user.username)
        user.reload()
        self.assertTrue(new_name == user.username)

    def test_cache_disabled(self):
        user = UserWithCacheDisabled.create(username='nothing')
        fetched_again = UserWithCacheDisabled.get(id=user.id)
        self.assertTrue(user is not fetched_again)
        UserWithCacheDisabled.enable_cache()
        fetched_again = UserWithCacheDisabled.get(id=user.id)
        fetched_again_again = UserWithCacheDisabled.get(id=user.id)
        self.assertTrue(fetched_again_again is fetched_again)

    def test_schema_with_dict(self):
        data = {
            '$this.thing': 'that',
            'a.t': {
                'a.b': 'thing',
                'dudeman': {
                    'hello.world': 'nothing',
                }
            },
        }
        list_data = [
            {'this.thing': 'that'}
        ]
        tmp = SchemaWithDict.create(data=data, list_data=list_data)
        raw = SchemaWithDict.collection.find_one({'_id': tmp.id})
        self.assertTrue('&dollar;this&period;thing' in raw['data'])
        self.assertTrue('a&period;t' in raw['data'])
        self.assertTrue('a&period;b' in raw['data']['a&period;t'])
        self.assertTrue(MongoSchema._unfix_dict_keys(raw['data']) == data)
        self.assertTrue(
            MongoSchema._unfix_dict_keys(raw['list_data']) == list_data)
        self.assertTrue(type(tmp.data) is dict)
        new_data = {
            'hello': 'world',
            'dudeman': {
                'h.w': 'nothing',
            }
        }
        tmp.data = new_data
        tmp.save()
        raw = SchemaWithDict.collection.find_one({'_id': tmp.id})
        self.assertTrue('h&period;w' in raw['data']['dudeman'])
        self.assertTrue(
            MongoSchema._unfix_dict_keys(raw['data']) == new_data, new_data)

    def test_del(self):
        fieldval = 'nothing'
        doc = WithOptionalField.create(field=fieldval)
        self.assertEqual(fieldval, doc.field)
        del doc['field']
        self.assertFalse(doc.field)
        doc.reload()
        self.assertFalse(doc.field)
        doc.field = fieldval
        doc.save()
        self.assertEqual(doc.field, fieldval)

    def test_update_single_field(self):
        user = _create_user()
        new_username = 'new_username'
        user.username = new_username
        user.update_single_field('username', new_username)
        raw_user = User.collection.find_one({'_id': user.id})
        self.assertEqual(user.username, new_username)
        self.assertEqual(raw_user['username'], new_username)

    def test_lazy_loading(self):
        referenced = Referenced.create(nothing='nothing')
        referencer = Referencer.create(referenced=referenced)
        self.assertEqual(referencer.referenced.nothing, 'nothing')
        referenced.referencer = referencer
        referenced.save()
        self.assertEqual(referenced.referencer.id, referencer.id)

    def test_count(self):
        self.assertEqual(User.count(), 0)
        username = 'aname'
        _create_user(username)
        self.assertEqual(User.count(), 1)
        self.assertEqual(User.count(username='nothing'), 0)
        self.assertEqual(User.count(username=username), 1)


class MongoSchemaFlaskTest(unittest.TestCase):

    def setUp(self):
        _setUp()
        set_api_prefix('/api/v0')

    def tearDown(self):
        _tearDown()

    def _execute_request(self, path, data=None, params=None, method='get',
                         return_resp=False):
        url = 'http://localhost:9002' + path
        func = getattr(requests, method)
        if method == 'get':
            params = params or {}
            if data:
                params['json'] = json.dumps(data)
            reply = func(url, params=params)
        else:
            reply = func(url, json=data, params=params)
        if return_resp:
            return reply
        else:
            reply.raise_for_status()
            return reply.json()

    def test_path_generation(self):
        self.assertEqual(
            '/api/v0/user/<oid>/get-username',
            User.doc_path_for('get-username'))
        self.assertEqual('/api/v0/user/', User.path_for())

    def _test_vanilla_get(self, user):
        dict_user = self._execute_request(user.path_for)
        self.assertEqual(str(user.id), dict_user['id'])

    def _test_get_single_field(self, user):
        username = self._execute_request(
            User.doc_path_for('get-username', oid=user.id))
        self.assertEqual(username, user.username)

    def _test_vanilla_list(self, user):
        user2 = _create_user(username='u2')
        all_users = self._execute_request(User.path_for())
        self.assertEqual(len(all_users), 2)
        self.assertEqual(all_users[0]['id'], str(user.id))
        self.assertEqual(all_users[1]['id'], str(user2.id))

    def test_get(self):
        user = _create_user()
        self._test_vanilla_get(user)
        self._test_get_single_field(user)
        self._test_vanilla_list(user)

    def test_create(self):
        data = {'username': 'great_user'}
        path = User.path_for()
        reply = self._execute_request(path, data=data, method='post')
        # need to test if _custom was appended because a custom function
        # was specified so go around create
        self.assertEqual(reply['username'], data['username'] + '_custom')

    def test_vanilla_patch(self):
        user = _create_user()
        path = User.doc_path_for(oid=user.id)
        data = {'username': 'new_username'}
        reply = self._execute_request(path, data=data, method='patch')
        self.assertTrue(reply['username'], data['username'])

    def test_custom_patch(self):
        user = _create_user()
        new_username = 'u2'
        data = {'username': new_username}
        path = User.doc_path_for('set-username', oid=user.id)
        reply = self._execute_request(path, data=data, method='patch')
        self.assertEqual(reply['username'], new_username)

    def test_custom_response(self):
        user = _create_user()
        path = User.doc_path_for('useless-function', oid=user.id)
        reply = self._execute_request(path)
        self.assertEqual(reply, {"1": 1})

    def test_static_custom_response(self):
        path = User.path_for('useless-function')
        reply = self._execute_request(path)
        self.assertEqual(reply, {"1": 1})

    def test_remove(self):
        user = _create_user()
        path = User.doc_path_for('remove', oid=str(user.id))
        self._execute_request(path, method='delete')
        with self.assertRaises(requests.HTTPError):
            self._execute_request(path)

    def test_flaskprep(self):
        # flask is loaded so the generator is not used
        with self.assertRaises(AssertionError):
            User.useless_function()
        user = _create_user()
        with self.assertRaises(AssertionError):
            user.useless_function()
        User.useless_function(keyword_arg='hello')
        user.useless_function(keyword_arg='nothing')

    def test_get_with_params(self):
        """
        We expect params via get() to be passed within a single parameter
        called json and map to a json-parsable structure so that types
        are preserved (they would be lost as regular url params)
        """
        user = _create_user()
        path = User.doc_path_for('get-with-params', oid=user.id)
        # first try to make the request without the params
        with self.assertRaises(requests.HTTPError):
            self._execute_request(path)
        data = {'param1': 1, 'param2': True}
        reply = self._execute_request(path, data=data)
        self.assertEqual(reply['param1'], data['param1'])
        self.assertEqual(reply['param2'], data['param2'])

    def test_auth(self):
        email = 'sam@gmail.com'
        create_path = EmailEntry.path_for('create')
        create_data = {'email': email}
        with self.assertRaises(requests.HTTPError):
            self._execute_request(
                create_path, data=create_data, method='post')
        email_entry = self._execute_request(
            create_path, data=create_data,
            params={'authparam': 'default_static'}, method='post')
        path = EmailEntry.path_for('custom_static')
        with self.assertRaises(requests.HTTPError):
            self._execute_request(path)
        self._execute_request(
            path, params={'authparam': 'admin_for_real'}, method='post')
        path = EmailEntry.doc_path_for('update', oid=email_entry['id'])
        data = {'email': 'new@email.com'}
        with self.assertRaises(requests.HTTPError):
            self._execute_request(path, data=data, method='patch')
        self._execute_request(
            path, data=data, params={'authparam': 'even_more_secret'},
            method='patch')

    def test_with_bad_oid(self):
        path = User.doc_path_for('get', oid='asdf')
        resp = self._execute_request(path, return_resp=True)
        self.assertEqual(resp.status_code, 404)


if __name__ == '__main__':
    unittest.main()
