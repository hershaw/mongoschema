# python core
import datetime
import time
import json
import copy

# 3rd party
from bson.objectid import ObjectId

try:
    from flask import request
except:
    request = None

LIST_TYPES = (list, tuple)

DICT_KEY_REPLACEMENTS = (
    ('$', '&dollar;'),
    ('.', '&period;')
)


def _getparams():
    """
    This function must return a dictionary because all api calls to a
    MongoDoc instance can only take **kwargs.
    """
    if request.method == 'GET':
        params = {}
        if request.args.get('json'):
            params = json.loads(request.args['json'])
    else:
        params = request.get_json()
    return params


def _functionify(string):
    return string.replace('-', '_')


def _my_import(name):
    """
    For lazy importing of modules
    """
    components = name.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


class MongoEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, datetime.datetime):
            return time.mktime(obj.timetuple())
        elif isinstance(obj, MongoDoc):
            return obj.to_dict()
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class RequiredNotFoundException(Exception):
    pass


class ValidationError(Exception):
    pass


class PrimaryKeyMissing(Exception):
    pass


class NoDefault(object):
    pass


class NoValue(object):

    def __str__(self):
        return ''

    def __unicode__(self):
        return u''

    def __bool__(self):
        return False

    __nonzero__ = __bool__


class MongoDocRefList(list):

    def __init__(self, deref_list, reflist):
        self.reflist = reflist
        super(MongoDocRefList, self).__init__(deref_list)

    def remove(self, obj):
        self.reflist.remove(obj.id)
        super(MongoDocRefList, self).remove(obj)

    def pop(self, index):
        self.reflist.pop(index)
        super(MongoDocRefList, self).pop(index)

    def append(self, obj):
        self.reflist.append(obj.id)
        super(MongoDocRefList, self).append(obj)


class MongoDoc(object):
    doc = dict()
    ms = None

    def __init__(self, doc, ms):
        self.ms = ms
        self.doc = doc

    def __getattr__(self, key):
        mf = self.ms.schema[key]
        if type(mf) in LIST_TYPES:
            if issubclass(mf[0].type, MongoSchema):
                return MongoDocRefList(
                    [mf[0].type.get(id=x) for x in self.doc[key]],
                    self.doc[key])
            return self.doc[key]
        elif type(mf) == dict:
            pass
        elif issubclass(mf.type, MongoSchema):
            if mf.required:
                return mf.type.get(id=self.doc[key])
            elif key in self.doc:
                return mf.type.get(id=self.doc[key])
            else:
                return NoValue()
        elif not mf.required and key not in self.doc:
            return NoValue()
        return self.doc[key]

    def __setattr__(self, key, value):
        if key in ['ms', 'doc']:
            super(MongoDoc, self).__setattr__(key, value)
        elif key in self.ms.schema:
            mf = self.ms.schema[key]
            self.doc[key] = MongoSchema._deref_if_needed(mf, value)
        else:
            raise KeyError(key)

    def __dict__(self):
        return self.doc

    def __repr__(self):
        if type(self) is MongoDoc:
            return 'MongoDoc %s<%s>' % (self.ms.__name__, self.id)
        else:
            return '%s<%s>' % (self.ms.__name__, self.id)

    def __str__(self):
        return json.dumps(self.doc, sort_keys=True,
                          indent=4, separators=(',', ': '),
                          cls=MongoEncoder)

    def __eq__(self, other):
        return self.id == other.id

    def __delitem__(self, key):
        del self.doc[key]
        q, up = {'_id': self.id}, {'$unset': {key: True}}
        self.ms.collection.update(q, up)

    def save(self):
        self.ms._writedoc(self.doc, 'update')

    def update(self, raw_dict):
        for key in raw_dict:
            self.ms.schema[key]
            self.doc[key] = raw_dict[key]
        self.save()

    def remove(self):
        self.ms.remove(id=self.id)

    def __unicode__(self):
        return unicode(self.__str__())

    def to_dict(self):
        if not self.ms.todict_follow_references:
            return copy.deepcopy(self.doc)
        else:
            copy_doc = {}
            for key in self.doc:
                if type(self.ms.schema[key]) in LIST_TYPES:
                    copy_doc[key] = self.doc[key]
                elif issubclass(self.ms.schema[key].type, MongoSchema):
                    copy_doc[key] = getattr(self, key).to_dict()
                else:
                    copy_doc[key] = self.doc[key]
        return copy_doc

    def reload(self):
        doc = self.ms.collection.find_one({'_id': self.id})
        mdoc = self.ms._fromdb(doc)
        self.doc = mdoc.doc

    def update_single_field(self, key, value):
        """
        Execute an $update only on the one field being passed in so you don't
        have to call save() on the whole doc.
        """
        self.__setattr__(key, value)
        q = {'_id': self.id}
        up = {'$set': {key: value}}
        self.ms.collection.update(q, up)

    @property
    def path_for(self):
        return self.ms.doc_path_for(oid=self.id)


class MongoField(object):

    def __init__(self, _type, default=NoDefault, default_func=None,
                 required=True, allowed_vals=None,
                 validate_regexp=None):
        self._type = None
        if type(_type) is not str:
            self._type = _type
        else:
            self._import_string = _type
        self.default = default
        self.default_func = default_func
        self.required = required
        self.allowed_vals = allowed_vals
        self.validate_init()
        self.validate_regexp = validate_regexp

    @property
    def type(self):
        if self._type is None:
            self._type = _my_import(self._import_string)
        return self._type

    def validate_init(self):
        allowed = self.allowed_vals
        if not allowed:
            return
        for val in allowed:
            if not isinstance(val, self.type):
                raise ValidationError(
                    'allowed_vals: expected %s, got %s for field' % (
                        self.type, type(val)))

    def filldefault(self):
        if self.default_func:
            return self.default_func()
        else:
            return self.default

    def has_default(self):
        return self.default is not NoDefault


class MongoSchemaWatcher(type):
    """
    This is to execute code after an instance of MongoSchema is subclassed by
    a MongoSchema user, specifically for ensuring indexes and initializing
    the schema before anyone has a chance to use the class (was leading
    to strange errors)
    """
    def __init__(cls, name, bases, clsdict):
        # cls._ensureindexes()
        # cls._initschema()
        cls._init()
        super(MongoSchemaWatcher, cls).__init__(name, bases, clsdict)


class MongoSchema(object):

    __metaclass__ = MongoSchemaWatcher

    collection = None
    schema = {
        'id': MongoField(ObjectId, default_func=ObjectId, required=False),
    }
    pkey = '_id'
    indexes = []
    cache = None
    doc_class = MongoDoc
    todict_follow_references = False
    cache_enabled = True

    def __init__(self):
        raise ValueError('Did you mean to use .create()?')

    @classmethod
    def _init(cls):
        cls._ensureindexes()
        cls._initschema()
        cls.cache = {}
        clsname = cls.__name__.lower()
        cls.api_path_scheme = '/%s/<oid>' % clsname

    @classmethod
    def _set_cache(cls, enabled_or_disabled):
        cls.cache_enabled = enabled_or_disabled

    @classmethod
    def enable_cache(cls):
        cls._set_cache(True)

    @classmethod
    def disable_cache(cls):
        cls._set_cache(False)

    @classmethod
    def clear_cache_and_init(cls):
        all_classes = cls._get_all_classes()
        for aclass in all_classes:
            aclass._init()

    @classmethod
    def _get_all_classes(cls):
        l = [cls]
        for subclass in cls.__subclasses__():
            l += subclass._get_all_classes()
        return l

    @classmethod
    def _ensureindexes(cls):
        for index_kwargs in cls.indexes:
            if type(index_kwargs) == list:
                index, ikwargs = index_kwargs
            else:
                index = index_kwargs
                ikwargs = {}
            cls.collection.ensure_index(index, **ikwargs)

    @classmethod
    def _initschema(cls):
        if 'id' not in cls.schema:
            cls.schema['id'] = MongoField(ObjectId, default_func=ObjectId,
                                          required=False)
        else:
            if cls.schema['id'].default_func is None:
                raise PrimaryKeyMissing('primary key default_func required')

    @classmethod
    def _fill_defaults(cls, doc, schema=None):
        schema = schema or cls.schema
        for key in schema:
            sitem = schema[key]
            if isinstance(sitem, dict):
                if key not in doc:
                    doc[key] = {}
                cls._fill_defaults(doc[key], sitem)
            if type(sitem) in LIST_TYPES:
                if key not in doc:
                    doc[key] = type(sitem)()
            if key not in doc and sitem.has_default():
                doc[key] = sitem.filldefault()
        return doc

    @classmethod
    def _check_entry_type(cls, key, entry, mf):
        if issubclass(mf.type, MongoSchema):
            if not type(entry) is ObjectId:
                raise ValidationError(
                    'Expected an ObjectId got %s' % type(entry))
        elif not isinstance(entry, mf.type):
            raise ValidationError(
                '%s.%s: Expected type %s, got %s' % (
                    cls.__name__, key, mf.type, type(entry)))
        if mf.allowed_vals and entry not in mf.allowed_vals:
            raise ValidationError(
                '%s: %s not in %s' % (key, entry, mf.allowed_vals))

    @classmethod
    def _validate_mongo_field(cls, key, doc, mf):
        if mf.required:
            if key not in doc:
                raise RequiredNotFoundException(key)
        elif key not in doc:
            # it's not required and it's not there. fuck it!
            return

        if key not in doc and mf.default:
            doc[key] = mf.filldefault()
            return

        cls._check_entry_type(key, doc[key], mf)

        if mf.validate_regexp:
            regexp = mf.validate_regexp
            if not regexp.match(doc[key]):
                raise ValidationError(
                    '"%s" does not match pattern: %s for key %s' % (
                        doc[key], regexp.pattern, key))

    @classmethod
    def _basic_schema_validation(cls, doc):
        for key in doc:
            if key not in cls.schema:
                raise ValidationError(
                    'Unknown key %s in %s' % (key, cls))

    @classmethod
    def _validate(cls, doc, schema=None):
        schema = schema or cls.schema
        for key in doc:
            if key not in schema:
                raise ValidationError(
                    'Could not find "%s" in schema for %s ' % (
                        key, cls.__name__))
        for key in schema:
            mf = schema[key]
            if isinstance(mf, MongoField):
                cls._validate_mongo_field(key, doc, mf)
            elif isinstance(mf, dict):
                cls._validate(doc[key], mf)
            elif type(mf) in LIST_TYPES:
                if len(mf) != 1:
                    raise ValidationError('dont know what to do with > 1')
                for entry in doc[key]:
                    cls._check_entry_type(key, entry, mf[0])
            else:
                raise ValidationError('Values must be dict or MongoField'
                                      ' was given %s instead' % type(mf))
        return MongoDoc(doc, cls)

    @classmethod
    def _writedoc(cls, doc, insert_or_save):
        doc = copy.deepcopy(doc)
        cls._validate(doc)
        cls._fordb(doc)
        if insert_or_save == 'insert':
            cls.collection.insert(doc)
        elif insert_or_save == 'update':
            docid = doc['_id']
            del doc['_id']
            cls.collection.update({'_id': docid}, {'$set': doc})
            doc['_id'] = docid
        else:
            raise ValueError('expected "insert" or "save"')
        cls._fromdb(doc)
        return doc

    @classmethod
    def add_to_cache(cls, mdoc):
        if not cls.cache_enabled:
            raise ValueError('Cannot cache when disabled')
        cls.cache[mdoc.id] = mdoc
        return mdoc

    @classmethod
    def _deref_if_needed(cls, mf, value):
        if type(mf) in LIST_TYPES and issubclass(mf[0].type, MongoSchema):
            # so the user can pass in an actual instance
            value = [x.id for x in value]
        elif type(mf) in LIST_TYPES:
            # it's just a list with regular types
            pass
        elif type(mf) is dict:
            # just a regular dictionary
            pass
        elif issubclass(mf.type, MongoSchema):
            if value is None:
                raise ValueError(
                    'Expected instance of %s, instead got None' % str(mf.type))
            value = value.id
        return value

    @classmethod
    def _fix_references(cls, doc):
        for key in doc:
            if key == 'id':
                continue
            doc[key] = cls._deref_if_needed(cls.schema[key], doc[key])

    @classmethod
    def create(cls, **doc):
        cls._fill_defaults(doc)
        cls._basic_schema_validation(doc)
        cls._fix_references(doc)
        doc = cls._writedoc(doc, 'insert')
        mdoc = cls.doc_class(doc, cls)
        if cls.cache_enabled:
            return cls.add_to_cache(mdoc)
        else:
            return mdoc

    @classmethod
    def _fromdb_fix_id(cls, doc):
        doc['id'] = doc['_id']
        del doc['_id']

    @classmethod
    def _fordb_fix_id(cls, kwargs, forquery=False):
        if 'id' not in kwargs and not forquery:
            kwargs['_id'] = cls.schema['id'].default_func()
        elif 'id' in kwargs:
            kwargs['_id'] = cls.schema['id'].default_func(kwargs['id'])
            del kwargs['id']

    @classmethod
    def _unfix_dict_keys(cls, doc):
        """
        Look for all instances of __dict__: ... and turn it back into a dict
        """
        if type(doc) is dict:
            for key in doc:
                old_key = key
                key = cls._fix_single_dict_key(key, fordb=False)
                if old_key != key:
                    doc[key] = doc[old_key]
                    del doc[old_key]
                doc[key] = cls._unfix_dict_keys(doc[key])
            return doc
        elif type(doc) is list:
            for i, item in enumerate(doc):
                doc[i] = cls._unfix_dict_keys(doc[i])
            return doc
        else:
            return doc

    @classmethod
    def _fix_single_dict_key(cls, key, fordb=False):
        for orig, replacewith in DICT_KEY_REPLACEMENTS:
            if fordb:
                # if converting from python code to db
                key = key.replace(orig, replacewith)
            else:
                # if converting from db to python code
                key = key.replace(replacewith, orig)
        return key

    @classmethod
    def _fix_dict_keys(cls, doc):
        """
        Because mongodb doesn't allow '.' to be in document keys
        but python does so we need to convert to a list before we save
        """
        if type(doc) is dict:
            for key in doc:
                old_key = key
                key = cls._fix_single_dict_key(key, fordb=True)
                if old_key != key:
                    doc[key] = doc[old_key]
                    del doc[old_key]
                doc[key] = cls._fix_dict_keys(doc[key])
            else:
                return doc
        elif type(doc) is list:
            for i, item in enumerate(doc):
                doc[i] = cls._fix_dict_keys(doc[i])
            return doc
        else:
            return doc

    @classmethod
    def _fordb(cls, doc):
        cls._fordb_fix_id(doc)
        cls._fix_dict_keys(doc)

    @classmethod
    def _fix_int_float(cls, doc):
        schema = cls.schema
        for key in schema:
            mf = schema[key]
            if not isinstance(mf, MongoField):
                continue
            if key in doc and mf.type == int and type(doc[key]) == float:
                doc[key] = int(doc[key])

    @classmethod
    def _fromdb(cls, doc):
        cls._fromdb_fix_id(doc)
        cls._fill_defaults(doc)
        cls._fix_int_float(doc)
        cls._unfix_dict_keys(doc)
        return cls.doc_class(doc, cls)

    @classmethod
    def get(cls, **kwargs):
        cls._mongodoc_to_id(kwargs)
        if cls.cache_enabled:
            if 'id' in kwargs and kwargs['id'] in cls.cache:
                return cls.cache[kwargs['id']]
        cls._fordb_fix_id(kwargs, forquery=True)
        doc = cls.collection.find_one(kwargs)
        if not doc:
            return None
        if cls.cache_enabled:
            if doc['_id'] in cls.cache:
                mdoc = cls.cache[doc['_id']]
            else:
                mdoc = cls._fromdb(doc)
                cls.add_to_cache(mdoc)
        else:
            mdoc = cls._fromdb(doc)
        return mdoc

    @classmethod
    def _mongodoc_to_id(cls, query):
        for key in query:
            obj = query[key]
            if issubclass(type(obj), MongoDoc):
                query[key] = obj.id

    @classmethod
    def find(cls, sort=None, **kwargs):
        # re-reference it for the id
        cls._mongodoc_to_id(kwargs)
        docs = cls.collection.find(kwargs)
        if sort:
            docs.sort(*sort)
        for doc in docs:
            yield cls._fromdb(doc)

    @classmethod
    def list(cls, sort=None, **kwargs):
        return [x for x in cls.find(sort=sort, **kwargs)]

    @classmethod
    def _remove_from_cache(cls, _id):
        if _id in cls.cache:
            del cls.cache[_id]

    @classmethod
    def remove(cls, **kwargs):
        if '_id' not in kwargs and 'id' in kwargs:
            kwargs['_id'] = kwargs['id']
            del kwargs['id']
        for doc in cls.collection.find(kwargs, projection={'_id': True}):
            cls.collection.remove(doc)
            if cls.cache_enabled:
                cls._remove_from_cache(doc['_id'])

    ############################################################
    # Flask stuff
    ############################################################

    @classmethod
    def register_app(cls, flask_app):
        cls._flask_app = flask_app
        clsname = cls.__name__
        cls._flask_app.add_url_rule(
            cls.api_path_scheme, clsname, cls._doc_route())
        cls._flask_app.add_url_rule(
            cls.static_path_for(), clsname + '_list', cls._static_route())

    @classmethod
    def _doc_route(cls, name=None):
        def real_route(oid):
            md = cls.get(id=oid)
            if name is not None:
                params = _getparams()
                retval = getattr(md, name)(**params)
                return json.dumps(retval, cls=MongoEncoder)
            else:
                return json.dumps(md, cls=MongoEncoder)
        return real_route

    @classmethod
    def doc_path_for(cls, name=None, oid=None):
        path = cls.api_path_scheme
        if name:
            path = '%s/%s' % (path, name)
        if oid:
            path = path.replace('<oid>', str(oid))
        return path

    @classmethod
    def doc_route(cls, name, **kwargs):
        path = cls.doc_path_for(name)
        funcname = _functionify(name)
        cls._flask_app.add_url_rule(
            path, name, cls._doc_route(funcname), **kwargs)

    @classmethod
    def _static_route(cls, name=None):
        def real_route():
            if name is None:
                return json.dumps(cls.list(), cls=MongoEncoder)
            else:
                params = _getparams()
                retval = getattr(cls, name)(**params)
                return json.dumps(retval, cls=MongoEncoder)
        return real_route

    @classmethod
    def static_path_for(cls, name=None):
        path = cls.api_path_scheme.replace('<oid>', '')
        if name:
            path = '%s/%s' % (path, name)
        return path

    @classmethod
    def static_route(cls, name=None, **kwargs):
        funcname = _functionify(name)
        path = cls.static_path_for(name=name)
        cls._flask_app.add_url_rule(
            path, name, cls._static_route(funcname), **kwargs)
