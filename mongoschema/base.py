# python core
import datetime
import time
import json
import inspect

# 3rd party
from bson.objectid import ObjectId

LIST_TYPES = (list, tuple)


class MongoEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, datetime.datetime):
            return time.mktime(obj.timetuple())
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

    __nonzero__=__bool__


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
            return mf.type.get(id=self.doc[key])
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


class MongoField(object):

    def __init__(self, type, default=NoDefault, default_func=None,
                 required=True, allowed_vals=None,
                 validate_regexp=None):
        self.type = type
        self.default = default
        self.default_func = default_func
        self.required = required
        self.allowed_vals = allowed_vals
        self.validate_init()
        self.validate_regexp = validate_regexp

    def validate_init(self):
        allowed = self.allowed_vals
        if not allowed:
            return
        for val in allowed:
            if not isinstance(val, self.type):
                raise ValidationError('allowed_vals: expected'
                                      ' %s, got %s for field' % (
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
    cache = {}
    doc_class = MongoDoc


    @classmethod
    def _init(cls):
        cls._ensureindexes()
        cls._initschema()

    @classmethod
    def clear_cache_and_init(cls):
        all_classes = cls._get_all_classes()
        for aclass in all_classes:
            aclass.cache = {}

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
                raise ValidationError('Could not find '
                                      '"%s" in schema for %s ' % (
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
        cls._validate(doc)
        cls._fordb(doc)
        if insert_or_save == 'insert':
            cls.collection.insert(doc)
        elif insert_or_save == 'update':
            docid = doc['_id']
            del doc['_id']
            cls.collection.update({'_id': docid}, {'$set' : doc})
            doc['_id'] = docid
        else:
            raise ValueError('expected "insert" or "save"')
        cls._fromdb(doc)
        return doc

    @classmethod
    def add_to_cache(cls, mdoc):
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
        cls._init()
        cls._fill_defaults(doc)
        cls._basic_schema_validation(doc)
        cls._fix_references(doc)
        cls._writedoc(doc, 'insert')
        mdoc = cls.doc_class(doc, cls)
        return cls.add_to_cache(mdoc)

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
    def _fordb(cls, doc):
        cls._fordb_fix_id(doc)

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
        return cls.doc_class(doc, cls)

    @classmethod
    def get(cls, **kwargs):
        cls._init()
        cls._mongodoc_to_id(kwargs)
        if 'id' in kwargs and kwargs['id'] in cls.cache:
            return cls.cache[kwargs['id']]
        cls._fordb_fix_id(kwargs, forquery=True)
        doc = cls.collection.find_one(kwargs)
        if not doc:
            return None
        mdoc = cls._fromdb(doc)
        if mdoc.id in cls.cache:
            return cls.cache[mdoc.id]
        else:
            return cls.add_to_cache(mdoc)

    @classmethod
    def _mongodoc_to_id(cls, query):
        for key in query:
            obj = query[key]
            if issubclass(type(obj), MongoDoc):
                query[key] = obj.id

    @classmethod
    def find(cls, sort=None, **kwargs):
        # re-reference it for the id
        cls._init()
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
    def remove(cls, **kwargs):
        if '_id' not in kwargs and 'id' in kwargs:
            kwargs['_id'] = kwargs['id']
            del kwargs['id']
        for doc in cls.collection.find(kwargs, only={'_id': True}):
            docs = cls.collection.remove(doc)
            if doc['_id'] in cls.cache:
                del cls.cache[doc['_id']]
