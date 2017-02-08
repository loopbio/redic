import string

# Python 2/3 compatibility helpers. These helpers are used internally and are
# not exported.
try:
    _ = unicode
except NameError:
    # noinspection PyShadowingBuiltins
    basestring = (str, bytes)
else:
    # noinspection PyShadowingBuiltins
    basestring = basestring
_METACLASS_ = '_metaclass_helper_'


def with_metaclass(meta, base=object):
    return meta(_METACLASS_, (base,), {})


class _KeyPart:
    def __init__(self, name, fmt, include_name):
        self.name = name
        self._include_name = include_name

        self._fmt = ('%s:{:%s}' % (self.name, fmt)) if (name and include_name) else ('{:%s}' % fmt)
        self._fmt_wildcard = ('%s:*' % self.name) if (name and include_name) else '*'

    def format(self, val):
        if val == '*':
            return self._fmt_wildcard
        try:
            return self._fmt.format(val)
        except ValueError:
            raise ValueError("could not format key '%s' value '%s'. did you pass the correct type" % (self.name, val))


class StringKeyPart(_KeyPart):
    def __init__(self, name, length=None, fmt=None, include_name=True):
        if fmt is None:
            fmt = 's' if length is None else ('<%ds' % length)
        _KeyPart.__init__(self, name, fmt, include_name)


class IntKeyPart(_KeyPart):
    def __init__(self, name, length=None, fmt=None, include_name=True):
        if fmt is None:
            fmt = 'd' if length is None else ('0%dd' % length)
        _KeyPart.__init__(self, name, fmt, include_name)


class KeyScheme(object):
    # the existance of this wrapper is largely because Python2.7 does not preserve attribute
    # definition order (fixed in Python 3.6)
    # https://www.python.org/dev/peps/pep-0520/
    def __init__(self, *keys, **kwargs):
        if not all(isinstance(k, _KeyPart) for k in keys):
            raise ValueError('schema must contain only key objects')

        self.name = kwargs.get('name')
        self.keys = keys
        self.key_parts = tuple(k.name for k in self.keys)

    def format(self, **kwargs):
        suffix = (':%s' % self.name) if self.name else ''

        parts = []
        for keyobj in self.keys:
            parts.append(keyobj.format(kwargs[keyobj.name]))
        return ':'.join(parts) + suffix

    def get_key(self, wildcard_ok, **kwargs):
        key_parts = self.key_parts
        if wildcard_ok:
            key_args = {k: '*' for k in key_parts}
            key_args.update(kwargs)
        else:
            if len(kwargs) != len(key_parts):
                raise ValueError('all %s key parts must be supplied: missing %s' % (
                    self.name, ', '.join(set(key_parts) - set(kwargs))))
            key_args = kwargs

        return self.format(**key_args)


class _KeySchemeMeta(object):
    def __init__(self, scheme, db_holder, prefix):
        self._scheme = scheme
        self._db_holder = db_holder
        self._prefix = prefix
        self._db_manual = None

    def connect(self, database):
        self._db_manual = database

    @property
    def _db(self):
        if self._db_manual is not None:
            return self._db_manual
        else:
            return self._db_holder.db

    def _get_key(self, wildcard_ok, **kwargs):
        k = self._prefix + self._scheme.get_key(wildcard_ok=wildcard_ok, **kwargs)
        # print "K=\t%s" % k
        return k

    def iter_keys(self, **kwargs):
        key = self._get_key(wildcard_ok=True, **kwargs)
        for _key in self._db.scan_iter(key):
            yield _key

    def iter_values(self, **kwargs):
        for key in self.iter_keys(**kwargs):
            yield self._db.get(key)

    def iter_items(self, **kwargs):
        for key in self.iter_keys(**kwargs):
            yield key, self._db.get(key)

    def get(self, **kwargs):
        key = self._get_key(wildcard_ok=False, **kwargs)
        # print 'get', id(self._db)
        return self._db.get(key)

    def set(self, val, ex=None, **kwargs):
        key = self._get_key(wildcard_ok=False, **kwargs)
        return self._db.set(key, val, ex=ex)

    def delete(self, **kwargs):
        key = self._get_key(wildcard_ok=False, **kwargs)
        return self._db.delete(key)


class BaseModel(type):
    def __new__(mcs, name, bases, attrs):
        # clsname = name
        if name == _METACLASS_ or bases[0].__name__ == _METACLASS_:
            return super(BaseModel, mcs).__new__(mcs, name, bases, attrs)

        cls = super(BaseModel, mcs).__new__(mcs, name, bases, attrs)

        flat = False
        prefix = ''
        schemes = {}
        for name, attr in cls.__dict__.items():
            if isinstance(attr, KeyScheme):
                ks = attr
                ks.name = name
                schemes[name] = ks
            elif isinstance(attr, _KeyPart):
                # put individual keys in a flat namespace
                schemes[name] = KeyScheme(attr, name=None)
                flat = True
            elif isinstance(attr, basestring) and (name == 'prefix'):
                prefix = attr

        if not schemes:
            raise ValueError('no keyparts specified')

        # replace the KeyScheme objects with a proxy that knows about the database connection
        for name, scheme in schemes.iteritems():
            setattr(cls, name,
                    _KeySchemeMeta(scheme, db_holder=cls, prefix=prefix))
        cls._schemes = schemes.keys()
        cls._flat_namespace = flat
        # print "constructed", clsname, "with", cls._schemes, "flat", flat

        return cls


# noinspection PyProtectedMember
class Model(with_metaclass(BaseModel)):
    db = None
    prefix = None

    def __init__(self, val=None, ex=None, database=None, **kwargs):
        self._did_set = False
        self._kwargs = kwargs

        # fixme: push/pop or make threadsafe???
        if database is not None:
            for scheme in self._schemes:
                getattr(self, scheme).connect(database)

        self.__execute(val, ex, **kwargs)

    def __execute(self, val, ex, **kwargs):
        if len(self._schemes) == 1:
            if val is not None:
                self._did_set = getattr(self, self._schemes[0]).set(val, ex=ex, **kwargs)
            else:
                self._result = getattr(self, self._schemes[0]).get(**kwargs)
        elif kwargs:
            # collect keys and values
            pipe = self.db.pipeline()
            for name in self._schemes:
                if self._flat_namespace:
                    k = getattr(self, name)._get_key(wildcard_ok=False, **{name: kwargs[name]})
                    if val is not None:
                        v = val[name]
                else:
                    k = getattr(self, name)._get_key(wildcard_ok=False, **kwargs[name])
                    if val is not None:
                        v = val[name]

                if val is not None:
                    # noinspection PyUnboundLocalVariable
                    getattr(pipe, 'set')(k, v)
                else:
                    getattr(pipe, 'get')(k)

            res = pipe.execute()
            if val is not None:
                self._did_set = all(res)
            else:
                self._result = {name: res[i] for i, name in enumerate(self._schemes)}

    @property
    def _did_get(self):
        return hasattr(self, '_result')

    def __call__(self, *args, **kwargs):
        try:
            return self._result
        except AttributeError:
            # retreive the result
            self.__execute(None, None, **self._kwargs)
            return self._result

    def __repr__(self):
        return "<Model(%s) set: %s get: %s>" % (
            ', '.join(getattr(self, scheme)._get_key(wildcard_ok=True) for scheme in self._schemes),
            self._did_set,
            self._did_get)

    @classmethod
    def iter_keys(cls):
        for scheme in cls._schemes:
            for key in getattr(cls, scheme).iter_keys():
                yield key

    @classmethod
    def empty(cls):
        for key in cls.iter_keys():
            cls.db.delete(key)
