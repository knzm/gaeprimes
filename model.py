from datetime import datetime
from google.appengine.ext import db


class NumericModelMixin(object):

    def __init__(self, parent=None, key_name=None, **kw):
        if key_name is None and "number" in kw:
            key_name = self.key_name_from_number(kw["number"])
        return super(NumericModelMixin, self).__init__(
            parent=parent, key_name=key_name, **kw)

    @classmethod
    def key_name_from_number(cls, number):
        return "key_%d" % number

    @classmethod
    def get_by_number(cls, number, **kw):
        return cls.get_by_key_name(cls.key_name_from_number(number), **kw)

    @classmethod
    def get_or_insert(cls, key_name=None, **kw):
        if key_name is None and "number" in kw:
            key_name = cls.key_name_from_number(kw["number"])
        super(NumericModelMixin, cls).get_or_insert(key_name, **kw)

    @classmethod
    def ensure_number(cls, number, **kw):
        key_name = cls.key_name_from_number(number)
        return cls.get_or_insert(key_name, number=number, **kw)

    def __repl__(self):
        return "<%s %d>" % (self.__class__, self.number)


class Root(db.Model):
    pass


class Sieve(NumericModelMixin, db.Model):
    number = db.IntegerProperty()


class Prime(NumericModelMixin, db.Model):
    number = db.IntegerProperty()
    owner = db.UserProperty()
    last_assigned_at = db.DateTimeProperty()

    def __init__(self, parent=None, key_name=None,
                 last_assigned_at=None, **kw):
        if last_assigned_at is None:
            last_assigned_at = datetime.now()
        return super(Prime, self).__init__(
            parent=parent, key_name=key_name,
            last_assigned_at=last_assigned_at, **kw)
