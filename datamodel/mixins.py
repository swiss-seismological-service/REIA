import functools
import datetime
import enum

from sqlalchemy import (
    Column, Integer, Float, DateTime, String)
from sqlalchemy.ext.declarative import declared_attr


class CreationInfoMixin(object):
    """
    `SQLAlchemy <https://www.sqlalchemy.org/>`_ mixin emulating type
    :code:`CreationInfo` from `QuakeML <https://quake.ethz.ch/quakeml/>`_.
    """
    creationinfo_author = Column(String)
    creationinfo_authoruri_resourceid = Column(String)
    creationinfo_agencyid = Column(String)
    creationinfo_agencyuri_resourceid = Column(String)
    creationinfo_creationtime = Column(DateTime,
                                       default=datetime.datetime.utcnow().isoformat(' ', 'seconds'))
    creationinfo_version = Column(String)
    creationinfo_copyrightowner = Column(String)
    creationinfo_copyrightowneruri_resourceid = Column(String)
    creationinfo_license = Column(String)


class PublicIdMixin(object):
    """
    `SQLAlchemy <https://www.sqlalchemy.org/>`_ mixin emulating type
    :code:`PublicId` from `QuakeML <https://quake.ethz.ch/quakeml/>`_.
    """
    publicid_resourceid = Column(String)


def ClassificationMixin(name, column_prefix=None):
    """
    `SQLAlchemy <https://www.sqlalchemy.org/>`_ mixin emulating type
    :code:`Classification` from `QuakeML <https://quake.ethz.ch/quakeml/>`_.
    """
    if column_prefix is None:
        column_prefix = '%s_' % name

    column_prefix = column_prefix.lower()

    @declared_attr
    def _concept(cls):
        return Column('%sconcept' % column_prefix, String)

    @declared_attr
    def _classificationsource_resourceid(cls):
        return Column('%sclassificationsource_resourceid'
                      % column_prefix, String)

    @declared_attr
    def _conceptschema_resourceid(cls):
        return Column('%sconceptschema_resourceid' % column_prefix, String)

    _func_map = (('concept', _concept),
                 ('classificationsource_resourceid',
                  _classificationsource_resourceid),
                 ('conceptschema_resourceid', _conceptschema_resourceid))

    def __dict__(func_map, attr_prefix):

        return {'{}{}'.format(attr_prefix, attr_name): attr
                for attr_name, attr in func_map}

    return type(name, (object,), __dict__(_func_map, column_prefix))


def QuantityMixin(name, quantity_type, column_prefix=None, optional=False,
                  index=False):
    """
    Mixin factory for common :code:`Quantity` types from
    `QuakeML <https://quake.ethz.ch/quakeml/>`_.

    Quantity types provide the fields:
        - `value`
        - `uncertainty`
        - `loweruncertainty`
        - `upperuncertainty`
        - `confidencelevel`.

    Note, that a `column_prefix` may be prepended.

    :param str name: Name of the class returned
    :param str quantity_type: Type of the quantity to be returned. Valid values
        are :code:`int`, :code:`real` or rather :code:`float` and :code:`time`.
    :param column_prefix: Prefix used for DB columns. If :code:`None`, then
        :code:`name` with an appended underscore :code:`_` is used. Capital
        Letters are converted to lowercase.
    :type column_prefix: str or None
    :param bool optional: Flag making the :code:`value` field optional
        (:code:`True`).

    The usage of :py:func:`QuantityMixin` is illustrated bellow:

    .. code::

        # define a ORM mapping using the Quantity mixin factory
        class FooBar(QuantityMixin('foo', 'int'),
                     QuantityMixin('bar', 'real'),
                     ORMBase):

            def __repr__(self):
                return '<FooBar (foo_value=%d, bar_value=%f)>' % (
                    self.foo_value, self.bar_value)


        # create instance of "FooBar"
        foobar = FooBar(foo_value=1, bar_value=2)

    """

    if column_prefix is None:
        column_prefix = '%s_' % name

    column_prefix = column_prefix.lower()

    def create_value(quantity_type, column_prefix, optional):

        def _make_value(sql_type, column_prefix, optional):

            @declared_attr
            def _value(cls):
                return Column('%svalue' % column_prefix, sql_type,
                              nullable=optional, index=index)

            return _value

        if 'int' == quantity_type:
            return _make_value(Integer, column_prefix, optional)
        elif quantity_type in ('real', 'float'):
            return _make_value(Float, column_prefix, optional)
        elif 'time' == quantity_type:
            return _make_value(DateTime, column_prefix, optional)

        raise ValueError('Invalid quantity_type: {}'.format(quantity_type))

    @declared_attr
    def _uncertainty(cls):
        return Column('%suncertainty' % column_prefix, Float)

    @declared_attr
    def _lower_uncertainty(cls):
        return Column('%sloweruncertainty' % column_prefix, Float)

    @declared_attr
    def _upper_uncertainty(cls):
        return Column('%supperuncertainty' % column_prefix, Float)

    @declared_attr
    def _confidence_level(cls):
        return Column('%sconfidenceLevel' % column_prefix, Float)

    _func_map = (('value',
                  create_value(quantity_type, column_prefix, optional)),
                 ('uncertainty', _uncertainty),
                 ('loweruncertainty', _lower_uncertainty),
                 ('upperuncertainty', _upper_uncertainty),
                 ('confidenceLevel', _confidence_level)
                 )

    def __dict__(func_map, attr_prefix):

        return {'{}{}'.format(attr_prefix, attr_name): attr
                for attr_name, attr in func_map}

    return type(name, (object,), __dict__(_func_map, column_prefix))


FloatQuantityMixin = functools.partial(QuantityMixin,
                                       quantity_type='float')
RealQuantityMixin = FloatQuantityMixin
IntegerQuantityMixin = functools.partial(QuantityMixin,
                                         quantity_type='int')
TimeQuantityMixin = functools.partial(QuantityMixin,
                                      quantity_type='time')


def EpochMixin(name, epoch_type=None, column_prefix=None):
    """
    Mixin factory for common :code:`Epoch` types from
    `QuakeML <https://quake.ethz.ch/quakeml/>`_.

    Epoch types provide the fields `starttime` and `endtime`. Note, that a
    `column_prefix` may be prepended.

    :param str name: Name of the class returned
    :param epoch_type: Type of the epoch to be returned. Valid values
        are :code:`None` or :code:`default`, :code:`open` and :code:`finite`.
    :type epoch_type: str or None
    :param column_prefix: Prefix used for DB columns. If :code:`None`, then
        :code:`name` with an appended underscore :code:`_` is used. Capital
        letters are converted to lowercase.
    :type column_prefix: str or None

    The usage of :py:func:`EpochMixin` is illustrated bellow:

    .. code::

        import datetime

        # define a ORM mapping using the "Epoch" mixin factory
        class MyObject(EpochMixin('epoch'), ORMBase):

            def __repr__(self):
                return \
                    '<MyObject(epoch_starttime={}, epoch_endtime={})>'.format(
                        self.epoch_starttime, self.epoch_endtime)


        # create instance of "MyObject"
        my_obj = MyObject(epoch_starttime=datetime.datetime.utcnow())

    """
    if column_prefix is None:
        column_prefix = '%s_' % name

    column_prefix = column_prefix.lower()

    class Boundary(enum.Enum):
        LEFT = enum.auto()
        RIGHT = enum.auto()

    def create_datetime(boundary, column_prefix, **kwargs):

        def _make_datetime(boundary, **kwargs):

            if boundary is Boundary.LEFT:
                name = 'starttime'
            elif boundary is Boundary.RIGHT:
                name = 'endtime'

            @declared_attr
            def _datetime(cls):
                return Column('%s%s' % (column_prefix, name), DateTime,
                              **kwargs)

            return _datetime

        return _make_datetime(boundary, **kwargs)

    if epoch_type is None or epoch_type == 'default':
        _func_map = (('starttime', create_datetime(Boundary.LEFT,
                                                   column_prefix,
                                                   nullable=False)),
                     ('endtime', create_datetime(Boundary.RIGHT,
                                                 column_prefix)))
    elif epoch_type == 'open':
        _func_map = (('starttime', create_datetime(Boundary.LEFT,
                                                   column_prefix)),
                     ('endtime', create_datetime(Boundary.RIGHT,
                                                 column_prefix)))
    elif epoch_type == 'finite':
        _func_map = (('starttime', create_datetime(Boundary.LEFT,
                                                   column_prefix,
                                                   nullable=False)),
                     ('endtime', create_datetime(Boundary.RIGHT,
                                                 column_prefix,
                                                 nullable=False)))
    else:
        raise ValueError('Invalid epoch_type: {!r}.'.format(epoch_type))

    def __dict__(func_map, attr_prefix):

        return {'{}{}'.format(attr_prefix, attr_name): attr
                for attr_name, attr in func_map}

    return type(name, (object,), __dict__(_func_map, column_prefix))


UniqueEpochMixin = EpochMixin('Epoch', column_prefix='')
UniqueOpenEpochMixin = EpochMixin('Epoch', epoch_type='open',
                                  column_prefix='')
UniqueFiniteEpochMixin = EpochMixin('Epoch', epoch_type='finite',
                                    column_prefix='')
