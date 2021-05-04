import functools

from sqlalchemy import (
    Column, Integer, Float, DateTime, Boolean, String)
from sqlalchemy.ext.declarative import declared_attr


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
        return Column('%sconfidencelevel' % column_prefix, Float)

    @declared_attr
    def _pdf_variable_content(cls):
        return Column('%spdf_variable_content' % column_prefix, String)

    @declared_attr
    def _pdf_probability_content(cls):
        return Column('%spdf_probability_content' % column_prefix, String)

    @declared_attr
    def _pdf_probability_used(cls):
        return Column('%spdf_probability_used' % column_prefix, Boolean)

    _func_map = (('value',
                  create_value(quantity_type, column_prefix, optional)),
                 ('uncertainty', _uncertainty),
                 ('loweruncertainty', _lower_uncertainty),
                 ('upperuncertainty', _upper_uncertainty),
                 ('confidencelevel', _confidence_level),
                 ('pdf_variable_content', _pdf_variable_content),
                 ('pdf_probability_content', _pdf_probability_content),
                 ('pdf_probability_used', _pdf_probability_used),
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
