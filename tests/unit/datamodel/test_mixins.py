from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import String
from sqlalchemy.exc import IntegrityError
from esloss.datamodel import ORMBase
from esloss.datamodel.mixins import EpochMixin, QuantityMixin, RealQuantityMixin, IntegerQuantityMixin
from datetime import datetime
import pytest


def test_QuantityMixin(client, db_session):
    class TestClassFloat(ORMBase, RealQuantityMixin('loss')):
        test_column = Column(String(10))

    class TestClassInteger(ORMBase, IntegerQuantityMixin('loss')):
        test_column = Column(String(10))

    class TestClassTime(ORMBase, QuantityMixin('loss', 'time')):
        test_column = Column(String(10))

    with pytest.raises(ValueError):
        class TestClassError(ORMBase, QuantityMixin('loss', 'error')):
            test_column = Column(String(10))

    TestClassInteger.__table__.create(db_session.bind)
    TestClassFloat.__table__.create(db_session.bind)
    TestClassTime.__table__.create(db_session.bind)

    test_class_float = TestClassFloat(test_column='hello', loss_value=1.32)
    test_class_int = TestClassInteger(test_column='hello', loss_value=1.3)
    test_class_time = TestClassTime(
        test_column='hello', loss_value=datetime.now())

    # test getting and returning element from database
    db_session.add(test_class_float)
    db_session.add(test_class_int)
    db_session.add(test_class_time)
    db_session.commit()

    test_class_float_from_db = db_session.query(TestClassFloat).first()
    test_class_int_from_db = db_session.query(TestClassInteger).first()

    # test that object saved to db is the same as retrieved
    assert test_class_float_from_db == test_class_float

    # test that object has attribute given by mixin
    assert hasattr(test_class_float_from_db, 'loss_value')
    assert hasattr(test_class_int_from_db, 'loss_uncertainty')

    # test object attributes have correct type
    assert isinstance(test_class_int_from_db.loss_value, int)
    assert isinstance(test_class_float_from_db.loss_value, float)
    assert isinstance(test_class_time.loss_value, datetime)


def test_EpochMixin(client, db_session):
    class TestClassDefault(ORMBase, EpochMixin('test')):
        test_column = Column(String(10))
    TestClassDefault.__table__.create(db_session.bind)

    class TestClassOpen(ORMBase, EpochMixin('test', epoch_type='open')):
        test_column = Column(String(10))
    TestClassOpen.__table__.create(db_session.bind)

    class TestClassFinite(ORMBase, EpochMixin('test', epoch_type='finite')):
        test_column = Column(String(10))
    TestClassFinite.__table__.create(db_session.bind)

    with pytest.raises(ValueError):
        class TestClassError(ORMBase, EpochMixin('test', epoch_type='error')):
            test_column = Column(String(10))

    test_class_open = TestClassOpen(test_column='test')
    db_session.add(test_class_open)
    db_session.commit()

    with pytest.raises(IntegrityError):
        test_class_finite = TestClassFinite(test_column='test')
        db_session.add(test_class_finite)
        db_session.commit()
    db_session.rollback()

    test_class_default = TestClassDefault(test_column='test')
    with pytest.raises(IntegrityError):
        db_session.add(test_class_default)
        db_session.commit()
    db_session.rollback()

    test_class_default.test_starttime = datetime.now()
    db_session.add(test_class_default)
    db_session.commit()
