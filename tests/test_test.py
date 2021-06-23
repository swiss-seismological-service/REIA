from datamodel import PostalCode


def test_a(client, db_session):
    print('test 1')
    pc = PostalCode(name='Mörel', plz=3983)
    db_session.add(pc)
    db_session.commit()
    k = db_session.query(PostalCode).first()
    assert k.plz == 3983


def test_b(client, db_session):
    print('test 2')
    assert(True)
