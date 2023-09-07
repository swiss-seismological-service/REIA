# Rapid Eearthquake Impact Assessment

### Build with

```
docker build -t reia_db .
```

### Run with

Make sure `.env` file is created and correct.

```
docker run --name openquake -p 127.0.0.1:8800:8800 -e LOCKDOWN=True -e OQ_ADMIN_LOGIN=secret -e OQ_ADMIN_PASSWORD=secret -e OQ_ADMIN_EMAIL=user@domain.ch -d openquake/engine:3.16
docker run --env-file=.env -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -p 5432:5432 -d reia_db
```

### Run Tests:

```
pip install -e .[dev]
pytest --cov=.
```
