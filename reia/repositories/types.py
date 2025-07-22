from geoalchemy2 import WKBElement
from geoalchemy2.shape import from_shape, to_shape
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

# Type and conversion function for the DB model's polygon field
PolygonType = WKBElement
db_to_shapely = to_shape
shapely_to_db = from_shape
SessionType = Session

DuplicateError = IntegrityError
