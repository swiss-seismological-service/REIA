from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from reia.config.settings import get_webservice_settings
from reia.services.logger import LoggerService
from reia.webservice.database import sessionmanager
from reia.webservice.routers import calculation, damage, loss, riskassessment

# Initialize logging once at startup
LoggerService.setup_logging()
logger = LoggerService.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Function that handles startup and shutdown events.
    To understand more, read https://fastapi.tiangolo.com/advanced/events/
    """
    logger.info("Starting REIA webservice")
    yield
    if sessionmanager._engine is not None:
        # Close the DB connection
        await sessionmanager.close()
        logger.info("Shutting down REIA webservice")


def create_app(
    *,
    extra_routers: list = None,
    title: str = "Rapid Earthquake Impact Assessment",
) -> FastAPI:

    app = FastAPI(
        lifespan=lifespan,
        root_path=get_webservice_settings().root_path,
        title=title)

    app.include_router(loss.router, prefix='/v1')
    app.include_router(damage.router, prefix='/v1')
    app.include_router(riskassessment.router, prefix='/v1')
    app.include_router(calculation.router, prefix='/v1')

    for r in extra_routers or []:
        app.include_router(r)

    return app


app = create_app()

app = CORSMiddleware(
    app=app,
    allow_origins=get_webservice_settings().allow_origins,
    allow_origin_regex=get_webservice_settings().allow_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)
