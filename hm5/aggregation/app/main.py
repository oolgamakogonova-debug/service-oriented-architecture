import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.clickhouse_queries import ClickHouseClient
from app.postgres_writer import PostgresWriter
from app.s3_exporter import S3Exporter
from app.scheduler import AggregationScheduler
from app.routes import router, set_dependencies

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting aggregation service...")

    ch = ClickHouseClient()
    pg = PostgresWriter()
    s3 = S3Exporter()
    scheduler = AggregationScheduler()

    set_dependencies(ch, pg, s3, scheduler)
    scheduler.start()

    yield

    logger.info("Shutting down aggregation service...")
    scheduler.shutdown()


app = FastAPI(title="Movie Analytics Aggregation", lifespan=lifespan)
app.include_router(router)