import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.config import (
    GENERATOR_ENABLED,
    GENERATOR_EVENTS_PER_SECOND,
    GENERATOR_NUM_USERS,
    GENERATOR_NUM_MOVIES,
)
from app.producer import KafkaProducer
from app.generator import EventGenerator
from app.routes import router, set_producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

generator_task = None
generator = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global generator_task, generator
    logger.info("Starting producer service...")
    producer = KafkaProducer()
    set_producer(producer)

    if GENERATOR_ENABLED:
        generator = EventGenerator(
            producer=producer,
            num_users=GENERATOR_NUM_USERS,
            num_movies=GENERATOR_NUM_MOVIES,
            events_per_second=GENERATOR_EVENTS_PER_SECOND,
        )
        generator_task = asyncio.create_task(generator.start())

    yield

    logger.info("Shutting down producer service...")
    if generator:
        generator.stop()
    if generator_task:
        generator_task.cancel()
        try:
            await generator_task
        except asyncio.CancelledError:
            pass
    producer.flush()


app = FastAPI(title="Movie Events Producer", lifespan=lifespan)
app.include_router(router)