import logging
from fastapi import APIRouter, HTTPException
from app.schemas import MovieEvent

logger = logging.getLogger(__name__)
router = APIRouter()

kafka_producer = None


def set_producer(producer):
    global kafka_producer
    kafka_producer = producer


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.post("/events")
async def publish_event(event: MovieEvent):
    if kafka_producer is None:
        raise HTTPException(status_code=503, detail="Producer not initialized")
    try:
        kafka_producer.send(event)
        return {"status": "accepted", "event_id": str(event.event_id)}
    except Exception as e:
        logger.exception("Failed to publish event")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/flush")
async def flush():
    if kafka_producer is None:
        raise HTTPException(status_code=503, detail="Producer not initialized")
    kafka_producer.flush()
    return {"status": "flushed"}