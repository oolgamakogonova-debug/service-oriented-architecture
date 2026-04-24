import asyncio
import logging
import random
import uuid
from datetime import datetime, timedelta, timezone
from app.schemas import MovieEvent, EventType, DeviceType
from app.producer import KafkaProducer

logger = logging.getLogger(__name__)


class EventGenerator:
    """
    Generates realistic sequences of user events.
    Each user session follows: VIEW_STARTED -> (VIEW_PAUSED -> VIEW_RESUMED)* -> VIEW_FINISHED
    Also generates LIKED and SEARCHED events randomly.
    """

    def __init__(
        self,
        producer: KafkaProducer,
        num_users: int = 100,
        num_movies: int = 50,
        events_per_second: int = 10,
    ):
        self.producer = producer
        self.users = [f"user_{i:04d}" for i in range(num_users)]
        self.movies = [f"movie_{i:04d}" for i in range(num_movies)]
        self.devices = list(DeviceType)
        self.events_per_second = events_per_second
        self._active_sessions: dict[str, dict] = {}
        self._running = False

    async def start(self):
        self._running = True
        logger.info(
            "Event generator started: %d events/sec, %d users, %d movies",
            self.events_per_second,
            len(self.users),
            len(self.movies),
        )
        while self._running:
            try:
                self._generate_batch()
                await asyncio.sleep(1.0 / max(self.events_per_second, 1))
            except Exception as e:
                logger.error("Generator error: %s", e)
                await asyncio.sleep(1)

    def stop(self):
        self._running = False
        self.producer.flush()
        logger.info("Event generator stopped")

    def _generate_batch(self):
        # Pick a random user
        user_id = random.choice(self.users)
        session_key = user_id

        if session_key in self._active_sessions:
            self._continue_session(session_key)
        else:
            # 80% chance to start a new viewing session, 10% LIKED, 10% SEARCHED
            rand = random.random()
            if rand < 0.8:
                self._start_session(user_id)
            elif rand < 0.9:
                self._generate_liked(user_id)
            else:
                self._generate_searched(user_id)

    def _start_session(self, user_id: str):
        session_id = str(uuid.uuid4())
        movie_id = random.choice(self.movies)
        device = random.choice(self.devices)
        now = datetime.now(timezone.utc)

        # Spread events over the last few days for richer analytics
        day_offset = random.choices(range(8), weights=[30, 20, 15, 10, 8, 7, 5, 5])[0]
        event_time = now - timedelta(
            days=day_offset,
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        event = MovieEvent(
            event_id=uuid.uuid4(),
            user_id=user_id,
            movie_id=movie_id,
            event_type=EventType.VIEW_STARTED,
            timestamp=event_time,
            device_type=device,
            session_id=session_id,
            progress_seconds=0,
        )
        self.producer.send(event)

        self._active_sessions[user_id] = {
            "session_id": session_id,
            "movie_id": movie_id,
            "device": device,
            "progress": 0,
            "last_time": event_time,
            "state": "playing",
            "steps_remaining": random.randint(1, 5),
        }

    def _continue_session(self, session_key: str):
        session = self._active_sessions[session_key]
        user_id = session_key
        time_delta = timedelta(seconds=random.randint(30, 300))
        session["last_time"] += time_delta
        session["progress"] += random.randint(30, 300)

        if session["steps_remaining"] <= 0:
            # Finish the session
            event = MovieEvent(
                event_id=uuid.uuid4(),
                user_id=user_id,
                movie_id=session["movie_id"],
                event_type=EventType.VIEW_FINISHED,
                timestamp=session["last_time"],
                device_type=session["device"],
                session_id=session["session_id"],
                progress_seconds=session["progress"],
            )
            self.producer.send(event)
            del self._active_sessions[session_key]
        else:
            session["steps_remaining"] -= 1
            if session["state"] == "playing":
                event_type = EventType.VIEW_PAUSED
                session["state"] = "paused"
            else:
                event_type = EventType.VIEW_RESUMED
                session["state"] = "playing"

            event = MovieEvent(
                event_id=uuid.uuid4(),
                user_id=user_id,
                movie_id=session["movie_id"],
                event_type=event_type,
                timestamp=session["last_time"],
                device_type=session["device"],
                session_id=session["session_id"],
                progress_seconds=session["progress"],
            )
            self.producer.send(event)

    def _generate_liked(self, user_id: str):
        now = datetime.now(timezone.utc)
        day_offset = random.randint(0, 7)
        event_time = now - timedelta(days=day_offset, hours=random.randint(0, 23))

        event = MovieEvent(
            event_id=uuid.uuid4(),
            user_id=user_id,
            movie_id=random.choice(self.movies),
            event_type=EventType.LIKED,
            timestamp=event_time,
            device_type=random.choice(self.devices),
            session_id=str(uuid.uuid4()),
            progress_seconds=0,
        )
        self.producer.send(event)

    def _generate_searched(self, user_id: str):
        now = datetime.now(timezone.utc)
        day_offset = random.randint(0, 7)
        event_time = now - timedelta(days=day_offset, hours=random.randint(0, 23))

        event = MovieEvent(
            event_id=uuid.uuid4(),
            user_id=user_id,
            movie_id=random.choice(self.movies),
            event_type=EventType.SEARCHED,
            timestamp=event_time,
            device_type=random.choice(self.devices),
            session_id=str(uuid.uuid4()),
            progress_seconds=0,
        )
        self.producer.send(event)