from pydantic import BaseModel, Field
from enum import Enum
from typing import Optional
from uuid import UUID
from datetime import datetime


class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class MovieEvent(BaseModel):
    event_id: UUID
    user_id: str = Field(..., min_length=1)
    movie_id: str = Field(..., min_length=1)
    event_type: EventType
    timestamp: datetime
    device_type: DeviceType
    session_id: str = Field(..., min_length=1)
    progress_seconds: int = Field(default=0, ge=0)