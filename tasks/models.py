from pydantic import BaseModel


class TaskStatus(BaseModel):
    status: str
    create_time: str
    start_time: str = None
    time_to_execute: float = None
