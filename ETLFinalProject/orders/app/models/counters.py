from pydantic import BaseModel


class CounterInDB(BaseModel):
    id: str
    value: int