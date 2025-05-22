from typing import Annotated, Dict, Any, List
from pydantic import BaseModel
from fastapi import  Query


class ResponseQuery(BaseModel):
    response: List[ Any]

class RequestQuery(BaseModel):
    query: str  