from typing import Iterator, Dict
import fastapi
import json
from fastapi import Depends
from server.api.schema.query import RequestQuery, ResponseQuery
from server.middleware.auth import get_current_user
from server.controllers import db_controlller
from server.utils.exceptions.http.exc_400 import http_exc_400_query_empty_bad_request
from fastapi.responses import StreamingResponse
router = fastapi.APIRouter(prefix="/queries", tags=["queries"])


@router.post(path="/")
async def query(
    request: RequestQuery,
    current_user = Depends(get_current_user)
):
    if not request.query:
        raise http_exc_400_query_empty_bad_request()

    user_query = request.query
    query_stream = db_controlller.query(
            user_name=current_user.user_name,
            query=user_query,
        ) 

    async def stream_response():
        yield '['
        first = True
        for row in query_stream:
            if not first:
                yield ',\n'
            else:
                first = False
            yield row
        yield ']'

    return StreamingResponse(stream_response(), media_type="application/json")