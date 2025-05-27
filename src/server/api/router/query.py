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
def query(
    request: RequestQuery,
    current_user = Depends(get_current_user)
):
    if not request.query:
        raise http_exc_400_query_empty_bad_request()

    user_query = request.query.lower()
    results_iterator = db_controlller.query(current_user.user_name, user_query)

    def json_stream(data_iter: Iterator[Dict]):
        yield '['
        first = True
        for item in data_iter:
            if not first:
                yield ','
            else:
                first = False
            yield json.dumps(item)
        yield ']'

    return StreamingResponse(json_stream(results_iterator), media_type="application/json")