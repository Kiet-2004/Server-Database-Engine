
import fastapi
from fastapi import Depends
from server.api.schema.query import RequestQuery, ResponseQuery
from server.middleware.auth import authe
from server.controllers.db_controlller import select
from server.utils.exceptions.http.exc_400 import http_exc_400_query_empty_bad_request

router = fastapi.APIRouter(prefix="/queries", tags=["queries"])


@router.post(
    path="/",
    response_model=ResponseQuery,
    dependencies=[Depends(authe)],
)
def query(
    request: RequestQuery
):
    """
    check empty query
    """
    if not request.query:
        raise http_exc_400_query_empty_bad_request()
    
    user_query = request.query.lower()
    data = select(user_query)

    return {
        "response": data
    }