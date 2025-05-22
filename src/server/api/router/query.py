
import fastapi
from fastapi import Depends
from server.api.schema.query import RequestQuery, ResponseQuery
from server.middleware.auth import authe
from server.controllers.db_controlller import select


router = fastapi.APIRouter(prefix="/queries", tags=["queries"])


@router.post(
    path="/",
    response_model=ResponseQuery,
    dependencies=[Depends(authe)],
)
def query(
    request: RequestQuery
):
    
    user_query = request.query
    data = select(user_query)

    return {
        "response": data
    }