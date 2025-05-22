import fastapi
from server.api.router.query import router as query_router

router = fastapi.APIRouter()

router.include_router(router=query_router)

