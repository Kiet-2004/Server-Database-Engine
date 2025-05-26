import fastapi
from server.api.router.query import router as query_router
from server.api.router.user import router as user_router
router = fastapi.APIRouter()

router.include_router(router=query_router)
router.include_router(router=user_router)

