import fastapi
from server.api.router.query import router as query_router
from server.api.router.auth import router as auth_router
router = fastapi.APIRouter()

router.include_router(router=query_router)
router.include_router(router=auth_router)

