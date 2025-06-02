# Entry point for the server application

from server.api.main import router
import fastapi
from server.middleware.exception_handler import exception_handler
from server.utils.exceptions import dpapi2_exception

def initialize_backend_application() -> fastapi.FastAPI:
    app = fastapi.FastAPI() 

    app.include_router(router)

    # Register the exception handler
    app.add_exception_handler(dpapi2_exception.StandardError, exception_handler)

    return app


# if __name__ == "__main__":
app = initialize_backend_application()