# Entry point for the server application

from server.api.main import router
import fastapi
from server.database.db_engine import engine
from server.config.settings import CSV_FILE

def initialize_backend_application() -> fastapi.FastAPI:
    app = fastapi.FastAPI() 

    app.include_router(router)

    return app


# if __name__ == "__main__":
app = initialize_backend_application()