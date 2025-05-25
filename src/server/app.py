# Entry point for the server application

from server.api.main import router
import fastapi
from server.database.db_engine import engine
from server.config.settings import CSV_FILE

def initialize_backend_application() -> fastapi.FastAPI:
    app = fastapi.FastAPI() 
    
    @app.on_event("startup")
    def load_csvs():
        engine.load_table("employees", "employees.csv")
    app.include_router(router)

    return app


# if __name__ == "__main__":
app = initialize_backend_application()