from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from server.utils.exceptions.dpapi2_exception import (
    InterfaceError, DatabaseError, DataError, OperationalError,
    IntegrityError, InternalError, ProgrammingError, NotSupportedError
)
 
def exception_handler(request: Request, exc: Exception):
    if isinstance(exc, InterfaceError):
        return JSONResponse(
            status_code=400,
            content={"detail": "Database API issue: " + str(exc)}
        )
    elif isinstance(exc, DatabaseError):
        return JSONResponse(
            status_code=500,
            content={"detail": "Database error: " + str(exc)}
        )
    elif isinstance(exc, DataError):
        return JSONResponse(
            status_code=422,
            content={"detail": "Invalid data: " + str(exc)}
        )
    elif isinstance(exc, OperationalError):
        return JSONResponse(
            status_code=503,
            content={"detail": "Operational error: " + str(exc)}
        )
    elif isinstance(exc, IntegrityError):
        return JSONResponse(
            status_code=409,
            content={"detail": "Integrity constraint violation: " + str(exc)}
        )
    elif isinstance(exc, InternalError):
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal database error: " + str(exc)}
        )
    elif isinstance(exc, ProgrammingError):
        return JSONResponse(
            status_code=400,
            content={"detail": "Programming error: " + str(exc)}
        )
    elif isinstance(exc, NotSupportedError):
        return JSONResponse(
            status_code=405,
            content={"detail": "Operation not supported: " + str(exc)}
        )
    else:
        return JSONResponse(
            status_code=500,
            content={"detail": "An unexpected error occurred: " + str(exc)}
        )