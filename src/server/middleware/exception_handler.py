from fastapi import Request
from fastapi.responses import JSONResponse
from server.utils.exceptions.dpapi2_exception import (
    InterfaceError, DatabaseError, DataError, OperationalError,
    IntegrityError, InternalError, ProgrammingError, NotSupportedError
)

def exception_handler(request: Request, exc: Exception):
    if isinstance(exc, InterfaceError):
        return JSONResponse(
            status_code=400,
            content={"type": "InterfaceError", "msg": str(exc)}
        )
    elif isinstance(exc, DataError):
        return JSONResponse(
            status_code=422,
            content={"type": "DataError", "msg": str(exc)}
        )
    elif isinstance(exc, OperationalError):
        return JSONResponse(
            status_code=503,
            content={"type": "OperationalError", "msg": str(exc)}
        )
    elif isinstance(exc, IntegrityError):
        return JSONResponse(
            status_code=409,
            content={"type": "IntegrityError", "msg": str(exc)}
        )
    elif isinstance(exc, InternalError):
        return JSONResponse(
            status_code=500,
            content={"type": "InternalError", "msg": str(exc)}
        )
    elif isinstance(exc, ProgrammingError):
        return JSONResponse(
            status_code=400,
            content={"type": "ProgrammingError", "msg": str(exc)}
        )
    elif isinstance(exc, NotSupportedError):
        return JSONResponse(
            status_code=501,
            content={"type": "NotSupportedError", "msg": str(exc)}
        )
    elif isinstance(exc, DatabaseError):
        return JSONResponse(
            status_code=500,
            content={"type": "DatabaseError", "msg": str(exc)}
        )
    else:
        return JSONResponse(
            status_code=500,
            content={"type": "UnknownError", "msg": str(exc)}
        )
