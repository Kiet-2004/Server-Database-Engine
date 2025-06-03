class StandardError(Exception):
    """Base class for all DB API exceptions."""
    pass

class Warning(StandardError):
    """Exception raised for important warnings."""
    pass

class Error(StandardError):
    """Base class for errors."""
    pass

class InterfaceError(Error):
    """Exception raised for problems with the DB API itself."""
    pass

class DatabaseError(Error):
    """Exception raised for problems with the database."""
    pass

class DataError(DatabaseError):
    """Exception raised for bad data, values out of range, etc."""
    pass

class OperationalError(DatabaseError):
    """Exception raised when the DB has an issue out of our control."""
    pass

class IntegrityError(DatabaseError):
    """Exception raised for integrity-related database errors."""
    pass

class InternalError(DatabaseError):
    """Exception raised for internal database errors."""
    pass

class ProgrammingError(DatabaseError):
    """Exception raised for programming errors with the operation."""
    pass

class NotSupportedError(DatabaseError):
    """Exception raised when the operation is not supported."""
    pass

def exception_handler(content: dict):
    if content["type"] == 'InterfaceError':
        return InterfaceError(content["msg"])
    elif content["type"] == 'DataError':
        return DataError(content["msg"])
    elif content["type"] == 'OperationalError':
        return OperationalError(content["msg"])
    elif content["type"] == 'IntegrityError':
        return IntegrityError(content["msg"])
    elif content["type"] == "InternalError":
        return InternalError(content["msg"])
    elif content["type"] == "ProgrammingError":
        return ProgrammingError(content["msg"])
    elif content["type"] == "NotSupportedError":
        return NotSupportedError(content["msg"])
    elif content["type"] == "DatabaseError":
        return DatabaseError(content["msg"])
    else:
        return StandardError(str(content))
    
    