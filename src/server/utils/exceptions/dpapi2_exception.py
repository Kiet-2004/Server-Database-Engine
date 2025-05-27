class StandardError(Exception):
    """Base class for all custom exceptions."""
    pass


class Warning(StandardError):
    """Base class for warnings."""
    pass


class Error(StandardError):
    """Base class for errors."""
    pass


class InterfaceError(Error):
    """Raised for issues with the database API."""
    pass


class DatabaseError(Error):
    """Raised for issues with the database."""
    pass


class DataError(DatabaseError):
    """Raised for bad data, values out of range, etc."""
    pass


class OperationalError(DatabaseError):
    """Raised for database issues out of our control."""
    pass


class IntegrityError(DatabaseError):
    """Raised for integrity constraint violations."""
    pass


class InternalError(DatabaseError):
    """Raised for internal database errors."""
    pass


class ProgrammingError(DatabaseError):
    """Raised for programming errors in database operations."""
    pass


class NotSupportedError(DatabaseError):
    """Raised for unsupported database operations."""
    pass