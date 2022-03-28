import functools
import logging
from typing import Set, Optional, Any, Callable, Tuple

import botocore

logger = logging.getLogger(__name__)


def handle_botocore_exceptions(
    warn: Tuple[str, ...] = ()
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Wrap a function and with a decorator which do a conversion from inner
    botocore exception to the BaseStorageError.
    Args:
        warn: List of the botocore codes that should not produce an error to the caller,
            and can safely be skipped with an warning message.
    Returns:
        A decorator that invokes exception handler with the decorated
        function as the wrapper argument and the arguments to wraps() as the
        remaining arguments.
    """

    def inner_handle_botocore_exception_handler(
        func: Callable[..., Any]
    ) -> Callable[..., Any]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await func(*args, **kwargs)
            except botocore.exceptions.ClientError as e:
                exc_args = (
                    "Calling function: %r failed: %s",
                    func.__qualname__,
                    str(e),
                )

                if e.response["Error"]["Code"] in warn:
                    logger.warning(*exc_args)
                else:
                    logger.exception(*exc_args)
                    raise BaseStorageError.from_boto(e) from e

        return wrapper

    return inner_handle_botocore_exception_handler


class BaseError(Exception):
    code = 500


class BaseStorageError(BaseError):
    """Base class for all storage exceptions."""

    botocore_code: Set[str] = set()

    @classmethod
    def from_boto(
            cls, exception: botocore.exceptions.ClientError
    ) -> "BaseStorageError":
        code = exception.response["Error"]["Code"]

        for err_cls in cls.__subclasses__():
            if code in err_cls.botocore_code:
                break
        else:
            err_cls = UnknownStorageError

        return err_cls(
            exception.response.get("Error", {}).get("Message", "Unknown boto error"),
            inner_error=exception,
        )

    def __init__(
            self, *args: Any,
            inner_error: Optional[botocore.exceptions.ClientError] = None
    ) -> None:
        self.inner_error = inner_error
        super().__init__(*args)


class UnknownStorageError(BaseStorageError):
    code = 500


class ObjectNotFoundError(BaseStorageError):
    botocore_code = {"ResourceNotFoundException"}
    code = 404
