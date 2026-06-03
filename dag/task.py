from dataclasses import dataclass
from typing import Callable, Generic, TypeVar
import inspect
from dag.context import Context

T = TypeVar("T")

@dataclass(frozen=True)
class Task(Generic[T]):
    fn: Callable[..., T]
    name: str


def task(fn: Callable[..., T]) -> Task[T]:

    sig = inspect.signature(fn)
    params = list(sig.parameters.values())

    # must have context
    if "context" not in sig.parameters:
        raise TypeError(f"{fn.__name__} must have a `context` parameter")

    ctx_param = sig.parameters["context"]

    # rule 1: context must be keyword-only (recommended)
    if ctx_param.kind is not inspect.Parameter.KEYWORD_ONLY:
        raise TypeError(
            f"{fn.__name__}: `context` must be keyword-only"
        )

    # rule 2: context must be last parameter
    if params[-1].name != "context":
        raise TypeError(
            f"{fn.__name__}: `context` must be the last parameter"
        )

    return Task(fn=fn, name=fn.__name__)