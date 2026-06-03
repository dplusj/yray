from dataclasses import dataclass
from typing import Callable, Generic, TypeVar
import inspect

T = TypeVar("T")

@dataclass(frozen=True)
class Task(Generic[T]):
    fn: Callable[..., T]
    name: str
    signature: inspect.Signature


def task(fn: Callable[..., T]) -> Task[T]:
    return Task(
        fn=fn,
        name=fn.__name__,
        signature=inspect.signature(fn),
    )