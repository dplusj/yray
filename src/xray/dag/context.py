from __future__ import annotations

from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class Context:
    """
    Runtime context injected by executor.
    Not part of DAG typing.
    """
    date: str | None = None
    params: dict[str, object] | None = None