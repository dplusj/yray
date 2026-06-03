from abc import ABC, abstractmethod
from typing import List, Any


class Executor(ABC):

    @abstractmethod
    def submit(
        self,
        node,
        dep_results: List[Any],
        kwargs: dict[str, Any],
        context: Any,
    ):
        pass

    @abstractmethod
    def gather(self, handle):
        pass