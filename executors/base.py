from abc import ABC, abstractmethod
from typing import List, Any
from dag.node import DagNode

class Executor(ABC):

    @abstractmethod
    def submit(
        self,
        node: DagNode,
        args: List[Any],
        kargs: dict[str, Any],
    ):
        pass

    @abstractmethod
    def gather(self, handle):
        pass