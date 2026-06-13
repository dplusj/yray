from typing import Any, List

from yray.dag.node import DagNode
from yray.executors.base import Executor


class LocalExecutor(Executor):
    def submit(self, node: DagNode, args: List[Any], kwargs: dict[str, Any]):
        """
        Immediately execute the function and return result.

        (Matches async-style executors like Ray/ThreadPool)
        """
        return node.task.fn(*args, **kwargs)

    def gather(self, handle):
        return handle