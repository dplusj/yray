from xray.executors.base import Executor
from xray.dag.node import DagNode
from typing import Any, List


class LocalExecutor(Executor):
    def submit(self, node: DagNode, args: List[Any], kwargs: dict[str, Any]):
        """
        Immediately execute the function and return result.

        (Matches async-style executors like Ray/ThreadPool)
        """
        return node.task.fn(*args, **kwargs)

    def gather(self, handle):
        return handle