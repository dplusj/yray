import ray
from executors.base import Executor
from dag.node import DagNode
from dag.context import Context
from typing import Any, List


class RayExecutor(Executor):

    def __init__(self):
        ray.init(ignore_reinit_error=True)

    def submit(self, node: DagNode, args: List[Any], kargs: dict[str, Any]):
        #TODO: put cpu/gpu requirements in node metadata
        print(node)
        remote_fn = ray.remote(
            num_cpus=1,
            num_gpus=0,
        )(node.task.fn)

        return remote_fn.remote(
            *args,
            **kargs
        )

    def gather(self, handle):
        return ray.get(handle)