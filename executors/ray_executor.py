import ray
from executors.base import Executor
from dag.node import DagNode
from dag.context import Context
from typing import Any


class RayExecutor(Executor):

    def __init__(self):
        ray.init(ignore_reinit_error=True)

    def submit(self, node: DagNode, dep_handles: list[Any], kwargs: dict[str, Any], context: Context):
        #TODO: put cpu/gpu requirements in node metadata
        print(node)
        remote_fn = ray.remote(
            num_cpus=1,
            num_gpus=0,
        )(node.task.fn)

        return remote_fn.remote(
            *dep_handles,
            context=context,
            **kwargs,
        )

    def gather(self, handle):
        return ray.get(handle)