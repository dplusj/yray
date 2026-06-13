
from xray.executors.base import Executor
from xray.dag.node import DagNode
from typing import Any, List
try:
    import ray
except ImportError:
    ray = None


class RayExecutor(Executor):
    def __init__(self):
        ray.init(ignore_reinit_error=True)

    def submit(self, node: DagNode, args: List[Any], kwargs: dict[str, Any]):
        #TODO: put cpu/gpu requirements in node metadata
        # print(node)
        remote_fn = ray.remote(
            num_cpus=1,
            num_gpus=0,
        )(node.task.fn)

        return remote_fn.remote(
            *args,
            **kwargs
        )

    def gather(self, handle):
        return ray.get(handle)