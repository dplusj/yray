import ray
from executors.base import Executor
from dag.node import DAGNode


class RayExecutor(Executor):

    def __init__(self):
        ray.init(ignore_reinit_error=True)

    def submit(self, node: DAGNode, dep_handles):

        remote_fn = ray.remote(
            num_cpus=node.cpu,
            num_gpus=node.gpu,
        )(node.fn)

        return remote_fn.remote(
            *dep_handles,
            **node.kwargs,
        )

    def gather(self, handle):
        return ray.get(handle)