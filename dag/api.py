from functools import wraps
from dag.node import DAGNode


def task(cpu=1, gpu=0, name=None):

    def decorator(fn):

        @wraps(fn)
        def wrapper(*deps, **kwargs):

            # convert inputs to DAG nodes if needed
            dep_nodes = [
                d if isinstance(d, DAGNode) else d
                for d in deps
            ]

            return DAGNode(
                fn=fn,
                deps=dep_nodes,
                cpu=cpu,
                gpu=gpu,
                name=name or fn.__name__,
                kwargs=kwargs,
            )

        return wrapper

    return decorator