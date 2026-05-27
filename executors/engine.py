
from dag.node import DAGNode
from executors.base import Executor

def topo_sort(node: DAGNode):

    visited = set()
    order = []

    def dfs(n):

        if n.node_id in visited:
            return

        visited.add(n.node_id)

        for d in n.deps:
            dfs(d)

        order.append(n)

    dfs(node)

    return order


class Engine:

    def __init__(self, executor: Executor):
        self.executor = executor

    def execute(self, output_node: DAGNode):

        order = topo_sort(output_node)

        handles = {}

        for node in order:

            dep_handles = [
                handles[d.node_id]
                for d in node.deps
            ]

            handle = self.executor.submit(
                node,
                dep_handles,
            )

            handles[node.node_id] = handle

        return self.executor.gather(
            handles[output_node.node_id]
        )