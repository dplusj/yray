
from typing import Any
from executors.base import Executor
from executors.plan import ExecutionPlan
from dag.context import Context



class Engine:

    def __init__(self, executor: Executor):
        self.executor = executor

    def run(self, plan: ExecutionPlan, context: Context)-> dict[str, Any]:

        handles: dict[str, Any] = {}

        for node in plan.topo_order:
            dep_handles = [
                handles[dep.node_id]
                for dep in node.deps
            ]

            # IMPORTANT: no waiting, just pass handles
            handle = self.executor.submit(
                node=node,
                dep_handles=dep_handles,
                kwargs=node.kwargs,
                context=context,
            )

            handles[node.node_id] = handle

        # return ONLY final outputs as handles
        return {
            name: handles[node.node_id]
            for name, node in plan.outputs.items()
        }

    def gather(self, output_handles):
        return {
            name: self.executor.gather(handle)
            for name, handle in output_handles.items()
        }

    # def execute(self, output_node: DAGNode):

    #     order = topo_sort(output_node)

    #     handles = {}

    #     for node in order:

    #         dep_handles = [
    #             handles[d.node_id]
    #             for d in node.deps
    #         ]

    #         handle = self.executor.submit(
    #             node,
    #             dep_handles,
    #         )

    #         handles[node.node_id] = handle

    #     return self.executor.gather(
    #         handles[output_node.node_id]
    #     )