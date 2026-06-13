from dataclasses import dataclass
from typing import Any

from ..dag.node import DagNode
from ..dag.pipeline import Pipeline


@dataclass(frozen=True)
class ExecutionPlan:

    outputs: dict[
        str,
        DagNode[Any],
    ]

    topo_order: list[
        DagNode[Any]
    ]


class Planner:
    @staticmethod
    def compile(
        pipeline: Pipeline,
    ) -> ExecutionPlan:

        visited: set[int] = set()

        order: list[
            DagNode[Any]
        ] = []

        def dfs(
            node: DagNode[Any],
        ) -> None:

            node_id = id(node)

            if node_id in visited:
                return

            visited.add(node_id)

            for dep in node.deps:
                dfs(dep)

            order.append(node)

        for output in pipeline.outputs.values():
            dfs(output)

        return ExecutionPlan(
            outputs=pipeline.outputs,
            topo_order=order,
        )