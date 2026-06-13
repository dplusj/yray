
from typing import Any

from xray.executors.base import Executor
from xray.executors.plan import ExecutionPlan
from xray.dag.context import Context
from xray.dag.node import DagNode
import inspect


class Engine:

    def __init__(self, executor: Executor):
        self.executor = executor

    def run(self, plan: ExecutionPlan, context: Context)-> dict[str, Any]:

        handles: dict[str, Any] = {}

        for node in plan.topo_order:

            # IMPORTANT: no waiting, just pass handles
            args, kargs = self._build_call(node, handles, context)
            future = self.executor.submit(
                node=node,
                args=args,
                kwargs=kargs,
            )

            handles[node.node_id] = future

        # return ONLY final outputs as handles
        return {
            name: handles[node.node_id]
            for name, node in plan.outputs.items()
        }
    
    def _build_call(self, node: DagNode, handles: dict[str, Any], context: Context)->tuple[list, dict]:
        sig = node.task.signature
        params = list(sig.parameters.values())

        args: list[Any] = []
        kwargs = {}

        # special storage for varargs
        varargs = None

        for p in params:

            if p.name == "context":
                continue

            value = node.bindings.get(p.name)

            # dependency resolution
            value = self._resolve_value(value, handles)

            # VAR_POSITIONAL → collect list
            if p.kind == inspect.Parameter.VAR_POSITIONAL:
                varargs = value if value is not None else []
                continue

            # normal param
            kwargs[p.name] = value

        # expand varargs correctly
        if varargs is not None:
            args.extend(varargs)

        if "context" in sig.parameters:
            kwargs["context"] = context

        return args, kwargs
    
    def _resolve_value(self, value: Any, handles: dict[str, Any]) -> Any:
        if value is None:
            return None

        if isinstance(value, list):
            return [self._resolve_value(v, handles) for v in value]

        if isinstance(value, DagNode):
            return handles[value.node_id]

        return value

    def gather(self, output_handles):
        return {
            name: self.executor.gather(handle)
            for name, handle in output_handles.items()
        }