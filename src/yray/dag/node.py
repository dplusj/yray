from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar
from uuid import uuid4

T = TypeVar("T")
U = TypeVar("U")

@dataclass(frozen=True)
class Task(Generic[T]):
    fn: Callable[..., T]
    name: str
    signature: inspect.Signature

    def __call__(self, *args: Any, **kwargs: Any)->DagNode[T]:
        return self.bind(*args, **kwargs)
    
    def bind(self, *args: Any, **kwargs: Any)->DagNode[T]:
        return _build_node(
            self,
            *args,
            **kwargs,
        )


def task(fn: Callable[..., T]) -> Task[T]:
    return Task(
        fn=fn,
        name=fn.__name__,
        signature=inspect.signature(fn),
    )

@dataclass
class TaskIR:
    node_id: str
    fn: Callable[..., Any]
    bindings: dict[str, Any]
    deps: list[str]

@dataclass
class DagNode(Generic[T]):
    task: Task[T] 
    node_id: str = field(default_factory=lambda: str(uuid4()))
    
    # dependency graph edges
    deps: tuple["DagNode[Any]", ...] = ()

    # parameter → value OR parameter → node_id reference
    bindings: dict[str, Any] = field(default_factory=dict)

def _build_node(
    task: Task[T],
    *args: Any,
    **kwargs: Any,
) -> DagNode[T]:

    sig = task.signature
    params = list(sig.parameters.values())

    bindings: dict[str, Any] = {}
    deps: list[DagNode[Any]] = []

    arg_i = 0

    for p in params:

        # 1. context is special (runtime injected)
        if p.name == "context":
            continue

        # 2. VAR_POSITIONAL => collect ALL remaining args
        if p.kind == inspect.Parameter.VAR_POSITIONAL:

            varargs = []

            while arg_i < len(args):
                v = args[arg_i]
                arg_i += 1

                if isinstance(v, DagNode):
                    deps.append(v)
                varargs.append(v)

            bindings[p.name] = varargs
            continue

        # 3. keyword override
        if p.name in kwargs:
            bindings[p.name] = kwargs[p.name]
            continue

        # 4. normal positional binding
        if arg_i >= len(args):
            raise TypeError(f"{task.name}: missing argument '{p.name}'")

        v = args[arg_i]
        arg_i += 1

        if isinstance(v, DagNode):
            deps.append(v)
        bindings[p.name] = v

    # 5. leftover args only allowed if VAR_POSITIONAL existed
    if arg_i < len(args):
        raise TypeError(f"{task.name}: too many positional arguments")

    return DagNode(
        task=task,
        deps=tuple(deps),
        bindings=bindings,
    )
