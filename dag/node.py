from __future__ import annotations
from uuid import uuid4
from dataclasses import dataclass, field
from typing import Generic, TypeVar, Any

from dag.task import Task

T = TypeVar("T")
U = TypeVar("U")


@dataclass
class DagNode(Generic[T]):
    node_id: str = field(default_factory=lambda: str(uuid4()))
    task: Task[T] = field(default=None)
    deps: tuple[DagNode[Any], ...] = ()
    kwargs: dict[str, Any] = field(default_factory=lambda: {})

    # def __call__(self, *deps: DagNode[Any]) -> DagNode[T]:
    #     """
    #     Optional functional style:
    #         node = train(load())
    #     """
    #     return DagNode(task=self.task, deps=deps)
    
def build_node(task: Task[T], *deps: DagNode[Any], **kwargs: Any) -> DagNode[T]:
    return DagNode(
        node_id=str(uuid4()),
        task=task,
        deps=tuple(deps),
        kwargs=kwargs,
    )