from dataclasses import dataclass, field
from typing import Callable, Any
import uuid


@dataclass
class DAGNode:
    fn: Callable
    deps: list["DAGNode"] = field(default_factory=list)

    name: str | None = None

    cpu: int = 1
    gpu: int = 0

    node_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    kwargs: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.name is None:
            self.name = self.fn.__name__

    def __call__(self, *deps, **kwargs):
        return DAGNode(
            fn=self.fn,
            deps=list(deps),
            name=self.name,
            cpu=self.cpu,
            gpu=self.gpu,
            kwargs=kwargs,
        )