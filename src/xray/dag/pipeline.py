from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from xray.dag.node import DagNode


@dataclass(frozen=True)
class Pipeline:
    name: str

    outputs: dict[
        str,
        DagNode[Any],
    ]