# XRay

A lightweight DAG execution framework for building reusable data, machine learning, and batch-processing pipelines.

XRay separates **workflow definition**, **planning**, and **execution**, allowing the same pipeline to run across different execution environments without modification.

---

# Installation

## Requirements

- Python 3.12+
- uv (recommended)

Install uv:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

---

## Install XRay (development)

Clone the repository:

```bash
git clone https://github.com/dplusj/xray.git
cd xray
```

Install dependencies:

```bash
uv sync
```

---

## Install Ray support (optional)

```bash
uv sync --extra ray
```

or

```bash
pip install "xray[ray]"
```

---

# Running Examples

XRay includes two example pipelines:

```text
examples/
├── local_pipeline.py
└── ray_pipeline.py
```

---

## Local Executor Example

Run pipeline locally without any distributed system:

```bash
uv run python examples/local_pipeline.py
```

This uses:

- LocalExecutor
- In-process execution
- Deterministic DAG evaluation

---

## Ray Executor Example

Run the same pipeline on Ray:

```bash
uv sync --extra ray
uv run python examples/ray_pipeline.py
```

This uses:

- RayExecutor
- Distributed task scheduling
- Remote execution

---

## Run Tests

```bash
uv run pytest
```

---

## Build Package

```bash
uv build
```

---

# Motivation

Most workflow systems tightly couple DAG definition and execution.

XRay treats a workflow as a reusable intermediate representation (IR):

```text
Task
  ↓
DagNode
  ↓
Pipeline
  ↓
ExecutionPlan
  ↓
Executor
```

A pipeline is defined once and can be executed repeatedly across different runtime contexts:

- Dates
- Symbols
- Regions
- Experiments
- Hyperparameter configurations
- Any custom runtime dimension

---

# Core Architecture

```text
                     Context
                         │
                         ▼

+---------+     +---------+     +------------+
|  Task   | --> | DagNode | --> |  Pipeline  |
+---------+     +---------+     +------------+
                                         │
                                         ▼

                                +----------------+
                                |    Planner     |
                                +----------------+
                                         │
                                         ▼

                                +----------------+
                                | ExecutionPlan  |
                                +----------------+
                                         │
                                         ▼

                                +----------------+
                                |   Executor     |
                                +----------------+
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              ▼                          ▼                          ▼

       LocalExecutor             RayExecutor              SlurmExecutor
```

---

# Core Concepts

## Task

A Task defines a unit of computation.

```python
from xray import task, Context

@task
def load_features(*, context: Context):
    return [1, 2, 3, 4]
```

Tasks:

- Define computation logic
- Are backend-independent
- Receive runtime Context

---

## DagNode

A DagNode wraps a Task and defines dependencies.

```python
features = load_features()

normalized = normalize_features(features)
```

Represents:

```text
load_features
      ↓
normalize_features
```

---

## Pipeline

A Pipeline defines final outputs of a workflow.

```python
from xray import Pipeline

pipeline = Pipeline(
    outputs={
        "model": model,
        "metrics": metrics,
    }
)
```

---

## Context

Runtime information injected by the executor.

```python
from xray import Context

ctx = Context(
    date="2024-01-01",
    params={
        "symbol": "AAPL",
        "region": "US",
    },
)
```

Used inside tasks:

```python
@task
def load_features(*, context: Context):
    symbol = context.params["symbol"]
    return symbol
```

---

## Planner

Converts Pipeline → ExecutionPlan.

```python
from xray import Planner

plan = Planner.compile(pipeline)
```

Plans are reusable across contexts.

---

## Executor

Executes a plan on a backend.

Available:

- LocalExecutor
- RayExecutor (optional)

```python
from xray import Engine, LocalExecutor

engine = Engine(executor=LocalExecutor())
```

---

# Quick Start

## Define Tasks

```python
from xray import task, Context

@task
def load_features(*, context: Context):
    return [1, 2, 3, 4, 5]


@task
def normalize_features(data, *, context: Context):
    total = sum(data)
    return [x / total for x in data]


@task
def train_model(data, *, hidden_size: int, lr: float, epochs: int, context: Context):
    return {
        "hidden_size": hidden_size,
        "lr": lr,
        "epochs": epochs,
    }
```

---

## Build DAG

```python
features = load_features()

normalized = normalize_features(features)

model = train_model(
    normalized,
    hidden_size=64,
    lr=1e-3,
    epochs=10,
)
```

---

## Define Pipeline

```python
from xray import Pipeline

pipeline = Pipeline(
    outputs={
        "model": model,
    }
)
```

---

## Compile Plan

```python
from xray import Planner

plan = Planner.compile(pipeline)
```

---

## Execute (Local)

```python
from xray import Engine, LocalExecutor, Context

engine = Engine(
    executor=LocalExecutor(),
)

result = engine.gather(
    engine.run(
        plan,
        Context(
            date="2024-01-01",
            params={
                "symbol": "AAPL",
            },
        ),
    )
)

print(result)
```

---

## Execute (Ray)

```python
from xray import Engine, Planner, Context
from xray.executors import RayExecutor

plan = Planner.compile(pipeline)

engine = Engine(
    executor=RayExecutor(),
)

result = engine.gather(
    engine.run(
        plan,
        Context(
            date="2024-01-01",
            params={
                "symbol": "AAPL",
            },
        ),
    )
)
```

---

# Multi-Model Example

```python
features = load_features()

normalized = normalize_features(features)

small = train_model(normalized, hidden_size=16, lr=1e-3, epochs=5)

medium = train_model(normalized, hidden_size=64, lr=1e-3, epochs=5)

large = train_model(normalized, hidden_size=128, lr=1e-4, epochs=5)
```

Graph:

```text
                 small
                /
load → normalize
                \
                 medium
                \
                 large
```

---

# Multi-Context Execution

```python
from xray import Context

contexts = [
    Context(date="2024-01-01"),
    Context(date="2024-01-02"),
    Context(date="2024-01-03"),
]

results = [
    engine.run(plan, ctx)
    for ctx in contexts
]

final = engine.gather(results)
```

---

# Backend Extension

```python
class Executor:
    def submit(self, node, deps, kwargs, context):
        ...

    def gather(self, handle):
        ...
```

Example:

```python
class SlurmExecutor(Executor):
    def submit(self, node, deps, kwargs, context):
        ...
```

No changes required to DAG or Planner.

---

# Features

- Typed DAG IR
- Context-aware execution
- Planner-based execution separation
- Local + Ray backends
- Fan-out DAG support
- Reusable execution plans
- Multi-context execution

---

# Roadmap

- Persistent caching
- DAG visualization
- Incremental execution
- Slurm backend
- Kubernetes backend
- Failure recovery
- Distributed tracing
```