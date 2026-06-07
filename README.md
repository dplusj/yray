# XRay

A lightweight DAG execution framework for building reusable data, machine learning, and batch-processing pipelines.

XRay separates **workflow definition**, **planning**, and **execution**, allowing the same pipeline to run across different execution environments without modification.

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

The architecture intentionally separates:

- **What to compute** → Task
- **How computations depend on each other** → DagNode
- **What outputs define a workflow** → Pipeline
- **How execution is planned** → Planner
- **Where execution happens** → Executor

---

# Core Concepts

## Task

A Task defines a unit of computation.

Tasks are pure computation definitions and contain no scheduling logic.

```python
@task
def load_features(
    *,
    context: Context,
):
    ...
```

A Task:

- Defines computation logic
- Declares inputs and outputs
- Receives runtime context
- Remains backend-independent

---

## DagNode

A DagNode represents a Task inside a DAG.

```text
load_features
      ↓
normalize_features
      ↓
train_model
```

A DagNode contains:

- Task reference
- Dependency edges
- Static configuration parameters
- Runtime metadata

Example:

```python
normalized = normalize_features(features)

model = train_model(
    normalized,
    hidden_size=64,
    lr=1e-3,
    epochs=10,
)
```

---

## Pipeline

A Pipeline defines one or more workflow outputs.

```python
pipeline = Pipeline(
    outputs={
        "model": model,
        "metrics": metrics,
    }
)
```

A Pipeline is reusable and contains no runtime state.

---

## Context

A Context contains runtime information injected by the executor.

```python
@dataclass(frozen=True)
class Context:
    date: str | None = None
    params: dict[str, object] | None = None
```

Example:

```python
Context(
    date="2024-01-01",
)
```

Additional runtime parameters:

```python
Context(
    date="2024-01-01",
    params={
        "symbol": "AAPL",
        "region": "US",
        "experiment": "baseline",
    },
)
```

Tasks access runtime information through Context:

```python
@task
def load_features(
    *,
    context: Context,
):
    symbol = context.params["symbol"]

    ...
```

Context is not part of DAG typing and is supplied only at execution time.

---

## Planner

The Planner converts a Pipeline into an executable execution plan.

Responsibilities:

- DAG traversal
- Dependency resolution
- Topological ordering
- Execution planning

```python
plan = Planner.compile(pipeline)
```

Planning is performed once and can be reused across many Contexts.

---

## Executor

The Executor executes an ExecutionPlan on a specific runtime backend.

Available backends:

- LocalExecutor
- RayExecutor

Future backends:

- SlurmExecutor
- HTCondorExecutor
- KubernetesExecutor

The Pipeline and Planner remain unchanged regardless of execution environment.

---

# Quick Start

## Define Tasks

```python
@task
def load_features(
    *,
    context: Context,
):
    return [1, 2, 3, 4, 5]


@task
def normalize_features(
    data,
    *,
    context: Context,
):
    total = sum(data)

    return [
        x / total
        for x in data
    ]


@task
def train_model(
    data,
    *,
    hidden_size: int,
    lr: float,
    epochs: int,
    context: Context,
):
    return {
        "hidden_size": hidden_size,
        "lr": lr,
        "epochs": epochs,
    }
```

## Build a DAG

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

Graph:

```text
load_features
      ↓
normalize_features
      ↓
train_model
```

## Define a Pipeline

```python
pipeline = Pipeline(
    outputs={
        "model": model,
    }
)
```

## Compile Once

```python
plan = Planner.compile(pipeline)
```

The execution plan can be reused indefinitely.

## Execute

```python
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

print(result)
```

---

# Multi-Model Training Example

Fan-out from a shared feature engineering stage.

```python
features = load_features()

normalized = normalize_features(features)

small_model = train_model(
    normalized,
    hidden_size=16,
    lr=1e-3,
    epochs=5,
)

medium_model = train_model(
    normalized,
    hidden_size=64,
    lr=1e-3,
    epochs=5,
)

large_model = train_model(
    normalized,
    hidden_size=128,
    lr=1e-4,
    epochs=5,
)
```

Graph:

```text
                 train_small
                /
load -> normalize
                \
                 train_medium
                \
                 train_large
```

Shared dependencies execute only once.

---

# Execute Across Multiple Contexts

The same execution plan can be reused across many runtime contexts.

```python
contexts = [
    Context(date="2024-01-01"),
    Context(date="2024-01-02"),
    Context(date="2024-01-03"),
]

handles = [
    engine.run(plan, ctx)
    for ctx in contexts
]

results = engine.gather(handles)
```

Typical use cases:

- Historical backtesting
- Daily batch processing
- Hyperparameter sweeps
- Multi-symbol research
- Scenario analysis
- Regional simulations

---

# Backend Extension

Create a new executor by implementing the Executor interface.

```python
class Executor(ABC):

    @abstractmethod
    def submit(
        self,
        node,
        dep_handles,
        kwargs,
        context,
    ):
        pass

    @abstractmethod
    def gather(
        self,
        handle,
    ):
        pass
```

Example:

```python
class SlurmExecutor(Executor):

    def submit(
        self,
        node,
        dep_handles,
        kwargs,
        context,
    ):
        ...

    def gather(
        self,
        handle,
    ):
        ...
```

No changes are required to Tasks, Pipelines, or the Planner.

---

# Features

- Typed DAG IR
- Multi-output pipelines
- Context-aware execution
- Topological execution planning
- Ray backend
- Backend abstraction layer
- Fan-in / fan-out DAG support
- Parallel execution across contexts
- Reusable execution plans
- Easy extension to Slurm and HTCondor

---

# Roadmap

- Persistent task caching
- Deterministic node hashing
- Artifact materialization
- Incremental recomputation
- Slurm backend
- HTCondor backend
- Kubernetes backend
- Workflow visualization
- Failure recovery
- Execution monitoring
- Pipeline UI

---

# Philosophy

XRay aims to be a small execution core rather than a full workflow platform.

The focus is on:

- Explicit DAG construction
- Reusable execution plans
- Backend portability
- Parallel execution
- Minimal abstractions

Define a workflow once.

Execute it anywhere.
