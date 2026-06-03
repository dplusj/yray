# XRay - A Python Parallel Runtime Scheduler

A minimal DAG execution engine with pluggable executors.

# Design Philosophy

The system separates **computation**, **planning**, and **execution** into distinct layers.

A `Task` defines computation semantics.

A `DagNode` references a Task within a DAG and defines dependency relationships.

A `Pipeline` is a reusable DAG template that can be instantiated multiple times using different execution contexts.

A `Context` binds runtime dimensions (date, symbol, model, region, scenario, etc.) to a Pipeline.

A `Planner` converts a Pipeline and Context into an executable plan.

A `Backend` executes the plan on a specific runtime (Ray, Slurm, Condor, Local, etc.).

An `Aggregator` combines outputs from multiple Pipeline instances into a final result.

The architecture intentionally separates:

- What to compute
- How to organize computation
- Where to execute computation

This allows the same Pipeline to run unchanged across multiple execution environments.

---

# Core Concepts

## Task

A Task defines a unit of computation.

Responsibilities:

- Computation logic
- Input/output contract
- Optional metadata

A Task is backend-independent.

```text
Task
 └── compute(inputs, context) -> output

## Features

- DAG IR nodes
- Topological execution planner
- Ray backend (can extend to slurm or other backend)
- Multi-model training pipeline example
- Easy extension to Slurm / Condor

## Install

```bash
pip install -r requirements.txt