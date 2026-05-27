# XRay - A Python Parallel Runtime Scheduler

A minimal DAG execution engine with pluggable executors.

## Features

- DAG IR nodes
- Topological execution planner
- Ray backend (can extend to slurm or other backend)
- Multi-model training pipeline example
- Easy extension to Slurm / Condor

## Install

```bash
pip install -r requirements.txt