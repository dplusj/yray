from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Any
from dag.context import Context

import torch
import torch.nn as nn
from torch.utils.data import TensorDataset, DataLoader

from dag.pipeline import Pipeline
from executors.plan import Planner
from executors.engine import Engine
from executors.ray_executor import RayExecutor
from dag.task import task
from dag.node import build_node
from datetime import datetime
import time



# ============================================================
# MODEL DEFINITIONS
# ============================================================

class SmallMLP(nn.Module):

    def __init__(self, hidden=32):
        super().__init__()

        self.net = nn.Sequential(
            nn.Linear(10, hidden),
            nn.ReLU(),
            nn.Linear(hidden, 1),
        )

    def forward(self, x):
        return self.net(x)


# ============================================================
# PIPELINE NODES
# ============================================================

#
# Node 1
# Load feature
#

@task
def load_features():

    print("Loading dataset")

    x = torch.randn(5000, 10)

    true_w = torch.randn(10, 1)

    y = x @ true_w + 0.1 * torch.randn(5000, 1)

    split = 4000

    return {
        "x_train": x[:split],
        "y_train": y[:split],
        "x_val": x[split:],
        "y_val": y[split:],
    }


#
# Node 2
# Transform / normalization
#
@task
def normalize_features(data):

    print("Normalizing features")

    x_train = data["x_train"]
    x_val = data["x_val"]

    mean = x_train.mean(dim=0, keepdim=True)
    std = x_train.std(dim=0, keepdim=True)

    x_train = (x_train - mean) / (std + 1e-6)
    x_val = (x_val - mean) / (std + 1e-6)

    return {
        "x_train": x_train,
        "y_train": data["y_train"],
        "x_val": x_val,
        "y_val": data["y_val"],
    }


#
# Node 3
# Training
#

@task
def train_model(
    data,
    hidden_size,
    lr,
    epochs,
):

    print(
        f"Training model hidden={hidden_size}"
    )

    model = SmallMLP(hidden=hidden_size)

    optimizer = torch.optim.Adam(
        model.parameters(),
        lr=lr,
    )

    criterion = nn.MSELoss()

    dataset = TensorDataset(
        data["x_train"],
        data["y_train"],
    )

    loader = DataLoader(
        dataset,
        batch_size=64,
        shuffle=True,
    )

    model.train()

    for epoch in range(epochs):

        total_loss = 0.0

        for xb, yb in loader:

            pred = model(xb)

            loss = criterion(pred, yb)

            optimizer.zero_grad()

            loss.backward()

            optimizer.step()

            total_loss += loss.item()

        print(
            f"epoch={epoch} "
            f"loss={total_loss:.4f}"
        )

    return {
        "model": model.state_dict(),
        "config": {
            "hidden_size": hidden_size,
            "lr": lr,
            "epochs": epochs,
        },
        "data": data,
    }


#
# Node 4
# Validation
#

@task
def validate_model(
    result
):

    print("Running validation")

    config = result["config"]

    model = SmallMLP(
        hidden=config["hidden_size"]
    )

    model.load_state_dict(
        result["model"]
    )

    model.eval()

    data = result["data"]

    x_val = data["x_val"]
    y_val = data["y_val"]

    with torch.no_grad():

        pred = model(x_val)

        mse = nn.functional.mse_loss(
            pred,
            y_val,
        ).item()

    return {
        "config": config,
        "val_mse": mse,
    }


#
# Compare all pipelines
#

@task
def compare_models(
    *results,
):

    print("\n=== MODEL COMPARISON ===")

    best = None

    for r in results:

        print(
            f"hidden={r['config']['hidden_size']} "
            f"mse={r['val_mse']:.6f}"
        )

        if best is None:
            best = r
        elif r["val_mse"] < best["val_mse"]:
            best = r

    print("\nBEST MODEL")
    print(best)

    return best


#
# Shared upstream pipeline
#

feature_node = build_node(load_features)

normalized_node = build_node(normalize_features, feature_node)


#
# Pipeline A
#

train_a = build_node(
    train_model,
    normalized_node,
    hidden_size=16,
    lr=1e-3,
    epochs=5,
)

validate_a = build_node(validate_model, train_a)


#
# Pipeline B
#

train_b = build_node(
    train_model,
    normalized_node,
    hidden_size=64,
    lr=1e-3,
    epochs=5,
)

validate_b = build_node(validate_model, train_b)


#
# Pipeline C
#

train_c = build_node(
    train_model,
    normalized_node,
    hidden_size=128,
    lr=1e-4,
    epochs=5,
)

validate_c = build_node(validate_model, train_c)


#
# Final DAG sink
#

final_graph = build_node(
    compare_models,
    validate_a,
    validate_b,
    validate_c
)


# ============================================================
# EXECUTION
# ============================================================

if __name__ == "__main__":

    print(feature_node)
    print("=======")
    print(normalized_node)
    pipeline = Pipeline(
        outputs={
            "model": final_graph,
        },
        name="example_ml",
    )

    plan = Planner.compile(pipeline)
    engine = Engine(RayExecutor())
    
    best_model = engine.gather(engine.run(plan, Context(date='2026-06-01')))
    print("\n=== ALL RESULTS ===")
    print(best_model)
