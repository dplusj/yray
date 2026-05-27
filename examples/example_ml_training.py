from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Any
import uuid

import torch
import torch.nn as nn
from torch.utils.data import TensorDataset, DataLoader

from executors.ray_executor import RayExecutor
from dag.node import DAGNode
from executors.engine import Engine
from dag.api import task


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

def validate_model(result):

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

def compare_models(*results):

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


# ============================================================
# DAG BUILD
# ============================================================

load_node = DAGNode(load_features)

normalize_node = DAGNode(normalize_features)

train_node = DAGNode(train_model)

validate_node = DAGNode(validate_model)

compare_node = DAGNode(compare_models)


#
# Shared upstream pipeline
#

dataset = load_node()

normalized = normalize_node(dataset)


#
# Pipeline A
#

train_a = train_node(
    normalized,
    hidden_size=16,
    lr=1e-3,
    epochs=5,
)

validate_a = validate_node(train_a)


#
# Pipeline B
#

train_b = train_node(
    normalized,
    hidden_size=64,
    lr=1e-3,
    epochs=5,
)

validate_b = validate_node(train_b)


#
# Pipeline C
#

train_c = train_node(
    normalized,
    hidden_size=128,
    lr=1e-4,
    epochs=5,
)

validate_c = validate_node(train_c)


#
# Final DAG sink
#

final_graph = compare_node(
    validate_a,
    validate_b,
    validate_c,
)


# ============================================================
# EXECUTION
# ============================================================

if __name__ == "__main__":

    executor = RayExecutor()

    runtime = Engine(executor)

    best_model = runtime.execute(
        final_graph
    )

    print("\nFINAL RESULT")
    print(best_model)
