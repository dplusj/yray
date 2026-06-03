from dag.task import task
from dag.context import Context
from dag.node import build_node

from dag.pipeline import Pipeline
from executors.plan import Planner
from executors.engine import Engine
from executors.ray_executor import RayExecutor
import random
import time
from datetime import datetime, timedelta

@task
def load_prices() -> list[int]:

    print("loading")

    return [1, 2, 3, 4, 5]


@task
def feature_engineering(
    prices: list[int],
) -> list[int]:

    print("features")
    #time.sleep(random.uniform(0.5, 1.5))

    return [x * 2 + random.randint(1, 10) for x in prices]


@task
def train_model(
    context: Context,
) -> str:

    print("training")

    return f"model-{context.date}"


@task
def compute_metrics(
    features: list[int],
) -> dict:

    print("metrics")

    return {
        "count": len(features),
        "mean": sum(features) / len(features),
    }

if __name__ == "__main__":

    price_node = build_node(load_prices)
    feature_node = build_node(feature_engineering, price_node)
    model_node = build_node(train_model)
    metrics_node = build_node(compute_metrics, feature_node)

    pipeline = Pipeline(
        outputs={
            "model": model_node,
            "metrics": metrics_node,
        },
        name="example_pipeline",
    )

    plan = Planner.compile(pipeline)
    engine = Engine(RayExecutor())
    # TODO: run longer period to check the change of the performance
    start_time = time.time()
    start = datetime(2024, 1, 1)

    dates = [
        (start + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(10)
    ]
    handles = []
    for d in dates:
        handles.append(engine.run(plan, Context(date=d)))
    results = [
        engine.gather(h)
        for h in handles
    ]
    print("\n=== ALL RESULTS ===")
    print(results)
    print(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")



