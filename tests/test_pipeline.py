from xray import Pipeline, task, Engine, LocalExecutor, Context, Planner

def test_local_pipeline():
    @task
    def generate_data():
        return [1, 2, 3]

    @task
    def train_model(data):
        return sum(data)

    data = generate_data()
    model_node = train_model(data)

    pipeline = Pipeline(
        name="training",
        outputs={
            "data": data,
            "model": model_node,
        },
    )

    plan = Planner.compile(pipeline)
    engine = Engine(
        executor=LocalExecutor(),
    )

    handles = engine.run(plan, Context(date="2024-01-01"))
    result = engine.gather(handles)

    assert result["data"] == [1, 2, 3]
    assert result["model"] == 6