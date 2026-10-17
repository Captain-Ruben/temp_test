from prefect import flow, task

@task
def say_hello(name: str):
    print(f"Hallo {name}!")
    return f"Hello {name}"

@flow(name="my-test-flow")
def my_flow(name: str = "World"):
    print("Prefect testflow is gestart!")
    result = say_hello(name)
    return result