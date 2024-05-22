from prefect import task, flow

@task()
def task_example():
    return "Hello World"

@flow(name="FILL_IN_FLOW_NAME_HERE!")
def main_flow():
    """
    Here you write your main flow code and call your tasks and/or subflows.
    """
    pass
