from prefect import flow, task


@flow(name='myflow')
def pipe2():
    print("hi")
    return None

