from prefect import flow


@flow(name="myflow")
def pipe2():
    print("hi")
    return None
