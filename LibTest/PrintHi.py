class Hello:

    def __init__(self, name:str = "Databricks"):
        pass
        self.greeting = f"Hello {name}!"


    def sayHi(self) -> str:
        return self.greeting
