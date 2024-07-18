class ToolsRegistryBase(type):
    """
    A metaclass for registering model classes.

    This metaclass automatically registers any class that uses it in a
    dictionary, making it easy to look up model classes by their name.

    Attributes:
        TOOLS_REGISTRY (dict): A class-level dictionary that maps model class names
                               (in lowercase) to the model classes themselves.
    """
    TOOLS_REGISTRY = {}

    def __new__(cls, name, bases, attrs):
        """
        Creates a new class instance and registers it in the MODEL_REGISTRY.

        Args:
            name (str): The name of the new class.
            bases (tuple): The base classes of the new class.
            attrs (dict): The attributes of the new class.

        Returns:
            type: The newly created class, now registered in MODEL_REGISTRY.
        """
        new_cls = super().__new__(cls, name, bases, attrs)
        if "filter_name" in attrs.keys():
            if attrs["filter_name"] is not None:
                cls.TOOLS_REGISTRY[new_cls.__name__.lower()] = new_cls
        else:
            cls.TOOLS_REGISTRY[new_cls.__name__.lower()] = new_cls
        return new_cls

    @classmethod
    def get(cls, name):
        return cls.TOOLS_REGISTRY.get(name.lower())

    @classmethod
    def register(cls, name):
        def wrapper(model_cls):
            cls.TOOLS_REGISTRY[name] = model_cls
            return model_cls
        return wrapper

    def __contains__(self, item):
        return item in self.TOOLS_REGISTRY

    def __iter__(self):
        return iter(self.TOOLS_REGISTRY)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.TOOLS_REGISTRY})"

    __str__ = __repr__
