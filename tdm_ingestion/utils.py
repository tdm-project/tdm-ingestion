import importlib


def import_class(class_path: str):
    class_path_splitted = class_path.split('.')
    module = '.'.join(class_path_splitted[:-1])
    cls = class_path_splitted[-1]
    return getattr(importlib.import_module(module), cls)
