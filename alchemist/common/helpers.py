from importlib import import_module


def import_object(path):
    parent_path = '.'.join(path.split('.')[:-1])  # assume the last part is the class name
    class_name = path.split('.')[-1]
    obj = getattr(import_module(parent_path), class_name)
    return obj


def import_class(path):
    parent_path = '.'.join(path.split('.')[:-1])  # assume the last part is the class name
    class_name = path.split('.')[-1]
    class_ = getattr(import_module(parent_path), class_name)
    return class_
