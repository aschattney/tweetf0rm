from inmemory_handler import InMemoryHandler
from file_handler import FileHandler
from mysql_handler import MySQLHandler
import copy

__all__ = ["InMemoryHandler", "FileHandler", "MySQLHandler"]
avaliable_handlers = copy.copy(__all__)


def create_handler(handler_config=None, redis_config=None):
    if handler_config is None:
        raise Exception("no config provided for handler!")
    if handler_config["name"] == "FileHandler":
        return FileHandler(**handler_config["args"])
    elif handler_config["name"] == "MySQLHandler":
        return MySQLHandler(handler_config["args"], redis_config)
    else:
        raise Exception("unknown handler specified in config file!")


def create_handlers(handler_configs=None, redis_config=None):
    handlers = []
    for handler_config in handler_configs:
        handlers.append(create_handler(handler_config, redis_config))
    return handlers
