from mysql_handler import MySQLHandler
import copy

__all__ = ["MySQLHandler"]
avaliable_handlers = copy.copy(__all__)


def create_handler(handler_config=None, redis_config=None):
    if handler_config is None:
        raise Exception("no config provided for handler!")
    elif handler_config["name"] == "MySQLHandler":
        return MySQLHandler(handler_config["args"], redis_config)
    else:
        raise Exception("unknown handler specified in config file!")


def create_handlers(handler_configs=None, redis_config=None):
    handlers = []
    for handler_config in handler_configs:
        handlers.append(create_handler(handler_config, redis_config))
    return handlers
