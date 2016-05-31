#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
import os
sys.path.insert(0, "D:\\Anaconda3\\envs\\python2.7\\Lib\\site-packages")
import argparse
import json
import pprint
from errors import InvalidConfig
from redis_helper import NodeQueue, NodeCoordinator
from utils import full_stack, node_id
from scheduler import Scheduler
import time
import os
import tarfile
import futures

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s-[%(asctime)s][%(module)s][%(funcName)s][%(lineno)d]: %(message)s')
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)


def check_config(config):
    if 'apikeys' not in config or 'redis_config' not in config or 'mysql_config' not in config:
        raise InvalidConfig("something is wrong with your config file... you have to have redis_config and apikeys and mysql_config")


def start_server(config, proxies):
    import copy

    check_config(config)
    config = copy.copy(config)

    folders_to_create = []
    buckets = ["tweets", "followers", "follower_ids", "friends", "friend_ids", "timelines", "users", "geos"]

    this_node_id = node_id()
    node_queue = NodeQueue(this_node_id, redis_config=config['redis_config'])
    node_queue.clear()

    scheduler = Scheduler(this_node_id, config=config, proxies=proxies)

    logger.info('starting node_id: %s' % this_node_id)

    node_coordinator = NodeCoordinator(config['redis_config'])
    # node_coordinator.clear()

    # the main event loop, actually we don't need one,
    # since we can just join on the crawlers and don't stop until a terminate command is issued to each crawler;
    # but we need one to report the status of each crawler and perform the tarball tashs...

    pre_time = time.time()
    last_load_balancing_task_ts = time.time()
    while True:

        if time.time() - pre_time > 120:
            logger.info(pprint.pformat(scheduler.crawler_status()))
            pre_time = time.time()
            if scheduler.is_alive():
                cmd = {'cmd': 'CRAWLER_FLUSH'}
                scheduler.enqueue(cmd)

        # block, the main process...for a command
        if not scheduler.is_alive():
            logger.info("no crawler is alive... waiting to recreate all crawlers...")
            time.sleep(120)  # sleep for two minutes and retry
            continue

        if time.time() - last_load_balancing_task_ts > 1800:  # try to balance the local queues every 30 mins
            last_load_balancing_task_ts = time.time()
            cmd = {'cmd': 'BALANCING_LOAD'}
            scheduler.enqueue(cmd)

        cmd = node_queue.get(block=True, timeout=360)

        if cmd:
            scheduler.enqueue(cmd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        help="config.json that contains a) twitter api keys; b) redis connection string;",
                        required=True)
    parser.add_argument('-p', '--proxies', help="the proxies.json file")

    args = parser.parse_args()

    proxies = None
    if args.proxies:
        args.proxies = args.proxies.rstrip()
        with open(os.path.abspath(args.proxies), 'rb') as proxy_f:
            proxies = json.load(proxy_f)['proxies']

    if args.config:
        args.config = args.config.rstrip()

    with open(os.path.abspath(args.config), 'rb') as config_f:
        config = json.load(config_f)

        try:
            start_server(config, proxies)
        except KeyboardInterrupt:
            print()
            logger.error('You pressed Ctrl+C!')
            pass
        except Exception as exc:
            logger.error(exc)
            logger.error(full_stack())
        finally:
            pass
