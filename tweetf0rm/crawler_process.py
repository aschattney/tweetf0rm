#!/usr/bin/python
# -*- coding: utf-8 -*-
#
import logging
from multiprocessing import Process
from redis_helper import CrawlerQueue
logger = logging.getLogger(__name__)

# MAX_QUEUE_SIZE = 32767
class CrawlerProcess(Process):

    def __init__(self, node_id, crawler_id, redis_config, handlers):
        super(CrawlerProcess, self).__init__()
        self.node_id = node_id
        self.crawler_id = crawler_id
        self.redis_config = redis_config
        self.crawler_queue = CrawlerQueue(node_id, crawler_id, redis_config=redis_config)
        self.crawler_queue.clear()
        self.handlers = handlers
        logger.debug("number of handlers attached: %d" % (len(handlers)))

    def get_crawler_id(self):
        return self.crawler_id

    def enqueue(self, request):
        # self.queue.put(request, block=True)
        self.crawler_queue.put(request)
        return True

    def get_cmd(self):
        # return  self.queue.get(block=True)
        return self.crawler_queue.get(block=True)

    def get_queue_size(self):
        self.crawler_queue.qsize()

    def run(self):
        pass
