#!/usr/bin/env python
# -*- coding: utf-8 -*-

# stream.py:

# KeywordsStreamer: straightforward class that tracks a list of keywords;
# most of the jobs are done by TwythonStreamer;
# the only thing this is just attach a WriteToHandler so results will be saved

import json
import logging
from twython import TwythonStreamer
from errors import MissingArgs

logger = logging.getLogger(__name__)

class Streamer(TwythonStreamer):
    def __init__(self, *args, **kwargs):

        # Constructor with apikeys
        #  apikeys: apikeys
        logger.info(kwargs)
        import copy
        apikeys = copy.copy(kwargs.pop('apikeys', None))
        if not apikeys:
            raise MissingArgs('apikeys is missing')

        self.apikeys = copy.copy(apikeys)  # keep a copy
        self.counter = 0
        self.write_to_handlers = kwargs.pop('write_to_handlers', None)
        kwargs.update(apikeys)

        super(Streamer, self).__init__(*args, **kwargs)

    def on_success(self, data):
        if 'text' in data:
            #self.counter += 1
            #if self.counter % 50 == 0:
            #    logger.info("received: %d" % self.counter)
            # logger.debug(data['text'].encode('utf-8'))
            for write_to_handler in self.write_to_handlers:
                write_to_handler.append(json.dumps(data), "tweets", "values")

    def on_error(self, status_code, data):
        logger.warn(status_code)

    def close(self):
        self.disconnect()
