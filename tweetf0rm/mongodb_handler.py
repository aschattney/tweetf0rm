#!/usr/bin/env python
# -*- coding: utf-8 -*-

# write_to_handler.py:
# handler that's collects the data, and write to the disk on a separate thread;

import logging
from errors import NotImplemented

logger = logging.getLogger(__name__)


class MongoDBHandler(object):
    def __init__(self):
        raise NotImplemented("placeholder, not implemented yet...")
