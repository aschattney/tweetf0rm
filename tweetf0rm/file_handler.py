#!/usr/bin/env python
# -*- coding: utf-8 -*-

# file_handler.py:
# handler that's collects the data, and write to the disk on a separate thread;

import logging
from base_handler import BaseHandler
import futures, os
from utils import full_stack

logger = logging.getLogger(__name__)

FLUSH_SIZE = 100


class FileHandler(BaseHandler):
    def __init__(self, output_folder='./data'):
        super(FileHandler, self).__init__()
        self.output_folder = os.path.abspath(output_folder)
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

        for bucket in self.buckets:
            bucket_folder = os.path.abspath('%s/%s' % (self.output_folder, bucket))
            if not os.path.exists(bucket_folder):
                os.makedirs(bucket_folder)

    def need_flush(self, bucket):
        return len(self.buffer[bucket]) > FLUSH_SIZE

    def flush(self, bucket):

        with futures.ProcessPoolExecutor(max_workers=3) as executor:
            # for each bucket it's a dict,
            # where the key needs to be the file name;
            # and the value is a list of json encoded value
            for bucket, items in self.buffer.iteritems():

                if len(items) > 0:
                    f = executor.submit(self.flush_file, bucket, items)

                    # send to a different process to operate, clear the buffer
                    self.clear(bucket)

                    # self.futures.append(f)

        return True

    def flush_file(self, bucket, items):
        try:
            bucket_folder = os.path.abspath('%s/%s' % (self.output_folder, bucket))

            for k, lines in items.iteritems():
                filename = os.path.abspath('%s/%s' % (bucket_folder, k))
                with open(filename, 'ab+') as f:
                    for line in lines:
                        f.write('%s\n' % line)

                logger.debug("flushed %d lines to %s" % (len(lines), filename))

        except:
            logger.error(full_stack())

        return True
