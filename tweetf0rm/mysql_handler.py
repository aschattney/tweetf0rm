#!/usr/bin/env python
# -*- coding: utf-8 -*-

# write_to_handler.py:
# handler that's collects the data, and write to the disk on a separate thread;

import logging
from urlparse import urlparse

from base_handler import BaseHandler
from redis_helper import NodeQueue
import pymysql
import json
import futures
import httplib
import re
import time
import requests
from datetime import datetime
from utils import node_id, hash_cmd
from utils import full_stack

logger = logging.getLogger(__name__)


def create_connection(connection_args):
    ca = connection_args
    MySQLHandler.connection = pymysql.connect(host=ca['host'], port=ca['port'],
                                              user=ca['user'], passwd=ca['password'],
                                              db=ca['db'], charset=ca['charset'],
                                              cursorclass=pymysql.cursors.DictCursor)


def flush_bucket(bucket, items):
    try:
        cursor = MySQLHandler.connection.cursor()
        for k, lines in items.iteritems():
            for line in lines:
                json_data = json.loads(line)
                execute_query(cursor, bucket, json_data)
            MySQLHandler.connection.commit()
            cursor = MySQLHandler.connection.cursor()
        MySQLHandler.connection.commit()
        logger.info("flushed" + bucket)
    except Exception as e:
        logger.error(e)
    return True


def insert_user_if_not_exists(cursor, user):
    if not user_already_exists(cursor, user['id']):
        if int(user['geo_enabled']) == 1:
            geo_enabled = 0
        else:
            geo_enabled = -1
        insert_user(cursor, user, geo_enabled)
        return True
    else:
        return False


def execute_query(cursor, bucket, json_data):
    if bucket == "tweets":
        json_data = modify_json_data(json_data)
        user = json_data['user']
        create_new_command = insert_user_if_not_exists(cursor, user)
        insert_tweet(cursor, json_data)
        if create_new_command:
            cmd = {"cmd": "CRAWL_USER", "user_id": user['id'], "bucket": "users"}
            cmd['cmd_hash'] = hash_cmd(cmd)
            MySQLHandler.node_queue.put(cmd)
    elif bucket == "users":
        if "profile_location" in json_data:
            profile_location = json_data["profile_location"]
            if profile_location is not None and profile_location != 'null':
                update_user_query = "UPDATE nist_user SET loc_id = %s WHERE id = %s"
                cursor = MySQLHandler.connection.cursor()
                cursor.execute(update_user_query, [profile_location['id'], json_data['id']])
                MySQLHandler.connection.commit()
                if not geo_location_already_exists(cursor, profile_location['id']):
                    cmd = {"cmd": "CRAWL_GEO", "geo_id": profile_location['id'], "bucket": "geos"}
                    cmd['cmd_hash'] = hash_cmd(cmd)
                    MySQLHandler.node_queue.put(cmd)
    elif bucket == "geos":

        json_data['geotagCount'] = 0
        if 'geotagCount' in json_data['attributes']:
            json_data['geotagCount'] = json_data['attributes']['geotagCount']

        centroid = [0, 0]
        if "centroid" in json_data:
            centroid = json_data['centroid']

        json_data['latitude'] = centroid[1]
        json_data['longitude'] = centroid[0]
        statement = InsertStatement()
        columns = ["id", "latitude", "longitude", "name", "full_name", "country", "country_code", "place_type",
                   "geotagCount"]
        for column in columns:
            statement.add_param(column, json_data[column])
        cursor.execute(statement.build_query("nist_location"), statement.build_values())


def modify_json_data(json_data):
    json_data['is_retweet'] = True if "retweeted_status" in json_data else False
    json_data['created_at'] = str(json_data['created_at']).replace(' 24:', " 00:")
    json_data['user_id'] = json_data['user']['id']
    json_data['original_tweet_id'] = json_data['retweeted_status']['id'] if 'retweeted_status' in json_data else 0
    json_data['created_at'] = datetime.strptime(json_data['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    json_data['created_at'] = json_data['created_at'].strftime('%Y-%m-%d %H:%M:%S +0000')
    json_data['created_at'] = json_data['created_at'].replace(" +0000", "")

    json_data['user']['created_at'] = datetime.strptime(json_data['user']['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    json_data['user']['created_at'] = json_data['user']['created_at'].strftime('%Y-%m-%d %H:%M:%S +0000')
    json_data['user']['created_at'] = json_data['user']['created_at'].replace(" +0000", "")

    json_data['short_link'] = json_data['entities']['urls'][0]['url'] if len(json_data['entities']['urls']) > 0 else None
    json_data['long_link'] = json_data['entities']['urls'][0]['expanded_url'] if len(json_data['entities']['urls']) > 0 else None
    if len(json_data['entities']['urls']) > 0:
        youtube_video_id = get_youtube_video_id(json_data['short_link'])
        if youtube_video_id is None:
            youtube_video_id = get_youtube_video_id(json_data['long_link'])
        if youtube_video_id is None:
            youtube_video_id = ""
        json_data['youtube_video_id'] = youtube_video_id
    else:
        json_data['youtube_video_id'] = None
    return json_data


def tweet_already_exists(cursor, tweet_id, original_tweet_id):
    tweet_already_exists_query = "SELECT COUNT(*) as amount FROM nist_tweet WHERE id=%s AND original_tweet_id = %s"
    cursor.execute(tweet_already_exists_query, [tweet_id, original_tweet_id])
    row = cursor.fetchone()
    amount = row['amount']
    return amount > 0


def user_already_exists(cursor, user_id):
    user_already_exists_query = "SELECT COUNT(*) AS amount FROM nist_user WHERE id = %s"
    cursor.execute(user_already_exists_query, [user_id])
    row = cursor.fetchone()
    return row['amount'] > 0


def geo_location_already_exists(cursor, geo_id):
    geo_location_already_exists_query = "SELECT COUNT(id) as amount FROM nist_location WHERE id = %s"
    cursor.execute(geo_location_already_exists_query, [geo_id])
    row = cursor.fetchone()
    return row['amount'] > 0


def insert_user(cursor, user, geo_enabled):
    user['user_name'] = user['name']
    if not user_already_exists(cursor, user['id']):
        statement = InsertStatement()
        columns = ["id", "screen_name", "user_name", "verified", "followers_count", "listed_count", "statuses_count",
                   "friends_count", "location", "favourites_count", "created_at"]
        for column in columns:
            statement.add_param(column, user[column])
        statement.add_param("loc_id", geo_enabled)
        cursor.execute(statement.build_query("nist_user"), statement.build_values())


def insert_tweet(cursor, json_data):
    if not tweet_already_exists(cursor, json_data['id'], json_data['original_tweet_id']):
        statement = InsertStatement()
        columns = ["id", "original_tweet_id", "user_id", "text", "created_at", "short_link", "long_link", "retweeted",
                   "is_retweet", "retweet_count", "favorite_count", "in_reply_to_user_id", "in_reply_to_status_id",
                   "favorited", "youtube_video_id"]
        for column in columns:
            statement.add_param(column, json_data[column])
        cursor.execute(statement.build_query("nist_tweet"), statement.build_values())


def get_youtube_video_id(link):
    short_link_regex = "^.*(youtu.be/|v/|u/\\w/|embed/|watch\\?v=|&v=)([^#&\\?]*).*"
    long_link_regex = "http(?:s)?://(?:www\\.)?youtube\\.com/.*?watch%3Fv%3D([\\d\\w\\-_]+).*"
    id = perform_regex(short_link_regex, link, 2)
    if id is not None:
        return id
    else:
        return perform_regex(long_link_regex, link, 1)


def perform_regex(regex, link, group):
    m = re.search(regex, link)
    if m is not None:
        return m.group(group)
    else:
        return None


def create_node_queue(redis_config):
    MySQLHandler.node_queue = NodeQueue(node_id(), redis_config=redis_config)


class MySQLHandler(BaseHandler):
    node_queue = None
    FLUSH_SIZE = 100
    connection = None
    connection_args = {"host": 'localhost',
                       "port": 3306,
                       "user": "root",
                       "password": "",
                       "db": "twitter_crawler",
                       "charset": "utf8mb4"}

    def __init__(self, connection_args, redis_config):
        super(MySQLHandler, self).__init__()
        if connection_args is not None:
            self.validate_connection_args(connection_args)
            create_connection(connection_args)
            create_node_queue(redis_config)
        else:
            raise Exception("no arguments provided for MySQLHandler")

    def validate_connection_args(self, connection_args):
        keys = ["host", "port", "user", "db", "charset"]
        for key in keys:
            if key not in connection_args or not str(connection_args[key]).strip():
                raise Exception(key + " missing in configuration args for MySQLHandler")

    def append(self, data=None, bucket=None, key='current_timestamp'):
        super(MySQLHandler, self).append(data, bucket, key)

    def need_flush(self, bucket):
        size = 0
        for key, tweets in self.buffer[bucket].iteritems():
            size += len(tweets)
        return size > self.FLUSH_SIZE

    def flush(self, bucket):
        with futures.ProcessPoolExecutor(max_workers=1) as executor:
            # for each bucket it's a dict,
            # where the key needs to be the file name;
            # and the value is a list of json encoded value
            for bucket, items in self.buffer.iteritems():
                if len(items) > 0:
                    f = executor.submit(flush_bucket, bucket, items)
                    # send to a different process to operate, clear the buffer
                    self.clear(bucket)
                    flush_bucket(bucket, items)
                    # self.clear(bucket)
                    self.futures.append(f)
        return True


class Statement(object):
    def __init__(self):
        self.params = dict()

    def add_param(self, key, value):
        if key not in self.params:
            self.params[key] = value

    def build_columns(self):
        result = []
        for key, value in self.params.iteritems():
            result.append("`" + key + "`")
        return ",".join(map(str, result))

    def build_prepared_values(self):
        length = len(self.params)
        result = []
        for position in range(0, length):
            result.append("%s")
        return ",".join(map(str, result))

    def build_values(self):
        result = []
        for key, value in self.params.iteritems():
            result.append(value)
        return result


class InsertStatement(Statement):
    def build_query(self, table_name):
        query = "INSERT IGNORE INTO %s (%s) VALUES (%s)" % (
            table_name, self.build_columns(), self.build_prepared_values())
        return query
