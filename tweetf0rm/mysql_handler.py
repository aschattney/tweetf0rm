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


def flush_bucket(connection, bucket, items):
    try:
        cursor = connection.cursor()
        for k, lines in items.iteritems():
            for line in lines:
                json_data = json.loads(line)
                logger.info(line)
                execute_query(cursor, bucket, json_data)
            connection.commit()
            cursor = connection.cursor()
        connection.commit()
    except Exception as e:
        logger.error(e)

    return True


def execute_query(cursor, bucket, json_data):
    if bucket == "tweets":
        json_data = modify_json_data(json_data)
        query = "INSERT INTO nist_user (id, screen_name, user_name, loc_id) VALUES (%s, %s, %s, %s)"
        insert_user(cursor, query, json_data)
        logger.info("after execute")
        query = "INSERT INTO nist_tweet (id, user_id, " \
                "short_link, long_link, text, created_at, retweeted, " \
                "retweet_count, is_retweet, favorited) VALUES " \
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        insert_tweet(cursor, query, json_data)
    else:
        raise Exception("unknown bucket: %s" % bucket)


def modify_json_data(json_data):
    if "retweet_status" in json_data:
        json_data['is_retweet'] = True
    else:
        json_data['is_retweet'] = False
    json_data['created_at'] = str(json_data['created_at']).replace(' 24:', " 00:")
    json_data['user_id'] = json_data['user']['id']
    logger.info(json_data['created_at'])
    json_data['created_at'] = datetime.strptime(json_data['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    json_data['created_at'] = json_data['created_at'].strftime('%Y-%m-%d %H:%M:%S +0000')
    json_data['created_at'] = json_data['created_at'].replace(" +0000", "")
    short_link = None
    if "url" in json_data:
        short_link = json_data["url"]
    else:
        short_link = get_short_link_from_tweet_text(json_data['text'])
    if short_link is not None:
        json_data['short_link'] = short_link
        json_data['long_link'] = unshorten_url(short_link)
    else:
        json_data['short_link'] = ''
        json_data['long_link'] = ''
    return json_data


def tweet_already_exists(cursor, tweet_id):
    tweet_already_exists_query = "SELECT COUNT(*) as amount FROM nist_tweet WHERE id=%s"
    cursor.execute(tweet_already_exists_query, [tweet_id])
    row = cursor.fetchone()
    amount = row['amount']
    return amount > 0


def user_already_exists(cursor, user_id):
    user_already_exists_query = "SELECT COUNT(*) AS amount FROM nist_user WHERE id = %s"
    cursor.execute(user_already_exists_query, [user_id])
    row = cursor.fetchone()
    return row['amount'] > 0


def insert_user(cursor, query, json_data):
    if not user_already_exists(cursor, json_data['user_id']):
        cursor.execute(query,
                       [json_data['user_id'], json_data['user']['screen_name'], json_data['user']['name'], 0])


def insert_tweet(cursor, query, json_data):
    if not tweet_already_exists(cursor, json_data['id']):
        cursor.execute(query,
                       [json_data['id'], json_data['user_id'],
                        json_data['short_link'],
                        json_data['long_link'], json_data['text'], json_data['created_at'],
                        json_data['retweeted'],
                        json_data['retweet_count'], json_data['is_retweet'], json_data['favorited']])


def unshorten_url(url):
    try:
        r = requests.head(url, timeout=8.0, allow_redirects=False)
    except:  # This is the correct syntax
        return url
    parsed = urlparse(r.url)
    h = None
    if parsed.scheme == 'https':
        h = httplib.HTTPS(parsed.netloc)
    else:
        h = httplib.HTTPConnection(parsed.netloc)
    resource = parsed.path
    if parsed.query != "":
        resource += "?" + parsed.query
    try:
        h.request('HEAD', resource, None, {'User-Agent': 'Mozilla/5.0 '
                                                         '(Windows NT 10.0; WOW64) '
                                                         'AppleWebKit/537.36 (KHTML, like Gecko)'
                                                         ' Chrome/48.0.2564.116 Safari/537.36'})
        response = h.getresponse()
        if int(response.status / 100) == 3 and response.getheader('Location'):
            next_link = response.getheader('Location')
            return unshorten_url(next_link)  # changed to process chains of short urls
        else:
            return url
    except:
        return url


def get_short_link_from_tweet_text(tweet_text):
    m = re.search(r'(?:https?://)?(?:(?:0rz\.tw)|(?:1link\.in)|(?:1url\.com)|(?:2\.gp)|(?:2big\.at)|'
                  r'(?:2tu\.us)|(?:3\.ly)|'
                  r'(?:307\.to)|(?:4ms\.me)|(?:4sq\.com)|(?:4url\.cc)|(?:6url\.com)|(?:7\.ly)|(?:a\.gg)|'
                  r'(?:a\.nf)|(?:aa\.cx)|'
                  r'(?:abcurl\.net)|(?:ad\.vu)|(?:adf\.ly)|(?:adjix\.com)|(?:afx\.cc)|'
                  r'(?:all\.fuseurl.com)|(?:alturl\.com)|'
                  r'(?:amzn\.to)|(?:ar\.gy)|(?:arst\.ch)|(?:atu\.ca)|(?:azc\.cc)|(?:b23\.ru)|'
                  r'(?:b2l\.me)|(?:bacn\.me)|'
                  r'(?:bcool\.bz)|(?:binged\.it)|(?:bit\.ly)|(?:bizj\.us)|(?:bloat\.me)|'
                  r'(?:bravo\.ly)|(?:bsa\.ly)|'
                  r'(?:budurl\.com)|(?:canurl\.com)|(?:chilp\.it)|(?:chzb\.gr)|(?:cl\.lk)|'
                  r'(?:cl\.ly)|(?:clck\.ru)|'
                  r'(?:cli\.gs)|(?:cliccami\.info)|(?:clickthru\.ca)|(?:clop\.in)|'
                  r'(?:conta\.cc)|(?:cort\.as)|'
                  r'(?:cot\.ag)|(?:crks\.me)|(?:ctvr\.us)|(?:cutt\.us)|(?:dai\.ly)|'
                  r'(?:decenturl\.com)|(?:dfl8\.me)|'
                  r'(?:digbig\.com)|(?:digg\.com)|(?:disq\.us)|(?:dld\.bz)|'
                  r'(?:dlvr\.it)|(?:do\.my)|(?:doiop\.com)|'
                  r'(?:dopen\.us)|(?:easyuri\.com)|(?:easyurl\.net)|(?:eepurl\.com)|'
                  r'(?:eweri\.com)|(?:fa\.by)|'
                  r'(?:fav\.me)|(?:fb\.me)|(?:fbshare\.me)|(?:ff\.im)|(?:fff\.to)|'
                  r'(?:fire\.to)|(?:firsturl\.de)|'
                  r'(?:firsturl\.net)|(?:flic\.kr)|(?:flq\.us)|(?:fly2\.ws)|(?:fon\.gs)|'
                  r'(?:freak\.to)|(?:fuseurl\.com)|'
                  r'(?:fuzzy\.to)|(?:fwd4\.me)|(?:fwib\.net)|(?:g\.ro.lt)|(?:gizmo\.do)|'
                  r'(?:gl\.am)|(?:go\.9nl.com)|'
                  r'(?:go\.ign.com)|(?:go\.usa.gov)|(?:goo\.gl)|(?:goshrink\.com)|'
                  r'(?:gurl\.es)|(?:hex\.io)|'
                  r'(?:hiderefer\.com)|(?:hmm\.ph)|(?:href\.in)|(?:hsblinks\.com)|'
                  r'(?:htxt\.it)|(?:huff\.to)|'
                  r'(?:hulu\.com)|(?:hurl\.me)|(?:hurl\.ws)|(?:icanhaz\.com)|'
                  r'(?:idek\.net)|(?:ilix\.in)|'
                  r'(?:is\.gd)|(?:its\.my)|(?:ix\.lt)|(?:j\.mp)|(?:jijr\.com)|(?:kl\.am)|'
                  r'(?:klck\.me)|'
                  r'(?:korta\.nu)|(?:krunchd\.com)|(?:l9k\.net)|(?:lat\.ms)|(?:liip\.to)|'
                  r'(?:liltext\.com)|'
                  r'(?:linkbee\.com)|(?:linkbun\.ch)|(?:liurl\.cn)|(?:ln-s\.net)|'
                  r'(?:ln-s\.ru)|(?:lnk\.gd)|'
                  r'(?:lnk\.ms)|(?:lnkd\.in)|(?:lnkurl\.com)|(?:lru\.jp)|(?:lt\.tl)|'
                  r'(?:lurl\.no)|(?:macte\.ch)|'
                  r'(?:mash\.to)|(?:merky\.de)|(?:migre\.me)|(?:miniurl\.com)|'
                  r'(?:minurl\.fr)|(?:mke\.me)|'
                  r'(?:moby\.to)|(?:moourl\.com)|(?:mrte\.ch)|(?:myloc\.me)|'
                  r'(?:myurl\.in)|(?:n\.pr)|'
                  r'(?:nbc\.co)|(?:nblo\.gs)|(?:nn\.nf)|(?:not\.my)|(?:notlong\.com)|'
                  r'(?:nsfw\.in)|'
                  r'(?:nutshellurl\.com)|(?:nxy\.in)|(?:nyti\.ms)|(?:o-x\.fr)|'
                  r'(?:oc1\.us)|(?:om\.ly)|'
                  r'(?:omf\.gd)|(?:omoikane\.net)|(?:on\.cnn.com)|(?:on\.mktw.net)|'
                  r'(?:onforb\.es)|'
                  r'(?:orz\.se)|(?:ow\.ly)|(?:ping\.fm)|(?:pli\.gs)|(?:pnt\.me)|'
                  r'(?:politi\.co)|(?:post\.ly)|'
                  r'(?:pp\.gg)|(?:profile\.to)|(?:ptiturl\.com)|(?:pub\.vitrue.com)|'
                  r'(?:qlnk\.net)|(?:qte\.me)|'
                  r'(?:qu\.tc)|(?:qy\.fi)|(?:r\.im)|(?:rb6\.me)|(?:read\.bi)|(?:readthis\.ca)|'
                  r'(?:reallytinyurl\.com)|'
                  r'(?:redir\.ec)|(?:redirects\.ca)|(?:redirx\.com)|(?:retwt\.me)|(?:ri\.ms)|'
                  r'(?:rickroll\.it)|'
                  r'(?:riz\.gd)|(?:rt\.nu)|(?:ru\.ly)|(?:rubyurl\.com)|(?:rurl\.org)|'
                  r'(?:rww\.tw)|(?:s4c\.in)|'
                  r'(?:s7y\.us)|(?:safe\.mn)|(?:sameurl\.com)|(?:sdut\.us)|(?:shar\.es)|'
                  r'(?:shink\.de)|'
                  r'(?:shorl\.com)|(?:short\.ie)|(?:short\.to)|(?:shortlinks\.co.uk)|'
                  r'(?:shorturl\.com)|'
                  r'(?:shout\.to)|(?:show\.my)|(?:shrinkify\.com)|(?:shrinkr\.com)|'
                  r'(?:shrt\.fr)|(?:shrt\.st)|'
                  r'(?:shrten\.com)|(?:shrunkin\.com)|(?:simurl\.com)|(?:slate\.me)|'
                  r'(?:smallr\.com)|(?:smsh\.me)|'
                  r'(?:smurl\.name)|(?:sn\.im)|(?:snipr\.com)|(?:snipurl\.com)|'
                  r'(?:snurl\.com)|(?:sp2\.ro)|(?:spedr\.com)|'
                  r'(?:srnk\.net)|(?:srs\.li)|(?:starturl\.com)|(?:su\.pr)|'
                  r'(?:surl\.co.uk)|(?:surl\.hu)|(?:t\.cn)|(?:t\.co)|'
                  r'(?:t\.lh.com)|(?:ta\.gd)|(?:tbd\.ly)|(?:tcrn\.ch)|(?:tgr\.me)|'
                  r'(?:tgr\.ph)|(?:tighturl\.com)|'
                  r'(?:tiniuri\.com)|(?:tiny\.cc)|(?:tiny\.ly)|(?:tiny\.pl)|(?:tinylink\.in)|'
                  r'(?:tinyuri\.ca)|'
                  r'(?:tinyurl\.com)|(?:tl\.gd)|(?:tmi\.me)|(?:tnij\.org)|(?:tnw\.to)|'
                  r'(?:tny\.com)|(?:to\.ly)|'
                  r'(?:togoto\.us)|(?:totc\.us)|(?:toysr\.us)|(?:tpm\.ly)|(?:tr\.im)|'
                  r'(?:tra\.kz)|(?:trunc\.it)|'
                  r'(?:twhub\.com)|(?:twirl\.at)|(?:twitclicks\.com)|(?:twitterurl\.net)|'
                  r'(?:twitterurl\.org)|'
                  r'(?:twiturl\.de)|(?:twurl\.cc)|(?:twurl\.nl)|(?:u\.mavrev.com)|'
                  r'(?:u\.nu)|(?:u76\.org)|'
                  r'(?:ub0\.cc)|(?:ulu\.lu)|(?:updating\.me)|(?:ur1\.ca)|(?:url\.az)|(?:url\.co.uk)|'
                  r'(?:url\.ie)|'
                  r'(?:url360\.me)|(?:url4\.eu)|(?:urlborg\.com)|(?:urlbrief\.com)|'
                  r'(?:urlcover\.com)|'
                  r'(?:urlcut\.com)|(?:urlenco\.de)|(?:urli\.nl)|(?:urls\.im)|'
                  r'(?:urlshorteningservicefortwitter\.com)|'
                  r'(?:urlx\.ie)|(?:urlzen\.com)|(?:usat\.ly)|(?:use\.my)|(?:vb\.ly)|'
                  r'(?:vgn\.am)|(?:vl\.am)|(?:vm\.lc)|'
                  r'(?:w55\.de)|(?:wapo\.st)|(?:wapurl\.co.uk)|(?:wipi\.es)|(?:wp\.me)|'
                  r'(?:x\.vu)|(?:xr\.com)|(?:xrl\.in)|'
                  r'(?:xrl\.us)|(?:xurl\.es)|(?:xurl\.jp)|(?:y\.ahoo.it)|(?:yatuc\.com)|'
                  r'(?:ye\.pe)|(?:yep\.it)|'
                  r'(?:yfrog\.com)|(?:yhoo\.it)|(?:yiyd\.com)|(?:youtu\.be)|(?:yuarel\.com)|'
                  r'(?:z0p\.de)|(?:zi\.ma)|'
                  r'(?:zi\.mu)|(?:zipmyurl\.com)|(?:zud\.me)|(?:zurl\.ws)|(?:zz\.gd)|'
                  r'(?:zzang\.kr))/[A-Z,a-z,0-9]*', tweet_text)
    if m is not None:
        return m.group(0)
    else:
        return None


class MySQLHandler(BaseHandler):
    FLUSH_SIZE = 100
    node_queue = None
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
            self.connection_args = connection_args
            self.connection = self.create_connection()
            self.node_queue = NodeQueue(node_id(), redis_config)
        else:
            raise Exception("no arguments provided for MySQLHandler")

    def validate_connection_args(self, connection_args):
        keys = ["host", "port", "user", "db", "charset"]
        for key in keys:
            if key not in connection_args or not str(connection_args[key]).strip():
                raise Exception(key + " missing in configuration args for MySQLHandler")

    def create_connection(self):
        ca = self.connection_args
        return pymysql.connect(host=ca['host'], port=ca['port'],
                               user=ca['user'], passwd=ca['password'],
                               db=ca['db'], charset=ca['charset'], cursorclass=pymysql.cursors.DictCursor)

    def append(self, data=None, bucket=None, key='current_timestamp'):
        super(MySQLHandler, self).append(data, bucket, key)
        json_data = json.loads(data)
        logger.info("MySQLHandler->append")
        # maybe move this chunk of code to a new command handler class, but not sure yet ..
        if bucket == "tweets":
            user = json_data['user']
            if int(user['geo_enabled']) == 1:
                # ToDo: check if user already exists in database
                cmd = {"cmd": "FETCH_USER", "user_id": user['id'], "bucket": {"value": "user"}}
                logger.info(cmd)
                cmd['cmd_hash'] = hash_cmd(cmd)
                self.node_queue.put(cmd)
        elif bucket == "users":
            if "profile_location" in json_data:
                profile_location = json_data["profile_location"]
                if profile_location is not None and profile_location != 'null':
                    cmd = {"cmd": "FETCH_GEO", "geo_id": profile_location['id'], "bucket": {"value": "geo"}}
                    logger.info(cmd)
                    cmd['cmd_hash'] = hash_cmd(cmd)
                    self.node_queue.put(cmd)

    def need_flush(self, bucket):
        size = 0
        for user_id, tweets in self.buffer[bucket].iteritems():
            size += len(tweets)
        logger.info("Buffer Size for bucket: " + bucket + " = " + str(size))
        return size > self.FLUSH_SIZE

    def flush(self, bucket):
        logger.info(bucket)
        # with futures.ProcessPoolExecutor(max_workers=1) as executor:
        # for each bucket it's a dict,
        # where the key needs to be the file name;
        # and the value is a list of json encoded value
        for bucket, items in self.buffer.iteritems():

            if len(items) > 0:
                # f = executor.submit(flush_bucket, bucket, items)

                # send to a different process to operate, clear the buffer
                # self.clear(bucket)
                flush_bucket(self.connection, bucket, items)
                self.clear(bucket)
                # self.futures.append(f)

        return True
