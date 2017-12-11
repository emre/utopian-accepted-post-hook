import datetime
import json
import logging
import os
import time

import dataset
import requests

logger = logging.getLogger('utopian-accepted-post-hook')
logger.setLevel(logging.INFO)
logging.basicConfig()

BOT_SLEEP_TIME = os.getenv("BOT_SLEEP_TIME", 30)

db_conn = None


def get_db_conn(connection_uri):
    global db_conn
    if not db_conn:
        db_conn = dataset.connect(connection_uri)
    return db_conn


def get_last_approved_posts(limit=10):
    r = requests.get(
        "https://api.utopian.io/api/posts?limit=%s" % limit).json()
    return r["results"]


def get_table(connection_uri):
    db = get_db_conn(connection_uri)
    return db["utopian_approve_logs"]


def add_log(connection_uri, author, permlink):
    get_table(connection_uri).insert(dict(
        author=author,
        permlink=permlink,
        created_at=datetime.datetime.now(),
    ))


def already_posted(connection_uri, author, permlink):
    return get_table(connection_uri).find_one(
        author=author,
        permlink=permlink,
    )


def post_to_discord(hook_url, message):
    r = requests.post(hook_url, json.dumps(message), headers={
        "Content-Type": "application/json",
    })


def check_posts(connection_uri, webhook_url):
    posts = get_last_approved_posts()
    for post in posts:
        message = "%s approved contribution: %s" % (
            post["moderator"],
            "https://utopian.io" + post["url"]
        )
        if already_posted(connection_uri, post["author"], post["url"]):
            logger.info("%s already posted. Skipping", post["url"])
            continue
        add_log(connection_uri, post["author"], post["url"])
        logger.info(message)
        post_to_discord(webhook_url, {
            "content": message})


def scheduler(connection_uri, webhook_url):
    while True:
        check_posts(connection_uri, webhook_url)
        logger.info("Sleeping for %s seconds.", BOT_SLEEP_TIME)
        time.sleep(BOT_SLEEP_TIME)


if __name__ == '__main__':
    scheduler(
        os.getenv("MYSQL_CONNECTION_STRING"),
        os.getenv("DISCORD_HOOK_URL"),
    )
