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

db_conn = None


def get_db_conn(connection_uri):
    global db_conn
    if not db_conn:
        db_conn = dataset.connect(connection_uri)
    return db_conn


def get_last_hidden_posts(limit=750):
    try:
        r = requests.get(
            "https://api.utopian.io/api/posts?limit=%s&status=flagged"
            % limit).json()
        return r["results"]
    except Exception as error:
        logger.error(error)
        logger.info("Retrying.")
        return get_last_hidden_posts(limit=limit)


def get_table(connection_uri):
    db = get_db_conn(connection_uri)
    return db["utopian_hidden_logs"]


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
    if r.status_code != 204:
        logger.error("Error: %s", r.text)
        time.sleep(3)
        return post_to_discord(hook_url, message)
    time.sleep(1)


def check_posts(connection_uri, webhook_url):
    posts = get_last_hidden_posts()
    for post in posts:
        message = "**[%s]** - %s hidden contribution: %s" % (
            post.get("json_metadata", {}).get("type", "Unknown"),
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


if __name__ == '__main__':
    check_posts(
        os.getenv("MYSQL_CONNECTION_STRING"),
        os.getenv("DISCORD_HOOK_URL"),
    )
