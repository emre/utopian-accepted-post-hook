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

from team_map import MOD_TO_TEAM

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


def check_posts(connection_uri, webhook_url):
    posts = get_last_hidden_posts()
    for post in posts:

        if 'moderator' in post.get("json_metadata", {}):
            # new posts will have that
            flagged = post.get("json_metadata", {}).get("moderator", {}).get(
                "flagged", '-'
            )

            if not flagged:
                continue
            moderator = post.get("json_metadata").get(
                "moderator", {}).get("account")
        else:
            try:
                moderator = post["moderator"]
            except KeyError as e:
                logger.error(e)
                continue
        message = "**[%s team]** **[%s]** - %s hid contribution: %s" % (
            MOD_TO_TEAM.get(moderator, 'unknown'),
            post.get("json_metadata", {}).get("type", "Unknown"),
            moderator,
            "https://utopian.io" + post["url"]
        )

        if already_posted(connection_uri, post["author"], post["url"]):
            logger.info("%s already posted. Skipping", post["url"])
            continue
        add_log(connection_uri, post["author"], post["url"])
        logger.info(message)

        from embeds import Webhook
        hidden_hook = Webhook(
            url=webhook_url,
        )
        hidden_hook.set_author(
            name=moderator,
            url="http://utopian.io/%s" % moderator,
            icon="https://img.busy.org/@%s?height=100&width=100" %
                 moderator,
        )

        hidden_hook.add_field(
            name="Action",
            value="Hide"
        )

        hidden_hook.add_field(
            name="Category",
            value=post.get("json_metadata", {}).get("type", "unknown"),
        )

        hidden_hook.add_field(
            name="Supervisor",
            value=MOD_TO_TEAM.get(moderator, 'unknown'),
        )

        hidden_hook.add_field(
            name="Contribution",
            value="[%s](%s)" % (
                post["title"], "https://utopian.io" + post["url"]),
            inline=False,
        )

        hidden_hook.add_field(
            name="Author",
            value="[%s](https://utopian.io/@%s)" % (
                post.get("author"),
                post.get("author")
            ),
        )

        hidden_hook.set_footer(
            ts=str(datetime.datetime.utcnow())
        )

        hidden_hook.post()


if __name__ == '__main__':
    check_posts(
        os.getenv("MYSQL_CONNECTION_STRING"),
        os.getenv("DISCORD_HOOK_URL"),
    )
