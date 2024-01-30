"""
Julia Geller and Ceara Zhang
DS4300 / Twitter Relational Database
Created 14 Jan 2024
Updated: 21 Jan 2024

twitter_mysql.py:
Twitter Database API for MySQL
"""

from dbutils import DBUtils
from twitter_objects import Tweet, Follows
from typing import List
import random
import pandas as pd
import redis


class TwitterAPI:

    def __init__(self, host="localhost", port=6379, db=0):

        # Connect to redis
        self.redis_db = redis.StrictRedis(host=host, port=port, db=db)

    # def setup_database(self):
    #     """
    #     Sets up the twitter database with its appropriate tables and schema.
    #     """
    #     # switch to the twitter database
    #     use_db_query = """
    #     USE twitter
    #     """
    #     self.dbu.execute(use_db_query)
    #
    #     # create the tweet table
    #     create_tweet_table_query = """
    #     CREATE TABLE tweet (
    #         user_id INT,
    #         tweet_ts DATETIME,
    #         tweet_text VARCHAR(140),
    #         INDEX idx_user_id (user_id),  -- additional index on user_id
    #         INDEX idx_tweet_ts (tweet_ts)
    #     )
    #     """
    #     self.dbu.execute(create_tweet_table_query)
    #
    #     # create the follows table
    #     create_follows_table_query = """
    #     CREATE TABLE follows (
    #         user_id INT,
    #         follows_id INT,
    #         PRIMARY KEY(user_id, follows_id),
    #         INDEX idx_user_id (user_id), -- additional index on user_id
    #         INDEX idx_follows_id (follows_id) -- additional index on follows_id
    #     )
    #     """
    #     self.dbu.execute(create_follows_table_query)

    def destroy_database(self):
        """
        Destroys the twitter database (for testing purposes).
        """
        # destroy the twitter database
        self.redis_db.flushall()

    def post_tweet(self, twt: Tweet):
        """
        UUse tweet ID as key and store tweet data as value

        Args:
        tweet: Tweet
        """
        tweet_key = f"Tweet:{twt.tweet_id}"
        tweet_data = {
            "user_id": twt.user_id,
            "tweet_text": twt.tweet_text,
            "tweet_ts": twt.tweet_ts
        }
        self.redis_db.hmset(tweet_key, tweet_data)

    def register_follows(self, fol: Follows):
        """
        Register a follow into the follows table.

        Args:
        fol: Follows
        """
        sql = "INSERT INTO follows (user_id, follows_id) VALUES (%s, %s) "
        val = (fol.user_id, fol.follows_id)
        self.dbu.insert_one(sql, val)

    def choose_rand_user(self) -> int:
        """
        Returns a random user from the follows table.

        Args:

        return:
        user_id: int
        """
        sql = """
            SELECT user_id
            FROM follows
            """
        user_ids = self.dbu.execute(sql)['user_id'].values
        user_id = random.choice(user_ids)
        return user_id

    def get_follows(self, user_id: int) -> List[int]:
        """
        Return the ids of each unique account that the given user follows.

        Args:
        user_id: int

        return:
        follows_ids: List[int]
        """
        sql = f"""
            SELECT follows_id
            FROM follows
            WHERE user_id = {user_id}"""
        follows_ids = list(set(self.dbu.execute(sql)['follows_id'].values))
        return follows_ids

    def get_tweets(self, user_ids: List[int]) -> pd.DataFrame:
        """
        Returns every tweet posted and the time it was posted
        for all tweets posted by the given user.

        Args:
        user_ids: List[int]

        return:
        result: pd.DataFrame
        """
        user_ids = '(' + ', '.join([str(id) for id in user_ids]) + ')'
        sql = f"""
            SELECT tweet_text, tweet_ts
            FROM tweet t
            WHERE user_id in {user_ids}
            ORDER BY t.tweet_ts DESC
            LIMIT 10;
            """
        result = self.dbu.execute(sql)
        return result

    def get_home_timeline(self, user_id: int) -> pd.DataFrame:
        """
        Retrieves the home timeline of the given user,
        query 10 most recent tweets from users they follow.

        Args:
        user_id: int

        return:
        result: pd.DataFrame
        """
        follows_ids = self.get_follows(user_id)
        result = self.get_tweets(follows_ids)
        return result

    def simulate_home_timeline_refresh(self, num_refreshes: int, verbose=False):
        """
        Simulate the home timeline refresh by selecting a random
        user each time to retrieve their home timeline of the 10 most recent tweets.

        Args:
        num_refreshes: int
        verbose: bool
        """
        for _ in range(num_refreshes):
            # pick a random user
            random_user_id = self.choose_rand_user()

            # retrieve and print home timeline for the random user
            home_timeline = self.get_home_timeline(random_user_id)
            if verbose:
                print(f"Home timeline for user {random_user_id}: {home_timeline}")
