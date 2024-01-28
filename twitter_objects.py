
"""
Julia Geller and Ceara Zhang
DS4300 / Twitter Relational Database
Created 14 Jan 2024
Updated: 21 Jan 2024

twitter_objects.py:
Create objects to be loaded into the twitter database
"""


class Tweet:

    def __init__(self, user_id: int, tweet_id: int, tweet_text: str, tweet_ts: str):
        """
        :param user_id: User ID
        :param tweet_text: the tweet text
        :param tweet_ts: the tweet timestamp
        """

        self.user_id = int(user_id)
        self.tweet_id = int(tweet_id)
        self.tweet_text = tweet_text
        self.tweet_ts = tweet_ts


class Follows:

    def __init__(self, user_id: int, follows_id: int):
        self.user_id = int(user_id)
        self.follows_id = int(follows_id)

