
"""
Julia Geller and Ceara Zhang
DS4300 / Twitter Redis Database
Created 27 Jan 2024
Updated: 27 Jan 2024

twitter_objects.py:
Create objects to be loaded into the twitter database
"""


class Tweet:

    def __init__(self, user_id: int, tweet_text: str, tweet_ts: str):
        """
        :param user_id: User ID
        :param tweet_text: the tweet text
        :param tweet_ts: the tweet timestamp
        """

        self.user_id = int(user_id)
        self.tweet_text = tweet_text
        self.tweet_ts = tweet_ts


class Follows:

    def __init__(self, user_id: int, follows_id: int):
        """
        :param user_id: User ID
        :param follows_id: person that's followed
        """
        self.user_id = int(user_id)
        self.follows_id = int(follows_id)

