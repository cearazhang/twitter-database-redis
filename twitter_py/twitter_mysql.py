"""
Julia Geller and Ceara Zhang
DS4300 / Twitter Redis Database
Created 27 Jan 2024
Updated: 30 Jan 2024

twitter_mysql.py:
Twitter Database API for Redis
"""

from twitter_objects import Tweet, Follows
from typing import List, Tuple
import random
import redis


class TwitterAPI:

    def __init__(self, username, password, db_name, host="localhost", port=6379, db=0):

        # Connect to redis
        self.redis_db = redis.StrictRedis(host=host, port=port, db=db,  decode_responses=True)
        self.next_tweet_id = 0 # next tweet id to assign

    def destroy_database(self):
        """
        Destroys the twitter database (for testing purposes),
        then closes the Redis connection
        """
        # destroy the twitter database
        self.redis_db.flushall()

        # close the Redis connection
        self.redis_db.close()
    
    def add_user(self, user_id: int):
        """
        Adds the user to the users set.

        Args:
        user_id: int
        """
        self.redis_db.sadd('users', user_id)
    
    def set_followers(self, user_id: int, follower_ids: List[int]):
        """
        Sets the followers that follow the given user.

        Args:
        user_id: int
        follower_ids: List[int]
        """
        for f_id in follower_ids:
            self.redis_db.sadd(f'followers_user_{user_id}', f_id)
    
    def set_followees(self, user_id: int, followee_ids: List[int]):
        """
        Sets the accounts that the given user follows.

        Args:
        user_id: int
        followee_ids: List[int]
        """
        for f_id in followee_ids:
            self.redis_db.sadd(f'followees_user_{user_id}', f_id)
    
    def register_follows(self, fol: Follows):
        """
        Register a follow into the user's followers and followees.

        Args:
        fol: Follows
        """
        # add the follower to the user's followers
        self.redis_db.sadd(f'followers_user_{fol.follows_id}', fol.user_id)
        # add the user to the followers followees
        self.redis_db.sadd(f'followees_user_{fol.user_id}', fol.follows_id)
        # add user to users list
        self.add_user(fol.user_id)

    def post_tweet(self, twt: Tweet):
            """
            Use tweet ID as key and store tweet data as value

            Args:
            tweet: Tweet
            """
            # add tweet to the tweet hashmap
            self.redis_db.hset(f'tweet:{self.next_tweet_id}', mapping={'user_id': twt.user_id,'tweet_id': self.next_tweet_id, 'tweet_text':twt.tweet_text, 'tweet_ts': twt.tweet_ts})

            follower_ids = self.redis_db.smembers(f'followers_user_{twt.user_id}')
            for f_id in follower_ids:
                # add tweet to user's home timeline
                self.redis_db.zadd(f'timeline_user_{f_id}', {self.next_tweet_id:twt.tweet_ts})
            self.next_tweet_id +=1

    def post_tweet_str1(self, twt: Tweet):
        """
        Post a tweet to the tweet table w/out updating the home timeline
        (strategy 1 OPTIONAL)

        Args:
        tweet: Tweet
        """

        # add tweet to the tweet hashmap
        tweet_id = self.next_tweet_id
        self.redis_db.hset(f'tweet:{tweet_id}', mapping={'user_id': twt.user_id, 'tweet_id': tweet_id, 'tweet_text':twt.tweet_text, 'tweet_ts': twt.tweet_ts})

        # increment the tweet ID for the next tweet
        self.next_tweet_id += 1

    def choose_rand_user(self) -> int:
        """
        Returns a random user from the users list.

        Args:

        return:
        user_id: int
        """
        user_ids = self.redis_db.smembers('users')
        user_id = random.choice(list(user_ids))
        return user_id

    def get_home_timeline_str1(self, user_id: int) -> List[Tuple[str, int]]:
        """
        OPTIONAL STRAT 1
        Retrieves the home timeline of the given user,
        query 10 most recent tweets from users they follow.

        Args:
        user_id: int

        return:
        result: List[Tuple[str, int]]
        """
        # get the user's followees
        followees_ids = self.redis_db.smembers(f'followees_user_{user_id}')

        # initialize an empty timeline
        timeline = []

        # retrieve 10 most recent tweets for each followee
        for followee_id in followees_ids:
            tweet_ids = self.redis_db.zrevrange(f'timeline_user_{followee_id}', 0, 9)
            tweets = [(self.redis_db.hget(f'tweet:{tweet_id}', 'tweet_text'),
                       self.redis_db.hget(f'tweet:{tweet_id}', 'tweet_ts')) for tweet_id in tweet_ids]
            timeline.extend(tweets)

        # sort the timeline by timestamp and return the latest 10 tweets
        result = sorted(timeline, key=lambda x: x[1], reverse=True)[:10]
        return result

    def get_home_timeline(self, user_id: int) -> List[Tuple[str, int]]:
        """
        Retrieves the home timeline of the given user,
        query 10 most recent tweets from users they follow.

        Args:
        user_id: int

        return:
        result: pd.DataFrame
        """
        # get 10 most recent tweets on user's home timeline
        tweet_ids = self.redis_db.zrevrange(f'timeline_user_{user_id}', 0, 9)
        result = [(self.redis_db.hget(f'tweet:{tweet_id}', 'tweet_text'), self.redis_db.hget(f'tweet:{tweet_id}', 'tweet_ts')) for tweet_id in tweet_ids]
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
                print(f"Home timeline for user {random_user_id}:")
                for tweet in home_timeline:
                    print('\tTweet: ', tweet[0])
                    print('\tTime: ', tweet[1], '\n')