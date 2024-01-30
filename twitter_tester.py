"""
Julia Geller and Ceara Zhang
DS4300 / Twitter Relational Database
Created 14 Jan 2024
Updated: 21 Jan 2024

twitter_tester.py:
Driver that uses Twitter API to post tweets and
retrieve user home timelines.
"""

import os
import pandas as pd
from twitter_mysql import TwitterAPI
from twitter_objects import Tweet, Follows
import time
from datetime import datetime


def main():
    # create user and password environment variables, must be done
    # upon re-running the program as these variables do not persist
    os.environ["TWITTER_USER"] = 'root'
    os.environ["TWITTER_PASSWORD"] = 'cece2002'  # TODO fill in with your password

    # save tweets and follows data as dataframes
    tweets_df = pd.read_csv('data/tweet.csv')
    follows_df = pd.read_csv('data/follows.csv')

    # connect to twitter database with this user
    api = TwitterAPI(os.environ["TWITTER_USER"], os.environ["TWITTER_PASSWORD"], "twitter")

    # set up the database with its appropriate schema
    # api.setup_database()

    # start a one-second timer
    sec_timer = time.time()
    # save a counter of how many tweets have been posted in one second
    twts_per_sec = 0

    # insert the tweet rows into the table (i.e. post the tweets)
    for _, row in tweets_df.iterrows():
        # format the current time as the tweet's time posted
        tweet_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # create the Tweet object
        tweet = Tweet(row['USER_ID'], row['TWEET_TEXT'], tweet_ts)
        # post the tweet
        api.post_tweet(tweet)
        # increment the number of tweets posted if the second timer is not up
        if time.time() - sec_timer <= 1:
            twts_per_sec += 1
    print(f'{twts_per_sec} tweets were posted in one second.')

    # insert the follows rows into the table (i.e. register the tweets)
    for _, row in follows_df.iterrows():
        fols = Follows(row['USER_ID'], row['FOLLOWS_ID'])
        # register the follows
        api.register_follows(fols)

    # start a one-second timer
    sec_timer = time.time()
    # save a counter of how many home timelines are retrieved per second
    ht_per_sec = 0
    while time.time() - sec_timer < 1:
        # retrieve one user's home timeline
        api.simulate_home_timeline_refresh(1)
        # increment counter
        ht_per_sec += 1
    # print number of home timelines retrieved per second
    print(f'{ht_per_sec} home timelines were retrieved in one second.')

    # destroy database when done (for re-running/testing purposes)
    api.destroy_database()


if __name__ == '__main__':
    main()
