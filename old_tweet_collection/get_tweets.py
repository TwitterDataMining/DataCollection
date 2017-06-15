from multiprocessing import Process, Lock
from pymongo import MongoClient
import json
import logging
import datetime
import os
import tweepy
import time
import fcntl
import csv
import argparse



# Input arguments
PROGRAM_DESCRIPTION = "Collects tweets for given user db"
parser = argparse.ArgumentParser(description=PROGRAM_DESCRIPTION)
parser.add_argument('user_db', type=str, help='users_db')
parser.add_argument('target_db', type=str, help='target collection_name')
args = vars(parser.parse_args())

def main():
    user_collection = args['user_db']
    tweet_collection = args['target_db']

    tweepy.debug(enable=False)
    logging.basicConfig(filename='get_tweets.log', level=logging.INFO, format='%(asctime)s %(message)s')
    urllib3_logger = logging.getLogger('urllib3')
    urllib3_logger.setLevel(logging.CRITICAL)

    # change_status(user_collection)
    credentials = read_credentials('conf.json')
    apis = create_api_connections(credentials["clients"])


    start_tweet_collection( apis, user_collection, tweet_collection)

def change_status(user_collection):
    """
    change the status of collecting to pending -
     before running the collection to cover previously failed collections
    :param user_collection:
    :return:
    """
    db = MongoClient(host="10.1.10.96")['stream_store']
    user_collection = db[user_collection]
    user_collection.update_many(
        {"collection_status": "collecting"},
        {"$set": {"collection_status": "pending"}}
    )


def read_credentials(filename):
    """
    :param: filename : name of the file that stores the connections in the format:
           name,consumer_key,consumer_secret,access_key,access_secret,owner
           where:
           name : used for naming the connection and collection where output is stored
           consumer_key : from twitter
           consumer_secret: from twitter
           access_key: from twitter
           access_secret: from twitter
           owner: username just for records - not used
    :return: list of twitter api connections

    """
    with open(filename) as credential_file:
        pyresponse = json.load(credential_file)
    return pyresponse


def create_api_connections(credentials):
    """

    :param credentials: Lis of a dictionary of credentials
    :return: the dictionary of { name : api_connection_object } using tweepy
    """

    apis = {}
    for account in credentials:
        access_key = account['token']
        consumer_secret = account['secret']
        access_secret = account['token_secret']
        consumer_key = account['key']
        name = account['name']

        # create the api connection
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_key, access_secret)
        api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())


        # add connection to dictionary
        apis[name] = api

    return apis

def filter_dict(tweet):
    filtered_tweet = {}
    
    # tweet fields
    filtered_tweet['text'] = tweet['text']
    filtered_tweet['entities'] = tweet['entities']
    
    # user fields
    filtered_tweet['user_id'] = tweet['user']['id']
    filtered_tweet['user_description'] = tweet['user']['description']
    filtered_tweet['user_name'] = tweet['user']['name']
    filtered_tweet['screen_name'] = tweet['user']['screen_name']
    filtered_tweet['statuses_count'] = tweet['user']['statuses_count']
    filtered_tweet['lang'] = tweet['lang']
    
    # social connection fields
    filtered_tweet['in_reply_to_user_id'] = tweet['in_reply_to_user_id']
    if 'retweeted_status' in tweet.keys():
        filtered_tweet['rt'] = True
        filtered_tweet['retweeted_status'] = tweet['retweeted_status']

    else:
        filtered_tweet['rt'] = False
    if 'quoted_status' in tweet.keys():
        filtered_tweet['quote'] = True
        filtered_tweet['quoted_status'] = tweet['quoted_status']
    else:
        filtered_tweet['quote'] = False

    # tweet fiels
    dt = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    filtered_tweet['created_at'] = dt
    filtered_tweet['id'] = tweet['id']

    return filtered_tweet

def get_tweets(user_id, api, api_name, lock):
    # initialize a list to hold all the tweepy Tweets
    alltweets = []
    statuses_count = 0
    collected = 0
    tweet_text = ""
    # make initial request for most recent tweets (200 is the maximum allowed count)
    try:
        tweets_raw = api.user_timeline(user_id=user_id, count=200)
        new_tweets = [filter_dict(twet) for twet in tweets_raw]
        #new_tweets = map(filter_dict, new_tweets)

    except:
        raise

    # save most recent tweets
    alltweets.extend(new_tweets)
    # screen_name = alltweets[0].user.screen_name
    # statuses_count = alltweets[0].user.statuses_count
    # print screen_name
    # save the id of the oldest tweet less one
    if len(alltweets) > 0:
        print alltweets[-1]
        oldest = alltweets[-1]['id'] - 1

    # keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0 and len(alltweets) < 1500:
        # print "getting tweets before %s" % (oldest)

        # all subsiquent requests use the max_id param to prevent duplicates
        try:
            new_tweets = api.user_timeline(user_id=user_id, count=200, max_id=oldest)
            new_tweets = map(filter_dict, new_tweets)
        except:
            raise
        # tweet_text += ' '.join([tweet.text for tweet in new_tweets if tweet.lang == 'en'])
        # save most recent tweets
        alltweets.extend(new_tweets)

        # update the id of the oldest tweet less one
        oldest = alltweets[-1]['id'] - 1
        oldest_date = alltweets[-1]['created_at']


        # print "...%s tweets downloaded so far" % (len(alltweets))

    # transform the tweepy tweets into a 2D array that will populate the csv
    # outtweets = [[tweet.id_str, tweet.created_at, tweet.text.encode("utf-8")] for tweet in alltweets]
    collected = len(alltweets)
    with open('user_old_tweet.csv', 'a') as record_file:
        timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
        fcntl.flock(record_file, fcntl.LOCK_EX)
        record_file.write("{0}, {1}, {2} ,{3}, {4}\n".format(timestamp, str(collected), user_id, str(oldest_date), api_name))
        fcntl.flock(record_file, fcntl.LOCK_UN)
    print ("collected {0} for user_id {1} oldest_date = {2}".format(str(collected), user_id, str(oldest_date)))
    return alltweets




def get_tweets_process( api_name, api, lock,user_collection,  tweet_collection):
    db = MongoClient(host="10.1.10.96")['stream_store']
    user_collection = db[user_collection]
    tweet_collection = db[tweet_collection]


    while True:
    # read and change userid database
        lock.acquire()
        user = user_collection.find_one({'collection_status' : 'pending'})
        print("user is {}".format(user))
        if user is None:
            lock.release()
            logging.debug("****** All users done so far *****")
            break

        result = user_collection.update_one(
            {"user_id": user['user_id']},
            {"$set": {"collection_status": "collecting"}}
        )
        lock.release()

        done = False

        # get_tweets, sleep if time limit
        while not done:
            try:
                tweets = get_tweets(user['user_id'], api, api_name, lock)
                done = True
            except tweepy.error.RateLimitError:
                logging.debug("*******Rate Limit Exception : "
                              "sleeping for 15 mins : api {0}".format(api_name))
                lock.acquire()
                record_file = open('sleep_logs.txt', 'a')
                timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                record_file.write("{0} :  {1}  sleeping\n".format(timestamp, api_name))
                record_file.close()
                lock.release()
                time.sleep(60 * 15)
	    except:
		lock.acquire()
                record_file = open('sleep_logs.txt', 'a')
                timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                record_file.write("{0} :  {1}  Something else wrong\n".format(timestamp, api_name))
                record_file.close()
                lock.release()
                time.sleep(60 * 15)
		


        # write tweets to the database
        lock.acquire()
        try:
            tweet_collection.insert_many(tweets)
        except:
            logging.debug("Error inserting user_id {0}".format(user['user_id']))
            user_collection.update_one(
                {"user_id": user['user_id']},
                {"$set": {"collection_status": "failed_insert"}}
            )
            lock.release()
            continue
        logging.debug("inserted {0} records for user_id {1}".format(str(len(tweets)), user['user_id']))
        lock.release()

        # change userid database
        lock.acquire()
        user_collection.update_one(
            {"user_id": user['user_id']},
            {"$set": {"collection_status": "done"}}
        )
        lock.release()


def start_tweet_collection(apis, user_collection, tweet_collection):
    """

    :param user_accounts:
    :param apis:
    :return:
    """
    lock = Lock()
    for name, each_api in apis.iteritems():
        p = Process(target=get_tweets_process, args=(name, each_api, lock, user_collection, tweet_collection))
        p.start()


if __name__ == "__main__":
    main()

