# done          Finished collecting
# pending       Waiting for collection
# collecting    Collecting by one of the thread 
# unauthorized  Unauthoried to access that user
# unknown       Meet a unknown error when collecing this user's network

from pymongo import MongoClient
import json
import logging
import tweepy
import time
import threading
from threading import Lock

# Global Variables
TVSHOWS = ["TheGoodPlace", "ThisIsUs", "Powerless", "LegionFX", "24Legacy"]


def main():
    logging.basicConfig(filename='get_social_network.log', level=logging.WARNING,
                        format='%(asctime)s %(threadName)s %(message)s')

    credentials = read_credentials('conf.json')
    apis = create_api_connections(credentials["clients"])

    start_socialnetwork_collection(apis)


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


def limit_handled(cursor):
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            logging.warning("sleep 16 minues")
            time.sleep(16 * 60)
        except tweepy.TweepError as tweep_error:
            if tweep_error.response is not None and tweep_error.response.status_code == 401:
                raise
            else:
                logging.exception("ERROR Unknown Tweepy exception")
                raise
        except:
            raise


def get_networks(user_id, api):
    # initialize a list to hold all the tweepy Tweets
    followers = set()
    friends = set()

    # logging.warning("user_id: {}".format(user_id))
    for follower in limit_handled(tweepy.Cursor(api.followers_ids, user_id=user_id).pages()):
        followers = followers | set(follower["ids"])
        if len(followers) > 1000000:
            break

    for friend in limit_handled(tweepy.Cursor(api.friends_ids, user_id=user_id).pages()):
        friends = friends | set(friend["ids"])
        if len(friends) > 1000000:
            break

    #logging.warning("Got {} followers, {} friends.".format(len(followers), len(friends)))

    return friends, followers


def get_network_process(api, lock, collection_name):
    collection = MongoClient()['stream_store']["network_" + collection_name]
    # user_collection = db.unique_users
    # tweet_collection = db.old_tweets

    while True:

        lock.acquire()
        user = collection.find_one({'collection_status': 'pending'})
        if user is None:
            lock.release()
            logging.warning("****** All users done so far, sleep 6 hours ****")
            time.sleep(60 * 60 * 6)
            continue

        try:
            collection.update_one(
                {"user_id": user['user_id']},
                {"$set": {"collection_status": "collecting"}})
        except:
            logging.exception("ERROR: update status")
            continue
        finally:
            lock.release()

        try:
            friends, followers = get_networks(user['user_id'], api)
        except tweepy.TweepError as tweep_error:
            if tweep_error.response is not None and tweep_error.response.status_code == 401:
                # logging.error("Unauthorized user id {}".format(user['user_id']))
                lock.acquire()
                collection.update_one(
                    {"user_id": user['user_id']},
                    {"$set": {"collection_status": "unauthorized"}})
                lock.release()
            else:
                # The unknown error we have met so far:
                # code: 34, message: Sorry, that page does not exist.
                # logging.error("ERROR Unknown error for user id {}".format(user['user_id']))
                lock.acquire()
                collection.update_one(
                    {"user_id": user['user_id']},
                    {"$set": {"collection_status": "unknown"}})
                lock.release()
            continue
        except:
            logging.exception("Unexcepted ERROR: get friends and followers")
            continue

        try:
            lock.acquire()
            collection.update_one(
                {"user_id": user['user_id']},
                {"$set": {
                    "collection_status": "done",
                    "friends": list(friends),
                    "followers": list(followers)
                }})
        except:
            logging.exception("ERROR: update mongo db")
            continue
        finally:
            lock.release()

        #logging.warning("Update user id {}".format(user['user_id']))


def start_socialnetwork_collection(apis):
    """
    :param apis:
    :return:
    """
    lock = Lock()
    count = 0
    for name, api in apis.items():
        tvshow = TVSHOWS[count % len(TVSHOWS)]
        t = threading.Thread(target=get_network_process, args=(api, lock, tvshow))
        t.start()
        count += 1


if __name__ == "__main__":
    main()
