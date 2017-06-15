"""
Read the tweet database and gathers the unique users and store them in another database
These user ids are used as status reference for collection of old tweets

"""
import argparse
import time
from pymongo import MongoClient
import logging
import datetime
import smtplib
from email.mime.text import MIMEText




# Time range is last 48 hours
TWO_DAY_IN_MILLISECONDS = 2 * 24 * 60 * 60 * 1000

# Input arguments
PROGRAM_DESCRIPTION = "Update unique user id into social network database every day"
parser = argparse.ArgumentParser(description=PROGRAM_DESCRIPTION)
parser.add_argument('tvshow', type=str, help='Hashtag of tv show')
parser.add_argument('target_collection', type=str, help='Target collection to store')
args = vars(parser.parse_args())


def main():
    tvshow = args['tvshow']
    target_collection = args['target_collection']
    logging.basicConfig(filename='collect_user_ids_new.log', level=logging.DEBUG, format='%(asctime)s %(message)s')
    logging.debug("begin user id collection: {0} for hashtags : {1}".format(datetime.date.today(), tvshow))

    remote_db = get_local_db()
    local_db = get_local_db()

    users = getUserID(remote_db, tvshow)
    inserted, repeat = insert_users(local_db, users, target_collection)


def insert_users(local_db, users, target_collection):
    '''

    :param local_db: database where user ids are stores
    :param users: list of users
    :param target_collection: name of collection where user ids are stored
    :return: None
    '''
    count_err = 0
    count_inserted = 0
    local_db[ target_collection ].create_index([("user_id", 1)], unique=True)
    for user in users:
        record = user['_id']
        record['collection_status'] = 'pending'
        try:
            local_db[target_collection].insert(record)
            count_inserted += 1
        except:

            count_err += 1

    logging.debug("inserted new users: {0}".format(count_inserted))
    logging.debug("repeat users found( not inserted: {0}".format(count_err))
    logging.debug("========================================================")
    return count_inserted, count_err



def getUserID(db, tvshow=''):
    """ Get unique user ids in last two days """
    queryString = {}

    todayInMilliSeconds = long(time.time() * 1000)
    twoDaysAgoInMilliSeconds = todayInMilliSeconds - TWO_DAY_IN_MILLISECONDS

    assert (tvshow != '')
    queryString['timestamp_ms'] = {'$gte': str(twoDaysAgoInMilliSeconds), "$lte": str(todayInMilliSeconds)}
    queryString['entities.hashtags.text'] = tvshow
    group = {'_id': {'user_id': '$user.id', 'screen': '$user.screen_name'}}
    project = {'_id': 0, 'user.id': 1, 'user.screen_name': 1, 'entities.hashtags.text' : 1}
    users = db.tweets.aggregate([
        {'$match': queryString},
        {'$project': project},
        {'$group': group}
    ])
    #print(queryString)
    return users


def get_local_db():
    return MongoClient()['stream_store']

if __name__ == "__main__":
    main()

