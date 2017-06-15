# Python 3 program
import argparse
import datetime
import logging
import time
from pymongo import MongoClient


# Global variable
TVSHOWS = ["Powerless", "24Legacy","LegionFX"]
# Time range is last 48 hours
TWO_DAY_IN_MILLISECONDS = 2 * 24 * 60 * 60 * 1000

# Document status
PENDING = "pending"

# Input arguments
PROGRAM_DESCRIPTION = "Update unique user id for each tv show into social network collection every day"
parser = argparse.ArgumentParser(description=PROGRAM_DESCRIPTION)
parser.add_argument('tvshow', type=str, help='Hashtag of tv show')
args = vars(parser.parse_args())


def main():
    tvshow = args['tvshow']
    assert(tvshow in TVSHOWS)

    logging.basicConfig(filename='update_user_ids.log', level=logging.DEBUG, format='%(asctime)s %(message)s')
    logging.debug("begin user id collection: {0} for hashtags : {1}".format(datetime.date.today(), tvshow))

    database = MongoClient()['stream_store']
    updateUserIdToSocialNetwork(database, tvshow)


def updateUserIdToSocialNetwork(db, tvshow=''):
    """ Get unique user ids in last two days """

    assert (tvshow != '')

    collection = db["network_" + tvshow]

    todayInMilliSeconds = int(time.time() * 1000)
    twoDaysAgoInMilliSeconds = todayInMilliSeconds - TWO_DAY_IN_MILLISECONDS

    query_string = {
        "timestamp_ms": {
            "$gte": str(twoDaysAgoInMilliSeconds),
            "$lte": str(todayInMilliSeconds)},
        'entities.hashtags.text': tvshow}
    projection_string = {'user.id': 1}

    insert_count = 0
    for item in db.tweets.find(query_string, projection_string):
        user_id = int(item["user"]["id"])
        if collection.find_one({"user_id": user_id}) is not None:
            continue
        user_record = {"user_id": user_id, "collection_status": PENDING}
        collection.insert_one(user_record)
        insert_count += 1
    logging.debug("Insert {} users".format(insert_count))
    

if __name__ == "__main__":
    main()

