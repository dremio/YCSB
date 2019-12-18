import json
from argparse import ArgumentParser

from pymongo import MongoClient
import logging

from mongo_updater import BaseMongoUpdater

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


class LocalMongoDataUpdater(BaseMongoUpdater):
    def __init__(self, database='dremio-perf-test'):
        self.client = MongoClient("mongodb://localhost:27017")
        self.db = self.client[database]
        super(LocalMongoDataUpdater, self).__init__(self.db)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--database", dest="database", type=str, help="Database to connect to")
    parser.add_argument("--collection", dest="collection", type=str, help="Collection to write to")
    parser.add_argument("--filename", dest="filename", type=str, help="filename to read from")
    args = parser.parse_args()

    data = json.loads(open(args.filename, "r").read())
    local_mongo_updater = LocalMongoDataUpdater(database=args.database)
    local_mongo_updater.batch_update(data, collection_name=args.collection)
