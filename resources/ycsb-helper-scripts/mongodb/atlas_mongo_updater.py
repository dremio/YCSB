import json
from argparse import ArgumentParser

from pymongo import MongoClient
from mongo_updater import BaseMongoUpdater


class MongoAtlasUpdater(BaseMongoUpdater):
    def __init__(self, database='dremio-perf-test'):
        username = "dremio"
        password = "dremio123"
        url = "mongodb+srv://{}:{}@benchmark-lk5uu.gcp.mongodb.net/test?retryWrites=true&w=majority"\
            .format(username, password)
        self.client = MongoClient(url)
        self.db = self.client[database]
        super(MongoAtlasUpdater, self).__init__(self.db)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--database", dest="database", type=str, help="Database to connect to")
    parser.add_argument("--collection", dest="collection", type=str, help="Collection to write to")
    parser.add_argument("--filename", dest="filename", type=str, help="filename to read from")
    args = parser.parse_args()

    data = json.loads(open(args.filename, "r").read())
    mongo_atlas_updater = MongoAtlasUpdater(database=args.database)
    mongo_atlas_updater.batch_update(data, collection_name=args.collection)
