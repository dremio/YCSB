import unittest

import pytest
from mock import Mock, MagicMock
from mongodb.mongo_updater import BaseMongoUpdater


class FakeCollection():
    def insert_many(self, docs):
        pass


@pytest.mark.usefixtures()
class TestBaseMongoUpdater(unittest.TestCase):
    def test_batch_update(self):
        docs = [{"_id": i, "doc": str(i)} for i in range(10)]
        self.collection = FakeCollection()
        self.db = {"test": self.collection}
        self.mongo_updater = BaseMongoUpdater(self.db, batch_size=10)
        self.collection.insert_many = MagicMock(return_value=docs)

        self.mongo_updater.batch_update(docs)
        self.collection.insert_many.assert_called_with(docs)