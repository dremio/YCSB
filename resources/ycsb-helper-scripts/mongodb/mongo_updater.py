import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


class BaseMongoUpdater(object):
    def __init__(self, db, batch_size=1000):
        self.BATCH_SIZE = batch_size
        self.db = db

    def _transform(self, chunk):
        new_chunk = []
        for doc in chunk:
            new_chunk.append(doc)
        return new_chunk

    def batch_update(self, docs, collection_name='test'):
        i = 0
        collection = self.db[collection_name]
        epoch = 1
        while i < len(docs):
            chunk = docs[i:i + self.BATCH_SIZE]
            transformed_chunk = self._transform(chunk)
            collection.insert_many(transformed_chunk)
            i += self.BATCH_SIZE
            logging.info("Inserted {} documents in one go. Epoch: {}".format(self.BATCH_SIZE, epoch))
            epoch += 1
