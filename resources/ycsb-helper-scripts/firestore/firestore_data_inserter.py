import json
import logging
from argparse import ArgumentParser
from google.cloud import firestore

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def get_document_id(serviceName):
    switcher = {
        'job': 'jobId',
        'namespace': 'entityPathKey'
    }
    return switcher.get(serviceName, "{} is invalid service name".format(serviceName))


parser = ArgumentParser()
parser.add_argument("--filename", dest="filename", type=str, help="filename to read from")
parser.add_argument("--servicename", required=True, help="Type of service to insert data into, Job or Namespace")
parser.add_argument("--tablename", required=False, help="Table name to insert data into", default="jobs")
args = parser.parse_args()

# Firestore batched write can contain up to 500 operations.
batchSize = 500
db = firestore.Client()
batch = db.batch()

logging.info("Loading the JSON file {}".format(args.filename))
data = json.loads(open(args.filename, "r").read())
logging.info("JSON file {} loaded successfully".format(args.filename))

totalRecords = len(data)
insertedRecords = 0
batchCount = 0
for d in data:
    # Replace Slash with Underscore as Firestore doesn't
    # allow forward slash in Document or Collection Id
    # https://firebase.google.com/docs/firestore/quotas#collections_documents_and_fields
    documentId = d[get_document_id(args.servicename)].replace("/", "_")
    doc_ref = db.collection(args.tablename).document(documentId)
    batch.set(doc_ref, d)
    batchCount += 1
    if batchCount == batchSize:
        batch.commit()
        insertedRecords += batchCount
        logging.info("Inserted {} out of {} records into table {} from file {}".format(insertedRecords, totalRecords,
                                                                                       args.tablename, args.filename))
        batchCount = 0
if batchCount > 0:
    batch.commit()
    insertedRecords += batchCount
    logging.info("Inserted {} out of {} records into table {} from file {}".format(insertedRecords, totalRecords,
                                                                                   args.tablename, args.filename))
logging.info("Successfully inserted {} records into table {} from file {}".format(totalRecords, args.tablename,
                                                                                  args.filename))
