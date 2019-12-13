import json
import logging
from argparse import ArgumentParser

from google.cloud import spanner_v1

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

class SpannerDataInserter(object):
    def __init__(self, span_id, db_id):
        self.client = spanner_v1.Client()
        self.instance = self.client.instance(span_id)
        self.db = self.instance.database(db_id)
        self.BATCH_SIZE = 100

    def run_query(self, transaction, query):
        transaction.execute_update(query)

    def run(self, query):
        self.db.run_in_transaction(self.run_query, query)

    def batch_insert(self, table, columns, records):
        i = 0
        while i < len(records):
            chunk = records[i:i+self.BATCH_SIZE]
            with self.db.batch() as batch:
                batch.insert(table=table, columns=columns, values=chunk)
                logging.info("Inserted {} records in one batch...".format(len(chunk)))
            i += self.BATCH_SIZE

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--filename", dest="filename", type=str, help="filename to read from")
    parser.add_argument("--tablename", required=False, help="Table name to insert data into", default="jobs")
    args = parser.parse_args()

    data = json.loads(open(args.filename, "r").read())

    keys = ['allDatasets', 'queueName', 'endTime', 'jobState', 'parentDataset', 'space', 'all', 'dataset', 'datasetVersion',
     'user', 'startTime', 'sql', 'duration', 'jobId', 'queryType']

    data_inserter = SpannerDataInserter("spandb1", "ycsb-db")

    for d in data:
        job_id = d["jobId"]
        all_ds = json.dumps(d["allDatasets"])
        dataset = d["dataset"]
        ds_version = d["datasetVersion"]
        duration = d["duration"]
        end_time = d["endTime"]
        job_result = d["all"]
        job_state = d["jobState"]
        parent_ds = json.dumps(d["parentDataset"])
        query_type = d["queryType"]
        queue_name = d["queueName"]
        space = d["space"]
        sql = json.dumps(d["sql"])
        start_time = d["startTime"]
        user = d["user"]
        query = "INSERT {} (JOBID, ALL_DATASETS, DATASET, DATASET_VERSION, DURATION, END_TIME," \
                "JOB_RESULT, JOB_STATE, PARENT_DATASET, QUERY_TYPE, QUEUE_NAME, SPACE, SQL, START_TIME," \
                "USER) VALUES ('{}', '{}', '{}', '{}', {}, {}, b'{}', '{}', '{}', '{}', '{}', '{}', '{}', " \
                "{}, '{}')"\
            .format(args.tablename, job_id, all_ds, dataset, ds_version, duration, end_time, job_result, job_state, parent_ds, query_type,
                    queue_name, space, sql, start_time, user)

        data_inserter.run(query)