import json
import logging
from argparse import ArgumentParser
from base64 import b64encode

from google.cloud import spanner_v1

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


class SpannerDataInserter(object):
    def __init__(self, span_id, db_id):
        self.client = spanner_v1.Client()
        self.instance = self.client.instance(span_id)
        self.db = self.instance.database(db_id)
        self.BATCH_SIZE = 400

    @staticmethod
    def run_query(transaction, query):
        transaction.execute_update(query)

    def run(self, query):
        self.db.run_in_transaction(self.run_query, query)

    def batch_insert(self, table, columns, records):
        i = 0
        while i < len(records):
            chunk = records[i:i + self.BATCH_SIZE]
            with self.db.batch() as batch:
                batch.insert(table=table, columns=columns, values=chunk)
                logging.info("Inserted {} records in one batch...".format(len(chunk)))
            i += self.BATCH_SIZE


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--filename", type=str, help="filename to read from")
    parser.add_argument("--tablename", required=False, help="Table name to insert data into", default="jobs")
    args = parser.parse_args()

    logging.info("Loading the JSON file {}".format(args.filename))
    data = json.loads(open(args.filename, "r").read())
    logging.info("Loaded data successfully from file {}".format(args.filename))
    data_inserter = SpannerDataInserter("spandb1", "ycsb-db")

    logging.info("Generating queries")
    columns = ('jobId', 'allDatasets', 'dataset', 'datasetVersion', 'duration', 'endTime', 'jobResult', 'jobState',
               'parentDataset', 'queryType', 'queueName', 'space', 'sql', 'startTime', 'user', 'version')
    records = []
    for d in data:
        job_id = d["jobId"]
        all_ds = json.dumps(d["allDatasets"])
        dataset = d["dataset"]
        ds_version = d["datasetVersion"]
        duration = d["duration"]
        end_time = d["endTime"]
        job_result = b64encode(b'{}'.format(json.dumps(d["jobResult"])).encode("utf-8"))
        job_state = d["jobState"]
        parent_ds = json.dumps(d["parentDataset"])
        query_type = d["queryType"]
        queue_name = d["queueName"]
        space = d["space"]
        sql = json.dumps(d["sql"])
        start_time = d["startTime"]
        user = d["user"]
        version = d["version"]
        records.append(
            (job_id, all_ds, dataset, ds_version, duration, end_time, job_result, job_state, parent_ds, query_type,
             queue_name, space, sql, start_time, user, version))
        query = "INSERT {} (jobId, allDatasets, dataset, datasetVersion, duration, endTime," \
                "jobResult, jobState, parentDataset, queryType, queueName, space, sql, startTime," \
                "user, version) VALUES ('{}', '{}', '{}', '{}', {}, {}, b'{}', '{}', '{}', '{}', '{}', '{}', '{}', " \
                "{}, '{}', {})".format(args.tablename, job_id, all_ds, dataset, ds_version, duration, end_time,
                                       job_result, job_state, parent_ds, query_type, queue_name, space, sql, start_time,
                                       user, version)

    data_inserter.batch_insert(args.tablename, columns, records)
    logging.info("Done")