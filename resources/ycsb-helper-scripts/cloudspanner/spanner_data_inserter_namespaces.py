import json
from argparse import ArgumentParser
from base64 import b64encode

from cloudspanner.spanner_data_inserter_jobs import SpannerDataInserter

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--filename", dest="filename", type=str, help="filename to read from")
    parser.add_argument("--tablename", required=False, help="Table name to insert data into", default="dac_namespace")
    args = parser.parse_args()

    data = json.loads(open(args.filename, "r").read())
    data_inserter = SpannerDataInserter("spandb1", "ycsb-db")

    columns = ('entityId', 'container', 'entityType', 'entityPathKey')
    records = []
    for d in data:
        container = b64encode(b'{}'.format(json.dumps(d["container"])).encode("utf-8"))
        entityId = d["entityId"]
        entityType = d["entityType"]
        entityPathKey = d["entityPathKey"]
        records.append((entityId, container, entityType, entityPathKey))
        query = "INSERT {} (entityId, container, entityType, entityPathKey)" \
                " VALUES ('{}', b'{}', '{}', {})"\
            .format(args.tablename, entityId, container, entityType, entityPathKey)

    data_inserter.batch_insert(args.tablename, columns, records)
    print ("Done..!!")