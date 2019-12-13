import json
from argparse import ArgumentParser
from base64 import b64encode

from spanner_data_inserter import SpannerDataInserter

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--filename", dest="filename", type=str, help="filename to read from")
    parser.add_argument("--tablename", required=False, help="Table name to insert data into", default="namespaces")
    args = parser.parse_args()

    data = json.loads(open(args.filename, "r").read())
    data_inserter = SpannerDataInserter("spandb1", "ycsb-db")

    columns = ('ENTITY_ID', 'CONTAINER', 'ENTITY_TYPE', 'NAMESPACE_INTERNAL_KEY')
    records = []
    for d in data:
        container = b64encode(b'{}'.format(json.dumps(d["container"])).encode("utf-8"))
        entityId = d["entityId"]
        entityType = d["entityType"]
        ns_internal_key = b64encode(b'{}'.format(d["entityPathKey"].encode("utf-8")))
        records.append((entityId, container, entityType, ns_internal_key))
        query = "INSERT {} (ENTITY_ID, CONTAINER, ENTITY_TYPE, NAMESPACE_INTERNAL_KEY)" \
                " VALUES ('{}', b'{}', '{}', b{})"\
            .format(args.tablename, entityId, container, entityType, ns_internal_key)

    data_inserter.batch_insert('namespaces', columns, records)
    print ("Done..!!")