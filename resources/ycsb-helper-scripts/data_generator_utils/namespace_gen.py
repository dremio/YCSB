import os
import random
import argparse

from data_generator_utils.fakeman import FakeMan
from data_generator_utils.jobs_gen import date_serializer

DATASET_TYPES = frozenset(["SPACE", "SOURCE", "HOME", "FOLDER", "DATASET"])
fake = FakeMan.get_faker()


def generate_entity_paths(num_tables, num_cols):
    data = []
    for i in range(1, num_tables + 1):
        for j in range(1, num_cols + 1):
            s = "/mysource/schema/table{}/file{}.txt".format(i, j)
            elements = ["/" + x for x in s.split("/")[1:]]
            vals = ["".join(elements[0:n + 1]) for n in range(len(elements))]
            for val in vals:
                data.append(val)
    return list(set(data))


def generate(entity_path_key):
    file_path = fake.file_path(depth=3)

    namespace_entity = {
        "entityPathKey": entity_path_key,
        "id": fake.pystr(),
        "childIds": [fake.pystr() for _ in range(2)],
        "container": {
            "uuid": fake.uuid4(),
            "type": {
                "entityType": fake.random_sample(elements=DATASET_TYPES, length=1)[0],
                "entityId": fake.pystr()
            },
            "fullPathList": file_path.split("/"),
            "config": fake.pydict(5, True, str)
        },
        "attributes": [{
            "typeUrl": fake.uri(),
            "value": fake.text()
        } for _ in range(5)]
    }

    return namespace_entity


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--num-namespaces", action="store", dest="n",
                        help="Number of namespace entities to generate", type=int, default=1)

    args = parser.parse_args()
    namespaces = []
    buffer_flush_max_size = 1000
    import json

    output_file_path = "namespaces_{}.json".format(args.n)
    ENTITY_PATHS = generate_entity_paths(10, 30)
    num_entity_paths_fixed = len(ENTITY_PATHS)
    if args.n < num_entity_paths_fixed:
        args.n = num_entity_paths_fixed
    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    pseudo_random_entity_paths = []
    for ep in ENTITY_PATHS:
        pseudo_random_entity_paths.append(ep)

    for _ in range(args.n - num_entity_paths_fixed):
        pseudo_random_entity_paths.append(fake.file_path(depth=random.choice([2, 3, 4])))

    with open(output_file_path, 'w') as f:
        # f.write(json.dumps(jobs, default=date_serializer))
        write_buffer = []
        f.write("[\n")
        for ep in pseudo_random_entity_paths:
            ns = generate(ep)
            write_buffer.append(json.dumps(ns, default=date_serializer))
            if len(write_buffer) == buffer_flush_max_size:
                f.write("{},\n".format(",\n".join(write_buffer)))
                print "Flushing buffer and writing to disk..."
                del write_buffer[:]

        if len(write_buffer) != 0:
            f.write("{}\n".format(",\n".join(write_buffer)))
            f.write("]")
        else:
            f.seek(-2, 1)  # seek to end of file; f.seek(0, 2) is legal
            # f.seek(f.tell() - 1, os.SEEK_SET)
            f.write("\n]")
