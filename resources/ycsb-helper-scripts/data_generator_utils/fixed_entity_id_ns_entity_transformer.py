import json
import os
from argparse import ArgumentParser

from data_generator_utils.jobs_gen import date_serializer
from data_generator_utils.namespace_gen import generate_entity_paths

from faker import Faker
from faker.providers import file

fake = Faker()
fake.seed_instance(4321)
fake.add_provider(file)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--input-filename", dest="input_filename", type=str, help="filename to read from")
    parser.add_argument("--output-filename", dest="output_filename", type=str, help="Output filename to write to")
    args = parser.parse_args()

    data = json.loads(open(args.input_filename, "r").read())
    ENTITY_PATHS = generate_entity_paths(10, 30)
    n = len(data)
    pseudo_random_entity_paths = []
    for ep in ENTITY_PATHS:
        pseudo_random_entity_paths.append(ep)

    for j in range(n - len(ENTITY_PATHS)):
        base_path = "".join(fake.file_path(depth=3).split(".")[:-1]) + "_" + str(j + 1)
        pseudo_random_entity_paths.append("{}.txt".format(base_path))

    assert n == len(pseudo_random_entity_paths)
    output_file_path = args.output_filename
    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    buffer_flush_max_size = min(n / 10, 100)

    with open(output_file_path, 'w') as f:
        # f.write(json.dumps(jobs, default=date_serializer))
        write_buffer = []
        f.write("[\n")
        for i, ep in enumerate(pseudo_random_entity_paths):
            ns = data[i]
            ns["entityPathKey"] = ep
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
