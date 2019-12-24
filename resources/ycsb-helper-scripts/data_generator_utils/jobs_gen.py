import os
import datetime
import argparse
import logging

from data_generator_utils.fakeman import FakeMan

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def generate_job():
    job = dict()
    jobState = ["NOT_SUBMITTED", "STARTING", "RUNNING", "COMPLETED", "CANCELLED", "FAILED",
                "CANCELLATION_REQUESTED", "ENQUEUED", "PLANNING"]
    request_type = ["GET_CATALOGS", "GET_COLUMNS", "GET_SCHEMAS", "GET_TABLES", "CREATE_PREPARE", "EXECUTE_PREPARE",
                    "RUN_SQL", "GET_SERVER_META"]

    fake = FakeMan.get_faker(4321)
    job["jobId"] = fake.uuid4()
    job["jobState"] = fake.random_sample(elements=jobState, length=1)[0]
    job["version"] = 1
    jobInfo = dict()
    jobInfo["sql"] = fake.text()
    jobInfo["requestType"] = fake.random_sample(elements=request_type, length=1)[0]
    jobInfo["client"] = fake.pystr()
    jobInfo["user"] = fake.name()
    start_time = fake.pyint()
    end_time = start_time + 100
    jobInfo["startTime"] = start_time
    jobInfo["endTime"] = end_time
    dataset_path = fake.file_path(depth=3)
    jobInfo["dataset"] = dataset_path
    jobInfo["datasetVersion"] = fake.pystr()
    jobInfo["space"] = fake.pystr()
    QUERY_TYPES = ["UI_RUN", "UI_PREVIEW", "UI_INTERNAL_PREVIEW", "UI_INTERNAL_RUN", "UI_EXPORT", "ODBC", "JDBC",
                   "REST",
                   "ACCELERATOR_CREATE", "ACCELERATOR_DROP", "UNKNOWN", "PREPARE_INTERNAL", "ACCELERATOR_EXPLAIN",
                   "UI_INITIAL_PREVIEW"]
    QUEUE_NAMES = ["Ui Previews", "High Cost Reflections", "Low Cost Reflections", "High Cost User Queries",
                   "Low Cost User Queries"]
    jobInfo["queueName"] = fake.random_sample(elements=QUEUE_NAMES, length=1)[0]
    jobInfo["duration"] = end_time - start_time
    jobInfo["queryType"] = fake.random_sample(elements=QUERY_TYPES, length=1)[0]
    jobInfo["appId"] = fake.pystr()
    jobInfo["failureInfo"] = fake.text()
    jobInfo["fieldOrigins"] = [{}]
    jobInfo["joins"] = [{}]
    jobInfo["resultMetadata"] = [{}]
    jobInfo["acceleration"] = {}
    DATASET_TYPES = ["INVALID_DATASET_TYPE", "VIRTUAL_DATASET", "PHYSICAL_DATASET", "PHYSICAL_DATASET_SOURCE_FILE",
                     "PHYSICAL_DATASET_SOURCE_FOLDER", "PHYSICAL_DATASET_HOME_FILE", "PHYSICAL_DATASET_HOME_FOLDER"]
    jobInfo["grandparents"] = [{
        "datasetPath": fake.file_path(depth=1).split("/")[:-1],
        "datasetType": fake.random_sample(elements=DATASET_TYPES, length=1)[0]
    } for _ in range(3)]

    jobInfo["parentDataset"] = [{
        "datasetPath": fake.file_path(depth=2).split("/")[:-1],
        "datasetType": fake.random_sample(elements=DATASET_TYPES, length=1)[0]
    } for _ in range(3)]

    jobInfo["allDatasets"] = [{
        "datasetPath": fake.file_path(depth=3).split("/")[:-1],
        "datasetType": fake.random_sample(elements=DATASET_TYPES, length=1)[0]
    } for _ in range(3)]

    jobInfo["downloadInfo"] = {}
    jobInfo["description"] = fake.text()
    jobInfo["materializationFor"] = {}
    jobInfo["originalCost"] = fake.pyfloat()
    jobInfo["partitions"] = fake.pylist(nb_elements=3)
    jobInfo["scanPaths"] = fake.pylist(nb_elements=3)
    jobInfo["detailedFailureInfo"] = fake.pydict(nb_elements=3)
    jobInfo["joinAnalysis"] = fake.pydict(nb_elements=3)
    jobInfo["context"] = fake.pylist(nb_elements=3)
    jobInfo["resourceSchedulingInfo"] = fake.pydict(nb_elements=3)
    jobInfo["outputTable"] = fake.pylist(nb_elements=3)
    jobInfo["cancellationInfo"] = fake.pydict(nb_elements=3)
    jobInfo["spillJobDetails"] = fake.pydict(nb_elements=3)
    jobInfo["batchSchema"] = fake.pystr()
    jobInfo["commandPoolWaitMillis"] = fake.pyint(min_value=0, max_value=100)
    job["jobInfo"] = jobInfo

    jobStats = {
        "inputBytes": fake.pyint(min_value=0, max_value=500),
        "outputBytes": fake.pyint(min_value=0, max_value=500),
        "inputRecords": fake.pyint(min_value=0, max_value=10),
        "outputRecords": fake.pyint(min_value=0, max_value=10),
        "isOutputLimited": fake.pybool()
    }
    job["jobStats"] = jobStats

    jobDetails = fake.pydict(5, True, int)
    jobDetails["tableDatasetProfiles"] = [{}]
    jobDetails["fsDatasetProfiles"] = [{}]
    jobDetails["topOperations"] = [{}]

    job["jobDetails"] = jobDetails

    ATTEMPT_REASON = ["NONE", "OUT_OF_MEMORY", "SCHEMA_CHANGE", "INVALID_DATASET_METADATA", "JSON_FIELD_CHANGE"]

    job["attemptReason"] = fake.random_sample(elements=ATTEMPT_REASON, length=1)[0]
    job["attemptId"] = fake.uuid4()
    nodeEndpoint = {
        "address": fake.ipv4_private(),
        "userPort": fake.pyint(min_value=0, max_value=65535),
        "fabricPort": fake.pyint(min_value=0, max_value=65535),
        "roles": fake.pydict(3, True, str),
        "startTime": fake.pyint(min_value=0, max_value=100),
        "provisionId": fake.uuid4(),
        "maxDirectMemory": fake.pyint(min_value=0, max_value=100),
        "nodeTag": fake.slug(),
        "jobsUri": fake.uri()
    }

    job["nodeEndpoint"] = nodeEndpoint
    job["accelerationDetails"] = fake.text()
    job["snowFlakeDetails"] = fake.text()
    job["extraInfo"] = [{
        "name": fake.name(),
        "data": fake.text()
    }]

    job["completed"] = fake.pybool()
    return job


def date_serializer(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--num-jobs", action="store", dest="n",
                        help="Number of job entries to generate", type=int, default=1)

    args = parser.parse_args()
    jobs = []

    buffer_flush_max_size = 1000
    import json

    output_file_path = "jobs_{}.json".format(args.n)
    if os.path.exists(output_file_path):
        os.remove(output_file_path)
    with open(output_file_path, 'w') as f:
        write_buffer = []
        f.write("[\n")
        i = 0
        for i in range(args.n):
            job = generate_job()
            write_buffer.append(json.dumps(job, default=date_serializer))
            if len(write_buffer) == buffer_flush_max_size:
                f.write("{},\n".format(",\n".join(write_buffer)))
                logging.info("Flushing buffer and writing to disk...")
                del write_buffer[:]
        if len(write_buffer) != 0:
            f.write("{}\n".format(",\n".join(write_buffer)))
            f.write("]")
        else:
            f.seek(-2, 1)  # seek to end of file; f.seek(0, 2) is legal
            f.write("\n]")
    logging.info("Done")