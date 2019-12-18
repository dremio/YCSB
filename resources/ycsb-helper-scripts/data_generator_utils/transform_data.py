import json
import argparse
import os

# Script Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--inputfilepath", required=True, help="Path to the input file with the base data")
parser.add_argument("--servicename", required=True, help="Type of service to convert data for, Job or Namespace")
parser.add_argument("--databasename", required=True,
                    help="Type of database to convert data for, Spanner or Firestore Native")
parser.add_argument("--writebufferflushsize", required=False,
                    help="Number of Json objects after which the formatted data gets written to the file", default=10)
args = parser.parse_args()

# Global Variables
outputFilePath = "{}_converted_{}_{}.json".format(os.path.splitext(args.inputfilepath)[0], args.servicename,
                                                  args.databasename)
writeBufferFlushSize = int(args.writebufferflushsize)

if writeBufferFlushSize < 2:
    raise Exception("writeBufferFlushSize should be greater than 2")


def keys_to_extract(service_name):
    switcher = {
        # Define nested keys in dot notation
        'job': "jobId,jobState,jobInfo.user,jobInfo.space,jobInfo.dataset,jobInfo.datasetVersion,jobInfo.sql,"
               "jobInfo.queryType,jobInfo.startTime,jobInfo.endTime,jobInfo.duration,jobInfo.queueName,"
               "jobInfo.parentDataset,jobInfo.allDatasets",
        'namespace': "entityPathKey,container.type.entityType,container.type.entityId"
    }
    return switcher.get(service_name, "{} is invalid service name".format(service_name))


def keys_to_convert_to_bytes(service_name):
    switcher = {
        'job': "all_keys",
        'namespace': "container"
    }
    return switcher.get(service_name, "{} is invalid service name".format(service_name))


def get_nested_key_value(data, nested_key_array):
    for nestedKey in nested_key_array:
        data = data.get(nestedKey, "error")
    return data


def extract_keys_from_json(json_data, keys_to_extract):
    keys_to_extract_array = keys_to_extract.split(',')
    json_data = json.loads(json_data)
    formatted_data = {}
    for key in keys_to_extract_array:
        if '.' in key:
            nested_key_array = key.split('.')
            nested_key_value = get_nested_key_value(json_data, nested_key_array)
            if nested_key_value == "error":
                raise Exception("Invalid Key")
            formatted_data[nested_key_array[-1]] = nested_key_value
        else:
            formatted_data[key] = json_data[key]
    return formatted_data


def transform_json_to_bytes(formatted_data, json_data, keys_to_convert):
    keys_to_convert_array = keys_to_convert.split(',')
    json_data = json.loads(json_data)
    for key in keys_to_convert_array:
        if '.' in key:
            nested_key_array = key.split('.')
            nested_key_value = get_nested_key_value(json_data, nested_key_array)
            if nested_key_value == "error":
                raise Exception("Invalid Key")
            formatted_data[nested_key_array[-1]] = json.dumps(nested_key_value).encode("utf-8")
        elif key == "all_keys":
            formatted_data["jobResult"] = json.dumps(json_data).encode("utf-8")
        else:
            formatted_data[key] = json.dumps(json_data[key]).encode("utf-8")
    formatted_data["version"] = 1
    return formatted_data


def format_json_and_write_to_file(input_file_path, output_file_path):
    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    extracted_keys = keys_to_extract(args.servicename.lower())
    converted_keys = keys_to_convert_to_bytes(args.servicename.lower())

    with open(input_file_path, 'r') as inputFile, open(output_file_path, 'wa') as outputFile:
        write_buffer = []
        for line in inputFile:
            line = line.rstrip("\n\r").rstrip(",")
            if line == '[':
                outputFile.write("{}\n".format(line))
            elif line == ']':
                if len(write_buffer) != 0:
                    outputFile.write("{}\n".format(",\n".join(write_buffer)))
                    outputFile.write(line)
                else:
                    # Remove the trailing comma
                    outputFile.seek(-2, 1)
                    outputFile.write("\n{}".format(line))
            else:
                formatted_data = extract_keys_from_json(line, extracted_keys)
                formatted_data = transform_json_to_bytes(formatted_data, line, converted_keys)
                write_buffer.append(json.dumps(formatted_data))
                if len(write_buffer) == writeBufferFlushSize:
                    print "Flushing buffer and writing to disk..."
                    outputFile.write("{},\n".format(",\n".join(write_buffer)))
                    del write_buffer[:]


format_json_and_write_to_file(args.inputfilepath, outputFilePath)
print ("Done..!!")
