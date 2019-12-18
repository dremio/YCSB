import json
import argparse
import os

# Script Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--inputfilepath", required=True, help="Path to the input file with the base data")
parser.add_argument("--servicename", required=True, help="Type of service to convert data for, Job or Namespace")
parser.add_argument("--databasename", required=True, help="Type of database to convert data for, Spanner or Firestore Native")
parser.add_argument("--writebufferflushsize", required=False, help="Number of Json objects after which the formatted data gets written to the file", default=10)
args = parser.parse_args()

# Global Variables
outputFilePath="{}_converted_{}_{}.json".format(os.path.splitext(args.inputfilepath)[0], args.servicename, args.databasename)
writeBufferFlushSize = int(args.writebufferflushsize)

if writeBufferFlushSize < 2:
   raise Exception("writeBufferFlushSize should be greater than 2")

def keys_to_extract(servicename):
    switcher={
                # Define nested keys in dot notation
                'job':"jobId,jobState,jobInfo.user,jobInfo.space,jobInfo.dataset,jobInfo.datasetVersion,jobInfo.sql,jobInfo.queryType,jobInfo.startTime,jobInfo.endTime,jobInfo.duration,jobInfo.queueName,jobInfo.parentDataset,jobInfo.allDatasets",
                'namespace':"entityPathKey,container.type.entityType,container.type.entityId"
             }
    return switcher.get(servicename,"{} is invalid service name".format(servicename))

def keys_to_convert_to_bytes(servicename):
    switcher={
                'job':"all_keys",
                'namespace':"container"
             }
    return switcher.get(servicename,"{} is invalid service name".format(servicename))

def get_nested_key_value(data, nestedKeyArray):
    for nestedKey in nestedKeyArray:
        data = data.get(nestedKey, "error")
    return data

def extract_keys_from_json(jsonData, keysToExtract):
    keysToExtractArray = keysToExtract.split(',')
    jsonData = json.loads(jsonData)
    formattedData = {}
    for key in keysToExtractArray:
        if '.' in key:
            nestedKeyArray = key.split('.')
            nestedKeyValue = get_nested_key_value(jsonData, nestedKeyArray)
            if nestedKeyValue == "error": 
                raise Exception("Invalid Key")
            formattedData[nestedKeyArray[-1]] = nestedKeyValue
        else:
           formattedData[key] = jsonData[key]
    return formattedData

def transform_json_to_bytes(formattedData, jsonData, keysToConvert):
    keysToConvertArray = keysToConvert.split(',')
    jsonData = json.loads(jsonData)
    for key in keysToConvertArray:
        if '.' in key:
            nestedKeyArray = key.split('.')
            nestedKeyValue = get_nested_key_value(jsonData, nestedKeyArray)
            if nestedKeyValue == "error": 
                raise Exception("Invalid Key")
            formattedData[nestedKeyArray[-1]] = json.dumps(nestedKeyValue).encode("utf-8")
        elif key == "all_keys":
            formattedData["jobResult"] = json.dumps(jsonData).encode("utf-8")
        else:
           formattedData[key]=json.dumps(jsonData[key]).encode("utf-8")
    formattedData["version"] = 1
    return formattedData

def format_json_and_write_to_file(inputfilepath, outputFilePath):
    if os.path.exists(outputFilePath):
        os.remove(outputFilePath)

    keysToExtract = keys_to_extract(args.servicename.lower())
    keysToConvert = keys_to_convert_to_bytes(args.servicename.lower())

    with open(inputfilepath, 'r') as inputFile, open(outputFilePath, 'wa') as outputFile:
        writeBuffer = []
        for line in inputFile:
            line = line.rstrip("\n\r").rstrip(",")
            if line == '[':
                outputFile.write("{}\n".format(line))
            elif line == ']':
                if len(writeBuffer) != 0:
                    outputFile.write("{}\n".format(",\n".join(writeBuffer)))
                    outputFile.write(line)
                else:
                    # Remove the trailing comma
                    outputFile.seek(-2,1)
                    outputFile.write("\n{}".format(line))
            else:
                formattedData = extract_keys_from_json(line, keysToExtract)
                formattedData = transform_json_to_bytes(formattedData, line, keysToConvert)
                writeBuffer.append(json.dumps(formattedData))
                if len(writeBuffer) == writeBufferFlushSize:
                    print "Flushing buffer and writing to disk..."
                    outputFile.write("{},\n".format(",\n".join(writeBuffer)))
                    del writeBuffer[:]

format_json_and_write_to_file(args.inputfilepath, outputFilePath)
print ("Done..!!")