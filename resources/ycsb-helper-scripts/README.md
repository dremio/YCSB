# Setup Environment

## Install virtualenv:
```
pip install virtualenv
```

## Execute following commands:
```
cd /path/to/Dremio-data-generator
virtualenv -p python2.7 dremio-data-gen
source dremio-data-gen/bin/activate
pip install -r requirements.txt
```

## To generate 100 jobs/namespaces in base data format:
```
python jobs_gen.py -n 100 
python namespace_gen.py -n 100
```

## Insert data into the database
### Firestore:
Run the firestore upload scripts, e.g for jobs:  
```
python spanner_data_inserter.py --filename jobs_100.json --servicename job --tablename jobs_100
```
Now run the firestore upload scripts, e.g for namespaces:  
```
python spanner_data_inserter.py --filename namespaces_100.json --servicename namespaces --tablename jobs_100
```

### Spanner:
Run the transformation script(only needed for spanner). E.g for jobs . 
```
python transform_data.py --inputFilePath jobs_100.json --serviceName Job  --databaseName Spanner --writeBufferFlushSize 1000
python transform_data.py --inputFilePath namespaces_100.json --serviceName namespace  --databaseName Spanner --writeBufferFlushSize 1000
```

Make sure that the tables are emptied before you run the insertion scripts

Now run the spanner upload scripts, e.g for jobs
```
python spanner_data_inserter.py --filename jobs_100_converted_Job_Spanner.json
```

Now run the spanner upload scripts, e.g for namespaces
```
python spanner_data_inserter_namespaces.py --filename namespaces_100_converted_Namespace_Spanner.json
```

### MongoDB:
Pre-requisites:  
The json files must also have an _id created or it will generate an object id by default

Namespaces:  
```
python atlas_mongo_updater.py --database dremio-performance-sharded  --collection dac_namespace  --filename namespaces_500K_1500_schema_paths.json
```

Jobs:  
```
python atlas_mongo_updater.py --database dremio-performance-sharded  --collection jobs --filename jobs_500000.json
```

Run YCSB Workloads for various databases(cloudspanner, mongodb, googlefirestore)
```
python /data/run-ycsb-tests.py --databasename <DATABASENAME> --ycsbfolderpath /data/YCSB --operationcount <OPERATIONCOUNT>
```

## FAQs:

Workload is failing to connect to the Firestore database:  
- Go to YCSB/workloads/workload_<WORKLOAD-TYPE>
- Check if the googlefirestore.serviceAccountKey is set to the right service account key.

Workload is failing to connect to the Spanner database:
- Make sure you have added Google Credentials  
    - Add Google Credentials:  
        - Execute the following command to add credentials to be used by YCSB  
                `gcloud auth application-default login`
        - Open URL in browser Authenticate my the Dremio account.  
        - Paste the code in the shell where gcloud command was executed.

Workload is failing to connect to the MongoDB database:  
- Make sure you have whitelisted the IP of the client.
