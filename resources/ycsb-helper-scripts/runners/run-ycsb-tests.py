import argparse
import os

# Script Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--databasename", required=True, help="Type of database to run the tests against "
                                                          "(googlefirestore, mongodb, cloudspanner)")
parser.add_argument("--ycsbfolderpath", required=True, help="Location of the YCSB folder")
parser.add_argument("--operationcount", required=False, default=1000, help="Number of operations to run")
parser.add_argument("--threadcount", required=False, default=10, help="Number of threads to run")
args = parser.parse_args()

# Global variables
operationCount = args.operationcount
databaseName = args.databasename
threadCount = args.threadcount
ycsbRootPath = os.path.abspath(args.ycsbfolderpath).rstrip('/')
ycsbDatabaseLogsPath = "{0}_ycsb_logs_{1}".format(databaseName, operationCount)
 
os.chdir(args.ycsbfolderpath)
if not os.path.exists(ycsbDatabaseLogsPath):
    os.makedirs(ycsbDatabaseLogsPath)

os.system("{0}/bin/ycsb run {1} -s -p operationcount={2} -p threadcount={3} -P workloads/workload_scan > {4}/scan.log"
          .format(ycsbRootPath, databaseName, operationCount, threadCount, ycsbDatabaseLogsPath))
os.system("{0}/bin/ycsb run {1} -s -p operationcount={2} -p threadcount={3} -P workloads/workload_repeated > "
          "{4}/repeated.log".format(ycsbRootPath, databaseName, operationCount, threadCount, ycsbDatabaseLogsPath))
os.system("{0}/bin/ycsb run {1} -s -p operationcount={2} -p recordcount=50 -p threadcount={3} "
          "-P workloads/workload_range > {4}/range.log"
          .format(ycsbRootPath, databaseName, operationCount, threadCount, ycsbDatabaseLogsPath))
os.system("{0}/bin/ycsb run {1} -s -p operationcount={2} -p recordcount=50 -p threadcount={3} "
          "-P workloads/workload_secondary > {4}/secondary.log"
          .format(ycsbRootPath, databaseName, operationCount, threadCount, ycsbDatabaseLogsPath))
os.system("{0}/bin/ycsb run {1} -s -p operationcount={2} -p threadcount={3} "
          "-P workloads/workload_putget > {4}/putget.log"
          .format(ycsbRootPath, databaseName, operationCount, threadCount, ycsbDatabaseLogsPath))
