/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package site.ycsb.workloads;

import java.util.HashMap;
import java.util.Properties;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.Workload;
import site.ycsb.WorkloadException;
import site.ycsb.measurements.Measurements;

/**
 * Scenario 2 of the Dremio performance test.
 */
public class RepeatedUpdatesWorkload extends Workload {

  // This array should expand if we want to test more threads working on distinct keys.
  private static final String[] TEST_JOB_IDS = new String[]{
      "bb2050d0-ee9a-4428-b6ad-09f43a0278c0",
      "ca64ca01-e396-4ff5-8491-96e7dff473cf",
      "0793a46e-7660-4613-b4cd-9fb1701b874d",
      "e5bf1e6f-51c3-4c0d-a571-9db8fd60bca1",
      "26642a82-17ef-4f02-b8fd-97be94ddb0a8"
  };

  private static class JobIdTracker {
    public String jobId;
  }

  @Override
  public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {
    final JobIdTracker tracker = new JobIdTracker();
    tracker.jobId = TEST_JOB_IDS[mythreadid % TEST_JOB_IDS.length];
    return tracker;
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    return true;
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    final JobIdTracker tracker = (JobIdTracker) threadstate;
    final long startTime = System.nanoTime();
    final HashMap<String, ByteIterator> updatedValues = new HashMap<>();
    // JobStates are numeric values. Iterate through job states.
    Status status = Status.OK;
    for (int i = 1; i <= 5 && status.isOk(); i++) {
      updatedValues.put("jobState", new StringByteIterator(getJobStateString(i)));
      status = db.update("jobs", tracker.jobId, updatedValues);
    }
    final long endTime = System.nanoTime();
    Measurements.getMeasurements().measure("RAPID_UPDATE", (int) (endTime - startTime) / 1000);
    return status.isOk();
  }

  private static String getJobStateString(int i) {
    switch(i) {
    case 0: return "NOT_SUBMITTED";
    case 1: return "STARTING";
    case 2: return "RUNNING";
    case 3: return "COMPLETED";
    case 4: return "CANCELED";
    case 5: return "FAILED";
    case 6: return "CANCELLATION_REQUESTED";
    case 7: return "ENQUEUED";
    case 8: return "PLANNING";
    default: return null;
    }
  }
}