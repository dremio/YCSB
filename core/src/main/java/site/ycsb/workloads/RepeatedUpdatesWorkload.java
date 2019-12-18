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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Pair;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.Workload;
import site.ycsb.WorkloadException;
import site.ycsb.measurements.Measurements;

/**
 * Scenario 2 of the Dremio performance test.
 */
public class RepeatedUpdatesWorkload extends Workload {

  private static final String JOB_IDS_FILE = "jobIds.json";

  private static class JobIdTracker {
    public List<String> jobIds;
  }

  @Override
  public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {
    final JobIdTracker tracker = new JobIdTracker();
    ClassLoader classLoader = getClass().getClassLoader();
    final File file;

    URL resource = classLoader.getResource(JOB_IDS_FILE);
    if (resource == null) {
      throw new IllegalArgumentException("file is not found!");
    } else {
      file = new File(resource.getFile());
    }
    JSONParser parser = new JSONParser();
    FileReader reader = null;
    try {
      reader = new FileReader(file);
      JSONObject jsonObject = (JSONObject) parser.parse(reader);
      List<String> jobIds = new ArrayList<>();
      JSONArray jobIdArray =
          (JSONArray) jsonObject.get(String.valueOf(mythreadid));
      for (Object o: jobIdArray) {
        jobIds.add(o.toString());
      }
      tracker.jobIds = jobIds;
      return tracker;
    } catch (IOException  | ParseException e) {
      e.printStackTrace();
    }
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
    Long version = null;

    for (String jobId: tracker.jobIds) {
      int i = 1;
      while ((i <= 5) && (status.isOk())) {
        updatedValues.put("jobState", new StringByteIterator(getJobStateString(i)));
        Pair statusVersionPair = db.findAndUpdate("jobs", jobId, version,
            updatedValues);
        version = (Long) statusVersionPair.getVersion();
        status = statusVersionPair.getStatus();
        i += 1;
      }
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