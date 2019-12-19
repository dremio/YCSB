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
import java.util.Vector;
import java.util.Properties;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Status;
import site.ycsb.Workload;
import site.ycsb.WorkloadException;
import site.ycsb.measurements.Measurements;

/**
 * Scenario 3 of the Dremio performance test.
 */
public class SecondaryIndexSearchWorkload extends Workload {
  private int recordCount;

  @Override
  public boolean doInsert(DB db, Object threadState) {
    return true;
  }
  @Override
  public void init(Properties p) throws WorkloadException {
    recordCount = Integer.parseInt(p.getProperty("recordcount"));
  }

  private static class ThreadOffset {
    public int start;
    public int end;
    public int count = 0;

    public ThreadOffset(int threadId) {
      /*
          ThreadId 0: start=0, end=25
          ThreadId 1: start=25, end=50
          ...
          ...
          ThreadId 49: start=1225, end=1250
      */
      this.start = threadId * 25;
      this.end = this.start + 25;
    }
  }

  public Object initThread(Properties p, int myThreadId, int threadCount) throws WorkloadException {
    return new ThreadOffset(myThreadId);
  }


    @Override
  public boolean doTransaction(DB db, Object threadState) {
    final ThreadOffset threadOffset = (ThreadOffset) threadState;
    final Status status;
    final Vector<HashMap<String, ByteIterator>> result = new Vector<>();
    final String startRange =  Integer.toString(threadOffset.start + threadOffset.count);
    final String endRange =  Integer.toString(threadOffset.end + threadOffset.count);

    final long startTime = System.nanoTime();
    status = db.scanWithCreatedTimeFilter("jobs", startRange, endRange, recordCount, null, result);
    final long endTime = System.nanoTime();
    threadOffset.count++;

    if (result.size() < recordCount) {
      return false;
    }
      final long latency = (endTime - startTime);
      Measurements.getMeasurements().measure("SECONDARY_INDEX_SEARCH", (int)(latency/1000));
    return status.isOk();
  }
}
