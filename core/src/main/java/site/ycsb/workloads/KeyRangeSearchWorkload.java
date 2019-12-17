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
import java.util.Vector;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Status;
import site.ycsb.Workload;
import site.ycsb.WorkloadException;
import site.ycsb.measurements.Measurements;

/**
 * Scenario 4 of the Dremio performance test.
 */
public class KeyRangeSearchWorkload extends Workload {

  private static int operationCount;
  private static int recordCount;

  @Override
  public void init(Properties p) throws WorkloadException {
    operationCount = Integer.parseInt(p.getProperty("operationcount"));
    recordCount = Integer.parseInt(p.getProperty("recordcount"));
  }

  private static class ThreadOffset {
    public int start;
    public int count;

    public ThreadOffset(int threadId, int threadCount) {
      this.start = (threadId) * (operationCount/threadCount);
    }
  }

  public Object initThread(Properties p, int myThreadId, int threadCount) throws WorkloadException {
    return new ThreadOffset(myThreadId, threadCount);
  }

  @Override
  public boolean doInsert(DB db, Object threadState) {
    return true;
  }

  @Override
  public boolean doTransaction(DB db, Object threadState) {
    final ThreadOffset threadOffset = (ThreadOffset) threadState;
    final Status status;
    long startTime = System.nanoTime();
    final Vector<HashMap<String, ByteIterator>> result = new Vector<>();
    final String startKey =  String.format("/mysource%s", (threadOffset.start + threadOffset.count));
    final String endKey =  String.format("/mysource%s/schema/table9/file30.txt", (threadOffset.start + threadOffset.count));
    status = db.scanWithNamespaceKeyFilter("dac_namespace", startKey, endKey, recordCount, null, result);
    threadOffset.count++;
    long endTime = System.nanoTime();
    if (result.size() < 50) {
      return false;
    }
    Measurements.getMeasurements().measure("KEY_RANGE_SEARCH", (int) (endTime - startTime) / operationCount);
    return status.isOk();
  }
}
