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

  private int operationCount;
  private int recordCount;

  @Override
  public void init(Properties p) throws WorkloadException {
    operationCount = Integer.parseInt(p.getProperty("operationcount"));
    recordCount = Integer.parseInt(p.getProperty("recordcount"));
  }

  private static class ThreadOffset {
    public int start;
    public int count;

    public ThreadOffset(int threadId, int threadCount, int operationCount) {
      this.start = (threadId) * (operationCount/threadCount);
      this.count = 0;
    }
  }

  public Object initThread(Properties p, int myThreadId, int threadCount) throws WorkloadException {
    return new ThreadOffset(myThreadId, threadCount, this.operationCount);
  }

  @Override
  public boolean doInsert(DB db, Object threadState) {
    return true;
  }

  @Override
  public boolean doTransaction(DB db, Object threadState) {
    final ThreadOffset threadOffset = (ThreadOffset) threadState;
    final Status status;
    final Vector<HashMap<String, ByteIterator>> result = new Vector<>();

    // Maximum index for source is 1499 in the inserted data.
    int mySourceIndex = (threadOffset.start + threadOffset.count) % 1500;
    // Maximum index for table is 9 in the inserted data.
    int tableIndex = (threadOffset.count % 7) + 3;
    final String startKey =  String.format("/mysource%s", mySourceIndex);
    final String endKey =  String.format("/mysource%s/schema/table%s/file30.txt", mySourceIndex, tableIndex);
    long startTime = System.nanoTime();
    status = db.scanWithNamespaceKeyFilter("dac_namespace", startKey, endKey, recordCount, null, result);
    long endTime = System.nanoTime();
    threadOffset.count++;

    if (result.size() < recordCount) {
      return false;
    }
    Measurements.getMeasurements().measure("KEY_RANGE_SEARCH", (int) (endTime - startTime) / 1000);
    return status.isOk();
  }
}
