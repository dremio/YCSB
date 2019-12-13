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

/**
 * Scenario 1 of the Dremio performance test.
 */
public class ScanReadWorkload extends Workload {

  private final HashMap<String, ByteIterator> rowTemplate = new HashMap<> {

  }

  private static class Counter {
    public int count;
  }

  public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {
    return new Counter();
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    // No-op implementation because data should be loaded in the KV-store in advance.
    return true;
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    final Counter counter = (Counter) threadstate;
    final Status status;
    if (counter.count < 4) {
      counter.count++;
      status = db.scan("jobs", null, 100000, null, new Vector<HashMap<String, ByteIterator>>());
    } else {
      counter.count = 0;
      status = insertRow(db);
    }

    return status.isOk();
  }

  private Status insertRow(DB db) {
    final Status status = db.insert("jobs",)
  }
}
