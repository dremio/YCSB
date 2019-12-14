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

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Status;
import site.ycsb.Workload;
import site.ycsb.measurements.Measurements;

/**
 * Scenario 3 of the Dremio performance test.
 */
public class SecondaryIndexSearchWorkload extends Workload {
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    return true;
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    final Status status;
    final long startTime = System.nanoTime();
    final Vector<HashMap<String, ByteIterator>> result = new Vector<>();
    status = db.scanWithCreatedTimeFilter("jobs", "800", "2000", 50, null, result);
    final long endTime = System.nanoTime();
    if (result.size() < 50) {
      return false;
    }
    Measurements.getMeasurements().measure("SECONDARY_INDEX_SEARCH", (int) (endTime - startTime) / 1000);
    return status.isOk();
  }
}
