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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.Utils;
import site.ycsb.Workload;
import site.ycsb.WorkloadException;

/**
 * Scenario 1 of the Dremio performance test.
 */
public class ScanReadWorkload extends Workload {
  private final static String[] START_KEYS = new String[] {

  };

  private static Map<String, ByteIterator> makeRowFromTemplate() {
    final Map<String, ByteIterator> rowTemplate = new HashMap<>();
    try {
      rowTemplate.put("allDatasets", new StringByteIterator("[{\"datasetType\": \"PHYSICAL_DATASET_SOURCE_FOLDER\", \"datasetPath\": [\"\", \"small\", \"career\", \"participant\"]}, {\"datasetType\": \"INVALID_DATASET_TYPE\", \"datasetPath\": [\"\", \"against\", \"professor\", \"really\"]}, {\"datasetType\": \"PHYSICAL_DATASET_SOURCE_FOLDER\", \"datasetPath\": [\"\", \"describe\", \"day\", \"century\"]}]"));
      rowTemplate.put("dataset", new StringByteIterator("/better/father/pass/glass.png"));
      rowTemplate.put("datasetVersion", new StringByteIterator("PAlvZvripDuQlQioTcbT"));
      rowTemplate.put("duration", new ByteArrayByteIterator(Utils.longToBytes(100)));
      rowTemplate.put("endTime", new ByteArrayByteIterator(Utils.longToBytes(5230)));
      rowTemplate.put("jobResult", new ByteArrayByteIterator("{\"nodeEndpoint\": {\"fabricPort\": 35452, \"roles\": {\"organization\": \"oYHVlQgCyAkGwwlabzXR\", \"add\": \"RBWRdtfMzMhGfWYssjNP\", \"indicate\": \"FwLoWmKAGnIAhIpicYWU\"}, \"jobsUri\": \"https://hamilton-jimenez.org/about/\", \"provisionId\": \"9490eddc-2a74-46ec-9f93-d6f6d919bb62\", \"startTime\": 77, \"address\": \"192.168.95.113\", \"nodeTag\": \"development-school\", \"userPort\": 36920, \"maxDirectMemory\": 94}, \"jobState\": \"ENQUEUED\", \"attemptReason\": \"SCHEMA_CHANGE\", \"extraInfo\": [{\"data\": \"College available explain first. Realize job friend do establish land billion.\\nSign decide leg necessary director member recent. Sense remember serve meeting start claim. Two less piece three.\", \"name\": \"Kenneth Frost\"}], \"attemptId\": \"386164bc-c646-4a89-8553-eaa3b1abd790\", \"snowFlakeDetails\": \"Road for control Republican ok red. Floor all police. It among everybody prevent discussion page writer. Baby best impact beautiful popular way career method.\\nReason arm too forget dark west though.\", \"jobId\": \"5bce4397-67e8-4262-b2e2-ad55854cabda\", \"jobStats\": {\"isOutputLimited\": true, \"outputRecords\": 5, \"inputBytes\": 189, \"inputRecords\": 5, \"outputBytes\": 322}, \"accelerationDetails\": \"Realize note able central attention. Effect human his trouble.\\nLoss morning fear behind skill group painting. Know drive face. Hospital from best reason you.\", \"jobInfo\": {\"resourceSchedulingInfo\": {\"east\": \"2000-11-23 13:21:23\", \"throw\": null, \"another\": \"1993-09-11 23:43:20\"}, \"spillJobDetails\": {\"skill\": null, \"woman\": \"http://pittman.com/register/\"}, \"startTime\": 5130, \"requestType\": \"RUN_SQL\", \"fieldOrigins\": [{}], \"appId\": \"fxeWocprxKBvlQZcUZrT\", \"dataset\": \"/better/father/pass/glass.png\", \"duration\": 100, \"queueName\": \"High Cost Reflections\", \"downloadInfo\": {}, \"failureInfo\": \"Red Mrs range. While character drive benefit back quickly. Production choose tell more.\\nCourt agreement relationship point. Safe them paper rock product look role.\", \"acceleration\": {}, \"detailedFailureInfo\": {\"provide\": 98110960710.6216, \"station\": \"BxNHKCHMQAeKEIbszhpw\", \"eat\": \"jiRVxOcdoDDLpHjjYICN\"}, \"space\": \"LbisciflMJShvuXzYPup\", \"materializationFor\": {}, \"cancellationInfo\": {\"successful\": \"1982-03-12 12:30:29\", \"guy\": \"https://horton-figueroa.info/\"}, \"datasetVersion\": \"PAlvZvripDuQlQioTcbT\", \"resultMetadata\": [{}], \"partitions\": [null, \"kristyrobinson@ellis-larson.info\"], \"queryType\": \"UI_INTERNAL_PREVIEW\", \"originalCost\": 69899071.584663, \"allDatasets\": [{\"datasetType\": \"PHYSICAL_DATASET_SOURCE_FOLDER\", \"datasetPath\": [\"\", \"small\", \"career\", \"participant\"]}, {\"datasetType\": \"INVALID_DATASET_TYPE\", \"datasetPath\": [\"\", \"against\", \"professor\", \"really\"]}, {\"datasetType\": \"PHYSICAL_DATASET_SOURCE_FOLDER\", \"datasetPath\": [\"\", \"describe\", \"day\", \"century\"]}], \"description\": \"School floor enter player since.\\nCup story yeah might field each. Hospital reason theory.\\nSign part these dark. Job food purpose PM deal kid. Feeling step office read evening rich kind.\", \"grandparents\": [{\"datasetType\": \"VIRTUAL_DATASET\", \"datasetPath\": [\"\", \"word\"]}, {\"datasetType\": \"PHYSICAL_DATASET_HOME_FOLDER\", \"datasetPath\": [\"\", \"theory\"]}, {\"datasetType\": \"PHYSICAL_DATASET_SOURCE_FILE\", \"datasetPath\": [\"\", \"short\"]}], \"user\": \"Eric Johnson\", \"joins\": [{}], \"sql\": \"Language question happen song rest want. Explain mention we school week charge eat. Key third indicate enter great skill first.\", \"scanPaths\": [\"EqJCdRjUrKmBVFNZikYQ\", 8861], \"parentDataset\": [{\"datasetType\": \"PHYSICAL_DATASET_HOME_FILE\", \"datasetPath\": [\"\", \"friend\", \"figure\"]}, {\"datasetType\": \"PHYSICAL_DATASET_HOME_FOLDER\", \"datasetPath\": [\"\", \"activity\", \"able\"]}, {\"datasetType\": \"VIRTUAL_DATASET\", \"datasetPath\": [\"\", \"south\", \"although\"]}], \"joinAnalysis\": {\"cold\": 67717665100510.0, \"ahead\": \"https://moreno-clark.biz/app/posts/category.htm\"}, \"commandPoolWaitMillis\": 74, \"client\": \"frnSrqJeAygzEslixtYm\", \"context\": [\"OShzpvukVkDhXkFxUrPp\"], \"endTime\": 5230, \"outputTable\": [\"http://banks-watkins.biz/author.jsp\", 9490, 1052], \"batchSchema\": \"DgXrJvnGTjtebgCIxnuK\"}, \"jobDetails\": {\"off\": 3963, \"topOperations\": [{}], \"tableDatasetProfiles\": [{}], \"area\": 6503, \"fsDatasetProfiles\": [{}], \"they\": 9146, \"instead\": 5582, \"court\": 1484, \"first\": 3912}, \"completed\": false}".getBytes()));
      rowTemplate.put("jobState", new StringByteIterator("ENQUEUED"));
      rowTemplate.put("parentDataset", new ByteArrayByteIterator("[{\"datasetType\": \"PHYSICAL_DATASET_HOME_FILE\", \"datasetPath\": [\"\", \"friend\", \"figure\"]}, {\"datasetType\": \"PHYSICAL_DATASET_HOME_FOLDER\", \"datasetPath\": [\"\", \"activity\", \"able\"]}, {\"datasetType\": \"VIRTUAL_DATASET\", \"datasetPath\": [\"\", \"south\", \"although\"]}]".getBytes("UTF-8")));
      rowTemplate.put("queryType", new StringByteIterator("UI_INTERNAL_PREVIEW"));
      rowTemplate.put("queueName", new StringByteIterator("QUEUE_NAME"));
      rowTemplate.put("space", new StringByteIterator("LbisciflMJShvuXzYPup"));
      rowTemplate.put("sql", new StringByteIterator("\"Language question happen song rest want. Explain mention we school week charge eat. Key third indicate enter great skill first.\""));
      rowTemplate.put("startTime", new ByteArrayByteIterator(Utils.longToBytes(5130)));
      rowTemplate.put("user", new StringByteIterator("Eric Johnson"));

    } catch (UnsupportedEncodingException ex) {
      // ignore.
    }

    return rowTemplate;
  }

  private static class Counter {
    public String startKey;
    public int count;
  }

  public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {
    Counter threadCounter = new Counter();
    threadCounter.startKey = START_KEYS[mythreadid % START_KEYS.length];
    return threadCounter;
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
      final Vector<HashMap<String, ByteIterator>> result = new Vector<>();
      status = db.scan("jobs", counter.startKey, 10000, null, result);
      if (result.size() < 10000) {
        return false;
      }
    } else {
      counter.count = 0;
      status = insertRow(db);
    }

    return status.isOk();
  }

  private Status insertRow(DB db) {
    final String newUuid = UUID.randomUUID().toString().toLowerCase();
    final Status status = db.insert("jobs", newUuid, makeRowFromTemplate());
    return status;
  }
}
