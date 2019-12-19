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
      "cc733c92-6853-45f6-8e49-bec741188ebb",
      "1920fba5-e53c-4cd0-993c-bbd5b83f4989",
      "54bf4122-1b33-4e17-b461-4b56f69d6025",
      "70b1da15-1d02-4c5a-9b94-204c6acbee48",
      "15222cd6-ab53-426f-beea-7d3d9689eacb",
      "84751533-758c-474d-93d6-d7392beb9184",
      "521878cb-4788-4392-8d7b-5408d60cf809",
      "e40cb3f9-1d99-4816-b464-59548605c288",
      "4ebeb629-f0da-4cf0-a59f-1f298c656a8e",
      "35ebe85d-7b2f-44a1-ab2d-4f4dc012759c",
      "ed882ad3-756d-40ad-ab4c-ce8a198f2e5e",
      "6dc54789-0b72-4c15-b248-30a59dc21535",
      "b79f0985-ee30-4dec-b2e2-5aedf77374c3",
      "bc7585f9-c73e-4ff2-9158-336368ac8d3b",
      "3d3399f6-fb43-4fb6-a84c-82240d133ec2",
      "813e3c40-7ab1-4be7-916c-72db3ffc3f20",
      "633d93b1-51a6-4bd1-84ff-0ca3930f6544",
      "90f3cf79-8f51-4513-9d8c-75f22b181997",
      "8cab36ee-c34f-479d-a67a-55cc0509ba54",
      "a8c10b5a-e39d-40cb-b318-2b6b3c987322",
      "d66e862a-0c59-4d12-956c-5ec15413486d",
      "58c35b82-aa94-4c72-824d-59a4392f1c2c",
      "c496979e-0a03-44ef-ac36-1df31df94fe7",
      "e681ff58-12f6-4a7e-986e-7c9185d69718",
      "e3054745-48d9-4728-92b3-5ea650720c75",
      "e0db797c-7a37-4f4c-83d2-6a3a9ea5d3d6",
      "060f6c42-d6d0-4e67-b19f-491c50c8e2a8",
      "a467fcc9-15ba-43ae-89f1-181c03e1f7e4",
      "434cf959-149b-4229-8418-82b50f785f7c",
      "4c488ba0-905d-40be-b92f-cef87a4a64f8",
      "883d90a0-2b3a-427c-898e-0c73a30a0159",
      "883d90a0-2b3a-427c-898e-0c73a30a0159",
      "498c98a9-74bd-4816-a2b8-c40aad11caaf",
      "2d1926a6-d3ea-4992-82cc-35fb9c200b33",
      "c1d0b7a9-8924-4365-b9dd-74effd5016b3",
      "9af68630-d5f3-413c-80a5-ce8c300c2256",
      "94e23121-db82-4c09-aacb-a3b6b446fd8b",
      "9e012075-7601-4aef-84c7-8ab76c07815e",
      "281839ff-b0b1-480a-acd5-3021a11f4f07",
      "508600e2-99f2-4fe8-ac8d-630b86dc2566",
      "cf2662a4-e604-46de-8643-cdc5deb5c864",
      "7e13edb2-0098-4e5b-be37-dbf4d5212395",
      "57ecf8f6-1eb8-4749-ab8a-dce8f1c852c3",
      "60fca931-0b4f-4079-be47-edd788af6009",
      "6c428960-8a9e-43c4-985b-c06bdb59a69c",
      "e4d4235d-b6d6-4aaa-8510-6835bd0bfe47",
      "674a944c-9df9-4091-a14b-4c8332d57fa1",
      "a701b72d-a493-45b6-ac75-93193ef2fd60",
      "49fa396f-64dc-4e54-8d16-9db4420f2d34",
      "a5bc8d77-b9e7-40eb-a300-f2c91f36bbae"
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
        System.out.println("counter.startKey:" + counter.startKey);
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
