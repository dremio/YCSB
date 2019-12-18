/**
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.db.cloudspanner;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Type;
import com.google.common.base.Joiner;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.StructReader;
import com.google.cloud.spanner.TimestampBound;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Pair;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.Utils;
import site.ycsb.workloads.CoreWorkload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;

/**
 * YCSB Client for Google's Cloud Spanner.
 */
public class CloudSpannerClient extends DB {

  /**
   * The names of properties which can be specified in the config files and flags.
   */
  public static final class CloudSpannerProperties {
    private CloudSpannerProperties() {}

    /**
     * The Cloud Spanner database name to use when running the YCSB benchmark, e.g. 'ycsb-database'.
     */
    static final String DATABASE = "cloudspanner.database";
    /**
     * The Cloud Spanner instance ID to use when running the YCSB benchmark, e.g. 'ycsb-instance'.
     */
    static final String INSTANCE = "cloudspanner.instance";
    /**
     * Choose between 'read' and 'query'. Affects both read() and scan() operations.
     */
    static final String READ_MODE = "cloudspanner.readmode";
    /**
     * The number of inserts to batch during the bulk loading phase. The default value is 1, which means no batching
     * is done. Recommended value during data load is 1000.
     */
    static final String BATCH_INSERTS = "cloudspanner.batchinserts";
    /**
     * Number of seconds we allow reads to be stale for. Set to 0 for strong reads (default).
     * For performance gains, this should be set to 10 seconds.
     */
    static final String BOUNDED_STALENESS = "cloudspanner.boundedstaleness";

    // The properties below usually do not need to be set explicitly.

    /**
     * The Cloud Spanner project ID to use when running the YCSB benchmark, e.g. 'myproject'. This is not strictly
     * necessary and can often be inferred from the environment.
     */
    static final String PROJECT = "cloudspanner.project";
    /**
     * The Cloud Spanner host name to use in the YCSB run.
     */
    static final String HOST = "cloudspanner.host";
    /**
     * Number of Cloud Spanner client channels to use. It's recommended to leave this to be the default value.
     */
    static final String NUM_CHANNELS = "cloudspanner.channels";
  }

  private static int fieldCount;

  private static boolean queriesForReads;

  private static int batchInserts;

  private static TimestampBound timestampBound;

  private static String standardQuery;

  private static String standardScan;

  private static String standardScanNoStartKey;

  private static final ArrayList<String> STANDARD_FIELDS = new ArrayList<>();

  private static final String JOBS_TABLE_NAME = "jobs";

  private static final String JOBS_PRIMARY_KEY_COLUMN = "jobId";

  private static final ArrayList<String> JOBS_FIELDS = new ArrayList<>(
      Arrays.asList(
          "jobId",
          "allDatasets",
          "dataset",
          "datasetVersion",
          "duration",
          "endTime",
          "jobResult",
          "jobState",
          "parentDataset",
          "queryType",
          "queueName",
          "space",
          "sql",
          "startTime",
          "user"));

  private static final String DAC_NAMESPACE_TABLE_NAME = "dac_namespace";

  private static final String DAC_NAMESPACE_PRIMARY_KEY_COLUMN = "entityPathKey";

  private static final ArrayList<String> DAC_NAMESPACE_FIELDS = new ArrayList<>(
      Arrays.asList(
          "entityPathKey",
          "entityType",
          "entityId",
          "container"));

  private static final String PRIMARY_KEY_COLUMN = "id";

  private static final Logger LOGGER = Logger.getLogger(CloudSpannerClient.class.getName());

  // Static lock for the class.
  private static final Object CLASS_LOCK = new Object();

  // Single Spanner client per process.
  private static Spanner spanner = null;

  // Single database client per process.
  private static DatabaseClient dbClient = null;

  // Buffered mutations on a per object/thread basis for batch inserts.
  // Note that we have a separate CloudSpannerClient object per thread.
  private final ArrayList<Mutation> bufferedMutations = new ArrayList<>();

  private static void constructStandardQueriesAndFields(Properties properties) {
    String table = properties.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
    final String fieldprefix = properties.getProperty(CoreWorkload.FIELD_NAME_PREFIX,
                                                      CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);
    standardQuery = new StringBuilder()
        .append("SELECT * FROM ").append("jobs").append(" WHERE jobId=@key").toString();
    standardScan = new StringBuilder()
        .append("SELECT * FROM ").append("jobs").append(" WHERE jobId>=@startKey LIMIT @count").toString();
    standardScanNoStartKey = new StringBuilder()
        .append("SELECT * FROM ").append("jobs").append(" LIMIT @count").toString();
    for (int i = 0; i < fieldCount; i++) {
      STANDARD_FIELDS.add(fieldprefix + i);
    }
  }

  private static Spanner getSpanner(Properties properties, String host, String project) {
    if (spanner != null) {
      return spanner;
    }
    String numChannels = properties.getProperty(CloudSpannerProperties.NUM_CHANNELS);
    int numThreads = Integer.parseInt(properties.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
    SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder()
        .setSessionPoolOption(SessionPoolOptions.newBuilder()
            .setMinSessions(numThreads)
            // Since we have no read-write transactions, we can set the write session fraction to 0.
            .setWriteSessionsFraction(0)
            .build());
    if (host != null) {
      optionsBuilder.setHost(host);
    }
    if (project != null) {
      optionsBuilder.setProjectId(project);
    }
    if (numChannels != null) {
      optionsBuilder.setNumChannels(Integer.parseInt(numChannels));
    }
    spanner = optionsBuilder.build().getService();
    Runtime.getRuntime().addShutdownHook(new Thread("spannerShutdown") {
        @Override
        public void run() {
          spanner.close();
        }
      });
    return spanner;
  }

  @Override
  public void init() throws DBException {
    synchronized (CLASS_LOCK) {
      if (dbClient != null) {
        return;
      }
      Properties properties = getProperties();
      String host = properties.getProperty(CloudSpannerProperties.HOST);
      String project = properties.getProperty(CloudSpannerProperties.PROJECT);
      String instance = properties.getProperty(CloudSpannerProperties.INSTANCE, "ycsb-instance");
      String database = properties.getProperty(CloudSpannerProperties.DATABASE, "ycsb-database");

      fieldCount = Integer.parseInt(properties.getProperty(
          CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
      queriesForReads = properties.getProperty(CloudSpannerProperties.READ_MODE, "query").equals("query");
      batchInserts = Integer.parseInt(properties.getProperty(CloudSpannerProperties.BATCH_INSERTS, "1"));
      constructStandardQueriesAndFields(properties);

      int boundedStalenessSeconds = Integer.parseInt(properties.getProperty(
          CloudSpannerProperties.BOUNDED_STALENESS, "0"));
      timestampBound = (boundedStalenessSeconds <= 0) ?
          TimestampBound.strong() : TimestampBound.ofMaxStaleness(boundedStalenessSeconds, TimeUnit.SECONDS);

      try {
        spanner = getSpanner(properties, host, project);
        if (project == null) {
          project = spanner.getOptions().getProjectId();
        }
        dbClient = spanner.getDatabaseClient(DatabaseId.of(project, instance, database));
      } catch (Exception e) {
        LOGGER.log(Level.SEVERE, "init()", e);
        throw new DBException(e);
      }

      LOGGER.log(Level.INFO, new StringBuilder()
          .append("\nHost: ").append(spanner.getOptions().getHost())
          .append("\nProject: ").append(project)
          .append("\nInstance: ").append(instance)
          .append("\nDatabase: ").append(database)
          .append("\nUsing queries for reads: ").append(queriesForReads)
          .append("\nBatching inserts: ").append(batchInserts)
          .append("\nBounded staleness seconds: ").append(boundedStalenessSeconds)
          .toString());
    }
  }

  private Status readUsingQuery(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Statement query;
    Iterable<String> columns = fields == null ? JOBS_FIELDS : fields;
    if (fields == null || fields.size() == fieldCount) {
      query = Statement.newBuilder(standardQuery).bind("key").to(key).build();
    } else {
      Joiner joiner = Joiner.on(',');
      query = Statement.newBuilder("SELECT ")
          .append(joiner.join(fields))
          .append(" FROM ")
          .append(table)
          .append(" WHERE jobId=@key")
          .bind("key").to(key)
          .build();
    }
    try (ResultSet resultSet = dbClient.singleUse(timestampBound).executeQuery(query)) {
      resultSet.next();
      decodeStruct(columns, resultSet, result);
      if (resultSet.next()) {
        throw new Exception("Expected exactly one row for each read.");
      }

      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "readUsingQuery()", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (queriesForReads) {
      return readUsingQuery(table, key, fields, result);
    }
    Iterable<String> columns = fields == null ? JOBS_FIELDS : fields;
    try {
      Struct row = dbClient.singleUse(timestampBound).readRow(table, Key.of(key), columns);
      decodeStruct(columns, row, result);
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "read()", e);
      return Status.ERROR;
    }
  }

  private Status scanUsingQuery(
      String table, String startKey, int recordCount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    Iterable<String> columns = fields == null ? JOBS_FIELDS : fields;
    Statement query;
    if (fields == null || fields.size() == fieldCount) {
      if (startKey != null) {
        query = Statement.newBuilder(standardScan).bind("startKey").to(startKey).bind("count").to(recordCount).build();
      } else {
        query = Statement.newBuilder(standardScanNoStartKey).bind("count").to(recordCount).build();
      }
    } else {
      Joiner joiner = Joiner.on(',');
      Statement.Builder statementBuilder = Statement.newBuilder("SELECT ")
          .append(joiner.join(fields))
          .append(" FROM ")
          .append(table);

      if (startKey != null) {
        query = statementBuilder
            .append(" WHERE jobId>=@startKey LIMIT @count")
            .bind("startKey").to(startKey)
            .bind("count").to(recordCount)
            .build();
      } else {
        query = statementBuilder
            .append(" LIMIT @count")
            .bind("count").to(recordCount)
            .build();
      }
    }

    try (ResultSet resultSet = dbClient.singleUse(timestampBound).executeQuery(query)) {
      while (resultSet.next()) {
        HashMap<String, ByteIterator> row = new HashMap<>();
        decodeStruct(columns, resultSet, row);
        result.add(row);
      }
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "scanUsingQuery()", e);
      return Status.ERROR;
    }
  }

  //Works with jobs and dac_namespace schema as well
  @Override
  public Status scan(
      String table, String startKey, int recordCount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    if (queriesForReads) {
      return scanUsingQuery(table, startKey, recordCount, fields, result);
    }

    Iterable<String> columns;
    if(table.equals(JOBS_TABLE_NAME)) {
      columns = JOBS_FIELDS;

    } else if(table.equals(DAC_NAMESPACE_TABLE_NAME)) {
      columns = DAC_NAMESPACE_FIELDS;

    } else {
      columns = fields == null ? STANDARD_FIELDS : fields;

    }
    KeySet keySet =
        KeySet.newBuilder().addRange(KeyRange.closedClosed(Key.of(startKey), Key.of())).build();
    try (ResultSet resultSet = dbClient.singleUse(timestampBound)
                                       .read(table, keySet, columns, Options.limit(recordCount))) {
      while (resultSet.next()) {
        HashMap<String, ByteIterator> row = new HashMap<>();
        decodeStruct(columns, resultSet, row);
        result.add(row);
      }
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "scan()", e);
      return Status.ERROR;
    }
  }

  //Works with jobs and dac_namespace schema as well
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String primaryKeyColumn;
    if(table.equals(JOBS_TABLE_NAME)) {
      primaryKeyColumn = JOBS_PRIMARY_KEY_COLUMN;

    } else if(table.equals(DAC_NAMESPACE_TABLE_NAME)) {
      primaryKeyColumn = DAC_NAMESPACE_PRIMARY_KEY_COLUMN;

    } else {
      primaryKeyColumn = PRIMARY_KEY_COLUMN;
    }

    Mutation.WriteBuilder m = Mutation.newInsertOrUpdateBuilder(table);
    m.set(primaryKeyColumn).to(key);
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      if (e.getValue() instanceof StringByteIterator) {
        m.set(e.getKey()).to(e.getValue().toString());
      } else if (e.getValue().bytesLeft() == 8) { // size of long == 8 bytes
        // Assume this is a long.
        m.set(e.getKey()).to(Utils.bytesToLong(e.getValue().toArray()));
      } else {
        m.set(e.getKey()).to(ByteArray.copyFrom(e.getValue().toArray()));
      }
    }
    try {
      dbClient.writeAtLeastOnce(Arrays.asList(m.build()));
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "update()", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Pair findAndUpdate(
      String table, String key,
      Object version, Map<String, ByteIterator> values) {
    HashSet<String> versionReqFields = new HashSet<>();
    HashMap<String, ByteIterator> versionValues = new HashMap<>();
    versionReqFields.add("version");
    Status readStatus = read(table, key, versionReqFields, versionValues);
    if (!readStatus.isOk()) {
      LOGGER.log(Level.SEVERE, "Unable to read() during findAndUpdate");
      return new Pair(Status.ERROR, null);
    }

    ByteIterator versionByteIter = versionValues.get("version");
    ByteIterator nextVersionByteIter;
    long nextVersion;
    if (versionByteIter == null) {
      // No version defined in the data. Just generate the version.
      nextVersion = 1L;
    } else {
      long currentVersion = Utils.bytesToLong(versionByteIter.toArray());
      if (version != null && !version.equals(currentVersion)) {
        // Version mismatch.
        LOGGER.log(Level.SEVERE, "Version mismatch on findAndUpdate current version:" + currentVersion + ", expected version:" + version);
        return new Pair(Status.ERROR, null);
      }
      nextVersion = currentVersion + 1;
    }
    nextVersionByteIter = new ByteArrayByteIterator(Utils.longToBytes(nextVersion));
    values.put("version", nextVersionByteIter);
    Status updateStatus = update(table, key, values);
    return new Pair(updateStatus, nextVersion);
  }

  //Works with jobs and dac_namespace schema as well
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String primaryKeyColumn;
    if(table.equals(JOBS_TABLE_NAME)) {
      primaryKeyColumn = JOBS_PRIMARY_KEY_COLUMN;

    } else if(table.equals(DAC_NAMESPACE_TABLE_NAME)) {
      primaryKeyColumn = DAC_NAMESPACE_PRIMARY_KEY_COLUMN;

    } else {
      primaryKeyColumn = PRIMARY_KEY_COLUMN;
    }

    if (bufferedMutations.size() < batchInserts) {
      Mutation.WriteBuilder m = Mutation.newInsertOrUpdateBuilder(table);
      m.set(primaryKeyColumn).to(key);
      for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
        if (e.getValue() instanceof StringByteIterator) {
          m.set(e.getKey()).to(e.getValue().toString());
        } else if (e.getValue().bytesLeft() == 8) { // size of long == 8 bytes
          // Assume this is a long.
          m.set(e.getKey()).to(Utils.bytesToLong(e.getValue().toArray()));
        } else {
          m.set(e.getKey()).to(ByteArray.copyFrom(e.getValue().toArray()));
        }
      }
      bufferedMutations.add(m.build());
    } else {
      LOGGER.log(Level.INFO, "Limit of cached mutations reached. The given mutation with key " + key +
          " is ignored. Is this a retry?");
    }
    if (bufferedMutations.size() < batchInserts) {
      return Status.BATCHED_OK;
    }
    try {
      dbClient.writeAtLeastOnce(bufferedMutations);
      bufferedMutations.clear();
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "insert()", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public void cleanup() {
    try {
      if (bufferedMutations.size() > 0) {
        dbClient.writeAtLeastOnce(bufferedMutations);
        bufferedMutations.clear();
      }
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "cleanup()", e);
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      dbClient.writeAtLeastOnce(Arrays.asList(Mutation.delete(table, Key.of(key))));
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "delete()", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status scanWithCreatedTimeFilter(String table, String startRange, String endRange, int recordCount,
                                          Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    String filterClause = "WHERE startTime";
    if (startRange != null && endRange != null) {
      filterClause = filterClause.concat(" BETWEEN ").concat(startRange).concat(" AND ").concat(endRange)
          .concat(" ORDER BY jobId").concat(" LIMIT ").concat(Integer.toString(recordCount));

    } else if (startRange != null) {
      filterClause = filterClause.concat(" >= ").concat(startRange).concat(" ORDER BY jobId")
          .concat(" LIMIT ").concat(Integer.toString(recordCount));

    } else if (endRange != null) {
      filterClause = filterClause.concat(" <= ").concat(endRange).concat(" ORDER BY jobId")
          .concat(" LIMIT ").concat(Integer.toString(recordCount));

    } else {
      LOGGER.log(Level.INFO, "No valid range is provided");
      return Status.BAD_REQUEST;

    }

    return scanWithFilterHelper(filterClause, JOBS_TABLE_NAME, JOBS_FIELDS,
        fields, result, "scanWithCreatedTimeFilter");
  }

  @Override
  public Status scanWithNamespaceKeyFilter(String table, String startKey, String endKey, int recordCount,
                                           Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    String filterClause = "WHERE entityPathKey";
    if (startKey != null && endKey != null) {
      filterClause = filterClause.concat(" BETWEEN '").concat(startKey).concat("' AND '").concat(endKey)
          .concat("' ORDER BY entityPathKey").concat(" LIMIT ").concat(Integer.toString(recordCount));

    } else if (startKey != null) {
      filterClause = filterClause.concat(" >= '").concat(startKey).concat("' ORDER BY entityPathKey")
          .concat(" LIMIT ").concat(Integer.toString(recordCount));

    } else if (endKey != null) {
      filterClause = filterClause.concat(" <= '").concat(endKey).concat("' ORDER BY entityPathKey")
          .concat(" LIMIT ").concat(Integer.toString(recordCount));

    } else {
      LOGGER.log(Level.INFO, "No valid range is provided");
      return Status.BAD_REQUEST;

    }

    return scanWithFilterHelper(filterClause, DAC_NAMESPACE_TABLE_NAME, DAC_NAMESPACE_FIELDS,
        fields, result, "scanWithNamespaceKeyFilter");
  }

  //Scan with filter only supports selecting all fields (select *)
  private Status scanWithFilterHelper(String filterClause, String tableName, ArrayList tableFields,
                                      Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
                                      String operationName) {
    Iterable<String> columns = fields == null ? tableFields : fields;
    Statement query = Statement.newBuilder("SELECT * FROM ").append(tableName).append(" ").append(filterClause).build();

    LOGGER.log(Level.FINE, operationName + " - SQL Query: {0}", query.getSql());

    try (ResultSet resultSet = dbClient.singleUse(timestampBound).executeQuery(query)) {
      while (resultSet.next()) {
        HashMap<String, ByteIterator> row = new HashMap<>();
        decodeStruct(columns, resultSet, row);
        result.add(row);
      }
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, operationName, e);
      return Status.ERROR;
    }
  }

  private static void decodeStruct(
      Iterable<String> columns, StructReader structReader, Map<String, ByteIterator> result) {
    for (String col : columns) {
      final ByteIterator cell;
      final Type type = structReader.getColumnType(col);
      if (structReader.isNull(col)) {
        result.put(col, null);
      } else {
        if (type.equals(Type.bytes())) {
          cell = new ByteArrayByteIterator(structReader.getBytes(col).toByteArray());
        } else if (type.equals(Type.int64())) {
          cell = new ByteArrayByteIterator(Utils.longToBytes(structReader.getLong(col)));
        } else {
          cell = new StringByteIterator(structReader.getString(col));
        }
        result.put(col, cell);
      }
    }
  }
}
