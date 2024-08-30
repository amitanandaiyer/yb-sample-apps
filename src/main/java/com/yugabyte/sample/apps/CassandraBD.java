package com.yugabyte.sample.apps;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.yugabyte.sample.common.*;
import org.apache.commons.cli.CommandLine;
import org.apache.log4j.Logger;

import java.lang.String;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.yugabyte.sample.common.CmdLineOpts;
import com.yugabyte.sample.common.TimeseriesLoadGenerator;


// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

public class CassandraBD  extends AppBase {
    private static final Logger LOG = Logger.getLogger(CassandraBD.class);
    // Static initialization of this workload's config.
    static {
        // Disable the read-write percentage.
        appConfig.readIOPSPercentage = -1;
        // Set the read and write threads to 1 each.
        appConfig.numReaderThreads = 32;
        appConfig.numWriterThreads = 4;
        // Set the number of keys to read and write.
        appConfig.numKeysToRead = -1;
        appConfig.numKeysToWrite = -1;
        // Set the TTL for the raw table.
        appConfig.tableTTLSeconds = 24 * 60 * 60;
    }
    private static int numBuckets = 100;
    private static int maxCountValue = 100000;
    private static boolean doUpdates = true;
    SimpleLoadGenerator loadGenerator;
    public static String bdTableName = "preauth_unique_count";
    // The rate at which each metric is generated in millis.
    private static long data_emit_rate_millis = 1 * 1000;
    // The total number of rows read from the DB so far.
    private static AtomicLong num_rows_read;
    // The shared prepared select statement for fetching the latest data point.
    private static volatile PreparedStatement preparedSelect;
    // The shared prepared statement for inserting into the raw table.
    private static volatile PreparedStatement preparedInsertRaw;
    // Lock for initializing prepared statement objects.
    private static final Object prepareInitLock = new Object();

    @Override
    public void initialize(CmdLineOpts configuration) {
        synchronized (this) {
            LOG.info("Initializing CassandraBD app...");

            // Read the various params from the command line.
            CommandLine commandLine = configuration.getCommandLine();
            if (commandLine.hasOption("num_buckets")) {
                numBuckets = Integer.parseInt(commandLine.getOptionValue("num_buckets"));
                LOG.info("num_buckets: " + numBuckets);
            }
            if (commandLine.hasOption("max_count_value")) {
                maxCountValue = Integer.parseInt(commandLine.getOptionValue("max_count_value"));
                LOG.info("maxCountValue: " + maxCountValue);
            }
            if (commandLine.hasOption("do_updates")) {
                doUpdates = Boolean.parseBoolean(commandLine.getOptionValue("do_updates"));
                LOG.info("do_updates: " + doUpdates);
            }

            // Initialize the number of rows read.
            num_rows_read = new AtomicLong(0);
            LOG.info("CassandraBD app is ready.");
        }
    }

    @Override
    public void dropTable() {
        // Drop the raw table.
        dropCassandraTable(bdTableName);
    }

    @Override
    public List<String> getCreateTableStatements() {
        // Create the raw table.
        String create_stmt = "CREATE TABLE IF NOT EXISTS " + bdTableName + " (" +
                "key INT, bucket INT, count INT, PRIMARY KEY ((key, bucket))) " +
                " WITH default_time_to_live = 0 AND transactions = { 'enabled' : true };";
        return Arrays.asList(create_stmt);
    }

    private PreparedStatement getPreparedSelectLatest()  {
        PreparedStatement pSelectLatestTmp = preparedSelect;
        if (pSelectLatestTmp == null) {
            synchronized (prepareInitLock) {
                if (preparedSelect == null) {
                    // Create the prepared statement object.
                    String in_clause = "(";
                    for (int i = 1; i < numBuckets; i++) {
                        in_clause = in_clause + i + ",";
                    }
                    in_clause = in_clause + numBuckets + ")";
                    String select_stmt =
                            String.format("SELECT count(*) from %s WHERE key = ? and bucket IN %s;", bdTableName, in_clause);

//                    "select count(*) from test.test where key='" + str(sys.argv[1]) + "'
//                    and bucket IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
//                    21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,
//                    43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,
//                    65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,
//                    87,88,89,90,91,92,93,94,95,96,97,98,99,100);"
                    preparedSelect = getCassandraClient().prepare(select_stmt);
                }
                pSelectLatestTmp = preparedSelect;
            }
        }
        return pSelectLatestTmp;
    }


    public SimpleLoadGenerator getLoadGenerator() {
        SimpleLoadGenerator sLoadGeneratorTmp = loadGenerator;
        if (sLoadGeneratorTmp == null) {
            synchronized (AppBase.class) {
                if (!doUpdates) {
                    appConfig.numKeysToWrite = appConfig.numUniqueKeysToWrite * numBuckets;
                    appConfig.numKeysToRead = 0;
                    appConfig.numReaderThreads = 0;
                    LOG.info("Doing Pure Inserts, so updated appConfig.numKeysToWrite: " + appConfig.numKeysToWrite);
                    LOG.info("Doing Pure Inserts, so updated appConfig.numKeysToRead : " + appConfig.numKeysToRead);
                    LOG.info("Doing Pure Inserts, so updated appConfig.numReaderThreads: " + appConfig.numReaderThreads);
                }
                if (loadGenerator == null) {
                    loadGenerator = new SimpleLoadGenerator(0,
                            appConfig.numUniqueKeysToWrite * numBuckets,
                            (doUpdates ? appConfig.numUniqueKeysToWrite * numBuckets - 1 : appConfig.maxWrittenKey * numBuckets - 1));
                }
                sLoadGeneratorTmp = loadGenerator;
            }
        }
        return sLoadGeneratorTmp;
    }

    @Override
    public long doRead() {
        long input_key = getLoadGenerator().getKeyToWrite().asNumber();
        int key = (int)(input_key / numBuckets);
        int bucket = 1 + (int)(input_key % numBuckets);

        // Bind the select statement.
        BoundStatement select = getPreparedSelectLatest().bind(key);
        // Make the query.
        ResultSet rs = getCassandraClient().execute(select);

        List<Row> rows = rs.all();
        num_rows_read.addAndGet(rows.size());
        return 1;
    }

    private PreparedStatement getPreparedInsertOrUpdateStmt()  {
        PreparedStatement preparedInsertRawLocal = preparedInsertRaw;
        if (preparedInsertRawLocal == null) {
            synchronized (prepareInitLock) {
                if (preparedInsertRaw == null) {
                    // Create the prepared statement object.
                    if (doUpdates) {
                        String update_stmt =
                                String.format("UPDATE %s USING TTL %d SET count = count + 1 WHERE key = ? AND bucket = ? IF COUNT < %d;",
                                        bdTableName, appConfig.tableTTLSeconds, maxCountValue);
                        preparedInsertRaw = getCassandraClient().prepare(update_stmt);

                    } else {
                        String insert_stmt =
                            String.format("INSERT INTO %s (key, bucket, count) VALUES (?, ?, 0)" +
                                            "IF NOT EXISTS USING TTL %d ",
                                        bdTableName, appConfig.tableTTLSeconds);
                        preparedInsertRaw = getCassandraClient().prepare(insert_stmt);
                    }
                }
                preparedInsertRawLocal = preparedInsertRaw;
            }
        }
        return preparedInsertRawLocal;
    }

    @Override
    public synchronized void resetClients() {
        synchronized (prepareInitLock) {
            preparedInsertRaw = null;
            preparedSelect = null;
        }
        super.resetClients();
    }

    @Override
    public synchronized void destroyClients() {
        synchronized (prepareInitLock) {
            preparedInsertRaw = null;
            preparedSelect = null;
        }
        super.destroyClients();
    }

    @Override
    public long doWrite(int threadIdx) {
        long input_key = getLoadGenerator().getKeyToWrite().asNumber();
        int key = (int)(input_key / numBuckets);
        int bucket = 1 + (int)(input_key % numBuckets);
        // Insert the row.
        BoundStatement boundStatement =
                getPreparedInsertOrUpdateStmt().bind(key, bucket);
        ResultSet resultSet = getCassandraClient().execute(boundStatement);

        return 1;
    }

    @Override
    public void appendMessage(StringBuilder sb) {
        super.appendMessage(sb);
        sb.append("Rows read: " + num_rows_read.get());
        sb.append(" | ");
    }

    @Override
    public List<String> getWorkloadDescription() {
        return Arrays.asList(
                "Designed to repro the CQL workload seen with on a CE.",
                " Data is written to a table with (key, bucket, count) where (key, bucket) forms the primary key. ",
                "Reads do a scratter gather from all the buckets for a given key. ",
                "Updates will update the count, as long as it is less the max count.");
    }

    @Override
    public List<String> getWorkloadOptionalArguments() {
        return Arrays.asList(
                "--num_threads_read " + appConfig.numReaderThreads,
                "--num_threads_write " + appConfig.numWriterThreads,
                "--num_buckets " + numBuckets,
                "--do_updates " + doUpdates,
                "--max_count_value " + maxCountValue);
    }
}

