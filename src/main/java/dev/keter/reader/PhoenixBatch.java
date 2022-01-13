/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.keter.reader;

import dev.keter.CompatUtil;
import dev.keter.ConfigUtil;
import dev.keter.PhoenixDataSourceOptions;
import dev.keter.SparkSchemaUtil;
import dev.keter.model.DynamicColumn;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.iterate.MapReduceParallelScanGrouper;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

public class PhoenixBatch implements Batch {
    private final StructType schema;
    private final PhoenixDataSourceOptions options;
    private final String whereClause;

    public PhoenixBatch(StructType schema, PhoenixDataSourceOptions options, String whereClause) {
        this.schema = schema;
        this.options = options;
        this.whereClause = whereClause;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        String zkUrl = options.getZkUrl();
        String tableName = options.getTableName();
        Map<String, String> otherOpts = options.getOtherOpts();
        Properties overriddenProps = ConfigUtil.extractOverriddenHbaseConf(otherOpts);

        String currentScnValue = otherOpts.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE);
        String tenantId = otherOpts.get(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID);

        // Generate splits based off statistics, or just region splits?
        boolean splitByStats = PhoenixConfigurationUtil.DEFAULT_SPLIT_BY_STATS;
        try {
            splitByStats = Boolean.parseBoolean(otherOpts.get(PhoenixConfigurationUtil.MAPREDUCE_SPLIT_BY_STATS));
        } catch (Exception ignored) {
        }

        if (currentScnValue != null) overriddenProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, currentScnValue);
        if (tenantId != null) overriddenProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);

        try (Connection conn = DriverManager.getConnection(
                JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + zkUrl, overriddenProps)) {
            // Get all defined columns from phoenix table
            List<ColumnInfo> tableColumns = PhoenixRuntime.generateColumnInfo(conn, tableName, null);
            // Generate dynamic columns
            List<ColumnInfo> dynamicColumns = options.getDynamicColumns().stream()
                    .map(DynamicColumn::toPhoenixColumnInfo).collect(Collectors.toList());

            Map<String, ColumnInfo> lookupMapColumnInfo = Stream.concat(tableColumns.stream(), dynamicColumns.stream())
                    .collect(Collectors.toMap(info -> SparkSchemaUtil.normalizeColumnName(info.getColumnName()),
                            Function.identity()));

            // Schema can be pruned so that we need to re-generate column infos
            // based on the new schema

            // This step is to make sure that generated column infos
            // always have the same order as the schema
            List<String> schemaNames = Arrays.asList(schema.names());

            List<ColumnInfo> columnInfos = schemaNames.stream()
                    .map(lookupMapColumnInfo::get).collect(Collectors.toList());

            if (columnInfos.isEmpty()) {
                // Case pruned schema is empty
                columnInfos = tableColumns;
            }

            String dynamicColumnsInfo = options.getDynamicColumns().stream()
                    .filter(col -> schemaNames.contains(col.getColumnName()))
                    .map(DynamicColumn::toString)
                    .collect(Collectors.joining(", "));
            if (!dynamicColumnsInfo.isEmpty()) {
                dynamicColumnsInfo = "(" + dynamicColumnsInfo + ")";
            }

            final Statement statement = conn.createStatement();
            final String selectStatement = QueryUtil.constructSelectStatement(tableName + dynamicColumnsInfo, columnInfos, whereClause);

            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            // Optimize the query plan so that we potentially use secondary indexes
            final QueryPlan queryPlan = pstmt.optimizeQuery(selectStatement);
            final Scan scan = queryPlan.getContext().getScan();

            // setting the snapshot configuration
            String snapshotName = otherOpts.get(PhoenixConfigurationUtil.SNAPSHOT_NAME_KEY);
            if (snapshotName != null)
                PhoenixConfigurationUtil.setSnapshotNameKey(queryPlan.getContext().getConnection().
                        getQueryServices().getConfiguration(), snapshotName);

            // Initialize the query plan so it sets up the parallel scans
            queryPlan.iterator(MapReduceParallelScanGrouper.getInstance());

            List<KeyRange> allSplits = queryPlan.getSplits();
            // Get the RegionSizeCalculator
            PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
            org.apache.hadoop.hbase.client.Connection connection =
                    phxConn.getQueryServices().getAdmin().getConnection();
            RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(queryPlan
                    .getTableRef().getTable().getPhysicalName().toString()));

            final List<InputPartition> partitions = new ArrayList<>(allSplits.size());
            for (List<Scan> scans : queryPlan.getScans()) {
                // Get the region location
                HRegionLocation location = regionLocator.getRegionLocation(
                        scans.get(0).getStartRow(),
                        false
                );

                String regionLocation = location.getHostname();

                // Get the region size
                long regionSize = CompatUtil.getSize(regionLocator, connection.getAdmin(), location);

                PhoenixDataSourceReadOptions phoenixDataSourceOptions =
                        new PhoenixDataSourceReadOptions(zkUrl, currentScnValue,
                                tenantId, selectStatement, overriddenProps);
                if (splitByStats) {
                    for (Scan aScan : scans) {
                        partitions.add(getInputPartition(phoenixDataSourceOptions,
                                new PhoenixInputSplit(Collections.singletonList(aScan), regionSize, regionLocation)));
                    }
                } else {
                    partitions.add(getInputPartition(phoenixDataSourceOptions,
                            new PhoenixInputSplit(scans, regionSize, regionLocation)));
                }
            }

            return partitions.toArray(new InputPartition[0]);
        } catch (Exception e) {
            throw new RuntimeException("Unable to plan query", e);
        }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new PhoenixPartitionReaderFactory();
    }

    private PhoenixInputPartition getInputPartition(PhoenixDataSourceReadOptions readOptions,
                                                    PhoenixInputSplit inputSplit) throws IOException, InterruptedException {
        return new PhoenixInputPartition(schema, readOptions, inputSplit, inputSplit.getLocations());
    }
}
