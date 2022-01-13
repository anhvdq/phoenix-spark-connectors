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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.iterate.*;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.phoenix.monitoring.ReadMetricQueue;
import org.apache.phoenix.monitoring.ScanMetricsHolder;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.spark.SerializableWritable;
import org.apache.spark.executor.InputMetrics;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.datasources.jdbc.SparkJdbcUtil;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

public class PhoenixPartitionReader implements PartitionReader<InternalRow> {
    private final StructType schema;
    private final PhoenixDataSourceReadOptions options;
    private final SerializableWritable<PhoenixInputSplit> inputSplit;

    private Iterator<InternalRow> iterator;
    private PhoenixResultSet resultSet;
    private InternalRow currentRow;

    public PhoenixPartitionReader(StructType schema, PhoenixDataSourceReadOptions options, SerializableWritable<PhoenixInputSplit> inputSplit) {
        this.schema = schema;
        this.options = options;
        this.inputSplit = inputSplit;
        initialize();
    }

    private QueryPlan getQueryPlan() throws SQLException {
        String scn = options.getScn();
        String tenantId = options.getTenantId();
        String zkUrl = options.getZkUrl();
        Properties overridingProps = options.getOverriddenProps();
        if (scn != null) {
            overridingProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, scn);
        }
        if (tenantId != null) {
            overridingProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        try (Connection conn = DriverManager.getConnection(
                JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + zkUrl, overridingProps)) {
            final Statement statement = conn.createStatement();
            final String selectStatement = options.getSelectStatement();
            if (selectStatement == null) {
                throw new NullPointerException();
            }

            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            // Optimize the query plan so that we potentially use secondary indexes
            return pstmt.optimizeQuery(selectStatement);
        }
    }

    private void initialize() {
        try {
            final QueryPlan queryPlan = getQueryPlan();
            final List<Scan> scans = inputSplit.value().getScans();
            List<PeekingResultIterator> iterators = new ArrayList<>(scans.size());
            StatementContext ctx = queryPlan.getContext();
            ReadMetricQueue readMetrics = ctx.getReadMetricsQueue();
            String tableName = queryPlan.getTableRef().getTable().getPhysicalName().getString();

            // Clear the table region boundary cache to make sure long running jobs stay up to date
            byte[] tableNameBytes = queryPlan.getTableRef().getTable().getPhysicalName().getBytes();
            ConnectionQueryServices services = queryPlan.getContext().getConnection().getQueryServices();
            services.clearTableRegionCache(CompatUtil.getTableName(tableNameBytes));

            long renewScannerLeaseThreshold = queryPlan.getContext().getConnection()
                    .getQueryServices().getRenewLeaseThresholdMilliSeconds();
            for (Scan scan : scans) {
                // For MR, skip the region boundary check exception if we encounter a split. ref: PHOENIX-2599
                scan.setAttribute(BaseScannerRegionObserver.SKIP_REGION_BOUNDARY_CHECK, Bytes.toBytes(true));

                PeekingResultIterator peekingResultIterator;
                ScanMetricsHolder scanMetricsHolder =
                        ScanMetricsHolder.getInstance(readMetrics, tableName, scan,
                                queryPlan.getContext().getConnection().getLogLevel());
                final TableResultIterator tableResultIterator =
                        new TableResultIterator(
                                queryPlan.getContext().getConnection().getMutationState(), scan,
                                scanMetricsHolder, renewScannerLeaseThreshold, queryPlan,
                                MapReduceParallelScanGrouper.getInstance());
                peekingResultIterator = LookAheadResultIterator.wrap(tableResultIterator);
                iterators.add(peekingResultIterator);
            }
            ResultIterator iterator = queryPlan.useRoundRobinIterator() ?
                    RoundRobinResultIterator.newIterator(iterators, queryPlan) :
                    ConcatResultIterator.newIterator(iterators);
            if (queryPlan.getContext().getSequenceManager().getSequenceCount() > 0) {
                iterator = new SequenceResultIterator(iterator, queryPlan.getContext().getSequenceManager());
            }
            // Clone the row projector as it's not thread safe and would be used simultaneously by
            // multiple threads otherwise.
            this.resultSet = new PhoenixResultSet(iterator, queryPlan.getProjector().cloneIfNecessary(),
                    queryPlan.getContext());

            this.iterator = SparkJdbcUtil.resultSetToSparkInternalRows(resultSet, schema, new InputMetrics());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean next() {
        if (!iterator.hasNext()) {
            return false;
        }
        currentRow = iterator.next();
        return true;
    }

    @Override
    public InternalRow get() {
        return currentRow;
    }

    @Override
    public void close() throws IOException {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }
}
