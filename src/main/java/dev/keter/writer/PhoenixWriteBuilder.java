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

package dev.keter.writer;

import com.google.common.base.Preconditions;
import dev.keter.ConfigUtil;
import dev.keter.PhoenixDataSource;
import dev.keter.SparkQueryUtil;
import dev.keter.SparkSchemaUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

public class PhoenixWriteBuilder implements WriteBuilder {
    private final PhoenixDataSourceWriteOptions options;
    private final String upsertStatement;

    public PhoenixWriteBuilder(StructType schema, CaseInsensitiveStringMap options) {
        Preconditions.checkArgument(options.containsKey(PhoenixDataSource.ZOOKEEPER_URL),
                "No Phoenix option " + PhoenixDataSource.ZOOKEEPER_URL + " defined");
        Preconditions.checkArgument(options.containsKey(PhoenixDataSource.TABLE),
                "No Phoenix option " + PhoenixDataSource.TABLE + " defined");

        this.options = createPhoenixDataSourceWriteOptions(options, schema);
        this.upsertStatement = constructUpsertStatement(this.options);
    }

    @Override
    public BatchWrite buildForBatch() {
        return new PhoenixBatchWrite(options, upsertStatement);
    }

    private PhoenixDataSourceWriteOptions createPhoenixDataSourceWriteOptions(CaseInsensitiveStringMap options,
                                                                              StructType schema) {
        String tableName = options.get(PhoenixDataSource.TABLE);
        String scn = options.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE);
        String tenantId = options.get(PhoenixRuntime.TENANT_ID_ATTRIB);
        String zkUrl = options.get(PhoenixDataSource.ZOOKEEPER_URL);
        boolean skipNormalizingIdentifier = options.getBoolean(PhoenixDataSource.SKIP_NORMALIZING_IDENTIFIER, false);
        boolean upsertDynamicColumns = options.getBoolean(PhoenixDataSource.UPSERT_DYNAMIC_COLUMNS, false);

        return new PhoenixDataSourceWriteOptions.Builder()
                .setTableName(tableName)
                .setZkUrl(zkUrl)
                .setScn(scn)
                .setTenantId(tenantId)
                .setSchema(schema)
                .setSkipNormalizingIdentifier(skipNormalizingIdentifier)
                .setUpsertDynamicColumns(upsertDynamicColumns)
                .setOverriddenProps(ConfigUtil.extractOverriddenHbaseConf(options))
                .build();
    }

    private String constructUpsertStatement(PhoenixDataSourceWriteOptions options) {
        String scn = options.getScn();
        String tenantId = options.getTenantId();
        String zkUrl = options.getZkUrl();
        Properties overridingProps = options.getOverriddenProps();
        String tableName = options.getTableName();

        if (scn != null) {
            overridingProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, scn);
        }
        if (tenantId != null) {
            overridingProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }

        try (Connection conn = DriverManager.getConnection(JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + zkUrl,
                overridingProps)) {

            String upsertSql;
            if (options.upsertDynamicColumns()) {
                List<StructField> columns = Arrays.asList(options.getSchema().fields());

                List<String> tableColumns = PhoenixRuntime.generateColumnInfo(conn, tableName, null)
                        .stream().map(col -> SparkSchemaUtil.normalizeColumnName(col.getColumnName()))
                        .collect(Collectors.toList());

                upsertSql = SparkQueryUtil.constructDynamicUpsertStatement(tableColumns, tableName, columns, null, options.skipNormalizingIdentifier());
            } else {
                List<String> colNames = new ArrayList<>(Arrays.asList(options.getSchema().names()));
                if (!options.skipNormalizingIdentifier()) {
                    colNames = colNames.stream().map(SchemaUtil::normalizeIdentifier).collect(Collectors.toList());
                }

                upsertSql = QueryUtil.constructUpsertStatement(tableName, colNames, null);
            }

            return upsertSql;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
