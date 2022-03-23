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

package dev.keter;

import dev.keter.model.DynamicColumn;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

public class PhoenixDataSource implements TableProvider, DataSourceRegister {
    public static final String TABLE = "table";
    public static final String ZOOKEEPER_URL = "zkUrl";
    public static final String DATE_AS_TIMESTAMP = "dateAsTimestamp";
    public static final String PHOENIX_CONFIGS = "phoenixConfigs";
    public static final String DYNAMIC_COLUMNS = "dynamicColumns";
    public static final String DYNAMIC_COLUMNS_SEPARATOR = ",";
    public static final String SKIP_NORMALIZING_IDENTIFIER = "skipNormalizingIdentifier";
    public static final String UPSERT_DYNAMIC_COLUMNS = "upsertDynamicColumns";

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        PhoenixDataSourceOptions dsOptions = ConfigUtil.extractPhoenixDataSourceOptions(options);
        String zkUrl = dsOptions.getZkUrl();
        String tableName = dsOptions.getTableName();
        Properties overriddenProps = ConfigUtil.extractOverriddenHbaseConf(dsOptions.getOtherOpts());
        boolean dateAsTimestamp = dsOptions.isDateAsTimestamp();

        try (Connection conn = DriverManager.getConnection(
                JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + zkUrl, overriddenProps)) {
            // Base columns in table
            List<ColumnInfo> tableColumns = PhoenixRuntime.generateColumnInfo(conn, tableName, null);
            // Dynamic columns
            List<ColumnInfo> dynamicColumns = dsOptions.getDynamicColumns().stream()
                    .map(DynamicColumn::toPhoenixColumnInfo)
                    .collect(Collectors.toList());

            List<ColumnInfo> columnInfos = Stream.concat(tableColumns.stream(), dynamicColumns.stream())
                    .collect(Collectors.toList());

            Seq<ColumnInfo> columnInfoSeq = JavaConverters.asScalaIteratorConverter(columnInfos.iterator()).asScala().toSeq();
            return SparkSchemaUtil.phoenixSchemaToCatalystSchema(columnInfoSeq, dateAsTimestamp);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> options) {
        String tableName = Objects.requireNonNull(options.get(TABLE));
        return new PhoenixTable(tableName, schema);
    }

    @Override
    public String shortName() {
        return "phoenix";
    }
}
