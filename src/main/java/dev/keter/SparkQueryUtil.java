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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.spark.sql.types.StructField;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.phoenix.util.SchemaUtil.getEscapedFullColumnName;

public class SparkQueryUtil {
    public static String constructDynamicUpsertStatement(List<String> tableColumns, String tableName, List<StructField> columns, HintNode.Hint hint, boolean skipNormalizingIdentifier) throws SQLException {

        if (columns.isEmpty()) {
            throw new IllegalArgumentException("At least one column must be provided for upserts");
        }

        String hintStr = "";
        if (hint != null) {
            final HintNode node = new HintNode(hint.name());
            hintStr = node.toString();
        }

        List<String> parameterList = Lists.newArrayList();
        for (int i = 0; i < columns.size(); i++) {
            parameterList.add("?");
        }

        List<String> escapedTableColumns = tableColumns.stream().map(SchemaUtil::getEscapedFullColumnName)
                .collect(Collectors.toList());

        List<String> upsertColumns = columns.stream().map(field -> {
            String colName = getEscapedFullColumnName(skipNormalizingIdentifier
                    ? field.name()
                    : SchemaUtil.normalizeIdentifier(field.name()));
            boolean isTableColumn = escapedTableColumns.contains(colName.toUpperCase());

            return isTableColumn ? colName : String.format("%s %s", colName,
                    SparkSchemaUtil.catalystTypeToPhoenixType(field.dataType()).getSqlTypeName());
        }).collect(Collectors.toList());

        return String.format(
                "UPSERT %s INTO %s (%s) VALUES (%s)",
                hintStr,
                tableName,
                Joiner.on(", ").join(upsertColumns),
                Joiner.on(", ").join(parameterList));
    }
}
