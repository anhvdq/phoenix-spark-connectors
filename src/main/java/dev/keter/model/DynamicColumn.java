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

package dev.keter.model;

import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ColumnInfo;

import java.util.Objects;

public class DynamicColumn {
    private final String columnName;
    private final String dataType;

    public DynamicColumn(String columnName, String dataType) {
        this.columnName = Objects.requireNonNull(columnName);
        this.dataType = Objects.requireNonNull(dataType);
    }

    public String getColumnName() {
        return columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public static DynamicColumn fromString(String info) {
        String[] split = info.split("::");
        return new DynamicColumn(split[0].trim().toUpperCase(), split[1].trim().toUpperCase());
    }

    public ColumnInfo toPhoenixColumnInfo() {
        return new ColumnInfo(columnName, PDataType.fromSqlTypeName(dataType).getSqlType());
    }

    @Override
    public String toString() {
        return columnName + " " + dataType;
    }
}
