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

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PhoenixDataSourceOptions {
    private final String zkUrl;
    private final String tableName;
    private final Boolean dateAsTimestamp;
    private final List<DynamicColumn> dynamicColumns;
    private final Map<String, String> otherOpts;

    public PhoenixDataSourceOptions(String zkUrl, String tableName, Boolean dateAsTimestamp, List<DynamicColumn> dynamicColumns, Map<String, String> otherOpts) {
        this.zkUrl = Objects.requireNonNull(zkUrl, String.format("'%s' can not be null or empty", PhoenixDataSource.ZOOKEEPER_URL));
        this.tableName = Objects.requireNonNull(tableName, String.format("'%s' can not be null or empty", PhoenixDataSource.TABLE));
        this.dateAsTimestamp = Objects.requireNonNull(dateAsTimestamp);
        this.dynamicColumns = Objects.requireNonNull(dynamicColumns);
        this.otherOpts = Objects.requireNonNull(otherOpts);
    }

    public String getZkUrl() {
        return zkUrl;
    }

    public String getTableName() {
        return tableName;
    }

    public Boolean isDateAsTimestamp() {
        return dateAsTimestamp;
    }

    public List<DynamicColumn> getDynamicColumns() {
        return dynamicColumns;
    }

    public Map<String, String> getOtherOpts() {
        return otherOpts;
    }
}
