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
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.*;
import java.util.stream.Collectors;

public class ConfigUtil {
    public static Properties extractOverriddenHbaseConf(Map<String, String> options) {
        Properties confToSet = new Properties();
        if (options != null) {
            String phoenixConfigs = options.get(PhoenixDataSource.PHOENIX_CONFIGS);
            if (phoenixConfigs != null) {
                String[] confs = phoenixConfigs.split(",");
                for (String conf : confs) {
                    String[] confKeyVal = conf.split("=");
                    try {
                        confToSet.setProperty(confKeyVal[0], confKeyVal[1]);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        throw new RuntimeException("Incorrect format for phoenix/HBase configs. "
                                + "Expected format: <prop1>=<val1>,<prop2>=<val2>,<prop3>=<val3>..", e);
                    }
                }
            }
        }
        return confToSet;
    }

    public static PhoenixDataSourceOptions extractPhoenixDataSourceOptions(CaseInsensitiveStringMap options) {
        String zkUrl = options.get(PhoenixDataSource.ZOOKEEPER_URL);
        String tableName = options.get(PhoenixDataSource.TABLE);
        Boolean dateAsTimestamp = options.getBoolean(PhoenixDataSource.DATE_AS_TIMESTAMP, false);
        String dynamicColsStr = options.get(PhoenixDataSource.DYNAMIC_COLUMNS);
        List<DynamicColumn> dynamicColumns = Collections.emptyList();
        if (dynamicColsStr != null && !dynamicColsStr.isEmpty()) {
            dynamicColumns = Arrays.stream(dynamicColsStr.split(PhoenixDataSource.DYNAMIC_COLUMNS_SEPARATOR))
                    .map(DynamicColumn::fromString)
                    .collect(Collectors.toList());
        }

        Map<String, String> otherOpts = options.entrySet().stream().filter(entry -> !entry.getKey().equals(PhoenixDataSource.ZOOKEEPER_URL)
                        && !entry.getKey().equals(PhoenixDataSource.TABLE)
                        && !entry.getKey().equals(PhoenixDataSource.DYNAMIC_COLUMNS))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new PhoenixDataSourceOptions(zkUrl, tableName, dateAsTimestamp, dynamicColumns, otherOpts);
    }
}
