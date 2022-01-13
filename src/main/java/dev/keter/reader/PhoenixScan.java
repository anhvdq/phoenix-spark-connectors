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

import dev.keter.PhoenixDataSourceOptions;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

public class PhoenixScan implements Scan {
    private final StructType schema;
    private final PhoenixDataSourceOptions options;
    private final String whereClause;

    public PhoenixScan(StructType schema, PhoenixDataSourceOptions options, String whereClause) {
        this.schema = schema;
        this.options = options;
        this.whereClause = whereClause;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public Batch toBatch() {
        return new PhoenixBatch(schema, options, whereClause);
    }
}
