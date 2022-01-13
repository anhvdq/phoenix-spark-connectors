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

import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.spark.SerializableWritable;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

public class PhoenixInputPartition implements InputPartition {
    private final StructType schema;
    private final PhoenixDataSourceReadOptions options;
    private final SerializableWritable<PhoenixInputSplit> phoenixInputSplit;
    private final String[] locations;

    PhoenixInputPartition(StructType schema, PhoenixDataSourceReadOptions options, PhoenixInputSplit phoenixInputSplit, String[] locations) {
        this.schema = schema;
        this.options = options;
        this.phoenixInputSplit = new SerializableWritable<>(phoenixInputSplit);
        this.locations = locations;
    }

    StructType getSchema() {
        return schema;
    }

    PhoenixDataSourceReadOptions getOptions() {
        return options;
    }

    SerializableWritable<PhoenixInputSplit> getPhoenixInputSplit() {
        return phoenixInputSplit;
    }

    @Override
    public String[] preferredLocations() {
        return locations;
    }
}
