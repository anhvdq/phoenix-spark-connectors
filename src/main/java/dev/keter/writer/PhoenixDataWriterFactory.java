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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;

public class PhoenixDataWriterFactory implements DataWriterFactory {
    private final PhoenixDataSourceWriteOptions options;
    private final String upsertSql;
    public PhoenixDataWriterFactory(PhoenixDataSourceWriteOptions options, String upsertSql) {
        this.options = options;
        this.upsertSql = upsertSql;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int i, long l) {
        return new PhoenixDataWriter(options, upsertSql);
    }
}
