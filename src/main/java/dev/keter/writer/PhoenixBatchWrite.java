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

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

public class PhoenixBatchWrite implements BatchWrite {
    private final PhoenixDataSourceWriteOptions writeOptions;
    private final String upsertStatement;

    public PhoenixBatchWrite(PhoenixDataSourceWriteOptions options, String upsertStatement) {
        this.writeOptions = options;
        this.upsertStatement = upsertStatement;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
        return new PhoenixDataWriterFactory(writeOptions, upsertStatement);
    }

    @Override
    public void commit(WriterCommitMessage[] writerCommitMessages) {
    }

    @Override
    public void abort(WriterCommitMessage[] writerCommitMessages) {
    }

    @Override
    public boolean useCommitCoordinator() {
        return false;
    }
}
