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

package org.apache.spark.sql.execution.datasources.jdbc

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._

private object PhoenixJdbcDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:phoenix")

  /**
   * This is only called for ArrayType (see JdbcUtils.makeSetter)
   */
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("VARCHAR", java.sql.Types.VARCHAR))
    case BinaryType => Some(JdbcType("BINARY(" + dt.defaultSize + ")", java.sql.Types.BINARY))
    case DoubleType => Some(JdbcType("DOUBLE", java.sql.Types.DOUBLE))
    case ByteType => Some(JdbcType("TINYINT", java.sql.Types.TINYINT))
    case ShortType => Some(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
    case _ => None
  }
}
