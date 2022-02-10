package dev.keter

import org.apache.phoenix.util.StringUtil.escapeStringConstant
import org.apache.phoenix.util.{DateUtil, SchemaUtil}
import org.apache.spark.sql.sources._

import java.sql.{Date, Timestamp}
import java.text.Format

class FilterExpressionCompiler {
  val dateformatter: Format = DateUtil.getDateFormatter(DateUtil.DEFAULT_DATE_FORMAT)
  val timeformatter: Format = DateUtil.getTimestampFormatter(DateUtil.DEFAULT_TIME_FORMAT)

  /**
   * Attempt to create Phoenix-accepted WHERE clause from Spark filters,
   * mostly inspired from Spark SQL JDBCRDD and the couchbase-spark-connector
   *
   * @return tuple representing where clause (derived from supported filters),
   *         array of unsupported filters and array of supported filters
   */
  def pushFilters(filters: Array[Filter]): (String, Array[Filter], Array[Filter]) = {
    if (filters.isEmpty) {
      return ("", Array[Filter](), Array[Filter]())
    }

    val filter = new StringBuilder("")
    val unsupportedFilters = Array[Filter]();
    var i = 0

    filters.foreach(f => {
      // Assume conjunction for multiple filters, unless otherwise specified
      if (i > 0) {
        filter.append(" AND")
      }

      f match {
        // Spark 1.3.1+ supported filters
        case And(leftFilter, rightFilter) => {
          val (whereClause, currUnsupportedFilters, _) = pushFilters(Array(leftFilter, rightFilter))
          if (currUnsupportedFilters.isEmpty)
            filter.append(whereClause)
          else
            unsupportedFilters :+ f
        }
        case Or(leftFilter, rightFilter) => {
          val (whereLeftClause, leftUnsupportedFilters, _) = pushFilters(Array(leftFilter))
          val (whereRightClause, rightUnsupportedFilters, _) = pushFilters(Array(rightFilter))
          if (leftUnsupportedFilters.isEmpty && rightUnsupportedFilters.isEmpty) {
            val (finalLeft, finalRight) = (handleQuote(leftFilter, whereLeftClause), handleQuote(rightFilter, whereRightClause))
            filter.append("(" + finalLeft + " OR " + finalRight + ")")
          }
          else {
            unsupportedFilters :+ f
          }
        }
        case Not(aFilter) => {
          val (whereClause, currUnsupportedFilters, _) = pushFilters(Array(aFilter))
          if (currUnsupportedFilters.isEmpty)
            filter.append(s" NOT ($whereClause)")
          else
            unsupportedFilters :+ f
        }
        case EqualTo(attr, value) => filter.append(s" ${escapeKey(attr)} = ${compileValue(value)}")
        case GreaterThan(attr, value) => filter.append(s" ${escapeKey(attr)} > ${compileValue(value)}")
        case GreaterThanOrEqual(attr, value) => filter.append(s" ${escapeKey(attr)} >= ${compileValue(value)}")
        case LessThan(attr, value) => filter.append(s" ${escapeKey(attr)} < ${compileValue(value)}")
        case LessThanOrEqual(attr, value) => filter.append(s" ${escapeKey(attr)} <= ${compileValue(value)}")
        case IsNull(attr) => filter.append(s" ${escapeKey(attr)} IS NULL")
        case IsNotNull(attr) => filter.append(s" ${escapeKey(attr)} IS NOT NULL")
        case In(attr, values) => filter.append(s" ${escapeKey(attr)} IN ${values.map(compileValue).mkString("(", ",", ")")}")
        case StringStartsWith(attr, value) => filter.append(s" ${escapeKey(attr)} LIKE ${compileValue(value + "%")}")
        case StringEndsWith(attr, value) => filter.append(s" ${escapeKey(attr)} LIKE ${compileValue("%" + value)}")
        case StringContains(attr, value) => filter.append(s" ${escapeKey(attr)} LIKE ${compileValue("%" + value + "%")}")
        case _ => unsupportedFilters :+ f
      }

      i = i + 1
    })

    (filter.toString(), unsupportedFilters, filters diff unsupportedFilters)
  }

  // Helper function to escape string values in SQL queries
  private def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeStringConstant(stringValue)}'"

    case timestampValue: Timestamp => getTimestampString(timestampValue)

    case dateValue: Date => getDateString(dateValue)

    // Borrowed from 'elasticsearch-hadoop', support these internal UTF types across Spark versions
    // Spark 1.4
    case utf if (isClass(utf, "org.apache.spark.sql.types.UTF8String")) => s"'${escapeStringConstant(utf.toString)}'"
    // Spark 1.5
    case utf if (isClass(utf, "org.apache.spark.unsafe.types.UTF8String")) => s"'${escapeStringConstant(utf.toString)}'"

    // Pass through anything else
    case _ => value
  }

  private def getTimestampString(timestampValue: Timestamp): String = {
    "TO_TIMESTAMP('%s', '%s', '%s')".format(timeformatter.format(timestampValue),
      DateUtil.DEFAULT_TIME_FORMAT, DateUtil.DEFAULT_TIME_ZONE_ID)
  }

  private def getDateString(dateValue: Date): String = {
    "TO_DATE('%s', '%s', '%s')".format(dateformatter.format(dateValue),
      DateUtil.DEFAULT_DATE_FORMAT, DateUtil.DEFAULT_TIME_ZONE_ID)
  }

  // Helper function to escape column key to work with SQL queries
  private def escapeKey(key: String): String = SchemaUtil.getEscapedFullColumnName(key)

  private def isClass(obj: Any, className: String) = {
    className.equals(obj.getClass().getName())
  }

  private def handleQuote(filter: Filter, whereClause: String): String = {
    filter match {
      case Or(_, _) => whereClause.substring(1, whereClause.length - 1)
      case And(_, _) => s"($whereClause)"
      case _ => whereClause
    }
  }
}
