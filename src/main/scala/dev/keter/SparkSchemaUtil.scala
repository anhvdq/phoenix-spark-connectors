package dev.keter

import org.apache.phoenix.query.QueryConstants
import org.apache.phoenix.schema.types._
import org.apache.phoenix.util.{ColumnInfo, SchemaUtil}
import org.apache.spark.sql.types._

object SparkSchemaUtil {

  def phoenixSchemaToCatalystSchema(columnList: Seq[ColumnInfo], dateAsTimestamp: Boolean = false): StructType = {
    val structFields = columnList.map(ci => {
      val structType = phoenixTypeToCatalystType(ci, dateAsTimestamp)
      StructField(normalizeColumnName(ci.getColumnName), structType)
    })
    new StructType(structFields.toArray)
  }

  def normalizeColumnName(columnName: String) = {
    val unescapedColumnName = SchemaUtil.getUnEscapedFullColumnName(columnName)
    var normalizedColumnName = ""
    if (unescapedColumnName.indexOf(QueryConstants.NAME_SEPARATOR) < 0) {
      normalizedColumnName = unescapedColumnName
    }
    else {
      // split by separator to get the column family and column name
      val tokens = unescapedColumnName.split(QueryConstants.NAME_SEPARATOR_REGEX)
      normalizedColumnName = if (tokens(0) == "0") tokens(1) else unescapedColumnName
    }
    normalizedColumnName
  }

  // Lookup table for Phoenix types to Spark catalyst types
  def phoenixTypeToCatalystType(columnInfo: ColumnInfo, dateAsTimestamp: Boolean): DataType = columnInfo.getPDataType match {
    case t if t.isInstanceOf[PVarchar] || t.isInstanceOf[PChar] => StringType
    case t if t.isInstanceOf[PLong] || t.isInstanceOf[PUnsignedLong] => LongType
    case t if t.isInstanceOf[PInteger] || t.isInstanceOf[PUnsignedInt] => IntegerType
    case t if t.isInstanceOf[PSmallint] || t.isInstanceOf[PUnsignedSmallint] => ShortType
    case t if t.isInstanceOf[PTinyint] || t.isInstanceOf[PUnsignedTinyint] => ByteType
    case t if t.isInstanceOf[PFloat] || t.isInstanceOf[PUnsignedFloat] => FloatType
    case t if t.isInstanceOf[PDouble] || t.isInstanceOf[PUnsignedDouble] => DoubleType
    // Use Spark system default precision for now (explicit to work with < 1.5)
    case t if t.isInstanceOf[PDecimal] =>
      if (columnInfo.getPrecision == null || columnInfo.getPrecision < 0) DecimalType(38, 18) else DecimalType(columnInfo.getPrecision, columnInfo.getScale)
    case t if t.isInstanceOf[PTimestamp] || t.isInstanceOf[PUnsignedTimestamp] => TimestampType
    case t if t.isInstanceOf[PTime] || t.isInstanceOf[PUnsignedTime] => TimestampType
    case t if (t.isInstanceOf[PDate] || t.isInstanceOf[PUnsignedDate]) && !dateAsTimestamp => DateType
    case t if (t.isInstanceOf[PDate] || t.isInstanceOf[PUnsignedDate]) && dateAsTimestamp => TimestampType
    case t if t.isInstanceOf[PBoolean] => BooleanType
    case t if t.isInstanceOf[PVarbinary] || t.isInstanceOf[PBinary] => BinaryType
    case t if t.isInstanceOf[PIntegerArray] || t.isInstanceOf[PUnsignedIntArray] => ArrayType(IntegerType, containsNull = true)
    case t if t.isInstanceOf[PBooleanArray] => ArrayType(BooleanType, containsNull = true)
    case t if t.isInstanceOf[PVarcharArray] || t.isInstanceOf[PCharArray] => ArrayType(StringType, containsNull = true)
    case t if t.isInstanceOf[PVarbinaryArray] || t.isInstanceOf[PBinaryArray] => ArrayType(BinaryType, containsNull = true)
    case t if t.isInstanceOf[PLongArray] || t.isInstanceOf[PUnsignedLongArray] => ArrayType(LongType, containsNull = true)
    case t if t.isInstanceOf[PSmallintArray] || t.isInstanceOf[PUnsignedSmallintArray] => ArrayType(ShortType, containsNull = true)
    case t if t.isInstanceOf[PTinyintArray] || t.isInstanceOf[PUnsignedTinyintArray] => ArrayType(ByteType, containsNull = true)
    case t if t.isInstanceOf[PFloatArray] || t.isInstanceOf[PUnsignedFloatArray] => ArrayType(FloatType, containsNull = true)
    case t if t.isInstanceOf[PDoubleArray] || t.isInstanceOf[PUnsignedDoubleArray] => ArrayType(DoubleType, containsNull = true)
    case t if t.isInstanceOf[PDecimalArray] => ArrayType(
      if (columnInfo.getPrecision == null || columnInfo.getPrecision < 0) DecimalType(38, 18) else DecimalType(columnInfo.getPrecision, columnInfo.getScale), containsNull = true)
    case t if t.isInstanceOf[PTimestampArray] || t.isInstanceOf[PUnsignedTimestampArray] => ArrayType(TimestampType, containsNull = true)
    case t if t.isInstanceOf[PDateArray] || t.isInstanceOf[PUnsignedDateArray] => ArrayType(TimestampType, containsNull = true)
    case t if t.isInstanceOf[PTimeArray] || t.isInstanceOf[PUnsignedTimeArray] => ArrayType(TimestampType, containsNull = true)
  }

  def catalystTypeToPhoenixType(dataType: DataType): PDataType[_] = dataType match {
    case StringType => PVarchar.INSTANCE
    case LongType => PLong.INSTANCE
    case IntegerType => PInteger.INSTANCE
    case ShortType=> PSmallint.INSTANCE
    case ByteType => PTinyint.INSTANCE
    case FloatType => PFloat.INSTANCE
    case DoubleType => PDouble.INSTANCE
    case DecimalType() => PDecimal.INSTANCE
    case TimestampType => PTimestamp.INSTANCE
    case DateType => PDate.INSTANCE
    case BooleanType => PBoolean.INSTANCE
    case BinaryType => PVarbinary.INSTANCE
    case ArrayType(IntegerType, _) => PIntegerArray.INSTANCE
    case ArrayType(BooleanType, _) => PBooleanArray.INSTANCE
    case ArrayType(StringType, _) => PVarcharArray.INSTANCE
    case ArrayType(BinaryType, _) => PVarbinaryArray.INSTANCE
    case ArrayType(LongType, _) => PLongArray.INSTANCE
    case ArrayType(ShortType, _) => PSmallintArray.INSTANCE
    case ArrayType(ByteType, _) => PTinyintArray.INSTANCE
    case ArrayType(FloatType, _) => PFloatArray.INSTANCE
    case ArrayType(DoubleType, _) => PDoubleArray.INSTANCE
    case ArrayType(DecimalType(), _) => PDecimalArray.INSTANCE
    case ArrayType(TimestampType, _) => PTimestampArray.INSTANCE
    case ArrayType(DateType, _) => PDateArray.INSTANCE
  }
}
