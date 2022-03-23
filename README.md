Phoenix Spark Connector
=======================

## Introduction

This repo in an enhanced version of the original [phoenix-connectors](https://github.com/apache/phoenix-connectors)
which use Spark 3's DataSource API V2.<br/>
Checkout Spark's [document](https://docs.google.com/document/d/1DDXCTCrup4bKWByTalkXWgavcPdvur8a4eEu8x1BzPM/edit#)

## Features
- Uses Spark 3's Data Source API
- Supports read and write dynamic columns

## Requirement
- Java 8
- Scala 2.12+
- Spark 3.0+
- Phoenix 5

## Build
```shell
$ mvn clean package
```
## How to use

### Reading
#### Read options
- zkUrl: Zookeeper address
- table: Phoenix table name
- dynamicColumns:
  - Extend table schema with dynamic columns
  - DF's schema will consist of table's default columns and specified dynamic columns 
```scala
val df = spark.read.format("phoenix")
  .option("zkUrl", "zookeeper_url") // required
  .option("table", "table_name") // required
  .option("dynamicColumns", "col1::varchar,col2::double") // optional
  .load()
```

### Writing
#### Write options
- zkUrl: Zookeeper address
- table: Phoenix table name
- upsertDynamicColumns:
  - Upsert undefined columns as dynamic columns
  - If false, DF columns must be the same as Table's schema
```scala
val df = ...
df.write
  .format("phoenix")
  .mode(SaveMode.Append)
  .option("zkUrl", "zookeeper_url") // required
  .option("table", "table_name") // required
  .option("upsertDynamicColumns", "true") // optional. Default = false
  .save()
```

## Reference
- https://github.com/apache/phoenix-connectors