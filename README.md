Phoenix Spark Connector
=======================

## Introduction

This repo in an enhanced version of the original [phoenix-connectors](https://github.com/apache/phoenix-connectors)
which use Spark 3's DataSource API V2.<br/>
Checkout Spark's [document](https://docs.google.com/document/d/1DDXCTCrup4bKWByTalkXWgavcPdvur8a4eEu8x1BzPM/edit#)

## Features
- Using Spark 3's Data Source API
- Add support for dynamic columns

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
```scala
val df = spark.read.format("phoenix")
  .option("zkUrl", "zookeeper_url") // required
  .option("table", "table_name") // required
  .option("dynamicColumns", "col1::varchar,col2::double") // optional
  .load()
```

### Writing
/// In progress

## Reference
- https://github.com/apache/phoenix-connectors