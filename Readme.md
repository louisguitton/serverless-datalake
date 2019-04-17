# 2019-04-16 - AWS Serverless Data Lake Immersion Day

## Lab 1: Kinesis Streams + Firehose + Analytics

### What we did

Stream faked video game scores by a pool of 50 users; count live how many games a given user had; dump that to S3 as minute batches.

### Learnings

- Partition by table name then Y/M/D/H
- Gzip or bz2 all files by default on s3
- `Kinesis Streams` could be replaced by `Kafka`, but you need to deploy Kafka on EC2s and it’s not great https://github.com/open-guides/og-aws#kinesis-streams-alternatives-and-lock-in

## Lab 2: Glue + Athena + Quicksight

### What we did

Link the game data lying S3 to a Hive table; then ETL that data and dump it back to S3 as parquet; then query that directly using Athena; and then build a dashboard using Quicksight.

### Learnings

- Use a "catalog", or a metastore that can surface data tables lying around in S3 / HDFS / SQL as a common layer for the rest of the pipeline
- Such catalogs are `Apache Hive Metastore`
- `Glue Data Catalog` is a managed Hive metastore
- `Glue ETL` is a managed spark cluster
- Glue has connectors to data sources (like airlfow operators/hooks, or springer taps); and you can leverage open source classifiers, eg: newRelic
- Beware of vendor lock in with Glue as it seems it has some magic going on, and the ETL jobs are not source controlled. That being said, it's very similar to how we manage today our ETL with ETLeap
- `Athena` is a managed `Presto` cluster
- When you pay per data scan (like Athena or Redshift Spectrum, but not like Redshift) it's smart to put limits to the number of rows you can scan, otherwise your analysts do "Select \*", it takes 10 minutes and if you have terrabytes of data, it just cost you 5k€
- Amazon Forecast API to be compared to the forecaster package
- Start from the use case to decide which data format to use (JSON vs CSV vs Parquet/ORC vs …)
- Same for compression

## Lab 3: EMR

### What we did

Spin up an EMR cluster with 3 nodes, connected to our data, and run pyspark and SQL ETL things from there.

### Learnings

- you can configure EMR to use the Glue Data Catalog, and it comes with
- zepelin noteoboks let you switch contexts from cell to cell: eg one cell in SQL, one cell in pyspark

## Lab 4: Redshift

We use Redshift pretty heavily already. Nothing really new was in the lab.
