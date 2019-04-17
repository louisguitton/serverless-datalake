SELECT * from csv_stream LIMIT 10;
SELECT COUNT(*) from csv_stream;
SELECT * from csv_stream ORDER BY eventtime DESC;
SELECT SUM(appscore) as count_appscore from csv_stream;

SELECT * from parquet_parquet LIMIT 10;
SELECT COUNT(*) from parquet_parquet;
SELECT * from parquet_parquet ORDER BY eventtime DESC;
SELECT SUM(appscore) as count_appscore from parquet_parquet;
