# Example Using glue catalog via spark
spark.sql("show databases").show()
df = spark.sql("select * from louis.csv_streams")
df.show()

# Example without glue catalog, reading data from S3
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
# Reading data from s3 using spark
spark = SparkSession \
 .builder \
 .getOrCreate()
raw_bucket = 's3://louis-bigdata-day/stream/*/*/*/*'
schemaString = "eventTime appId appScore appData"
fields = [
    StructField(field_name, StringType(), True)
    for field_name in schemaString.split()
]
schema = StructType(fields)
raw_bucket_df = spark.read.csv(raw_bucket, schema)
raw_bucket_df.cache()
raw_bucket_df.show()
raw_bucket_df.registerTempTable("stream")

raw_bucket_df.write.parquet("s3://louis-bigdata-day/parquet_emr")
