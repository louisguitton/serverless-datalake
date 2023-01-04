import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession


from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, ArrayType, BooleanType

glueContext = GlueContext(SparkContext.getOrCreate())

# Data Catalog: database and table name
db_name = "usersdb"
tbl_name = "users_users_api_public_users_settings"

# S3 location for output
output_dir = "s3://of-article-entities/personalize"

# df = spark.read.load(users_db_data)
# Read data into a DynamicFrame using the Data Catalog metadata
df = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = tbl_name)

user_settings_schema = StructType([
    StructField("settings", StructType([
        StructField("meta.language", StringType(), True),
        StructField("favourite.club", IntegerType(), True),
        StructField("following.clubs", ArrayType(IntegerType()), True),
        StructField("push.digest_news", BooleanType(), True),
        StructField("following.leagues", ArrayType(IntegerType()), True),
        StructField("meta.geoip_country", StringType(), True),
        StructField("ordered_menu_items", ArrayType(StructType([
            StructField("id", IntegerType()),
            StructField("type", StringType()),
        ])), True),
        StructField("push.breaking_news", BooleanType(), True),
        StructField("push.enabled_teams", ArrayType(StructType([
            StructField("id", IntegerType()),
            StructField("eventTypes", ArrayType(StringType())),
        ])), True),
        StructField("favourite.national_team", IntegerType(), True),
        StructField("following.national_teams", ArrayType(IntegerType()), True),
        StructField("meta.psychological_country", StringType(), True),
    ]), True),
    StructField("updatedAt", TimestampType(), True)
])
user_settings_schema_renamed = StructType([
    StructField("settings", StructType([
        StructField("meta_language", StringType(), True),
        StructField("favourite_club", IntegerType(), True),
        StructField("following_clubs", ArrayType(IntegerType()), True),
        StructField("push_digest_news", BooleanType(), True),
        StructField("following_leagues", ArrayType(IntegerType()), True),
        StructField("meta_geoip_country", StringType(), True),
        StructField("ordered_menu_items", ArrayType(StructType([
            StructField("id", IntegerType()),
            StructField("type", StringType()),
        ])), True),
        StructField("push_breaking_news", BooleanType(), True),
        StructField("push_enabled_teams", ArrayType(StructType([
            StructField("id", IntegerType()),
            StructField("eventTypes", ArrayType(StringType())),
        ])), True),
        StructField("favourite_national_team", IntegerType(), True),
        StructField("following_national_teams", ArrayType(IntegerType()), True),
        StructField("meta_psychological_country", StringType(), True),
    ]), True),
    StructField("updatedAt", TimestampType(), True)
])


parsed_df = df.select(
    F.col("key").alias('user_id'),
    F.from_json(df.value, user_settings_schema).cast(user_settings_schema_renamed).alias("settings")
)


followed_teams = parsed_df.select(
    "user_id",
    F.explode("settings.settings.following_clubs").alias('team_id')
)

# Write it out in Parquet
glueContext.write_dynamic_frame.from_options(frame = followed_teams, connection_type = "s3", connection_options = {"path": output_dir}, format = "parquet")
