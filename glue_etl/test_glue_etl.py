#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, ArrayType, BooleanType
import json

spark = SparkSession.builder.appName("Glue stuff").getOrCreate()


# In[9]:


users_db_data = "s3://of-lakeformation-users/users_users_api_public_users_settings/version_14/part-00000-d530da31-0c19-4260-87f6-81b97a5778ee-c000.snappy.parquet"


# In[ ]:


get_ipython().system('aws s3 cp s3://of-lakeformation-users/users_users_api_public_users_settings/version_14/part-00000-d530da31-0c19-4260-87f6-81b97a5778ee-c000.snappy.parquet users_db_data.parquet')


# In[2]:


full_user_settings_schema = StructType([
    StructField("value", StructType([
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
    ]), True),
    StructField("key", StringType(), True)
])


# In[52]:


df = spark.read.load("users_db_data.parquet")


# In[53]:


df.show()


# In[7]:


df.printSchema()


# In[8]:


df.show()


# In[36]:


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


# In[42]:


parsed_df = df.select(
    F.col("key").alias('user_id'),
    F.from_json(df.value, user_settings_schema).cast(user_settings_schema_renamed).alias("settings")
)
parsed_df.printSchema()


# In[50]:


followed_teams = parsed_df.select(
    "user_id",
    F.explode("settings.settings.following_clubs").alias('team_id')
)


# In[51]:


followed_teams.show()


# In[ ]:




