from kafka import KafkaConsumer
from json import loads
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, StructField
from pyspark.sql.functions import col, from_json
from pyspark.sql import functions as F
from sqlalchemy import create_engine



#streaming_consumer = KafkaConsumer('MyFirstKafkaTopic', bootstrap_servers = "localhost:9092")

#for data in streaming_consumer:
    #print(data)

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.postgresql:postgresql:42.2.10 pyspark-shell'

# specify the topic we want to stream data from.
kafka_topic_name = "MyFirstKafkaTopic"

# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
        .builder \
        .appName("KafkaStreaming ") \
        .getOrCreate()

# Only display Error messages in the console.
spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

# Select the value part of the kafka message and cast it to a string.
stream_df = stream_df.selectExpr("CAST(value as STRING)")


schema = StructType([
        StructField('title', StringType(), True),
        StructField('category', StringType(), True),
        StructField('description', StringType(), True),
        StructField('is_image_or_video', StringType(), True),
        StructField('save_location', StringType(), True),
        StructField('tag_list', StringType(), True),
        StructField('downloaded', IntegerType(), True),
        StructField('unique_id', StringType(), True),
        StructField('follower_count', IntegerType(), True),
        StructField('image_src', StringType(), True),
])

#convert json into multiple columns 
stream_df = stream_df.withColumn("value", from_json(stream_df["value"], schema)).select(col("value.*"))
stream_df = stream_df.drop("index") 
stream_df = stream_df.withColumn("follower_count",F.regexp_replace("follower_count", "k", "000"))
stream_df = stream_df.withColumn("follower_count",F.regexp_replace("follower_count", "M", "000000"))
stream_df = stream_df.withColumn("save_location",F.regexp_replace("save_location", "Local save in", ""))
stream_df = stream_df.select('title','category','description', 'is_image_or_video','save_location','tag_list','downloaded','unique_id', 'follower_count', 'image_src')
stream_df = stream_df.withColumn("follower_count", stream_df["follower_count"].cast('int'))

def spark_to_postgres(df, epoch_id): 
    df.printSchema()
    df.write \
        .mode('append') \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://localhost:5432/pintreststreaming') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'experimental_data') \
        .option('user', 'postgres') \
        .option('password', 'twinkleandfluffy') \
        .save()

# outputting the messages to the console 
#stream_df.writeStream \
 #   .format("console") \
  #  .outputMode("append") \
   # .start() \
   # .awaitTermination()

stream_df.writeStream \
    .foreachBatch(spark_to_postgres) \
    .start() \
    .awaitTermination()

