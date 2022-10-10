import findspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
findspark.init(os.environ["SPARK_HOME"])
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import when, col, regexp_replace

# Adding the packages required to get data from S3  
os.environ['PYSPARK_SUBMIT_ARGS']='--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell'

# Creating our Spark configuration
def create_spark():
    conf = SparkConf() \
        .setAppName('S3toSpark') \
        .setMaster('local[*]')

    sc=SparkContext(conf=conf)

    # Configure the setting to read from the S3 bucket
    accessKeyId = "AKIARYREOUUALFR5AIFR"
    secretAccessKey = "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"
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set('fs.s3a.access.key', accessKeyId)
    hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
    hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS
    
    # Create our Spark session
    spark=SparkSession(sc)
    df = spark.read.json("s3a://pintrestbucket/*.json") 
# Read from the S3 bucket
def transform_data():
#spark processing code 
    df = df.withColumn("follower_count",F.regexp_replace("follower_count", "k", "000"))
    df = df.withColumn("follower_count",F.regexp_replace("follower_count", "M", "000000"))
    df = df.withColumn("save_location",F.regexp_replace("save_location", "Local save in", ""))
    df = df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df.columns])
    df = df.drop("index") 
    df = df.select('title','category','description', 'is_image_or_video','save_location','tag_list','downloaded','unique_id', 'follower_count', 'image_src')

def write_data(df):
    df.write\
    .format("org.apache.spark.sql.cassandra")\
            .mode("append")\
            .options(table = 'pintrest_data', keyspace = 'data') \
            .save()

def spark_session():
    create_spark()
    transform_data()
    write_data()

spark_session()