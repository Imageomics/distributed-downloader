from pyspark.sql.types import StructField, StructType, IntegerType, StringType, LongType, TimestampType

schema = StructType([
    StructField("gbifID", LongType(), True),
    StructField("type", StringType(), True),
    StructField("format", StringType(), True),
    StructField("identifier", StringType(), True),
    StructField("references", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("source", StringType(), True),
    StructField("audience", StringType(), True),
    StructField("created", TimestampType(), True),
    StructField("creator", StringType(), True),
    StructField("contributor", StringType(), True),
    StructField("publisher", StringType(), True),
    StructField("license", StringType(), True),
    StructField("rightsHolder", StringType(), True)
])
