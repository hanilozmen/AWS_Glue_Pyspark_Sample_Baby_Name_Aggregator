# import pyspark modules
from pyspark.context import SparkContext
import pyspark.sql.functions as f

# import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# initialize context and sessions.
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

# parameters
glue_db = "virtual"
glue_tbl = "input"
s3_write_path = "s3://glue-testanil/output"

### 1- EXTRACT PROCESS ###

dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(database=glue_db, table_name=glue_tbl)
# Convert dynamic frame to data frame to use standard pyspark functions.
data_frame = dynamic_frame_read.toDF()

### 2- TRANSFORM PROCESS ###

data_frame_aggregated = data_frame.groupby("first name").agg(f.mean(f.col("year")).alias("avg_year"), )
data_frame_aggregated = data_frame_aggregated.orderBy(f.desc("avg_year"))

### 3- LOAD PROCESS ###

# Create 1 partition, since data is small.
data_frame_aggregated = data_frame_aggregated.repartition(1)

# Convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(data_frame_aggregated, glue_context, "dynamic_frame_write")

# Write data back to S3
glue_context.write_dynamic_frame.from_options(frame=dynamic_frame_write,
                                              connection_type="s3",
                                              connection_options={"path": s3_write_path},
                                              format="csv")
