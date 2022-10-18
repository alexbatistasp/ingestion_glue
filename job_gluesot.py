import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions




args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1666053051041 = glueContext.create_dynamic_frame.from_catalog(
    database="dbsor",
    table_name="tbtaxitrip_sor",
    transformation_ctx="AWSGlueDataCatalog_node1666053051041",
)

df1 = AWSGlueDataCatalog_node1666053051041.toDF()
# add 2 new columns year and month which will be used for #partitioning the data        
partitiondf = (df1
                .withColumn('startdate', functions.col('pickup_datetime'))
            )
gluedf = DynamicFrame.fromDF(
                partitiondf, glueContext, "gluedf")


# Script generated for node Drop Fields
DropFields_node1666053075318 = DropFields.apply(
    frame=gluedf,
    paths=[
        "rate_code",
        "store_and_fwd_flag",
        "dropoff_datetime",
        "pickup_longitude",
        "pickup_latitude",
        "dropoff_longitude",
        "dropoff_latitude",
        "year",
        "month",
        "day",
    ],
    transformation_ctx="DropFields_node1666053075318",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1666053597327 = ApplyMapping.apply(
    frame=DropFields_node1666053075318,
    mappings=[
        ("medallion", "string", "medallion", "string"),
        ("hack_license", "string", "hack_license", "string"),
        ("vendor_id", "string", "vendor_id", "string"),
        ("pickup_datetime", "string", "pickup_datetime", "timestamp"),
        ("passenger_count", "string", "passenger_count", "int"),
        ("trip_time_in_secs", "string", "trip_time_in_secs", "int"),
        ("trip_distance", "string", "trip_distance", "double"),
        ("startdate", "string", "startdate", "date"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1666053597327",
)

# Script generated for node Amazon S3
AmazonS3_node1666054590537 = glueContext.getSink(
    path="s3://< bucket >/tbsot/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=["startdate"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1666054590537",
)
AmazonS3_node1666054590537.setCatalogInfo(
    catalogDatabase="dbsot", catalogTableName="tbsot"
)
AmazonS3_node1666054590537.setFormat("glueparquet")
AmazonS3_node1666054590537.writeFrame(ChangeSchemaApplyMapping_node1666053597327)

job.commit()
