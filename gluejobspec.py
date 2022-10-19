import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


AWSGlueDataCatalog_node1666142526001 = glueContext.create_dynamic_frame.from_catalog(
    database="dbsot",
    table_name="tbtaxitrip_sot",
    transformation_ctx="AWSGlueDataCatalog_node1666142526001",
)


DropFields_node1666142683708 = DropFields.apply(
    frame=AWSGlueDataCatalog_node1666142526001,
    paths=["medallion", "pickup_datetime", "startdate"],
    transformation_ctx="DropFields_node1666142683708",
)


ChangeSchemaApplyMapping_node1666142722902 = ApplyMapping.apply(
    frame=DropFields_node1666142683708,
    mappings=[
        ("hack_license", "string", "hack_license", "string"),
        ("vendor_id", "string", "vendor_id", "string"),
        ("passenger_count", "int", "total_passengers", "int"),
        ("trip_time_in_secs", "int", "total_hours", "double"),
        ("trip_distance", "double", "total_miles", "double"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1666142722902",
).toDF()


NewDF = (ChangeSchemaApplyMapping_node1666142722902
    .withColumn('total_hours',SqlFuncs.col('total_hours')/60/60)
    )
"""
gluedf = DynamicFrame.fromDF(
                NewDF, glueContext, "gluedf")
"""

Aggregate_node1666142812187 = sparkAggregate(
    glueContext,
    parentFrame=NewDF,
    groups=["hack_license", "vendor_id"],
    aggs=[["total_passengers", "sum"], ["total_hours", "sum"], ["total_miles", "sum"]],
    transformation_ctx="Aggregate_node1666142812187",
)



dffinal = ApplyMapping.apply(
    frame=Aggregate_node1666142812187,
    mappings=[
        ("hack_license", "string", "hack_license", "string"),
        ("vendor_id", "string", "vendor_id", "string"),
        ("`sum(total_passengers)`", "int", "total_passengers", "int"),
        ("`sum(trip_time_in_secs)`", "int", "total_hours", "double"),
        ("`sum(total_passengers)`", "double", "total_miles", "double"),
    ],
    transformation_ctx="dffinal",
)


AmazonS3_node1666142875577 = glueContext.getSink(
    path="s3://<bucket>/tbspec/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1666142875577",
)
AmazonS3_node1666142875577.setCatalogInfo(
    catalogDatabase="dbspec", catalogTableName="tbspec"
)
AmazonS3_node1666142875577.setFormat("glueparquet")
AmazonS3_node1666142875577.writeFrame(dffinal)
job.commit()
