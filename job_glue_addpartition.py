import sys
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from datetime import date
import boto3
import copy


args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'database_ingestion',
                           'table_ingestion',
                           's3inputpath',
                           's3outputpath'])

def add_partition(db,tb,s3path,partition):
    glue = boto3.client('glue')
    gettb = glue.get_table(DatabaseName=db,Name=tb)
    storage_descriptor = gettb['Table']['StorageDescriptor']
    storage_descriptor = copy.deepcopy(storage_descriptor)
    storage_descriptor['Location'] = s3path
    
    create_partition = glue.create_partition(
        DatabaseName=db,
        TableName=tb,
        PartitionInput={
            'Values': partition,
            'StorageDescriptor': storage_descriptor
        }
    )
    print(create_partition) 

# Inputs to ingestion
db = args['database_ingestion']
tb = args['table_ingestion']
date_ingestion = date.today()
partitions_values = (str(date_ingestion.year),str(date_ingestion.month),str(date_ingestion.day))
s3inputpath = args['s3inputpath']
s3outputpath =  args['s3outputpath']
s3outputpath += "{}/{}/{}".format(str(date_ingestion.year),str(date_ingestion.month),str(date_ingestion.day))

# Show inputs to ingestion
print ("Glue JOB NAME: ", args['JOB_NAME'])
print ("Database: ",db)
print ("Table: ", tb)
print ("Date Ingestion: ", date_ingestion)
print ("Partition Value: ", partitions_values)
print ("S3 Input Path: ", s3inputpath)
print ("S3 Output Path: ", s3outputpath)

# Start Context's
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Get Data From S3 Input Path 
GetDataFromInputDF0 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": True,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [s3inputpath],
        "recurse": True,
    },
    transformation_ctx="GetDataFromInput",
)
# Put Data To S3 Output Path
PutDataToSor = glueContext.write_dynamic_frame.from_options(
    frame=GetDataFromInputDF0,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": s3outputpath
    },
    format_options={"compression": "snappy"},
    transformation_ctx="PutDataToSor",
)
# Add Partition Glue Catalog
add_partition(db,tb,s3outputpath,partitions_values)
job.commit()
