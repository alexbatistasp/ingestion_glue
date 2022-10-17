#!/usr/local/bin/python3.8
import boto3

ACCOUNT_ID=""
ACCESS_KEY=""
SECRET_KEY=""
AWS_DEFAULT_REGION="us-east-1"
# Create the resource for dynamoDB

session = boto3.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY)



glue = session.client('glue')

response = glue.get_database(
    CatalogId=ACCOUNT_ID,
    Name='dbsor'
)

bkt_tb_sor = "s3://my-bucket-{}-sor-alex-demo/tbtaxitrip_sor".format(str(ACCOUNT_ID))

print(bkt_tb_sor)
print (response)

response = glue.create_table(
    CatalogId=ACCOUNT_ID,
    DatabaseName='dbsor',
    TableInput={
        'Name': 'tbsor',
        'Description': 'Table Sor',        
        'StorageDescriptor': {
            "Columns": [
                {
                    "Name": "medallion",
                    "Type": "string"
                },
                {
                    "Name": "hack_license",
                    "Type": "string"
                },
                {
                    "Name": "vendor_id",
                    "Type": "string"
                },
                {
                    "Name": "rate_code",
                    "Type": "string"
                },
                {
                    "Name": "store_and_fwd_flag",
                    "Type": "string"
                },
                {
                    "Name": "pickup_datetime",
                    "Type": "string"
                },
                {
                    "Name": "dropoff_datetime",
                    "Type": "string"
                },
                {
                    "Name": "passenger_count",
                    "Type": "string"
                },
                {
                    "Name": "trip_time_in_secs",
                    "Type": "string"
                },
                {
                    "Name": "trip_distance",
                    "Type": "string"
                },
                {
                    "Name": "pickup_longitude",
                    "Type": "string"
                },
                {
                    "Name": "pickup_latitude",
                    "Type": "string"
                },
                {
                    "Name": "dropoff_longitude",
                    "Type": "string"
                },
                {
                    "Name": "dropoff_latitude",
                    "Type": "string"
                }
            ],
            "Location": bkt_tb_sor,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "Compressed": False,
            "NumberOfBuckets": -1,
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {
                    "serialization.format": "1"
                }
            },
            "BucketColumns": [],
            "SortColumns": [],
            "Parameters": {},
            "SkewedInfo": {
                "SkewedColumnNames": [],
                "SkewedColumnValues": [],
                "SkewedColumnValueLocationMaps": {}
            },
            "StoredAsSubDirectories": False
        },
        "PartitionKeys": [
            {
                "Name": "year",
                "Type": "int"
            },
            {
                "Name": "month",
                "Type": "int"
            },
            {
                "Name": "day",
                "Type": "int"
            }
        ],
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "EXTERNAL": "TRUE"            
        }
    }
)


print (response)
"""

response = client.create_partition(
    CatalogId='569781470788',
    DatabaseName='dbsor',
    TableName='tbsor',
    PartitionInput={
        'Values': [
            'string',
        ],
        'LastAccessTime': datetime(2015, 1, 1),
        'StorageDescriptor': {
            'Columns': [
                {
                    'Name': 'string',
                    'Type': 'string',
                    'Comment': 'string'
                },
            ],
            'Location': 'string',
            'InputFormat': 'string',
            'OutputFormat': 'string',
            'Compressed': True|False,
            'NumberOfBuckets': 123,
            'SerdeInfo': {
                'Name': 'string',
                'SerializationLibrary': 'string',
                'Parameters': {
                    'string': 'string'
                }
            },
            'BucketColumns': [
                'string',
            ],
            'SortColumns': [
                {
                    'Column': 'string',
                    'SortOrder': 123
                },
            ],
            'Parameters': {
                'string': 'string'
            },
            'SkewedInfo': {
                'SkewedColumnNames': [
                    'string',
                ],
                'SkewedColumnValues': [
                    'string',
                ],
                'SkewedColumnValueLocationMaps': {
                    'string': 'string'
                }
            },
            'StoredAsSubDirectories': True|False
        },
        'Parameters': {
            'string': 'string'
        },
        'LastAnalyzedTime': datetime(2015, 1, 1)
    }
)
"""
