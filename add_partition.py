import boto3
import copy
from datetime import date

def teste(dbname,tbname,location,partitions_values):
    # Extract the Glue Database and Table name from Environment Variables
    DATABASE_NAME = dbname
    TABLE_NAME = tbname    
    
    
    print(type(partitions_values))
    print(partitions_values) # Output: [‘YYYY’, ‘MM', ‘DD’, ‘HH']
    
    # Initialise the Glue client using Boto 3
    ACCOUNT_ID=""
    ACCESS_KEY=""
    SECRET_KEY=""
    AWS_DEFAULT_REGION="us-east-1"
    # Create the resource for dynamoDB

    session = boto3.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY)

    glue_client = session.client('glue')

    try:
        # Check if the partition already exists. If yes, skip adding it again
        get_partition_response = glue_client.get_partition(
            DatabaseName=DATABASE_NAME,
            TableName=TABLE_NAME,
            PartitionValues=partitions_values
        )
        print('Glue partition already exists.')
    except Exception as e:
        # Check if the exception is EntityNotFoundException. If yes, go ahead and create parition
        if type(e).__name__ == 'EntityNotFoundException':
            print('Retrieve Table Details:')
            get_table_response = glue_client.get_table(
                DatabaseName=DATABASE_NAME,
                Name=TABLE_NAME
            )
            print
            # Extract the existing storage descriptor and Create custom storage descriptor with new partition location
            storage_descriptor = get_table_response['Table']['StorageDescriptor']
            custom_storage_descriptor = copy.deepcopy(storage_descriptor)
            custom_storage_descriptor['Location'] = location

            # Create new Glue partition in the Glue Data Catalog
            create_partition_response = glue_client.create_partition(
                DatabaseName=DATABASE_NAME,
                TableName=TABLE_NAME,
                PartitionInput={
                    'Values': partitions_values,
                    'StorageDescriptor': custom_storage_descriptor
                }
            )
            print('Glue partition created successfully.') 
        else:
            # Handle exception as per your business requirements
            print(e)   




dting = date.today()
partition_value = (str(dting.year),str(dting.month),str(dting.day))

teste("dbsor","tbsor","s3://<bucket name>/tbtaxitrip_sor/2022/10/17/",partition_value)
