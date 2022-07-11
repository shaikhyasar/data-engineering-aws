import json
import awswrangler as wr
from urllib.parse import unquote_plus
import os
import boto3

def csvtoparquet(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
    
    key_list = key.split("/")
    print(f'key_list: {key_list}')
    db_name = key_list[len(key_list)-3]
    table_name = key_list[len(key_list)-2]
    print(f'Bucket: {bucket}')
    print(f'Key: {key}')
    print(f'DB Name: {db_name}')
    print(f'Table Name: {table_name}')
    input_path = f"s3://{bucket}/{key}"
    print(f'Input_Path: {input_path}')
    output_path = f"s3://{os.environ(['CLEAN_BUCKET_NAME'])}/{db_name}/{table_name}"
    print(f'Output_Path: {output_path}')
    input_df = wr.s3.read_csv([input_path])
    current_databases = wr.catalog.databases()
    wr.catalog.databases()
    if db_name not in current_databases.values:
        print(f'- Database {db_name} does not exist ... creating')
        wr.catalog.create_database(db_name)    
    else:
        print(f'- Database {db_name} already exists')
    result = wr.s3.to_parquet(
        df=input_df,
        path=output_path,
        dataset=True,
        database=db_name,
        table=table_name,
        mode="append")
    print("RESULT: ")
    print(f'{result}')
    return result
