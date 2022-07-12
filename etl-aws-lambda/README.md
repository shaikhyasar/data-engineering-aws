
# Convert the CSV file to Parquet file by AWS Lambda using Serverless

The repo will convert csv file from S3 bucket to Parquet file to specified s3 bucket location. Using serverless module which will be helpful to build lambda function in the local.

## About the setup
In order to deploy the this repo, you need to run the following command:
```
$ npm install -g serverless
```
You need to install **AWS CLI** in your local and configure your aws, so that, serverless can able to deploy the aws lambda on your behalf

```
aws configure
```
You will be prompt to enter those values. You can able to get in IAM (Identity and Access Management)
```
AWS Access Key ID :
AWS Secret Access Key :
Default region name:
Default output format:
```


## About the code
Import the required package. There are some external package needed inorder to use. In lambda, you cannot use **requirements.txt** to install external package. in console level, you need to dockerize the external package and place it as a layer, so that, your function can access those packages. Check below to see, how to achieve to install external packages

```
import json
import awswrangler as wr
from urllib.parse import unquote_plus
import os
import boto3
```

### Bundling dependencies

In case you would like to include third-party dependencies, you will need to use a plugin called `serverless-python-requirements`. You can set it up by running the following command:

```bash
serverless plugin install -n serverless-python-requirements
```

Running the above will automatically add `serverless-python-requirements` to `plugins` section in your `serverless.yml` file and add it as a `devDependency` to `package.json` file. The `package.json` file will be automatically created if it doesn't exist beforehand. Now you will be able to add your dependencies to `requirements.txt` file (`Pipfile` and `pyproject.toml` is also supported but requires additional configuration) and they will be automatically injected to Lambda package during build process. For more details about the plugin's configuration, please refer to [official documentation](https://github.com/UnitedIncome/serverless-python-requirements).


When the trigger happened (In our case, new csv file is uploaded to specified s3 location), the lambda function will be triggered.
```
for record in event['Records']:
    bucket = record['s3']['bucket']['name']
    key = unquote_plus(record['s3']['object']['key'])
```
In above code, we are getting the bucket name and file path, we can use those values to recreate same path for our new converted file.

Now we are creating database name and table name from the file path, because we are going to add it in AWS Glue, where AWS Athena and AWS Redshift can able to do sql query on s3 directly
```
key_list = key.split("/")
db_name = key_list[len(key_list)-3]
table_name = key_list[len(key_list)-2]
current_databases = wr.catalog.databases()
if db_name not in current_databases.values:
    print(f'- Database {db_name} does not exist ... creating')
    wr.catalog.create_database(db_name)    
else:
    print(f'- Database {db_name} already exists')
```
Now, we are converting csv to parquet using AWS wrangler to_parquet method. The file will be appened if that specified file already exist.
```
result = wr.s3.to_parquet(
        df=input_df,
        path=output_path,
        dataset=True,
        database=db_name,
        table=table_name,
        mode="append")
```
We can check AWS Cloudwatch to check the logs of our print statements

### Deployment

In order to deploy the this repo, you need to run the following command:
```
$ npm install -g serverless
```


```
$ serverless deploy
```

### Invocation

After successful deployment, you can invoke the deployed function by using the following command:

```bash
serverless invoke --function csvtoparquet
```

Which should result in response similar to the following:

```json
{
    "statusCode": 200,
    "body": "The file has been converted successfully"
}
```

### Local development

You can invoke your function locally by using the following command:

```bash
serverless invoke local --function csvtoparquet
```

Which should result in response similar to the following:

```
{
    "statusCode": 200,
    "body": "The file has been converted successfully"
}
```


