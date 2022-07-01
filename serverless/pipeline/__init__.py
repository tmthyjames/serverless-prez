import os
from pathlib import Path
import re
import boto3
import json

import numpy as np
import pandas as pd
# import geopandas as gpd
import os
from typing import List, Dict, Callable, Set
import requests
import os
import shutil
import time

############ TO IMPORT PROJECT PYTHON MODULES ############
# import sys
# sys.path.insert(0, '../src/')
# from src.data import data
# from src.data.data import target_data
# from src.data import get_data
# from src.pipeline.to_s3 import processed_files, transfer_to_s3


s3 = boto3.resource('s3')
ath = boto3.client('athena')
lambda_client = boto3.client('lambda')
api_client = boto3.client('apigateway')


def isfile_s3(bucket, key: str) -> bool:
    """Returns T/F whether the file exists."""
    objs = list(bucket.objects.filter(Prefix=key))
    return len(objs) == 1 and objs[0].key == key


def isdir_s3(bucket, key: str) -> bool:
    """Returns T/F whether the directory exists."""
    objs = list(bucket.objects.filter(Prefix=key))
    return len(objs) > 1


def sql_create_from_dataframe(df, name):
    sql_text = pd.io.sql.get_schema(df, name)\
                    .replace('TABLE', 'EXTERNAL TABLE')\
                    .replace('"geometry" TEXT', '"geometry" binary')\
                    .replace('"', '')\
                    .replace('TEXT', 'STRING')\
                    .replace('REAL', 'DOUBLE')\
                    .replace(' INTEGER', ' INT')
    return sql_text


class Hera(object):
    """
    class to make setting up a data pipeline easy. order of operations
    s3 -> athena -> lambda -> api_gateway
    after execution, you should be able to make an HTTP request to your
    API endpoint and get back the data specified in your lambda function
    passed to the `create_lambda` method.
    """

    BUCKET_NAME = ENTRY_POINT = 'serverless-prez'
    BUCKET = s3.Bucket(BUCKET_NAME)
    LAMBDA_NAME = 'serverless-prez_main' # add as config param in __init__
    LAMBDA_LOC = 'src/aws/lambda.zip'
    STAGE_NAME = 'dev'

    def __init__(self, filepaths: Set, partitions: List=None):
        processed_dir_loc = filepaths
        self.processed_files = [i for i in processed_dir_loc.rglob('*[!.].*') if i.is_file()]


    def to_s3(self, partitions: List=None,
              dtypes: Dict=None,
              build_athena_model: bool=True,
              path: str='.*/.*',
              file_type: str='to_csv', **kwargs) -> str:

        s3.create_bucket(Bucket=BUCKET_NAME)

        for file in self.processed_files:
            file_str = str(file).lower()
            key = file_str.replace('../data/', '')
            if re.search(path, file_str):
                if not isfile_s3(self.BUCKET, key):
                    s3.Bucket(self.BUCKET_NAME).upload_file(file_str, key)

    def _get_dirs(self):
        self.dirs = {}
        for file in self.processed_files:
            file_str = str(file)
            file_path = file_str.split('/')
            path = '/'.join([f for f in file_path[:-1] if '=' not in f])
            if path not in self.dirs and os.path.isdir(path):
                self.dirs[path] = file_str
        return self.dirs

    def build_athena_model(self, target_data, **kwargs) -> Dict:

        path = kwargs.get('path', '.*/.*')

        self._get_dirs()
        query_details = {}

        for parent, file in self.dirs.items():

            if re.search(path, parent):

                folder, subfolder = parent.split('/')[3:]
                partition_by = target_data[folder][subfolder]['partition_by']
                partition_statement = f"{' '.join([i + ' STRING,' for i in partition_by]).rstrip(',')}"

                print(partition_statement)

                s3_path = parent.replace('../data/', '')
                table_suffix = "_".join(s3_path.split('/')[1:])
                table_name = f"{self.BUCKET_NAME}." + table_suffix

                if '.geojson' in file:
                    df = gpd.read_file(file)

                    with open(f'../data/meta/{table_suffix}.json', 'r') as f:
                        schema = json.load(f)
                    for c in df.columns:
                        if c == 'geometry': continue
                        df[c] = df[c].astype(schema['properties'][c])

                    metadata = f"""ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'
                                    STORED AS INPUTFORMAT 'com.esri.json.hadoop.EnclosedJsonInputFormat'
                                    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                                    LOCATION 's3://{self.BUCKET_NAME}/{s3_path}/';"""
                else:
                    df = pd.read_parquet(file)
                    metadata = f"""STORED AS PARQUET
                                    LOCATION 's3://{self.BUCKET_NAME}/{s3_path}/'
                                    tblproperties ("parquet.compression"="SNAPPY");"""

                if partition_by:
                    metadata = f" PARTITIONED BY ({partition_statement})\n {metadata}"
                    sql_text = sql_create_from_dataframe(df, table_name) + metadata
                    for p in partition_by:
                        sql_text = re.sub(p + ' STRING', '', sql_text, count=1)
                    sql_text = re.sub('\s,', '', sql_text)

                else:
                    sql_text = sql_create_from_dataframe(df, table_name) + metadata

                ###### remove illegal athena chars: <
                sql_text = sql_text.replace('<', '_lt_') # replace with _lt_ for `less than`

                print(sql_text)
                with open(f'../src/database/{table_name}.sql', 'w') as file_obj:
                    file_obj.write(sql_text)

                query = ath.start_query_execution(
                    QueryString=sql_text,
                    ResultConfiguration={'OutputLocation': 's3://placedly/database/queries'}
                )
                query_details[table_name] = query

                time.sleep(5)

                if partition_by:
                    ath.start_query_execution(
                        QueryString=f"MSCK REPAIR TABLE `{table_name}`;",
                        ResultConfiguration={'OutputLocation': 's3://placedly/database/queries'}
                    )


    def create_lambda(self, file:str, name:str, **kwargs) -> Dict:
        image_uri = kwargs.get('image_uri')
        if not image_uri:
            raise ValeError('must provide an ImageUri')
        # destination = make_archive(file)
        # print(destination)
        # s3.Bucket(self.BUCKET_NAME).upload_file(str(destination), self.LAMBDA_LOC)
        response = lambda_client.create_function(
            Code={
                # 'S3Bucket': self.BUCKET_NAME,
                # 'S3Key': self.LAMBDA_LOC,
                'ImageUri': image_uri
            },
            Description='Placedly',
            FunctionName=self.LAMBDA_NAME,
            # Handler='lambda.lambda_handler',
            Publish=True,
            Role='arn:aws:iam::639381120660:role/placedly',
            # Runtime='python3.8',
            Timeout=30,
            PackageType='Image',
        )
        return response


    def update_lambda(self, file:str, name:str, **kwargs):
        destination = make_archive(file)
        s3.Bucket(self.BUCKET_NAME).upload_file(str(destination), self.LAMBDA_LOC)
        response = lambda_client.update_function_code(
            FunctionName=self.LAMBDA_NAME,
            S3Bucket=BUCKET,
            S3Key=self.LAMBDA_LOC,
            Publish=True,
        )
        return response


    def delete_lambda(self, name:str, **kwargs):
        response = lambda_client.delete_function(
            FunctionName=name
        )
        return response


    def create_api(self, name:str, method: str='GET', integration: str='AWS_PROXY', stage: str=None, **kwargs) -> Dict:

        # Create rest api
        rest_api = api_client.create_rest_api(
            name=name
        )

        self.rest_api_id = rest_api["id"]

        # Get the rest api's root id
        root_resource_id = api_client.get_resources(
            restApiId=self.rest_api_id
        )['items'][0]['id']

        # Create an api resource
        api_resource = api_client.create_resource(
            restApiId=self.rest_api_id,
            parentId=root_resource_id,
            pathPart=self.ENTRY_POINT
        )

        api_resource_id = api_resource['id']

        # Add a post method to the rest api resource
        api_method = api_client.put_method(
            restApiId=self.rest_api_id,
            resourceId=api_resource_id,
            httpMethod=method,
            authorizationType='NONE',
            requestParameters={
              'method.request.querystring.placedly': False
            }
        )

        print(api_method)

        put_method_res = api_client.put_method_response(
            restApiId=self.rest_api_id,
            resourceId=api_resource_id,
            httpMethod=method,
            statusCode='200'
        )

        # The uri comes from a base lambda string with the function ARN attached to it
        arn_uri = f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:639381120660:function:{self.LAMBDA_NAME}/invocations"

        put_integration = api_client.put_integration(
            restApiId=self.rest_api_id,
            resourceId=api_resource_id,
            httpMethod=method,
            type=integration,
            integrationHttpMethod='POST',
            uri=arn_uri,
            credentials='arn:aws:iam::639381120660:role/placedly',
            requestTemplates={
                "application/json":"{\"placedly\":\"$input.params('placedly')\"}"
            }
        )

        put_integration_response = api_client.put_integration_response(
            restApiId=self.rest_api_id,
            resourceId=api_resource_id,
            httpMethod=method,
            statusCode='200',
            selectionPattern=''
        )

        # this bit sets a stage 'dev' that is built off the created apigateway
        # it will look something like this:
        # https://<generated_api_id>.execute-api.<region>.amazonaws.com/dev/lamda_name
        deployment = api_client.create_deployment(
            restApiId=self.rest_api_id,
            stageName=self.STAGE_NAME,
        )

        return deployment


    def _drop_athena_tables(self, **kwargs):
        self._get_dirs()
        for path, file in self.dirs.items():
            s3_path = path.replace('../data/', '')
            db_name = f"{self.BUCKET_NAME}." + "_".join(s3_path.split('/')[1:])
            query = ath.start_query_execution(
                QueryString=f"DROP TABLE {db_name};",
                ResultConfiguration={'OutputLocation': 's3://placedly/database/queries'}
            )


    def _test_api_endpoint(self, **kwargs):
        url = f"https://{self.rest_api_id}.execute-api.us-east-1.amazonaws.com/{self.STAGE_NAME}/{self.ENTRY_POINT}?state=fl"
        resp = requests.get(url)
        return resp.json()


    @staticmethod
    def run_pipeline(skip:list=[], **kwargs):

        paths = Path('../data/processed/')
        hera = Hera(paths)

        steps = {
            1: {
                'func': hera.to_s3,
                'args': {**kwargs}
            },
            2: {
                'func': hera.build_athena_model,
                'args': {
                    'target_data': target_data,
                    **kwargs
                }
            },
            3: {
                'func': hera.create_lambda,
                'args': {
                    'file': '../src/aws/lambda.py',
                    'name': 'placedly_main',
                    **kwargs
                }
            },
            4: {
                'func': hera.create_api,
                'args': {
                    'name': 'placedly_main',
                    **kwargs
                }
            }
        }

        for step, call in steps.items():
            if step in skip:
                continue
            call['func'](**call['args'])

        time.sleep(10)

        if 4 not in skip:
            test = hera._test_api_endpoint()
            return test

        return 200
