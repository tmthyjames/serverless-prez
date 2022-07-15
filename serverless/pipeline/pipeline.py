import os
from pathlib import Path
import re
import json
import shutil
import time
from typing import List, Dict, Callable, Set

import numpy as np
import pandas as pd
import geopandas as gpd
import boto3
import requests

from serverless import settings


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


class Pipeline(object):
    """
    class to make setting up a data pipeline easy. order of operations
    s3 -> athena -> lambda -> api_gateway
    after execution, you should be able to make an HTTP request to your
    API endpoint and get back the data specified in your lambda function
    passed to the `create_lambda` method.
    """

    BUCKET_NAME = ENTRY_POINT = 'serverlessprez'
    BUCKET = s3.Bucket(BUCKET_NAME)
    LAMBDA_NAME = 'serverlessprez_lambda' # add as config param in __init__
    LAMBDA_LOC = 'src/aws/lambda.zip'
    STAGE_NAME = 'dev'

    def __init__(self, filepaths: Set, partitions: List=None):

        processed_dir_loc = filepaths
        self.processed_files = (
            i for i in (
                settings.root_path/settings.data.processed_path
            ).rglob('*[!.].*') if i.is_file()
        )


    def to_s3(self, partitions: List=None,
              dtypes: Dict=None,
              build_athena_model: bool=True,
              path: str='.*/.*',
              file_type: str='to_csv', **kwargs) -> str:

        s3.create_bucket(Bucket=self.BUCKET_NAME)

        for file in self.processed_files:
            file_str = str(file).lower()
            key = file_str.replace(str(settings.root_path).lower(), '').strip('/')
            if re.search(path, file_str):
                if not isfile_s3(self.BUCKET, key):
                    s3.Bucket(self.BUCKET_NAME).upload_file(file_str, key)

    def _get_dirs(self):
        self._dirs = {}
        for file in self.processed_files:
            file_str = str(file)
            file_path = file_str.split('/')
            path = '/'.join([f for f in file_path[:-1] if '=' not in f])
            if path not in self._dirs and os.path.isdir(path):
                self._dirs[path] = file_str
        return self._dirs

    @property
    def dirs(self):
        if hasattr(self, '_dirs'):
            return self._dirs
        else:
            return self._get_dirs()

    def build_athena_model(self, **kwargs) -> Dict:

        path = kwargs.get('path', '.*/.*')

        self._get_dirs()
        query_details = {}

        ath.start_query_execution(
            QueryString=f"CREATE DATABASE IF NOT EXISTS {self.BUCKET_NAME}",
            ResultConfiguration={'OutputLocation': f's3://{self.BUCKET_NAME}/database/queries'}
        )

        for parent, file in self._dirs.items():

            if re.search(path, parent):

                folder, subfolder = Path(parent).relative_to(settings.root_path/settings.data.processed_path).parts

                s3_path = parent.replace(str(settings.root_path), '').strip('/')
                table_suffix = "_".join(s3_path.split('/')[2:]) # document hard coded value, put in _settings.py
                table_name = f"{self.BUCKET_NAME}." + table_suffix

                if '.geojson' in file:
                    df = gpd.read_file(file)
                    metadata = f"""ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'
                                    STORED AS INPUTFORMAT 'com.esri.json.hadoop.EnclosedJsonInputFormat'
                                    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                                    LOCATION 's3://{self.BUCKET_NAME}/{s3_path}/';"""
                else:
                    df = pd.read_parquet(file)
                    metadata = f"""STORED AS PARQUET
                                    LOCATION 's3://{self.BUCKET_NAME}/{s3_path}/'
                                    tblproperties ("parquet.compression"="SNAPPY");"""

                partition_by = settings.etl.get(folder).get(subfolder).partition_by
                if partition_by:
                    partition_statement = f"{' '.join([i + ' STRING,' for i in partition_by]).rstrip(',')}"
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
                (settings.root_path/settings.data.DB_TABLE_DEFINITIONS).mkdir(parents=True, exist_ok=True)
                with open((settings.root_path/settings.data.DB_TABLE_DEFINITIONS)/table_name, 'w') as file_obj:
                    file_obj.write(sql_text)

                query = ath.start_query_execution(
                    QueryString=sql_text,
                    ResultConfiguration={'OutputLocation': f's3://{self.BUCKET_NAME}/database/queries'}
                )
                query_details[table_name] = query

                time.sleep(5)

                if partition_by:
                    ath.start_query_execution(
                        QueryString=f"MSCK REPAIR TABLE `{table_name}`;",
                        ResultConfiguration={'OutputLocation': f's3://{self.BUCKET_NAME}/database/queries'}
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
            Description='serverlessprez',
            FunctionName=self.LAMBDA_NAME,
            # Handler='lambda.lambda_handler',
            Publish=True,
            Role='arn:aws:iam::639381120660:role/serverlessprez',
            # Runtime='python3.8',
            Timeout=60,
            MemorySize=512,
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
            # requestParameters={
            #   'method.request.querystring.serverlessprez': False
            # }
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
            credentials='arn:aws:iam::639381120660:role/serverlessprez',
            requestTemplates={
                "application/json":"{\"serverlessprez\":\"$input.params('serverlessprez')\"}"
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
        for path, file in self._dirs.items():
            s3_path = path.replace('../data/', '')
            db_name = f"{self.BUCKET_NAME}." + "_".join(s3_path.split('/')[1:])
            query = ath.start_query_execution(
                QueryString=f"DROP TABLE {db_name};",
                ResultConfiguration={'OutputLocation': 's3://serverlessprez/database/queries'}
            )


    def _test_api_endpoint(self, **kwargs):
        url = f"https://{self.rest_api_id}.execute-api.us-east-1.amazonaws.com/{self.STAGE_NAME}/{self.ENTRY_POINT}"
        self.api_url = url
        resp = requests.get(url)
        return resp.json()


    @staticmethod
    def run_pipeline(skip:list=[], **kwargs):

        paths = Path('../data/processed/')
        pipeline = Pipeline(paths)

        steps = {
            1: {
                'func': pipeline.to_s3,
                'args': {**kwargs}
            },
            2: {
                'func': pipeline.build_athena_model,
                'args': {
                    'target_data': target_data,
                    **kwargs
                }
            },
            3: {
                'func': pipeline.create_lambda,
                'args': {
                    'file': '../lambda_app/app/lambda.py',
                    'name': pipeline.LAMBDA_NAME,
                    **kwargs
                }
            },
            4: {
                'func': pipeline.create_api,
                'args': {
                    'name': pipeline.LAMBDA_NAME,
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
            test = pipeline._test_api_endpoint()
            return test

        return 200
