import typing
from pathlib import Path
import json

import pandas as pd
import boto3
import s3fs


ath = boto3.client('athena')


class QueryAthena:

    def __init__(self, output_loc='s3://serverlessprez/database/queries'):
        self.output_loc = output_loc

    def execute_query(self, sql, **kwargs):

        full_result_set = []

        query = ath.start_query_execution(
            QueryString=sql,
            ResultConfiguration={'OutputLocation': self.output_loc}
        )

        qid = query['QueryExecutionId']

        query_state = ath.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']['State']

        while query_state in ['RUNNING', 'QUEUED']:
            query_state = ath.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']['State']

        results_df = pd.read_csv(
            f"{self.output_loc}/{qid}.csv",
            dtype={
                'zip': str,
                'county': str,
                'fipstate': str,
                'fipscty': str
            }
        )

        return results_df


def lambda_handler(event, context):

    query_string_params = event.get('queryStringParameters') or {}
    sql_from_url_param = query_string_params.get('sql')

    if sql_from_url_param:
        sql = sql_from_url_param
    else:
        sql = """
            SELECT * FROM "serverlessprez"."census_population"
            where population_2020 >= 200000
        """

    df = QueryAthena().execute_query(sql)

    return {
        'statusCode': 200,
        'body': df.to_json(orient='records')
    }
