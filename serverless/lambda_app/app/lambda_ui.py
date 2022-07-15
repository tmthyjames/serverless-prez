import json
import boto3


ath = boto3.client('athena')


def results_to_df_structured_json(results):

    columns = [
        col['Label']
        for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']
    ]

    listed_results = []
    for res in results['ResultSet']['Rows'][1:]:
        values = []
        for field in res['Data']:
            try:
                values.append(list(field.values())[0])
            except:
                values.append(list(' '))

        listed_results.append(
            dict(zip(columns, values))
        )

    return listed_results


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

        results = ath.get_query_results(QueryExecutionId=qid)

        results_json_struct = results_to_df_structured_json(results)

        return results_json_struct


def lambda_handler(event, context):

    query_string_params = event.get('queryStringParameters') or {}
    sql_from_url_param = query_string_params.get('sql')

    if sql_from_url_param:
        sql = sql_from_url_param
    else:
        sql = """
            SELECT * FROM "serverlessprez"."census_population"
            where population_2020 >= 200000
            LIMIT 10;
        """

    results_json = QueryAthena().execute_query(sql)

    return {
        'statusCode': 200,
        'body': json.dumps(results_json)
    }
