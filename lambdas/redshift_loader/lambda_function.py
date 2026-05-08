import boto3
import os
import json

redshift_client = boto3.client('redshift-data')

DATABASE = os.environ['REDSHIFT_DATABASE']
WORKGROUP = os.environ['REDSHIFT_WORKGROUP']
SCHEMA = os.environ['REDSHIFT_SCHEMA']
IAM_ROLE = os.environ['IAM_ROLE']

def run_copy(table_name, s3_path):
    sql = f"""
        COPY {SCHEMA}.{table_name}
        FROM '{s3_path}'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET;
    """

    response = redshift_client.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        Sql=sql
    )

    return response


def lambda_handler(event, context):
    try:
        print("Event received:", json.dumps(event))

        table_name = event['table_name']
        s3_path = event['s3_path']

        response = run_copy(table_name, s3_path)

        return {
            "status": "SUCCESS",
            "query_id": response['Id']
        }

    except Exception as e:
        print("Error:", str(e))
        return {
            "status": "FAILED",
            "error": str(e)
        }