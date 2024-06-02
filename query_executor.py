from tqdm import tqdm
import pandas as pd
from time import sleep
from tabulate import tabulate
import pydoc
from datetime import datetime
import re

class QueryExecutor:
    def __init__(self, session, athena_database, athena_table, log_type, athena_bucket):
        self.session = session
        self.athena_database = athena_database
        self.athena_table = athena_table
        self.log_type = log_type
        self.athena_bucket = athena_bucket

    def execute_athena_query(self, query):
        print(f"\033[92m{query}\033[00m")
        athena_client = self.session.client('athena')
        query_execution = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': self.athena_database},
            ResultConfiguration={'OutputLocation': f"s3://{self.athena_bucket}/"}
        )
        query_execution_id = query_execution['QueryExecutionId']
        with tqdm(total=100, desc="Running Athena query") as pbar:
            while True:
                query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                query_state = query_status['QueryExecution']['Status']['State']
                if query_state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                sleep(1)
                pbar.update(1)
        if query_state == 'SUCCEEDED':
            print("Athena query executed successfully.")
            return query_execution_id
        else:
            print(f"Athena query failed with state: {query_state}")
            return None

    def get_query_results(self, query_execution_id):
        athena_client = self.session.client('athena')
        paginator = athena_client.get_paginator('get_query_results')
        result_data = []
        for results in paginator.paginate(QueryExecutionId=query_execution_id):
            result_data.extend(results['ResultSet']['Rows'])
        rows = []
        for row in result_data:
            row_data = []
            for col in row['Data']:
                if 'VarCharValue' in col:
                    row_data.append(col['VarCharValue'])
                else:
                    row_data.append('')
            rows.append(row_data)
        if len(rows) > 0:
            df = pd.DataFrame(rows[1:], columns=rows[0])
        else:
            df = pd.DataFrame()  
        return df

    def display_results(self, df):
        output = tabulate(df, headers='keys', tablefmt='mysql')
        safe_athena_database = re.sub(r'[^a-zA-Z0-9]', '_', self.athena_database)[:20]
        safe_athena_table = re.sub(r'[^a-zA-Z0-9]', '_', self.athena_table)[:20]
        safe_log_type = re.sub(r'[^a-zA-Z0-9]', '_', self.log_type)[:20]
        filename = f"/tmp/{datetime.now().strftime('%Y%m%d%H%M%S')}-aws-athena-tool-{safe_athena_database}-{safe_athena_table}-{safe_log_type}.txt"
        with open(filename, 'w') as f:
            f.write(output)
            print(f"\033[93mAthena query results saved in {filename}\033[00m")
        pydoc.pager(output)
