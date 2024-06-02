from datetime import datetime

class AthenaManager:
    def __init__(self, session, athena_database='aws-athena-tool', athena_table='aws-athena-tool'):
        self.session = session
        self.athena_database = athena_database
        self.today = datetime.today()
        self.athena_table = athena_table + f"_{self.today.strftime('%Y%m%d')}"
        self.athena_bucket = ''
        self.log_prefix = ''

    def create_athena_database(self):
        print("Creating Athena database...")
        athena_client = self.session.client('athena')
        try:
            athena_client.start_query_execution(QueryString=f"CREATE DATABASE IF NOT EXISTS `{self.athena_database}`",
                                                ResultConfiguration={'OutputLocation': f"s3://{self.athena_bucket}/"})
            print(f"\033[92mAthena database {self.athena_database} created successfully.\033[0m")
        except Exception as e:
            print(f"Error creating Athena database: {e}")

    def create_athena_table(self, bucket_name, selected_resource, log_type):
        print("Creating Athena table...")
        athena_client = self.session.client('athena')
        try:
            if log_type == 'cloudfront':
                self.athena_table = "cloudfront_logs"
                s3_full_path = f"s3://{bucket_name}/"
                query = f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS `{self.athena_database}`.`cloudfront_logs` (
                    `date` DATE,
                    time STRING,
                    x_edge_location STRING,
                    sc_bytes BIGINT,
                    c_ip STRING,
                    cs_method STRING,
                    cs_host STRING,
                    cs_uri_stem STRING,
                    sc_status INT,
                    cs_referrer STRING,
                    cs_user_agent STRING,
                    cs_uri_query STRING,
                    cs_cookie STRING,
                    x_edge_result_type STRING,
                    x_edge_request_id STRING,
                    x_host_header STRING,
                    cs_protocol STRING,
                    cs_bytes BIGINT,
                    time_taken FLOAT,
                    x_forwarded_for STRING,
                    ssl_protocol STRING,
                    ssl_cipher STRING,
                    x_edge_response_result_type STRING,
                    cs_protocol_version STRING,
                    fle_status STRING,
                    fle_encrypted_fields INT,
                    c_port INT,
                    time_to_first_byte FLOAT,
                    x_edge_detailed_result_type STRING,
                    sc_content_type STRING,
                    sc_content_len BIGINT,
                    sc_range_start BIGINT,
                    sc_range_end BIGINT
                    )
                    ROW FORMAT DELIMITED 
                    FIELDS TERMINATED BY '\\t'
                    LOCATION '{s3_full_path}'
                    TBLPROPERTIES ( 'skip.header.line.count'='2' )
                    """
            elif log_type == 'elbv2':
                self.athena_table = f"{self.athena_table}_{selected_resource}"
                s3_full_path = f"s3://{bucket_name}/{self.log_prefix}/"
                query = f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS `{self.athena_database}`.`{self.athena_table}` (
                    type string,
                    time string,
                    elb string,
                    client_ip string,
                    client_port int,
                    target_ip string,
                    target_port int,
                    request_processing_time double,
                    target_processing_time double,
                    response_processing_time double,
                    elb_status_code int,
                    target_status_code string,
                    received_bytes bigint,
                    sent_bytes bigint,
                    request_verb string,
                    request_url string,
                    request_proto string,
                    user_agent string,
                    ssl_cipher string,
                    ssl_protocol string,
                    target_group_arn string,
                    trace_id string,
                    domain_name string,
                    chosen_cert_arn string,
                    matched_rule_priority string,
                    request_creation_time string,
                    actions_executed string,
                    redirect_url string,
                    lambda_error_reason string,
                    target_port_list string,
                    target_status_code_list string,
                    classification string,
                    classification_reason string,
                    traceability_id string
                    )
                    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
                    WITH SERDEPROPERTIES (
                    'serialization.format' = '1',
                    'input.regex' = 
                '([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) (.*) (- |[^ ]*)\" \"([^\"]*)\" ([A-Z0-9-_]+) ([A-Za-z0-9.-]*) ([^ ]*) \"([^\"]*)\" \"([^\"]*)\" \"([^\"]*)\" ([-.0-9]*) ([^ ]*) \"([^\"]*)\" \"([^\"]*)\" \"([^ ]*)\" \"([^\s]+?)\" \"([^\s]+)\" \"([^ ]*)\" \"([^ ]*)\" ?([^ ]*)?( .*)?')
                    LOCATION '{s3_full_path}';
                """

            athena_client.start_query_execution(QueryString=query,
                                                ResultConfiguration={'OutputLocation': f"s3://{self.athena_bucket}/"})
            if log_type == 'cloudfront':
                print(f"\033[92mAthena table cloudfront_logs created successfully.\033[0m")
            elif log_type == 'elbv2':
                print(f"\033[92mAthena table {self.athena_table} created successfully.\033[0m")
        except Exception as e:
            print(f"Error creating Athena table: {e}")

    def delete_athena_table(self):
        print("Deleting Athena table...")
        athena_client = self.session.client('athena')
        try:
            athena_client.start_query_execution(QueryString=f"DROP TABLE IF EXISTS `{self.athena_database}`.`{self.athena_table}`",
                                                ResultConfiguration={'OutputLocation': f"s3://{self.athena_bucket}/"})
            print(f"\033[92mAthena table {self.athena_table} deleted successfully.\033[0m")
        except Exception as e:
            print(f"Error deleting Athena table: {e}")