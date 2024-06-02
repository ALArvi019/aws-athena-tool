from aws_auth import AWSAuthenticator
from log_check import LogChecker
from athena_utils import AthenaManager
from query_executor import QueryExecutor
from logger import Logger
import re

class AWSLogAnalyzer:
    def __init__(self):
        self.authenticator = AWSAuthenticator(region='eu-west-1')
        self.athena_database = ''
        self.athena_table = ''
        self.logger = Logger()
        
        self.queries = {
            "elbv2" : {   
                "2": f"SELECT * FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" LIMIT 20",
                "3": f"SELECT count(client_ip) AS retries, client_ip FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE target_status_code LIKE '4%' GROUP BY client_ip ORDER BY retries DESC;",
                "4": f"SELECT count(client_ip) AS retries, client_ip FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" GROUP BY client_ip ORDER BY retries DESC;",
                "5": f"SELECT count(client_ip) AS retries, client_ip FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE request_url LIKE '%@@@endpoint@@@%' GROUP BY client_ip ORDER BY retries DESC;",
                "6": f"SELECT count(request_url) AS retries, request_url FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE target_status_code LIKE '4%' GROUP BY request_url ORDER BY retries DESC;",
                "7": f"SELECT count(request_url) AS retries, client_ip, request_url FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE target_status_code LIKE '4%' GROUP BY request_url, client_ip ORDER BY retries DESC;",
                "8": f"SELECT count(request_url) AS retries, request_url FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE target_status_code LIKE '4%' AND time BETWEEN parse_datetime('@@@start_time@@@','yyyy-MM-dd-HH:mm:ss') AND parse_datetime('@@@end_time@@@','yyyy-MM-dd-HH:mm:ss') GROUP BY request_url ORDER BY retries DESC;",
            },
            "cloudfront": {
                "2": f"SELECT * FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" LIMIT 20",
                "3": f"SELECT SUM(cs_bytes) AS total_bytes FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE date BETWEEN DATE '@@@start_date@@@' AND DATE '@@@end_date@@@' LIMIT 100;",
                "4": f"SELECT DISTINCT * FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" LIMIT 10;",
                "5": f"SELECT count(c_ip) AS retries, c_ip FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE cs_uri_stem LIKE '%@@@endpoint@@@%' GROUP BY c_ip ORDER BY retries DESC;",
                "6": f"SELECT count(cs_uri_stem) AS retries, c_ip, cs_uri_stem FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE sc_status > @@@min_status_code@@@ AND sc_status < @@@max_status_code@@@ GROUP BY cs_uri_stem, c_ip ORDER BY retries DESC;",
                "7": f"SELECT count(cs_uri_stem) AS retries, cs_uri_stem FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE sc_status > @@@min_status_code@@@ AND sc_status < @@@max_status_code@@@ AND time BETWEEN parse_datetime('@@@start_time@@@','yyyy-MM-dd-HH:mm:ss') AND parse_datetime('@@@end_time@@@','yyyy-MM-dd-HH:mm:ss') GROUP BY cs_uri_stem ORDER BY retries DESC;"
            }
        }

    def replace_placeholders(self, query):
        placeholders = re.findall(r'@@@(\w+)@@@', query)
        for placeholder in placeholders:
            attribute_name = placeholder
            if hasattr(self, attribute_name):
                value = getattr(self, attribute_name)
            else:
                value = input(f"Enter value for {attribute_name}: ")
            query = query.replace(f"@@@{attribute_name}@@@", value)
        return query
    

    def main(self):
        profiles = self.authenticator.get_aws_profiles()
        if not profiles:
            self.logger.log("No AWS profiles found. Please configure AWS CLI profiles.", "red")
            return
        while True:
            self.logger.log("Select AWS Profile:", "cyan")
            for i, profile in enumerate(profiles):
                self.logger.log(f"{i + 1}. {profile}")
            selected_profile_index = input("Enter the number corresponding to the profile: ")
            if not selected_profile_index.isdigit():
                self.logger.log("Invalid profile index.", "red")
                continue
            selected_profile_index = int(selected_profile_index) - 1
            if selected_profile_index < 0 or selected_profile_index >= len(profiles):
                self.logger.log("Invalid profile index.", "red")
                continue
            selected_profile = profiles[selected_profile_index]
            break

        
        self.logger.log("You have selected the following profile:")
        self.logger.log(selected_profile, "green")

        self.authenticator.select_region(selected_profile)

        athena_manager = AthenaManager(self.authenticator.session)
        athena_manager.athena_bucket = f"{self.authenticator.account_id}-athena-{self.authenticator.region}"
        log_checker = LogChecker(self.authenticator.session)


        while True:
            self.logger.log("Select log type:", "cyan")
            self.logger.log("1. CloudFront")
            self.logger.log("2. ELBv2")
            log_type_index = input("Enter the number corresponding to the log type: ")
            try:
                log_type_index = int(log_type_index)
            except ValueError:
                self.logger.log("Invalid log type", "red")
                continue
            if log_type_index not in [1, 2]:
                self.logger.log("Invalid log type", "red")
                continue
            log_type = 'cloudfront' if log_type_index == 1 else 'elbv2'
            break

        while True:
            self.logger.log("Do you want to enter the resource name manually or scan automatically?", "cyan")
            self.logger.log("1. Enter manually")
            self.logger.log("2. Scan automatically All resources")
            resource_choice = input("Enter the number corresponding to your choice: ")
            if resource_choice not in ["1", "2"]:
                self.logger.log("Invalid choice.", "red")
                continue
            break

        while True:
            if resource_choice == "1":
                if log_type == 'cloudfront':
                    selected_resource = input("Enter the CloudFront distribution ID: ")
                    response = log_checker.check_cloudfront_logs_enabled(selected_resource)
                    if not response:
                        self.logger.log(f"Logs are not enabled for the specified CloudFront distribution. {selected_resource}", "red")
                        continue
                    else:
                        selected_bucket = response
                        self.logger.log("Logs are enabled for the specified CloudFront distribution.", "green")
                        break
                elif log_type == 'elbv2':
                    selected_resource = input("Enter the ELB ARN: ")
                    response = log_checker.check_elbv2_logs_enabled(selected_resource)
                    if not response:
                        self.logger.log(f"Logs are not enabled for the specified ALB. {selected_resource}", "red")
                        continue
                    else:
                        selected_bucket = response
                        self.logger.log("Logs are enabled for the specified ALB.", "green")
                        break
            elif resource_choice == "2":
                if log_type == 'cloudfront':
                    log_buckets = log_checker.get_cloudfront_with_logs_enabled()
                elif log_type == 'elbv2':
                    log_buckets = log_checker.get_elb_with_logs_enabled()
                
                if not log_buckets:
                    self.logger.log(f"No log buckets found in the account for {log_type}.", "red")
                    return

                self.logger.log("Select log bucket:", "cyan")
                for i, bucket in enumerate(log_buckets.keys()):
                    self.logger.log(f"{i + 1}. {bucket}")
                selected_resource_index = int(input("Enter the number corresponding to the resource: ")) - 1
                selected_resource = list(log_buckets.keys())[selected_resource_index]
                selected_bucket = log_buckets[selected_resource]
                break
            else:
                self.logger.log("Invalid choice.", "red")
                continue
        
        if not selected_bucket:
            self.logger.log("No log bucket found.", "red")
            return

        if log_type == 'elbv2':
            log_checker.select_s3_folder(selected_bucket, self.authenticator.account_id, self.authenticator.region)
        
        self.log_prefix = log_checker.log_prefix
        athena_manager.log_prefix = self.log_prefix    
        athena_manager.create_athena_database()
        self.athena_database = athena_manager.athena_database
        athena_manager.athena_bucket = selected_bucket
        athena_manager.create_athena_table(selected_bucket, selected_resource, log_type)
        self.athena_table = athena_manager.athena_table

        query_executor = QueryExecutor(self.authenticator.session, athena_manager.athena_database, athena_manager.athena_table, log_type, athena_manager.athena_bucket)

        while True:
            self.logger.log("Select a query or enter 0 to exit:", "cyan")
            
            if log_type == 'elbv2':
                self.logger.log("0. Exit and delete Athena table")
            else:
                self.logger.log("0. Exit")
            self.logger.log("1. Enter a custom query")
            for i, query in self.queries[log_type].items():
                self.logger.log(f"{i}. {query}")

            query_choice = input("Enter the number corresponding to the query: ")

            if query_choice == "0":
                if log_type == 'elbv2':
                    athena_manager.delete_athena_table()
                break
            elif query_choice == "1":
                query = input("Enter the query: ")
            elif query_choice in self.queries[log_type].keys():
                query = self.queries[log_type][query_choice]

            if "@@@" in query:
                query = self.replace_placeholders(query)

            query_execution_id = query_executor.execute_athena_query(query)
            if query_execution_id:
                df = query_executor.get_query_results(query_execution_id)
                query_executor.display_results(df)

