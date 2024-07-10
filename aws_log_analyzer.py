import argparse
from aws_auth import AWSAuthenticator
from log_check import LogChecker
from athena_utils import AthenaManager
from query_executor import QueryExecutor
from logger import Logger
import re
import sys

class AWSLogAnalyzer:
    def __init__(self, profile=None, region='eu-west-1', log_type=None, resource_choice=None, selected_resource=None, start_time=None, end_time=None, endpoint=None, min_status_code=None, max_status_code=None):
        self.authenticator = AWSAuthenticator(region=region)
        self.logger = Logger()
        self.profile = profile
        self.region = region
        self.log_type = log_type
        self.resource_choice = resource_choice
        self.selected_resource = selected_resource
        self.start_time = start_time
        self.end_time = end_time
        self.endpoint = endpoint
        self.min_status_code = min_status_code
        self.max_status_code = max_status_code
        self.queries = {
            "elbv2": {
                "0": f"DONT CHANGE THIS",
                "1": f"DONT CHANGE THIS",
                "2": f"SELECT * FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" LIMIT 20",
                "3": f"SELECT count(client_ip) AS retries, client_ip FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE target_status_code LIKE '4%' GROUP BY client_ip ORDER BY retries DESC;",
                "4": f"SELECT count(client_ip) AS retries, client_ip FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" GROUP BY client_ip ORDER BY retries DESC;",
                "5": f"SELECT count(client_ip) AS retries, client_ip, request_url FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE request_url LIKE '%@@@endpoint@@@%' GROUP BY client_ip, request_url ORDER BY retries DESC;",
                "6": f"SELECT count(request_url) AS retries, request_url FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE target_status_code LIKE '4%' GROUP BY request_url ORDER BY retries DESC;",
                "7": f"SELECT count(request_url) AS retries, client_ip, request_url FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE target_status_code LIKE '4%' GROUP BY request_url, client_ip ORDER BY retries DESC;",
                "8": f"SELECT count(request_url) AS retries, request_url FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE target_status_code LIKE '4%' AND time BETWEEN parse_datetime('@@@start_time@@@','yyyy-MM-dd-HH:mm:ss') AND parse_datetime('@@@end_time@@@','yyyy-MM-dd-HH:mm:ss') GROUP BY request_url ORDER BY retries DESC;",
                "9": f"SELECT count(request_url) AS retries, request_url FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE time LIKE '@@@start_time@@@%' or time LIKE '@@@end_time@@@%' GROUP BY request_url ORDER BY retries DESC;",
            },
            "cloudfront": {
                "0": f"DONT CHANGE THIS",
                "1": f"DONT CHANGE THIS",
                "2": f"SELECT * FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" LIMIT 20",
                "3": f"SELECT SUM(cs_bytes) AS total_bytes FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE date BETWEEN DATE '@@@start_date@@@' AND DATE '@@@end_date@@@' LIMIT 100;",
                "4": f"SELECT DISTINCT * FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" LIMIT 10;",
                "5": f"SELECT count(c_ip) AS retries, c_ip FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE cs_uri_stem LIKE '%@@@endpoint@@@%' GROUP BY c_ip ORDER BY retries DESC;",
                "6": f"SELECT count(cs_uri_stem) AS retries, c_ip, cs_uri_stem FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE sc_status > @@@min_status_code@@@ AND sc_status < @@@max_status_code@@@ GROUP BY cs_uri_stem, c_ip ORDER BY retries DESC;",
                "7": f"SELECT count(cs_uri_stem) AS retries, cs_uri_stem FROM \"@@@athena_database@@@\".\"@@@athena_table@@@\" WHERE sc_status > @@@min_status_code@@@ AND sc_status < @@@max_status_code@@@ AND time BETWEEN parse_datetime('@@@start_time@@@','yyyy-MM-dd-HH:mm:ss') AND parse_datetime('@@@end_time@@@','yyyy-MM-dd-HH:mm:ss') GROUP BY cs_uri_stem ORDER BY retries DESC;"
            }
        }

    def replace_placeholders(self, query):
        # Verifica que query sea una cadena
        if not isinstance(query, str):
            raise ValueError("The query must be a string.")
        
        # Encuentra todos los placeholders en la consulta
        placeholders = re.findall(r'@@@(\w+)@@@', query)
        for placeholder in placeholders:
            attribute_name = placeholder
            # Comprueba si la instancia tiene el atributo
            if hasattr(self, attribute_name) and getattr(self, attribute_name) is not None:
                value = getattr(self, attribute_name)
            else:
                # Si no tiene el atributo, solicita al usuario que ingrese el valor
                value = input(f"Enter value for {attribute_name}: ")
            # Reemplaza el placeholder en la consulta con el valor correspondiente
            query = query.replace(f"@@@{attribute_name}@@@", value)
        return query

    def setup(self, selected_bucket, log_checker):
        if self.log_type == 'elbv2':
            log_checker.select_s3_folder(selected_bucket, self.authenticator.account_id, self.authenticator.region)
        self.log_prefix = log_checker.log_prefix
        self.athena_manager.log_prefix = self.log_prefix
        self.athena_manager.create_athena_database()
        self.athena_database = self.athena_manager.athena_database
        self.athena_manager.athena_bucket = selected_bucket
        self.athena_manager.create_athena_table(selected_bucket, self.selected_resource, self.log_type)
        self.athena_table = self.athena_manager.athena_table
        self.query_executor = QueryExecutor(self.authenticator.session, self.athena_manager.athena_database, self.athena_manager.athena_table, self.log_type, self.athena_manager.athena_bucket)

    def run_interactive(self):
        profiles = self.authenticator.get_aws_profiles()
        if not profiles:
            self.logger.log("No AWS profiles found. Please configure AWS CLI profiles.", "red")
            return
        selected_profile = self.select_aws_profile(profiles)
        if not selected_profile:
            return

        self.authenticator.select_region(selected_profile)
        self.athena_manager = AthenaManager(self.authenticator.session)
        self.athena_manager.athena_bucket = f"{self.authenticator.account_id}-athena-{self.authenticator.region}"
        log_checker = LogChecker(self.authenticator.session)

        self.log_type = self.select_log_type()
        if not self.log_type:
            return

        selected_bucket = self.select_log_bucket(log_checker)
        if not selected_bucket:
            return

        self.setup(selected_bucket, log_checker)
        self.query_choice()
    
    def run_with_arguments(self):
        profiles = self.authenticator.get_aws_profiles()
        if self.profile not in profiles:
            self.logger.log("Profile not found. Make sure the profile name is correct.", "red")
            self.logger.log("Actual profiles:", "cyan")
            for profile in profiles:
                self.logger.log(profile)
            return
        self.logger.log(f"Actual profile: {self.profile}", "cyan")
        self.logger.log(f"Actual resource: {self.selected_resource}", "cyan")
        self.authenticator.select_region(self.profile, True)
        self.athena_manager = AthenaManager(self.authenticator.session)
        self.athena_manager.athena_bucket = f"{self.authenticator.account_id}-athena-{self.authenticator.region}"
        log_checker = LogChecker(self.authenticator.session)

        selected_bucket = self.select_manual_resource(log_checker, self.selected_resource)

        if not selected_bucket:
            self.logger.log("No log bucket found.", "red")
            return

        self.setup(selected_bucket, log_checker)
        self.query_choice()


    def select_aws_profile(self, profiles):
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
            self.logger.log("You have selected the following profile:")
            self.logger.log(selected_profile, "green")
            return selected_profile

    def select_log_type(self):
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
            return 'cloudfront' if log_type_index == 1 else 'elbv2'

    def select_log_bucket(self, log_checker):
        while True:
            self.logger.log("Do you want to enter the resource name manually or scan automatically?", "cyan")
            self.logger.log("1. Enter manually")
            self.logger.log("2. Scan automatically All resources")
            resource_choice = input("Enter the number corresponding to your choice: ")
            if resource_choice not in ["1", "2"]:
                self.logger.log("Invalid choice.", "red")
                continue

            if resource_choice == "1":
                return self.select_manual_resource(log_checker)
            elif resource_choice == "2":
                return self.select_auto_resource(log_checker)

    def select_manual_resource(self, log_checker, selected_resource=None):
        if self.log_type == 'cloudfront':
            if not selected_resource:
                selected_resource = input("Enter the CloudFront distribution ID: ")
                self.selected_resource = selected_resource
            response = log_checker.check_cloudfront_logs_enabled(selected_resource)
            if not response:
                self.logger.log(f"Logs are not enabled for the specified CloudFront distribution. {selected_resource}", "red")
                return None
            else:
                selected_bucket = response
                self.logger.log(f"Logs are enabled for the specified CloudFront distribution: {selected_resource}", "green")
                return selected_bucket
        elif self.log_type == 'elbv2':
            if not selected_resource:
                selected_resource = input("Enter the ELB ARN: ")
                self.selected_resource = selected_resource
            response = log_checker.check_elbv2_logs_enabled(selected_resource)
            if not response:
                self.logger.log(f"Logs are not enabled for the specified ALB. {selected_resource}", "red")
                return None
            else:
                selected_bucket = response
                self.logger.log(f"Logs are enabled for the specified ALB: {selected_resource}", "green")
                return selected_bucket

    def select_auto_resource(self, log_checker):
        if self.log_type == 'cloudfront':
            log_buckets = log_checker.get_cloudfront_with_logs_enabled()
        elif self.log_type == 'elbv2':
            log_buckets = log_checker.get_elb_with_logs_enabled()

        if not log_buckets:
            self.logger.log(f"No log buckets found in the account for {self.log_type}.", "red")
            return None

        self.logger.log("Select log bucket:", "cyan")
        for i, bucket in enumerate(log_buckets.keys()):
            self.logger.log(f"{i + 1}. {bucket}")
        selected_resource_index = int(input("Enter the number corresponding to the resource: ")) - 1
        selected_resource = list(log_buckets.keys())[selected_resource_index]
        selected_bucket = log_buckets[selected_resource]
        return selected_bucket

    def query_choice(self):
        while True:
            self.logger.log("Select a query or enter 0 to exit:", "cyan")

            if self.log_type == 'elbv2':
                self.logger.log("0. Exit and delete Athena table")
            else:
                self.logger.log("0. Exit")
            self.logger.log("1. Enter a custom query")
            for i, query in self.queries[self.log_type].items():
                if i == "0" or i == "1":
                    continue
                self.logger.log(f"{i}. {query}")

            query_choice = input("Enter the number corresponding to the query: ")

            if not query_choice.isdigit():
                self.logger.log("Invalid choice: not a digit.", "red")
                continue

            if query_choice not in map(str, range(len(self.queries[self.log_type]))):
                self.logger.log("Invalid choice: not in available queries.", "red")
                continue

            if query_choice == "0":
                if self.log_type == 'elbv2':
                    self.athena_manager.delete_athena_table()
                break
            elif query_choice == "1":
                query = input("Enter the query: ")
            elif query_choice in self.queries[self.log_type].keys():
                query = self.queries[self.log_type][query_choice]

            if "@@@" in query:
                query = self.replace_placeholders(query)

            query_execution_id = self.query_executor.execute_athena_query(query)
            if query_execution_id:
                df = self.query_executor.get_query_results(query_execution_id)
                self.query_executor.display_results(df)

    def main(self):
        if len(sys.argv) > 1 and sys.argv[1] == "wizard":
            self.run_interactive()
        else:
            parser = argparse.ArgumentParser(description="AWS-Athena-Tool")
            parser.add_argument("--profile", required=True, help="AWS CLI profile name")
            parser.add_argument("--region", required=True, help="AWS region")
            parser.add_argument("--log_type", required=True, choices=["cloudfront", "elbv2"], help="Log type")
            parser.add_argument("--selected_resource", required=True, help="Cloudfront distribution ID or LB ARN")

            try:
                args = parser.parse_args()
            except SystemExit as e:
                if e.code != 0:
                    parser.print_help()
                sys.exit(e.code)

            self.profile = args.profile
            self.region = args.region
            self.log_type = args.log_type
            self.selected_resource = args.selected_resource

            self.run_with_arguments()

