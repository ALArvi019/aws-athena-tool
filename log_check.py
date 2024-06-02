from tqdm import tqdm
from datetime import datetime
import botocore

class LogChecker:
    def __init__(self, session):
        self.session = session
        self.today = datetime.today()
        self.default_year = self.today.strftime("%Y")
        self.default_month = self.today.strftime("%m")
        self.default_day = self.today.strftime("%d")
        self.log_prefix = ''

    def get_elb_with_logs_enabled(self):
        elb_client = self.session.client('elbv2')
        load_balancers = elb_client.describe_load_balancers()
        log_enabled_elbs = {}

        total_lb = len(load_balancers['LoadBalancers'])
        with tqdm(total=total_lb, desc="Fetching ELBv2 load balancers") as pbar:
            for lb in load_balancers['LoadBalancers']:
                lb_name = lb.get('LoadBalancerName')
                attributes = elb_client.describe_load_balancer_attributes(LoadBalancerArn=lb['LoadBalancerArn'])
                for attr in attributes['Attributes']:
                    if attr['Key'] == 'access_logs.s3.enabled' and attr['Value'] == 'true':
                        for attr in attributes['Attributes']:
                            if attr['Key'] == 'access_logs.s3.bucket':
                                log_bucket_name = attr['Value']
                        log_enabled_elbs[lb_name] = log_bucket_name
                pbar.update(1)
        return log_enabled_elbs

    def get_cloudfront_with_logs_enabled(self):
        cf_client = self.session.client('cloudfront')
        distributions = cf_client.list_distributions()
        log_enabled_distributions = {}

        total_dist = len(distributions['DistributionList']['Items'])
        with tqdm(total=total_dist, desc="Fetching CloudFront distributions") as pbar:
            for dist in distributions['DistributionList']['Items']:
                dist_id = dist.get('Id')
                config = cf_client.get_distribution_config(Id=dist_id)
                if config['DistributionConfig']['Logging']['Enabled']:
                    log_enabled_distributions[dist_id] = config['DistributionConfig']['Logging']['Bucket'].split('.s3')[0]
                    self.log_prefix = config['DistributionConfig']['Logging']['Prefix']
                pbar.update(1)
        return log_enabled_distributions

    def check_cloudfront_logs_enabled(self, distribution_id):
        cf_client = self.session.client('cloudfront')
        try:
            distribution_config = cf_client.get_distribution_config(Id=distribution_id)
            if 'Logging' in distribution_config['DistributionConfig'] and distribution_config['DistributionConfig']['Logging']['Enabled']:
                self.log_prefix = distribution_config['DistributionConfig']['Logging']['Prefix']
                return distribution_config['DistributionConfig']['Logging']['Bucket'].split('.s3')[0]
            else:
                return False
        except Exception as e:
            print(f"Error checking CloudFront logs: {e}")
            return False

    def check_elbv2_logs_enabled(self, elb_arn):
        elb_client = self.session.client('elbv2')
        try:
            attributes = elb_client.describe_load_balancer_attributes(LoadBalancerArn=elb_arn)
            for attr in attributes['Attributes']:
                if attr['Key'] == 'access_logs.s3.enabled' and attr['Value'] == 'true':
                    for attr in attributes['Attributes']:
                        if attr['Key'] == 'access_logs.s3.bucket':
                            log_bucket_name = attr['Value']
                    return log_bucket_name
            return False
        except Exception as e:
            print(f"Error checking ELBv2 logs: {e}")
            return False
        
    def select_s3_folder(self, bucket_name, account_id, region):
        while True:
            year = input(f"Enter the year (default {self.default_year}): ") or self.default_year
            month = input(f"Enter the month (default {self.default_month}): ") or self.default_month
            day = input(f"Enter the day (default {self.default_day}): ") or self.default_day
            if year == "":
                        year = self.default_year
            if month == "":
                       month = self.default_month
            if day == "":
                        day = self.default_day

            if self.check_is_valid_date(year, month, day):
                s3_path = f"AWSLogs/{account_id}/elasticloadbalancing/{region}/{year}/{month}/{day}"
                if self.check_s3_path_exists(bucket_name, s3_path):
                    self.log_prefix = s3_path
                    break
                else:
                    print("No logs found for the specified date. Please enter a valid date.")
            else:
                print("Invalid input. Please enter a valid year, month, and day.")

    def check_s3_path_exists(self, bucket, path):
        s3_client = self.session.client('s3')
        try:
            path = path.rstrip('/') 
            s3_client.list_objects(Bucket=bucket, Prefix=path, Delimiter='/', MaxKeys=1)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                print(f"Error checking S3 path: {e}")
                return False
        
    def check_is_valid_date(self, year, month, day):
        try:
            year = int(year)
            month = int(month)
            day = int(day)
            if year < 1000 or year > 9999 or month < 1 or month > 12 or day < 1 or day > 31:
                return False
            return True
        except ValueError:
            return False