import boto3
import botocore
import subprocess
from logger import Logger

class AWSAuthenticator:
    def __init__(self, region):
        self.session = ''
        self.region = region
        self.account_id = ''
        self.logger = Logger()

    def get_aws_profiles(self):
        try:
            result = subprocess.run(['aws', 'configure', 'list-profiles'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if result.returncode != 0:
                raise Exception("AWS CLI returned an error")
            profiles = result.stdout.decode('utf-8').split()
            return profiles
        except Exception as e:
            self.logger.log(f"Error getting AWS profiles: {e}", "red")
            return ["default"]

    def authenticate_with_mfa(self, profile, region):
        self.session = boto3.Session(profile_name=profile, region_name=region)
        sts_client = self.session.client('sts')
        while True:
            try:
                sts_client.get_caller_identity()
                self.account_id = sts_client.get_caller_identity()['Account']
                break
            except botocore.exceptions.ParamValidationError as e:
                self.logger.log("Invalid MFA code. Make sure the MFA code is correct.", "red")
                continue
            except botocore.exceptions.ProfileNotFound as e:
                self.logger.log("Profile not found. Make sure the profile name is correct.", "red")
                continue
            except botocore.exceptions.RefreshWithMFAUnsupportedError as e:
                self.logger.log("MFA is required for this profile. Please provide MFA code.", "red")
                continue
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'AccessDenied':
                    try:
                        if not self.session.get_credentials().get_frozen_credentials():
                            mfa_serial = input("Enter MFA device serial number: ")
                            mfa_code = input("Enter MFA code: ")
                            mfa_token = input("Enter MFA token: ")
                            response = sts_client.get_session_token(DurationSeconds=3600, SerialNumber=mfa_serial, TokenCode=mfa_code)
                            self.session = boto3.Session(aws_access_key_id=response['Credentials']['AccessKeyId'],
                                                    aws_secret_access_key=response['Credentials']['SecretAccessKey'],
                                                    aws_session_token=response['Credentials']['SessionToken'],
                                                    region_name=region)
                            break
                    except botocore.exceptions.ClientError as e:
                        self.logger.log(f"Error authenticating with MFA: {e}", "red")
                        continue
                else:
                    self.logger.log(f"Error authenticating with MFA: {e}", "red")
                    continue

    def select_region(self, selected_profile, from_args=False):
        while True:
            self.logger.log(f"Actual region: {self.region}", "cyan")
            if not from_args:
                select_region = input("Do you want to change the region? (y/N): ")
            else:
                select_region = "n"
                
            if select_region == "y":
                # check if region is valid
                region = input("Enter the region: ")
                self.authenticate_with_mfa(selected_profile, region)
                try:
                    self.session.client('sts').get_caller_identity()
                    self.region = region
                    break
                except botocore.exceptions.ClientError as e:
                    self.logger.log(f"Error: {e}", "red")
                    continue
            elif select_region == "n" or select_region == "":
                self.authenticate_with_mfa(selected_profile, self.region)
                break
            else:
                self.logger.log("Invalid choice.", "red")
                continue
