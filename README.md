# AWS Athena Tool
The AWS Athena Tool is an application that allows you to run queries in Athena to analyze logs in ELB and Cloudfront.

## Prerequisites
- Docker
- AWS profiles installed to the `~/.aws` folder

## Installation
1. Clone the repository:
2. Ensure your AWS CLI is configured with the necessary AWS profiles. You can configure profiles using the command:
`aws configure --profile profile-name`

3. Run the `RUN.sh` script:
`./RUN.sh`

## File Structure
-  `main.py`: Main file to run the application.
-  `aws_auth.py`: Contains the `AWSAuthenticator` class for authentication.
-  `log_check.py`: Contains the `LogChecker` class for checking log enablement.
-  `athena_utils.py`: Contains the `AthenaManager` class for managing the database and tables in Athena.
-  `query_executor.py`: Contains the `QueryExecutor` class for executing queries in Athena.
-  `aws_log_analyzer.py`: Contains the `AWSLogAnalyzer` class that coordinates the whole process and manages dynamic queries.
-  `Dockerfile`: Defines the Docker environment to run the application.
-  `RUN.sh`: Script to build the Docker image and run the container.

## Usage
To run the application, simply execute the `RUN.sh` script:
`./RUN.sh`

### User Interface
1. Select the AWS profile to authenticate.
2. Select region (default eu-west-1).
3. Select ALBv2 or Cloudfront logs.
4. Select resource manually or scan in account profile.
5. The application will verify the enabled log resources in ELB and CloudFront.
6. Necessary databases and tables will be created in Athena.
7. You will be prompted to select a predefined query or enter a custom query.
8. If you select a predefined query, you may be asked to input additional values to replace placeholders in the query.
9. Query results will be displayed in the console and save in /tmp folder.

### Predefined Queries
The application comes with several predefined queries, from which you can select to analyze your logs. These queries include analysis of 4xx errors, counting client IPs, requested URLs, among others.
  
## Example Predefined Queries
1.  `SELECT * FROM "athena_database"."athena_table" LIMIT 20`
2.  `SELECT count(client_ip) AS retries, client_ip FROM "athena_database"."athena_table" WHERE target_status_code LIKE '4%' GROUP BY client_ip ORDER BY retries DESC;`
3.  `SELECT count(client_ip) AS retries, client_ip FROM "athena_database"."athena_table" GROUP BY client_ip ORDER BY retries DESC;`
4.  `SELECT count(client_ip) AS retries, client_ip FROM "athena_database"."athena_table" WHERE request_url LIKE '%endpoint%' GROUP BY client_ip ORDER BY retries DESC;`
5.  `SELECT count(request_url) AS retries, request_url FROM "athena_database"."athena_table" WHERE target_status_code LIKE '4%' GROUP BY request_url ORDER BY retries DESC;`
6.  `SELECT count(request_url) AS retries, client_ip, request_url FROM "athena_database"."athena_table" WHERE target_status_code LIKE '4%' GROUP BY request_url, client_ip ORDER BY retries DESC;`
7.  `SELECT count(request_url) AS retries, request_url FROM "athena_database"."athena_table" WHERE target_status_code LIKE '4%' AND time BETWEEN parse_datetime('start_time','yyyy-MM-dd-HH:mm:ss') AND parse_datetime('end_time','yyyy-MM-dd-HH:mm:ss') GROUP BY request_url ORDER BY retries DESC;`

## Contributions
Contributions are welcome. Please open an issue or send a pull request with your improvements or fixes.