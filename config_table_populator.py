import json
import boto3
import time
from datetime import datetime
import dateutil.tz
import logging
import urllib3
MAX_RETRIES = 3
WAIT_TIME_SECONDS = 10

logger = logging.getLogger()
logger.setLevel(logging.INFO)

http = urllib3.PoolManager()

def send_lambda_failure_notification(function_name, error_message):
    try:
        print('In failure notification definition using webhook...')
        webhook_url = "https://libertyholdings.webhook.office.com/webhookb2/84ae269b-3328-4244-9c58-87c0541029c0@66b8ffa6-81c0-4ea6-93fb-06f390dc67f6/IncomingWebhook/1c7314d5bffd4561aa4d694641deb2f3/540ec8e7-b8ad-4612-9c4a-140cef8ece9c/V2D2XxrsBWC76nparKmE63SCKVqIts7TmneUOC8PDMJOk1"
        payload = {
            "title": f"Lambda Failure Notification : {function_name}",
            "text": (
                f"Lambda function **'{function_name}'** encountered an error.\n\n"
                f"**Error Details:**\n{error_message}\n\n"
                f"**Timestamp:** {datetime.utcnow().isoformat()}Z\n\n"
                f"Please investigate the issue and check CloudWatch logs for more details."
            )
        }
        encoded_payload = json.dumps(payload).encode('utf-8')
        # Send POST request to the Webhook URL
        
        response = http.request('POST', webhook_url, body=encoded_payload, headers={'Content-Type': 'application/json'})

        # Log success or failure
        if response.status == 200:
            print(f"Webhook notification sent successfully for Lambda function: {function_name}")
        else:
            print(f"Failed to send Webhook notification. Response: {response.status}, {response.text}")

    except Exception as webhook_error:
        print(f"Failed to send failure notification via Webhook: {webhook_error}")

def get_account_id():
    """
    Retrieve the AWS account ID using the STS client. 
    Returns the AWS account ID
    """
    client = boto3.client("sts")
    return client.get_caller_identity()["Account"]

env_account_mapping = {
    "non-prod": "649505956583",
    "pre-prod": "681131072283",
    "prod": "014390686996"
}

account_id = get_account_id()
print(f"The account_id found is: {account_id}")

current_env = None
for env, account in env_account_mapping.items():
    if account == account_id and not env.endswith("-full-load"):
        current_env = env
        break

if current_env is None:
    raise Exception("The environment does not exist for the current AWS account. Please check the logs.")

def default_global_ingest_config(global_ingest_config):
    """
    Set default values for global ingestion configuration.
    Args:
        global_ingest_config: Global ingestion configuration.
    """
    global_ingest_config.setdefault('truncate_table_flag', 'Y')
    global_ingest_config.setdefault('drop_table_flag', 'N')
    global_ingest_config.setdefault('soft_rule_template_name', '') # Empty string, not being used at the moment
    global_ingest_config.setdefault('target_database', 'changeaudit')
    global_ingest_config.setdefault('source_file_bucket', '')

def default_global_active_table_config(global_active_table_config, source_name):
    """
    Set default values for global active table configuration.
    Args:
        global_active_table_config: Global active table configuration
        source_name: Name of the data source.
    """
    global_active_table_config.setdefault('src_system_name', source_name)
    global_active_table_config.setdefault('tgt_database_name', f"{source_name}_main")
    global_active_table_config.setdefault('src_database_name', 'changeaudit')
    global_active_table_config.setdefault('contract_sync_flag', 'Y')

def default_table_config(table):
    """
    Set default values for table configuration.
    Args: 
        table: Table configuration.
    """
    # Set default values for active config
    if 'active_table_config' in table:
        table_name = table['table_name']
        table['active_table_config'].setdefault('tgt_table_name', f"a_{table_name}")
        table['active_table_config'].setdefault('src_table_name', table_name)
        table['active_table_config'].setdefault('group_number', '1')
        table['active_table_config'].setdefault('incremental_column_name', '')
        table['active_table_config'].setdefault('change_audit_flag', 'N')

    # Set default values for ingestion config
    table['ingestion_config'].setdefault('source_file_date_format', 'YYYYMMDD')
    table['ingestion_config'].setdefault('source_file_type', 'csv')
    table['ingestion_config'].setdefault('source_file_extension', '.csv')
    table['ingestion_config'].setdefault('source_file_delimiter', ';')
    table['ingestion_config'].setdefault('source_file_header_row_exist', 'true')
    table['ingestion_config'].setdefault('source_file_header_file_exist', 'N')
    table['ingestion_config'].setdefault('enabled_flag', 'Y')

def generate_statements(env_config, global_ingest_config, global_active_table_config, env_name, source_name):
    """
    Generate DELETE and INSERT statements for tables based on environment configuration.
    Args:
        env_config (dict): Environment configuration.
        global_ingest_config (dict): Global ingestion configuration.
        global_active_table_config (dict): Global active table configuration.
        env_name (str): Name of the environment.
        source_name (str): Name of the data source.
    Returns:
        tuple: A tuple containing the delete and insert statements.
    """
    delete_statements = []
    generic_file_loads_insert_statements = []
    active_table_insert_statements = []

    delete_edp_query = f"DELETE FROM data_control.edp_generic_file_loads WHERE source_system_name = '{source_name}';"
    delete_statements.append(delete_edp_query)
    
    active_table_config_present = 'global_active_table_config' in env_config and any('active_table_config' in table for table in env_config['tables'])
    
    if active_table_config_present:
        delete_active_table_query = f"DELETE FROM data_control.active_table_job_config_attributes_iceberg WHERE src_system_name = '{source_name}';"
        delete_statements.append(delete_active_table_query)

    for table in env_config['tables']:
        default_table_config(table)
        ingest_config = table['ingestion_config']
        source_file_location = ingest_config['source_file_location']
        source_file_bucket = global_ingest_config.get('source_file_bucket', '')
        if source_file_location.startswith("s3://"):
             full_source_file_location = source_file_location
        elif source_file_bucket:
             full_source_file_location = f"s3://{source_file_bucket}/{source_file_location.lstrip('/')}"
        else:
             full_source_file_location = f"{source_file_location}"

        table_name = table['table_name']
        
        localtime = dateutil.tz.gettz('Africa/Johannesburg')
        insert_datetime = datetime.now(tz=localtime).strftime('%Y-%m-%d-%H-%M')
        target_s3_bucket = global_ingest_config.get('target_s3_location', None)
        if target_s3_bucket:
            tgt_location = f"s3://{target_s3_bucket}/active/${{source_system_name}}/${{tgt_database_name}}/${{tgt_table_name}}/"
        else:
            tgt_location = "s3://ct-ire-edp-${env-name}-${system-area}/${db_prefix}/${source_system_name}/${tgt_database_name}/${tgt_table_name}/"
        
        column_names_str = ingest_config['source_file_column_names'][0] if ingest_config['source_file_column_names'] else ''
        
        if current_env == "prod":
            changeaudit_env = "prd"
        elif current_env == "non-prod":
            changeaudit_env = "dev"
        else:
            changeaudit_env = "pre-prod"
        if 'target_s3_location' in global_ingest_config and global_ingest_config['target_s3_location']:
            custom_bucket = global_ingest_config['target_s3_location']
            target_s3_location = f"s3://{custom_bucket}/changeaudit/{source_name}/{table_name}"
        else:
            target_s3_location = f"s3://ct-ire-edp-{changeaudit_env}-datastaging-op/changeaudit/{source_name}/{table_name}"
        
        # New optional fields
        control_file_header_row_exist = ingest_config.get('control_file_header_row_exist', None)
        control_file_columns_names = ingest_config.get('control_file_columns_names', None)
        generic_file_loads_insert = (
            f"SELECT "
            f"'{table['table_name']}' AS source_file_name_pk, "
            f"'{source_name}' AS source_system_name, "
            f"'{ingest_config['source_file_type']}' AS source_file_type, "
            f"'{full_source_file_location}' AS source_file_location, "
            f"'{ingest_config['source_file_name_wild_card']}' AS source_file_name_wild_card, "
            f"'{ingest_config['source_file_date_format']}' AS source_file_date_format, "
            f"'{ingest_config['source_file_extension']}' AS source_file_extension, "
            f"'{ingest_config['source_file_delimiter']}' AS source_file_delimiter, "
            f"'{ingest_config['source_file_header_row_exist']}' AS source_file_header_row_exist, "
            f"'{ingest_config['source_file_header_file_exist']}' AS source_file_header_file_exist, "
            f"'{column_names_str}' AS source_file_column_names, "
            f"'{ingest_config['source_file_unique_key_cols']}' AS source_file_unique_key_cols, "
            f"'{ingest_config['load_frequency']}' AS load_frequency, "
            f"'{global_ingest_config['target_database']}' AS target_database, "
            f"'{source_name}_{table['table_name']}' AS target_table_name, "
            f"'{target_s3_location}' AS target_s3_location, "
            f"'{global_ingest_config['truncate_table_flag']}' AS truncate_table_flag, "
            f"'{global_ingest_config['drop_table_flag']}' AS drop_table_flag, "
            f"'{ingest_config['enabled_flag']}' AS enabled_flag, "
            f"'{ingest_config['partition_columns']}' AS partition_columns, "
            f"'{ingest_config.get('job_template_name', ' ')}' AS job_template_name, "
            f"'{global_ingest_config['soft_rule_template_name']}' AS soft_rule_template_name, "
            f"'{ingest_config.get('source_file_control_footer_exist', '')}' AS source_file_control_footer_exist, "
            f"'{env_name}' AS environment, "
            f"'{ingest_config.get('control_file_ind', 'N')}' AS control_file_ind, "
            f"'{insert_datetime}' AS insert_datetime, "
            f"'{ingest_config['worker_type']}' AS worker_type, "
            f"{ingest_config['worker_num']} AS worker_num, "
            f"{'NULL' if control_file_header_row_exist is None else repr(control_file_header_row_exist)} AS control_file_header_row_exist, "
            f"{'NULL' if control_file_columns_names is None else repr(control_file_columns_names)} AS control_file_columns_names "
        )

        generic_file_loads_insert_statements.append(generic_file_loads_insert)

        if 'active_table_config' in table:
            active_table_insert = (
                f"SELECT "
                f"'{global_active_table_config['src_system_name']}' AS src_system_name, "
                f"'{table['active_table_config']['tgt_table_name']}' AS tgt_table_name, "
                f"'{global_active_table_config['tgt_database_name']}' AS tgt_database_name, "
                f"'{tgt_location}' AS tgt_location, "
                f"'{table['active_table_config']['soft_rule_template_name']}' AS soft_rule_template_name, "
                f"'{global_active_table_config['src_database_name']}' AS src_database_name, "
                f"'{source_name}_{table['active_table_config']['src_table_name']}' AS src_table_name, "
                f"'{table['active_table_config']['key_cols']}' AS key_cols, "
                f"'{table['active_table_config']['order_cols']}' AS order_cols, "
                f"'{table['active_table_config']['filter_condition']}' AS filter_condition, "
                f"'{table['active_table_config']['sort_order']}' AS sort_order, "
                f"'{global_active_table_config['job_template_name']}' AS job_template_name, "
                f"'{table['active_table_config']['group_number']}' AS group_number, "
                f"'{table['active_table_config']['change_audit_flag']}' AS change_audit_flag, "
                f"'{', '.join(global_active_table_config['ignore_column'])}' AS ignore_column, "
                f"'{ingest_config['enabled_flag']}' AS enabled_flag, "
                f"'{global_active_table_config['contract_sync_flag']}' AS contract_sync_flag, "
                f"'{table['active_table_config']['incremental_column_name']}' AS incremental_column_name, "
                f"'{table['active_table_config']['order_cols_1']}' AS order_cols_1, "
                f"'{table['active_table_config']['order_cols_2']}' AS order_cols_2, "
                f"'{table['active_table_config']['worker_type']}' AS worker_type, "
                f"{table['active_table_config']['worker_num']} AS worker_num "
            )

            active_table_insert_statements.append(active_table_insert)

    return delete_statements, generic_file_loads_insert_statements, active_table_insert_statements

def execute_athena_query(query, database, s3_output):
    """
    Execute Athena query.
    Args:
        query (str): SQL query statement.
        database (str): Database to execute the query against.
        s3_output (str): S3 output location for query results.
    Returns:
        tuple: A tuple containing the response and the query execution ID.
    """
    athena_client = boto3.client('athena', region_name='eu-west-1')
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output
        }
    )
    query_execution_id = response['QueryExecutionId']
    
    # Wait for query to complete
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']
        
        if query_execution_status == 'SUCCEEDED':
            print(f"Query SUCCEEDED: {query_execution_id}")
            break
        elif query_execution_status in ['FAILED', 'CANCELLED']:
            print(f"Query {query_execution_status}: {query_execution_id}")
            break
        time.sleep(10)
    
    return response, query_execution_id

def wait_for_athena_query(query_execution_id):
    """
    Waits for an Athena query to complete.
    Args:
        query_execution_id (str): The execution ID of the Athena query.
    """
    athena_client = boto3.client('athena')
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            if status == 'SUCCEEDED':
                print(f"Query {query_execution_id} completed successfully.")
            else:
                raise Exception(f"Query {query_execution_id} failed with status: {status}")
            break
        time.sleep(10)

def check_timeout_and_notify(context):
    """Check remaining time and send notification if Lambda is close to timeout."""
    remaining_time = context.get_remaining_time_in_millis()
    if remaining_time < 10000:  # Less than 10 seconds remaining
        print(f"Approaching timeout. Remaining time: {remaining_time}ms.")
        send_lambda_failure_notification(
            function_name=context.function_name,
            error_message="Lambda function is approaching its timeout limit."
        )
        raise Exception("Timeout warning raised.")

def lambda_handler(event, context):
    """
    Lambda function entry point.
    Args:
        event (dict): Event data passed to the function.
        context (object): LambdaContext object.
    Returns:
        dict: A dictionary containing the delete and insert statements.
    """
    retries = 0
    while retries < MAX_RETRIES:
        try:
            print("The config populator function has started.")
            #raise Exception("Failure notification test.................")
            # Parse the S3 event to get bucket name and file key
            bucket_name = event['Records'][0]['s3']['bucket']['name']
            file_key = event['Records'][0]['s3']['object']['key']
            print(f"Bucket name: {bucket_name}, File key: {file_key}")
            check_timeout_and_notify(context)
            # Read the JSON file from S3
            s3_client = boto3.client('s3')
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            file_content = response['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)

            # Process the JSON content
            print("Processing the JSON content received.")
            source_name = json_content['source_name']

            if current_env and current_env in json_content['environments']:
                env_config = json_content['environments'][current_env]
                default_global_ingest_config(env_config['global_ingestion_config'])

                active_table_config_present = (
                    'global_active_table_config' in env_config
                    and any('active_table_config' in table for table in env_config['tables'])
                )

                if active_table_config_present:
                    default_global_active_table_config(
                        env_config['global_active_table_config'], json_content['source_name']
                    )

                delete_statements, generic_file_loads_insert_statements, active_table_insert_statements = generate_statements(
                    env_config,
                    env_config['global_ingestion_config'],
                    env_config['global_active_table_config'] if active_table_config_present else {},
                    current_env,
                    source_name
                )

            database = 'data_control'
            s3_output = f's3://aws-athena-query-results-{account_id}-eu-west-1/'

            # Step 1: Execute Delete Statements and Wait for Completion
            print("Executing delete statements.")
            for statement in delete_statements:
                response, query_execution_id = execute_athena_query(statement, database, s3_output)
                if not query_execution_id:
                    raise Exception(f"The DELETE statement failed: {statement}")
                wait_for_athena_query(query_execution_id)  # Wait for the query to complete
                print(f"DELETE statement completed successfully: {statement}")

            # Step 2: Execute Insert Statements for edp_generic_file_loads
            if generic_file_loads_insert_statements:
                generic_file_loads_insert_query = (
                    f"INSERT INTO data_control.edp_generic_file_loads "
                    f"{' UNION ALL '.join(generic_file_loads_insert_statements)};"
                )
                print("Executing combined insert statements for edp_generic_file_loads.")
                response, query_execution_id = execute_athena_query(generic_file_loads_insert_query, database, s3_output)
                if not query_execution_id:
                    raise Exception("The combined INSERT statement for edp_generic_file_loads failed.")
                wait_for_athena_query(query_execution_id)  # Wait for the query to complete
                print(f"INSERT statement for edp_generic_file_loads completed successfully.")

            # Step 3: Execute Insert Statements for active_table_job_config_attributes_iceberg
            if active_table_config_present and active_table_insert_statements:
                active_table_insert_query = (
                    f"INSERT INTO data_control.active_table_job_config_attributes_iceberg "
                    f"{' UNION ALL '.join(active_table_insert_statements)};"
                )
                print("Executing combined insert statements for active_table_job_config_attributes_iceberg.")
                response, query_execution_id = execute_athena_query(active_table_insert_query, database, s3_output)
                if not query_execution_id:
                    raise Exception("The combined INSERT statement for active_table_job_config_attributes_iceberg failed.")
                wait_for_athena_query(query_execution_id)  # Wait for the query to complete
                print(f"INSERT statement for active_table_job_config_attributes_iceberg completed successfully.")
		
            return {
                'statusCode': 200,
                'body': json.dumps('Processing completed successfully.')
            }
        
        
        except Exception as e:
            print(f"Error during processing: {e}")
            retries += 1
            if retries < MAX_RETRIES:
                wait_time = WAIT_TIME_SECONDS * (2 ** retries)
                print(f"Retrying {retries}/{MAX_RETRIES} after waiting {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print("Max retries reached. Processing failed.")
                return {
                    'statusCode': 500,
                    'body': json.dumps(f'Lambda processing failed after {MAX_RETRIES} retries. Error: {str(e)}')
                }
            send_lambda_failure_notification(function_name=context.function_name, error_message=str(e))
            raise
