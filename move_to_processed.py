import boto3
import json
import re
from datetime import datetime
import logging
import urllib3


logger = logging.getLogger()
logger.setLevel(logging.INFO)

http = urllib3.PoolManager()

def send_lambda_failure_notification(function_name, error_message):
    try:
        print('In failure notification definition using webhook...')
        webhook_url = "https://libertyholdings.webhook.office.com/webhookb2/107ab5b7-2181-48b6-a08d-867a3b9b1be6@66b8ffa6-81c0-4ea6-93fb-06f390dc67f6/IncomingWebhook/f48a60e53c244883aec8f818ec712481/540ec8e7-b8ad-4612-9c4a-140cef8ece9c/V2D48F_4WdLacduICMppdTvVOKYoYTTQHBvNM6RTzgpEY1"
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
    client = boto3.client("sts")
    return client.get_caller_identity()["Account"]
    
def move_files(bucket, source_prefix, destination_prefix):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=source_prefix)
    for item in response.get('Contents', []):
        copy_source = {'Bucket': bucket, 'Key': item['Key']}
        new_key = item['Key'].replace(source_prefix, destination_prefix)
        s3_client.copy_object(CopySource=copy_source, Bucket=bucket, Key=new_key)
        s3_client.delete_object(Bucket=bucket, Key=item['Key'])

s3_client = boto3.client('s3')

env_account_mapping = {
    "dev": "649505956583",
    "pre-prod": "681131072283",
    "prod": "014390686996"
}

account_id = get_account_id()
print(f"The account_id found is: {account_id}")

current_env = None

for env, account in env_account_mapping.items():
    if account == account_id:
        current_env = env
        break
    
if current_env is None:
    raise Exception("The environment does not exist for the current AWS account.")
else:
    print(f"The current environment is: {current_env}")

def lambda_handler(event, context):
    # Parse SNS message
    function_name = context.function_name
    try:
        sns_message = event['Records'][0]['Sns']['Message']
        print(f"This is the full SNS message received: {sns_message}")
        
        # Get the name of the table from the SNS message
        match = re.match(r'^(.*?) table processed: (.*?) in (.*?)$', sns_message)
        if match:
            source_system_name = match.group(1).strip()
            original_table_name = match.group(2).strip()
            env = match.group(3).strip()
            print(f"This is the source_system_name received by sns --> {source_system_name}")
            print(f"This is the table name received by sns --> {original_table_name}")
            print(f"This is the environment received by sns --> {env}")
        else:
            print("Error: Unable to extract message components from SNS message.")
            return {
                'statusCode': 400,
                'body': json.dumps('Unable to extract message components from SNS message.')
            }
        
        if source_system_name not in original_table_name:
            print(f"Error: The source system name '{source_system_name}' doesn't match what's found in the table name.")
            return {
                'statusCode': 400,
                'body': json.dumps(f"The source system name '{source_system_name}' doesn't match what's found in the table name: {original_table_name}.")
            }

        match_table = re.search(fr'{source_system_name}_(\w+)', original_table_name)
        if match_table:
            extracted_table_name = match_table.group(1)
            print("Extracted table_name to be used as the table prefix:", extracted_table_name)
        else:
            print(f"No matching table_name found for {source_system_name} in {original_table_name}.")

        base_path = f"landing/{source_system_name}/{extracted_table_name}/"
        print(f"This is the base_path: {base_path}")
        #source_bucket = f"ct-ire-edp-{current_env}-dumps"
        if 'insure' in source_system_name:
            if current_env == 'dev':
                source_bucket = 'ct-ire-edp-non-prod-africa-insure'
            elif current_env == 'prod':
                source_bucket = 'ct-ire-edp-prod-africa-insure'
        elif 'ohi' in source_system_name:
            if current_env == 'dev':
                source_bucket = 'ct-ire-edp-non-prod-africa-health'
            elif current_env == 'prod':
                source_bucket = 'ct-ire-edp-prod-africa-health'
        else:
            source_bucket = f"ct-ire-edp-prd-dumps"
        print(f"This is the source bucket: {source_bucket}")
        today_partition = datetime.now().strftime('%Y-%m-%d-%H-%M')
        destination_base = f"{base_path}processed/{today_partition}/"
        print(f"This is the destination_base: {destination_base}")
        
        for prefix in ['control', 'data', 'header']:
            source_prefix = f"{base_path}{prefix}/"
            destination_prefix = f"{destination_base}{prefix}/"
            move_files(source_bucket, source_prefix, destination_prefix)
            print(f"Source Prefix in for loop: {source_prefix}")
            print(f"Destination Prefix in for loop: {destination_prefix}")
            
        return {
            'statusCode: 200,'
            'body': json.dumps('Files moved successfully')
        }
    except Exception as error:
        error_message = f"An error occurred: {str(error)}"
        logger.error(error_message)
        send_lambda_failure_notification(function_name, error_message)
        raise
