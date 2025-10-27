import urllib3
import json
import boto3
import time
from datetime import datetime
import dateutil.tz
import re

# Initialize the DynamoDB resource
dynamodb = boto3.resource('dynamodb')

# Reference your table
table = dynamodb.Table('non-prod-failures')

http = urllib3.PoolManager()
localtime = dateutil.tz.gettz('Africa/Johannesburg')
time_now = datetime.now(tz=localtime).strftime('%Y-%m-%d-%H-%M')

def get_account_id():
    """
    Retrieve the AWS account ID using the STS client.
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
    if account == account_id:
        current_env = env
        break

if current_env is None:
    raise Exception("The environment does not exist for the current AWS account. Please check the logs.")

print(f"This Lambda is running in the {current_env} environment.")

def lambda_handler(event, context):
    # Make sure to replace this with the correct webhook
    url = "https://libertyholdings.webhook.office.com/webhookb2/84ae269b-3328-4244-9c58-87c0541029c0@66b8ffa6-81c0-4ea6-93fb-06f390dc67f6/IncomingWebhook/c138a082ecc74b4994660b794fa65c9e/540ec8e7-b8ad-4612-9c4a-140cef8ece9c/V2si77KzgXjjzLFo4ensxNcctUBKFqTLyxrbWaTTx0wps1"
    print(event)
    sns_message_raw = event['Records'][0]['Sns']['Message']
    print('sns_message_raw...................',sns_message_raw)
    time_now = datetime.now(tz=localtime).strftime('%Y-%m-%d-%H-%M')
    
    try:
        sns_message = json.loads(sns_message_raw)
    except json.JSONDecodeError:
        print(f"Error decoding JSON: {sns_message_raw}")
        
        # Extract details from raw message using regex
        source_system_match = re.search(r'Source System: ([\w_]+)', sns_message_raw)
        table_name_match = re.search(r'Table: ([\w_]+)', sns_message_raw)
        error_message_match = re.search(r'Error: (.+?)\. Please investigate', sns_message_raw)
        job_name_match = re.search(r'job: ([\w-]+)', sns_message_raw)
        job_run_id_match = re.search(r'JobRunID: ([\w_]+)', sns_message_raw)
        
        raw_formatted_message = (
            f"Environment: {current_env.upper()}  \n"
            f"Source System: {source_system_match.group(1) if source_system_match else 'N/A'}  \n"
            f"Table Name: {table_name_match.group(1) if table_name_match else 'N/A'}  \n"
            f"Error Message: {error_message_match.group(1) if error_message_match else 'N/A'}  \n"
            f"Job Name: {job_name_match.group(1) if job_name_match else 'N/A'}  \n"
            f"JobRun ID: {job_run_id_match.group(1) if job_run_id_match else 'N/A'}  \n"
            f"Execution Date/Time: {time_now}  \n"
            f"Note to Prod: Please investigate this and update the team on this message. Thank you.  \n"
        )
        
        print(f"Formatted message: {raw_formatted_message}")

        message = {
            "text": raw_formatted_message
        }
        job_run_id = f"{job_run_id_match.group(1) if job_run_id_match else 'N/A'}"
        source_system_name = f"{source_system_match.group(1) if source_system_match else 'N/A'}"
        table_name = f"{table_name_match.group(1) if table_name_match else 'N/A'}"
        error_message = f"{error_message_match.group(1) if error_message_match else 'N/A'}"
        glue_job_name = f"{job_name_match.group(1) if job_name_match else 'N/A'}"
        date_inserted = datetime.now(tz=localtime).strftime("%Y-%m-%d")
        
        item = {
            'job_run_id': job_run_id,
            'environment': current_env,
            'source_system_name': source_system_name,
            'table_name': table_name,
            'error_message':error_message,
            'glue_job_name': glue_job_name,
            'pipeline_run_date': time_now,
            'date_inserted': date_inserted
        }

        # Insert the item into DynamoDB
        table.put_item(Item=item)
        
        encoded_message = json.dumps(message).encode('utf-8')
        try:
            resp = http.request('POST', url, body=encoded_message, headers={'Content-Type': 'application/json'})
            print({
                "message": raw_formatted_message,
                "status_code": resp.status,
                "response": resp.data.decode('utf-8')
            })
        except Exception as e:
            print(f"Error sending the HTTP request: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps('Error sending the HTTP request')
            }

        return {
            'statusCode': 200,
            'body': json.dumps('Message processed successfully')
        }

    print(f"This is the full SNS message (JSON): {sns_message}")
    
    formatted_message = (
        f"Environment: {sns_message.get('Environment', 'N/A').upper()}  \n"
        f"Source System: {sns_message.get('Source_System', 'N/A').upper()}  \n"
        f"Table Name: {sns_message.get('tgt_table_name', 'N/A')}  \n"
        f"Error Message: {sns_message.get('ErrorMessage', 'N/A')}  \n"
        f"Job Name: {sns_message.get('JobName', 'N/A')}  \n"
        f"JobRun ID: {sns_message.get('Id', 'N/A')}  \n"
        f"Execution Date/Time: {sns_message.get('cdc_batch_date_id', 'N/A')}  \n"
        f"Note to Prod: {sns_message.get('Message', 'Please investigate this and update the team on this message. Thank you.')}  \n"
    )
    
    print(f"Formatted message: {formatted_message}")

    message = {
        "text": formatted_message
    }

    encoded_message = json.dumps(message).encode('utf-8')
    try:
        resp = http.request('POST', url, body=encoded_message, headers={'Content-Type': 'application/json'})
        print({
            "message": formatted_message,
            "status_code": resp.status,
            "response": resp.data.decode('utf-8')
        })
    except Exception as e:
        print(f"Error sending the HTTP request: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error sending the HTTP request')
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Message processed successfully')
    }
