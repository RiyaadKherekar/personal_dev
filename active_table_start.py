# The purpose of the following code is to execute the active table SF based on the SNS message delivered 
# by the ingestion Glue job. The Glue job extracts the table_name from the args which is used within the
# payload which this Lambda creates to start the active table SF.

# Example test message you can use to test this function:
# {
#   "Records": [
#     {
#       "Sns": {
#         "Message": "<your_source_system_name> table processed: <your_table_name> in <your_environment>"
#       }
#     }
#   ]
# }

# ToDo -- Monday 15 Jan 2024. The following needs to be implemented:
# This Lambda function needs to kick off the corresponding ACTIVE table according to 
# src_system_name and table_name so that the correct Step Function is kicked off.
# Riyaad: I have now implemented the above and left notes describing what we trying to achieve.

import json
import boto3
from datetime import datetime
import uuid
import re
import logging
import urllib3


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

def lambda_handler(event, context):
    function_name = context.function_name
    try:
        current_date = datetime.now().strftime("%Y%m%d%H%M%S")
    
        # Extract the SNS message from the Lambda event
        sns_message = event['Records'][0]['Sns']['Message']
        print(f"This is the original SNS message received --> {sns_message}")

        # Using regular expression to extract source_system_name, table_name, and env
        # ^ - special character denotes start of a line or string
        # .*? combo of characters in the pattern
        # . matches any character except newline
        # * specifies zero or more occurences of the preceding character
        # ? makes the * non greedy
        # ^(.*?) means that it will match any characters including zero characters at the beginning of a line
        # or string. So in this case, we understand that the source_system_name + table_name = target_table_name
        # in the changeaudit database. For example, we should see it in this manner:
        # dev_changeaudit.<database_name>_<table_name>. Since the ActiveTable StepFunction requires the
        # target_table name in the active table config, it would need to be a_table_name. So we basically matching
        # the source system name to the first instance of the table name and stripping it from the table name
        # then replacing that with a a_ to generate the input table name. 
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
        
        # List of source system names for which StepFunction execution should be skipped
        skip_source_systems = ["kenya_exergy", "kenya_turnquest", "stonehouse", "kenya_ohi", "lesotho_ohi", "uganda_ohi", "malawi_ohi", "mauritius_ohi", "mozambique_ohi", "uganda_turnquest", "everest_botswana_insure","everest_eswatini_insure","everest_lesotho_insure","everest_namibia_insure","everest_uganda_insure","everest_zambia_insure"]

        if source_system_name not in original_table_name:
            print(f"Error: The source system name '{source_system_name}' doesn't match what's found in the table name.")
            return {
                'statusCode': 400,
                'body': json.dumps(f"The source system name '{source_system_name}' doesn't match what's found in the table name: {original_table_name}.")
            }
    
        match_table = re.search(fr'{source_system_name}_(\w+)', original_table_name)
        if match_table:
            extracted_table_name = match_table.group(1)
            print("Extracted table_name:", extracted_table_name)
            a_add_table = f'a_{extracted_table_name}'
            print("Correct table_name:", a_add_table)
        else:
            print(f"No matching table_name found for {source_system_name} in {original_table_name}.")
            return {
                'statusCode': 400,
                'body': json.dumps(f"No matching table_name found for {source_system_name} in {original_table_name}.")
            }

        if a_add_table == 'a_client_contract_reference_delete':
            print("Not going to create a StepFunction for this table when the sns comes through.")
            return {
                'statusCode': 200,
                'body': json.dumps('Skipping StepFunction execution for a_client_contract_reference_delete.')
            }
        
        # Check if the source_system_name should skip or trigger the StepFunction
        if source_system_name in skip_source_systems:
            print(f"Skipping StepFunction execution as source_system_name is '{source_system_name}'.")
            return {
                'statusCode': 200,
                'body': json.dumps(f"StepFunction execution skipped for source_system_name '{source_system_name}'.")
            }

        # Trigger the StepFunction for other source_system_names
        active_table_step_function_arn = f'arn:aws:states:eu-west-1:649505956583:stateMachine:sf-{env}-active-tables-{source_system_name}'

        # Create the input payload for the second Step Function
        input_payload = {
            "event": [
                {
                    "Result": {
                        "job_name": f"CDC_{source_system_name}",
                        "execution_env": f"{env}",
                        "execution_table_names": [a_add_table]
                    }
                }
            ]
        }
    
        print(f"This is the incoming payload: {input_payload}")
        
        stepfunctions_client = boto3.client('stepfunctions')

        unique_id = str(uuid.uuid4().hex)[:10]
        date_suffix = datetime.now().strftime("%Y%m%d%H")
        table_name = a_add_table[:60]
        execution_name = f"{table_name}_{date_suffix}_{unique_id}"

        # Pass the input payload to the second Step Function
        response = stepfunctions_client.start_execution(
            stateMachineArn=active_table_step_function_arn,
            name=execution_name,
            input=json.dumps(input_payload)
        )

        
        return {
            'statusCode': 200,
            'body': json.dumps('Lambda function executed successfully')
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        send_lambda_failure_notification(function_name, e)
        return {
            'statusCode': 500,
            'body': json.dumps('Lambda function encountered an error, please check the logs.')
        }
