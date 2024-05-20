from decimal import Decimal
import boto3

# Initialize a DynamoDB client
dynamodb = boto3.resource('dynamodb')

# Specify the table
table = dynamodb.Table('vkyc_audit_status')

# Initialize the scan
response = table.scan(
            ProjectionExpression="audit_id, session_id, client_name, user_id, audit_init_time, admin_id, audit_end_time, audit_result, audit_endt_time, feedback, message_data.client_name, message_data.user_id"
        )

# Fetch and process the initial set of items
items = response['Items']
# Process these items as needed, for example, print them or save to a file

# Continue scanning until all items are fetched
while 'LastEvaluatedKey' in response:
    print("getting")
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'],ProjectionExpression="audit_id, session_id, client_name, user_id, audit_init_time, admin_id, audit_end_time, audit_result, audit_endt_time, feedback, message_data.client_name, message_data.user_id")
    items.extend(response['Items'])


with open("audit_dump.csv","w") as a:
    a.write(str(str(items)))


