import boto3

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('kwikid_vkyc_session_status')

# Query sessions where user_id is blank or missing
response = table.scan(
    FilterExpression='attribute_not_exists(user_id) OR user_id = :blank',
    ExpressionAttributeValues={':blank': ''}
)

print("response", response)
# Print the sessions
for item in response['Items']:
    print(item)
