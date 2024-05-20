import boto3
from boto3.dynamodb.conditions import Key, Attr
from decouple import config as env_config

aws_access_key_id = env_config("aws_access_key_id")
aws_secret_access_key = env_config("aws_secret_access_key")
aws_region_name = env_config("aws_region_name")

IAM_based = True

def make_boto_object(service, client_or_resource):
    if client_or_resource=="client":
        if IAM_based:
            return boto3.client(service,region_name=aws_region_name)
        else:
            return boto3.client(service,
                            region_name=aws_region_name,
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)

    if client_or_resource=="resource":
        if IAM_based:
            return boto3.resource(service,region_name=aws_region_name)
        else:
            return boto3.resource(service,
                            region_name=aws_region_name,
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)
class SQS:
        FREE_QUEUE = '{}_free_vkyc'
        PRIORITY_QUEUE = '{}_priority_vkyc'
        SCHEDULE_QUEUE = '{}_schedule_vkyc'
        ACCOUNT_ID = "087203045977"
        QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/"+ACCOUNT_ID+"/{}"
        USER_WAITING_QUEUE = "{}_user_waiting"
        CONCURRENT_VIDEO_AUDIT_QUEUE = "{}_concurrent_video_audit_queue"

dynamodb_client = make_boto_object('dynamodb', 'client')
dynamodb = make_boto_object('dynamodb', "resource")
s3 = make_boto_object('s3','client')
s3_resource = make_boto_object('s3','resource')
sqs=make_boto_object('sqs','client')


kb_session_table_prod = 'kreditbee_ipv'
kb_session_table_uat = 'kreditbee_uat_ipv'
kb_session_table_dev = 'kreditbee_dev_ipv'


kb_user_status_prod = 'kreditbee_user_status'
kb_user_status_dev = 'kreditbee_dev_user_status'
kb_user_status_uat = 'kreditbee_uat_user_status'

kb_session_status_prod = 'kreditbee_session_status'
kb_session_status_dev = 'kreditbee_dev_session_status'
kb_session_status_uat = 'kreditbee_uat_session_status'

fino_agent_table = 'fino_vkyc_agent'
fino_user_status_table = 'fino_vkyc_user_status'
fino_session_status = 'fino_vkyc_session_status'

kb_configure_row_prod = 'kreditbee_demo'
kb_configure_row_dev = 'kreditbee_dev_demo'
kb_configure_row_uat = 'kreditbee_uat_demo'

agent_status_table = 'kwikid_vkyc_agent'
session_status_table = 'kwikid_vkyc_session_status'
user_status_table = 'kwikid_vkyc_user_status'
CLIENT_TABLE = 'kwik_id_client_credentials'
audit_status_table = "vkyc_audit_status"

MANUAL_ALLOCATION_TABLE = "kwik_id_user_manual_allocation"
EKYC_STATUS_TABLE = "Kwikid_vkyc_ekyc"

kwikid_agent_logs_table = "kwikid_agent_logs"


kwikid_vkyc_agent = 'kwikid_vkyc_agent'
kwikid_vkyc_user_status = 'kwikid_vkyc_user_status'
kwikid_vkyc_session_status = 'kwikid_vkyc_session_status'
CASE_PUSH_TABLE = "kwikid_vkyc_casepush"

S3_BUCKET = "rbl-prod-kwikid-vkyc"
BPO_STATE_COUNT_TABLE='ipv_bpo_state'
URL = "https://vcip.rblbank.com/api/v4/agent/"


session_status_mapper = {"agent_alloted":"VCIP_PENDING",
                         "kyc_request_accepted":"VCIP_PENDING",
                         "kyc_request_rejected":"VCIP_PENDING",
                         "kyc_result_rejected":"VCIP_REJECTED",
                         "kyc_result_approved":"VCIP_APPROVED",
                         "session_expired":"VCIP_EXPIRED",
                         "user_abandoned":"user_abandoned",
                         "VCIP_APPROVED": "VCIP_APPROVED",
                         "vkyc_requested":"NOT_ASSIGNED_TO_AGENT",
                         "waiting":"NOT_ASSIGNED_TO_AGENT",
                         "kyc_rejected":"VCIP_SESSION_INVALID"}

time_frame_expiry_period = {"BFL":4320, "RBL":43200,"Retailasset":43200}
ekyc_expiry_time_period = {"BFL": 4320, "RBL": 4310, "Digiremit": 4320,"Retailasset":4320}

ENCRYPTION_KEY = "88888888".encode()
