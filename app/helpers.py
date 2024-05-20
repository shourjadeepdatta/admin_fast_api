from app.config import kb_session_table_prod, kb_user_status_prod
from app.config import kb_session_table_dev, kb_user_status_dev
from app.config import kb_session_table_uat, kb_user_status_uat
from app.config import kb_configure_row_prod, kb_configure_row_dev, kb_configure_row_uat
from app.config import kb_session_status_prod, kb_session_status_dev, kb_session_status_uat
from app.config import fino_agent_table, fino_session_status, fino_user_status_table,session_status_table
from app.config import kwikid_vkyc_agent, kwikid_vkyc_session_status, kwikid_vkyc_user_status, ENCRYPTION_KEY, dynamodb
import json
from boto3.dynamodb.conditions import Key
import decimal
from base64 import b64decode,b64encode
from io import BytesIO
from Crypto import Random
from Crypto.Cipher import AES
import base64
from hashlib import md5
# from envelope_encryption import EnvelopeEncryption
import bcrypt
import datetime
import time
import requests

from app.postgres_helper import put_item_in_postgres, update_postgres_table


def unpad(data):
    return data[:-(data[-1] if type(data[-1]) == int else ord(data[-1]))]

def bytes_to_key(data, salt, output=48):
    # extended from https://gist.github.com/gsakkis/4546068
    assert len(salt) == 8, len(salt)
    data += salt
    key = md5(data).digest()
    final_key = key
    while len(final_key) < output:
        key = md5(key + data).digest()
        final_key += key
    return final_key[:output]


def decrypt(encrypted, passphrase):
    encrypted = base64.b64decode(encrypted)
    assert encrypted[0:8] == b"Salted__"
    salt = encrypted[8:16]
    key_iv = bytes_to_key(passphrase, salt, 32 + 16)
    key = key_iv[:32]
    iv = key_iv[32:]
    aes = AES.new(key, AES.MODE_CBC, iv)
    return unpad(aes.decrypt(encrypted[16:]))

def decrypt_string(encrypted_str):
    try:
        decrypted_string = decrypt(encrypted_str, ENCRYPTION_KEY)
        print('decrypted string', decrypted_string)
        return decrypted_string
    except Exception as e:
        print(e)
        return None

def get_s3_file(s3_obj,bucket,s3_key):
    obj = s3_obj.get_object(Bucket =bucket,Key = s3_key)
    return obj['Body'].read().decode('utf-8')

def upload_s3_file(s3_obj,bucket,key,file_object,to_encrypt = False):
    '''
    We are passing the boto3 s3 object here to differentiate between Think's/Client's s3
    Rather than a file path, this file upload function accepts a file object(a BytesIO buffer)
    '''
    if to_encrypt:
        # env_encrypt = EnvelopeEncryption()
        # crypted_data,crypted_key = env_encrypt.encrypt_data(file_object.read())
        # s3_obj.put_object(Body=crypted_data , Bucket=bucket, Key=key,Metadata={"public_key":b64encode(crypted_key)})
        pass
    else:
        if ("FINO" in key) or ("fino_prod" in key):
            print("FINO ki file------------------------")
            s3_obj.put_object(Body=file_object.read(), Bucket=bucket, Key=key)
            try:
                s3_obj.put_object_acl(ACL="public-read",Bucket=bucket,Key=key)
            except Exception as e:
                print("error while setting the file to public mode",e)
                pass

        else:
            s3_obj.put_object(Body=file_object.read(), Bucket=bucket, Key=key)


def getTableName(tableType):
    if tableType == 'Agent':
        table_name = fino_agent_table
    elif tableType == 'User Status':
        table_name = fino_user_status_table
    elif tableType == 'Session Status':
        table_name = fino_session_status
    return table_name


def getPOCTableName(tableType):
    if tableType == 'Agent':
        table_name = kwikid_vkyc_agent
    elif tableType == 'User Status':
        table_name = kwikid_vkyc_user_status
    elif tableType == 'Session Status':
        table_name = kwikid_vkyc_session_status
    return table_name


def getConfigureColName(env):
    if env == 'Production':
        col_name = kb_configure_row_prod
    elif env == 'Dev':
        col_name = kb_configure_row_dev
    elif env == 'UAT':
        col_name = kb_configure_row_uat
    return col_name


def generateSessionsStatusFilterExpression(filter):
    j = json.loads(filter)
    if j['queue'] != 'All' and j['session_status'] != 'All':
        exp = 'queue=:q AND session_status=:ss AND start_time BETWEEN :st and :et'
    elif j['queue'] == 'All' and j['session_status'] != 'All':
        exp = 'session_status=:ss AND start_time BETWEEN :st and :et'
    elif j['session_status'] == 'All' and j['queue'] != 'All':
        exp = 'queue=:q AND start_time BETWEEN :st and :et'
    else:
        exp = 'start_time BETWEEN :st and :et'
    return exp


def genExpAttrValue(filter):
    j = json.loads(filter)
    if j['queue'] != 'All' and j['session_status'] != 'All':
        exp = {
            ':q': j['queue'],
            ':ss': j['session_status'],
            ':st': j['start_time'],
            ':et': j['end_time']
        }
    elif j['queue'] == 'All' and j['session_status'] != 'All':
        exp = {
            ':ss': j['session_status'],
            ':st': j['start_time'],
            ':et': j['end_time']
        }
    elif j['session_status'] == 'All' and j['queue'] != 'All':
        exp = {
            ':q': j['queue'],
            ':st': j['start_time'],
            ':et': j['end_time']
        }
    else:
        exp = {
            ':st': j['start_time'],
            ':et': j['end_time'],
        }
    return exp


def change_all_decimal_to_float(json_data):
    if type(json_data) == list:
        res_json = []
        for item in json_data:
            res_json.append(_change_all_decimal_to_float(item))
        return res_json
    elif type(json_data) == dict:
        return _change_all_decimal_to_float(json_data)
    else:
        return json_data
    pass

def _change_all_decimal_to_float(json_data):
    item_temp = {}
    for key in json_data.keys():
        if isinstance(json_data[key],decimal.Decimal):
            item_temp[key]=float(json_data[key])
        else:
            item_temp[key]=json_data[key]
    return item_temp

def format_json_array(input_json):

  def default(obj):
    if isinstance(obj, decimal.Decimal):
      return int(obj)
    raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)

  output_json = json.loads(json.dumps(input_json, default=default))
  return output_json

def download_file_s3(bucket_name,uri):
    try:
        return s3.get_object(Bucket=bucket_name, Key=uri)['Body'].read()
        #print(x)
    except Exception as e:
        print("Exception = ", str(e))
        raise Exception(str(e))


def upload_file_s3(bucket_name,file_name,data):
    try:
        resp = s3.put_object(Body=data, Bucket=bucket_name, Key=file_name)
        #s3.put_object_acl(ACL="public-read",Bucket=bucket_name,Key=file_name)
        #s3_res.Object(bucket_name, "res-"+file_name).put(Body=data)
        #s3.Object(bucket_name, file_name).put(Body=data)
        return resp
    except Exception as e:
        print("Exception = ", str(e))
        return str(e)

def download_file_s3_local(bucket_name,local_file,s3_loc):
    try:
        resp = s3_resource.Bucket(bucket_name).download_file(s3_loc,local_file)
        #return s3.get_object(Bucket=bucket_name, Key=uri)['Body'].read()
        #print(x)
    except Exception as e:
        print("Exception = ", str(e))
        raise Exception(str(e))


def upload_file_s3_local(bucket_name,local_file,s3_loc):
    try:
        resp = s3_resource.Bucket(bucket_name).upload_file(local_file,s3_loc)
        #s3.put_object_acl(ACL="public-read",Bucket=bucket_name,Key=file_name)
        #s3_res.Object(bucket_name, "res-"+file_name).put(Body=data)
        #s3.Object(bucket_name, file_name).put(Body=data)
        return resp
    except Exception as e:
        print("Exception = ", str(e))
        return str(e)

def session_mapping(session_status, feedback):
    try:
        rbl_session_status = session_status_mapper[session_status]
        
        if rbl_session_status == "VCIP_SESSION_INVALID":
            if feedback != "":
                try:
                    if json.loads(feedback)["type"] == "Reschedule":
                        rbl_session_status = "VCIP_RESCHEDULED"
                except Exception as e:
                    print("error at checking feedback",str(e))

                if expiring_at_link_expiry:
                    if expiry_reason is None:
                        rbl_session_status = "VCIP_EXPIRED"

    except Exception as e:
        print("assigning rbl_session_status error",str(e))
        rbl_session_status = str(e)

    pass

def get_first_session_detail(user_id):
    session_table = dynamodb.Table(session_status_table)
    session_data_list = session_table.query(IndexName="user_id-index",KeyConditionExpression=Key("user_id").eq(user_id))
    items = session_data_list.get('Items')

    older_sessions = sorted(items , key=lambda item: float(item["start_time"]))

    if len(older_sessions) > 0:
        older_session_data = _change_all_decimal_to_float(older_sessions[0])
        return older_session_data
    else:
        return None

def get_latest_session_detail(user_id):
    session_table = dynamodb.Table(session_status_table)
    session_data_list = session_table.query(
      IndexName="user_id-index",
      KeyConditionExpression=Key("user_id").eq(user_id),
      ProjectionExpression="session_id, session_status, start_time"
      )
    items = session_data_list.get('Items')

    older_sessions = sorted(items , key=lambda item: float(item["start_time"]), reverse=True)

    if len(older_sessions) > 0:
        return older_sessions[0].get("session_status")
    else:
        return None

def get_ekyc_detail(user_id):
    try:
        ekyc_table = dynamodb.Table("Kwikid_vkyc_ekyc")
        ekyc_response = ekyc_table.query(IndexName='user_id-client_name-index', KeyConditionExpression=Key('user_id').eq(str(user_id)) & Key('client_name').eq("ekyc")).get('Items')

        if len(ekyc_response) > 0:
            item = _change_all_decimal_to_float(ekyc_response [0])
            return item
        else:
            return None
    except Exception as e:
        print("Exception while get_ekyc_detail = ", e)
        return None

def convert_dynamodb_item_to_json(item):
    if not isinstance(item, dict):
       return item 

    json_item = {}
    for k, v in item.items():
        for dtype, value in v.items():
            if dtype == 'S':
                json_item[k] = value.encode('utf-8').decode('utf-8')
            elif dtype == 'N':
                json_item[k] = float(value) if '.' in value else int(value)
            elif dtype == 'B':
                json_item[k] = value.encode('utf-8')
            elif dtype == 'BOOL':
                json_item[k] = bool(value)
            elif dtype == 'NULL':
                json_item[k] = None
            elif dtype == 'M':
                json_item[k] = convert_dynamodb_item_to_json(value)
            elif dtype == 'L':
                json_item[k] = [convert_dynamodb_item_to_json(i) for i in value]
            elif dtype == 'SS':
                json_item[k] = set([i.encode('utf-8').decode('utf-8') for i in value])
            elif dtype == 'NS':
                json_item[k] = set([float(i) if '.' in i else int(i) for i in value])
            elif dtype == 'BS':
                json_item[k] = set([i.encode('utf-8') for i in value])
            elif dtype == 'object':
                json_item[k] = value
    return json_item


def convert_decimal_to_str(item):
    for k, v in item.items():
        if isinstance(v, Decimal):
            item[k] = str(v)
    return item


def phog(event_name,client_name,user_id="",session_id="",title="",extras={}):
    try:
        headers = {
                    "Content-Type":"application/json"
                }
        extras.update({'server': '04'})
        body = {
              "event_data": {
                "api_key": "phc_prvPks52XGLj8Jo61lYKhUuvXbYKpHqUwFCqCXfZEoS",
                "event": event_name,
                "distinct_id": user_id,
                "user_id": user_id,
                "agent_id": "",
                "properties": {
                  "session_id": session_id,
                  "user_id": user_id,
                  "agent_id": "",
                  "client_name": client_name,
                  "category": "be",
                  "title": title,
                  "type": "event",
                  "href": "",
                  "extras": json.dumps(extras),
                  "hit_time": str(datetime.datetime.now()),
                  "timestamp": int(time.time())
                }
              }
        }
        res = requests.request("POST","https://faceailive.thinkanalytics.in/hypertrail/e", headers=headers, data=json.dumps(body),timeout=1, verify=False)
        print("phog - response: {}".format(res.text.encode("utf-8")))
    except Exception as e:
        print("error while pushing to posthog: {}".format(e))


def update_table(table_object, primary_key_name, primary_key_value, update_expression_string,
                 expression_attribute_value, sort_key_name=None, sort_key_value=None, ):
    if sort_key_name:
        if not expression_attribute_value:
            update_response = table_object.update_item(
                Key={
                    primary_key_name: primary_key_value,
                    sort_key_name: sort_key_value
                },
                UpdateExpression=update_expression_string,
                ReturnValues="UPDATED_NEW")
        else:
            update_response = table_object.update_item(
                Key={
                    primary_key_name: primary_key_value,
                    sort_key_name: sort_key_value
                },
                UpdateExpression=update_expression_string,
                ExpressionAttributeValues=expression_attribute_value,
                ReturnValues="UPDATED_NEW")
    else:
        if not expression_attribute_value:
            update_response = table_object.update_item(
                Key={
                    primary_key_name: primary_key_value
                },
                UpdateExpression=update_expression_string,
                ReturnValues="UPDATED_NEW")
        else:
            update_response = table_object.update_item(
                Key={
                    primary_key_name: primary_key_value
                },
                UpdateExpression=update_expression_string,
                ExpressionAttributeValues=expression_attribute_value,
                ReturnValues="UPDATED_NEW")

    try:
        update_postgres_table(table_object.table_name, primary_key_name, primary_key_value, update_expression_string, expression_attribute_value, sort_key_name, sort_key_value)
    except Exception as e:
        print("update_postgres_table exception = ", e)
    return update_response

# print(decrypt_string("U2FsdGVkX1+zeh9R5tFdq8AFOB6DzlRhz6S36+rNyYE="))
