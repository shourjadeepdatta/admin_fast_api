from flask import request, jsonify, Blueprint, g, copy_current_request_context, current_app
import boto3
import botocore
from boto3.dynamodb.conditions import Key, Attr
from app.config import dynamodb, dynamodb_client,s3,s3_resource, CLIENT_TABLE,BPO_STATE_COUNT_TABLE,sqs,URL, agent_status_table, session_status_table, user_status_table,audit_status_table,kwikid_agent_logs_table,S3_BUCKET,MANUAL_ALLOCATION_TABLE,EKYC_STATUS_TABLE,CASE_PUSH_TABLE
from auth.decorators import login_required, create_token
import json
from app.helpers import getTableName, getConfigureColName, getPOCTableName,download_file_s3,upload_file_s3,download_file_s3_local,upload_file_s3_local
from app.helpers import generateSessionsStatusFilterExpression, genExpAttrValue,get_s3_file,decrypt_string,format_json_array,get_first_session_detail
import datetime
import time
import decimal
import shortuuid
from io import BytesIO
import requests
import traceback
from app.helpers import change_all_decimal_to_float as cadtf, convert_dynamodb_item_to_json, convert_decimal_to_str, phog
from app.config import SQS,sqs,time_frame_expiry_period, ekyc_expiry_time_period
import re
from app.RBL_helpers import *
from werkzeug.utils import secure_filename
import pandas as pd
from decimal import Decimal

from app.config import session_status_mapper,CASE_PUSH_TABLE

from multiprocessing import Process#, Queue

from multiprocessing.pool import ThreadPool

from itertools import chain

from app.api.getsessionlistnewer import get_session_list_new, get_latest_case_status_by_case_creation_date, get_last_case_status_by_attempt_date, get_all_sessions_by_attempt_date,get_all_sessions_by_case_creation_date
from app.api.getsessionlistnewer_all import get_session_list_new as get_session_list_new_all
from app.api.routes.getsessionlist import bp as get_sessionlist_bp

from app.api.getsessionlist import get_session_list, get_ekyc_req_time
from app.api.getAllUserSession import globalSearch
from app.api.getPendingUserList import pendingList
from app import bp, celery_generate_report
from app.api.getCachedSession import getSessionData as getCachedSession
from app.api.routes.bulk_manualassign import api as bulk_manualassign_detached
from app.api.routes.voicedispose import api as voicedispose_detached

from app.api.routes.postgres_session import get_session_list_pg
from app.api.postgres import insert, add_item,  query_table

import threading
import queue


@bp.route('/health')
def health():
    return "ok"

@bp.route('/update_summary_data/<session_id>',methods=['POST'])
def update_summary_data(session_id):
    data = request.form
    try:
        summary_data = json.loads(data["summary_data"])
    except:
        summary_data = data["summary_data"]
    return jsonify({"dt":json.dumps(summary_data)})
    


@bp.route('/editAgent/<agent_id>',methods=["GET","POST"])
@login_required(permission='usr')
def editAgent(agent_id):
    try:
        print("*********************************")
        client_name = g.user_id
        print(client_name)
        try:
            agent_table = dynamodb.Table(agent_status_table)
        except Exception as e:
            return jsonify({
                'msg': 'database error',
                'status_code': 500,
                'success': False}), 500

        try:
            response = agent_table.query(KeyConditionExpression=Key('agent_id').eq(agent_id) & Key('client_name').eq(client_name))
            print(response)
        except Exception as e:
            print(e)
            return jsonify({
                'msg': 'Failed to search in database',

                'status_code': 500,
                'success': False}), 500

        items = response.get('Items')
        print('Items ------ ', items)
        if not items:
            return jsonify({
                "msg": "Agent not Present",
                'status_code': 404,
                'success': False}), 404
    
        item = items[0]
        #item = json.loads(json.dumps(item[0]), parse_float=Decimal)

        try:
            agent_id = item['agent_id']
        except:
            agent_id = None
        try:
            agent_name = item['agent_name']
        except:
            agent_name = None

        try:
            agent_role = item['agent_role']
        except:
            agent_role = None 

        try:
            email = item['email']
        except:
            email = None 
        try:
            is_admin = int(item['IS_ADMIN'])
        except:
            is_admin = 0

        try:
            agent_img_url = item['agent_img_url']
        except:
            agent_img_url = None

        try:
            group = item['region']
        except:
            group = None
        if request.method == "GET":
            resp = {"agent_id":agent_id,"agent_role":agent_role,"email":email,"is_admin":is_admin,"agent_name":agent_name,"group":group,"agent_img_url":agent_img_url}
            #resp.update(item)
            resp['dialer_ext_no'] = str(item.get('dialer_ext_no'))
            return jsonify(resp)
            # return jsonify()
        if request.method == "POST":
            update_items = request.get_json()
            
            try:
                item.update(update_items)
                print(item)
                agent_table = dynamodb.Table(agent_status_table)
                res = agent_table.put_item(Item=item)
                print(res)
            except Exception as e:
                print("update error for edit",str(e))
                return jsonify({"Success":False})


            return jsonify({"Success":True})

    except Exception as e:
        traceback.print_exc()
        return jsonify(success=False, exception=str(e)), 500
        pass


@bp.route('/config/uploadFile/<client_name>',methods=['POST'])
def uploadFile(client_name):
    '''
        This endpoint is used to upload files to the storage space.
    '''
    try:
        file=request.files['file']
        if file:
            filename = secure_filename(file.filename)

            '''
                We are saving the file into a temp storage (BytesIO).
            '''
            file_virtual_storage = BytesIO()
            file.save(file_virtual_storage)
            file_virtual_storage.seek(0)
            
            bucket="uat.vkyc.kwikid"
            file_key = "CONFIG/FILES/{}/{}".format(client_name,filename)
            upload_s3_file(s3,bucket,file_key,file_virtual_storage,False)

            #sending the download url for the file
            DOMAIN_NAME ="uat.vkyc.getkwikid.com"
            GET_FILE_URL = 'https://{}:3357/v1/download_content/{}/'.format(DOMAIN_NAME,bucket)
            download_file_link = GET_FILE_URL + file_key
            print(download_file_link)
            return jsonify({"success":"True",filename:download_file_link})
        else:
            return "no files provided"
    except Exception as e:
        print(e)
        return None

@bp.route('/config/uploadFile/locbased',methods=['POST', 'GET'])
def uploadFile_locbased():

    try:
        file=request.files['file']
        content = request.form
        loc_to_insert = content["loc_to_insert"]
        if file:
            #filename = secure_filename(file.filename)

            file_virtual_storage = BytesIO()
            file.save(file_virtual_storage)
            file_virtual_storage.seek(0)

            #bucket="prod-ghfl-videokyc-s3"
            #file_key = "CONFIG/FILES/{}/{}".format(client_name,filename)
            #return jsonify({"S3_BUCKET":str(S3_BUCKET),"loc_to_insert":str(loc_to_insert)})
            upload_s3_file(s3,S3_BUCKET,loc_to_insert,file_virtual_storage,False)
            #s3.put_object_acl(ACL="public-read", Bucket=bucket, Key=loc_to_insert)

            #now its fixed for UAT later will change for domain based

            #DOMAIN_NAME ="uat.vkyc.getkwikid.com"
            #GET_FILE_URL = 'https://{}:3357/v1/download_content/{}/'.format(DOMAIN_NAME,bucket)
            #download_file_link = GET_FILE_URL + file_key
            #print(download_file_link)
            return jsonify({"success":"True","filename":"dummy"})
        else:
            return "no files provided"
    except Exception as e:
        print(e)
        return str(e)
        #return None


@bp.route('/login', methods=['POST'])
def login():
    print("Login was called")
    content = request.json
    print("Login was called", content)
    username = content["username"]
    password = content["password"]
    try:
        if username == "ta_1234" and password == "tan@1234":
            response = jsonify({"message": "User login successfully"})
            response.status_code = 200
        else:
            response = jsonify({"message": "User login failed"})
            response.status_code = 200
    except Exception as e:
        print("Exception -> ", e)
        response = jsonify({"message": "Incorrect request parameters"})
        response.status_code = 400
        return response
    print(content["username"])
    return response

@bp.route('/checkExternalAPI/<client_name>/<user_id>/<session_id>',methods=["GET"])
# @login_required(permission='usr')
def checkExternalAPI(client_name,user_id,session_id):
    response={}
    path_to_check={
        "agent_video_screen":"videokyc/videos/{}/{}/{}/agent_video_screen.webm",
        "user_video":"videokyc/videos/{}/{}/{}/user.webm",
        "agent_video":"videokyc/videos/{}/{}/{}/agent.webm",
        # "summary_data_json":"videokyc/summary/{}/{}/{}/{session_id}.json".format(session_id=session_id)
        "3005":"videokyc/summary/{}/{}/{}/3005_resp",
        "3004":"videokyc/summary/{}/{}/{}/3004_resp",
        "stage1":"videokyc/summary/{}/{}/{}/stage1_data",
        "stage2":"videokyc/summary/{}/{}/{}/stage2_resp_data",
    }
    for key in path_to_check:
        path = path_to_check[key]
        path = path.format(client_name,user_id,session_id)
        try:
            # print(path)
            s3_resource.Object('uat.vkyc.kwikid', path).load()
            response[key]="FOUND"
            # print("Exists")
        except botocore.exceptions.ClientError as e:
            print(e)
            if e.response['Error']['Code'] == "404":
                # The object does not exist.
                # print("Object Does not exist")
                response[key]="NOT FOUND"
            else:
                # Something else has gone wrong.
                raise
    return jsonify(response)


@bp.route('/getSessionData/<client_name>', methods=['GET'])
@login_required(permission='usr')
def getSessionData(client_name):
    '''
    This endpoint returns the basic session details like agent_id,session_id,user_id and agent_status.
    '''
    try:
        client_table = dynamodb.Table(getPOCTableName('Agent'))
        data = client_table.query(
            IndexName='login-client_name-index',
            KeyConditionExpression=Key('login').eq(
                1) & Key('client_name').eq(client_name)
        )
        items = data.get('Items')
        final_out = []
        for index, item in enumerate(data["Items"]):
            try:
                agent_id = item["agent_id"]
            except Exception as e:
                agent_id = ""
            try:
                session_id = item["session_id"]
            except Exception as e:
                session_id = ""
            try:
                if item['IS_HALTED'] == "1":
                    agent_status = 'halted'
                else:
                    agent_status = item["agent_status"]
            except Exception as e:
                agent_status = ""
            try:
                user_id = item["user_id"]
            except Exception as e:
                user_id = ""

            print ('Client Table ', items)
            result_out = {
                'agent_id': agent_id,
                'session_id': session_id,
                'agent_status': agent_status,
                'user_id': user_id
            }

            final_out.append(result_out)
        final_out_dict = {}
        final_out_dict.update({'sessions': final_out, 'status_code': 200,
                               'success': True})
        return jsonify(final_out_dict), 200

    except Exception as e:
        print ('Exception -> ', e)
        return jsonify({
            'msg': 'database error',
            'status_code': 500,
            'success': False}), 500



@bp.route('/getQueueDataNew/<client_name>', methods=['GET'])
@login_required(permission='usr')
def getQueueDataNew(client_name):
    try:
        free_count = 0
        try:
            free_count_response = sqs.get_queue_attributes(
                QueueUrl= SQS.QUEUE_URL.format(SQS.FREE_QUEUE.format(client_name)),
                AttributeNames=['ApproximateNumberOfMessages'])
            free_count = free_count_response['Attributes']['ApproximateNumberOfMessages']
        except:
            pass

        priority_count = 0
        try:
            priority_count_response = sqs.get_queue_attributes(
                QueueUrl= SQS.QUEUE_URL.format(SQS.PRIORITY_QUEUE.format(client_name)),
                AttributeNames=['ApproximateNumberOfMessages'])
            priority_count = priority_count_response['Attributes']['ApproximateNumberOfMessages']
        except:
            pass

        return jsonify({"free_count":free_count,"priority_count":priority_count})
    except Exception as e:
        return jsonify({"e": str(e)}), 500

@bp.route('/getQueueData/<client_name>', methods=['GET'])
@login_required(permission='usr')
def getQueueData(client_name):
    print("Get Queue was called")
    try:
        client_table = dynamodb.Table(getPOCTableName('User Status'))

        #changing index from current_state-queue_mode-index to current_state-user_mode-index
        #as queue mode doesnt exist temporary
        
        free_queue_data = client_table.query(
            IndexName='current_state-user_mode-index',
            KeyConditionExpression=Key('current_state').eq(
                'waiting') & Key('user_mode').eq('free'),
            FilterExpression='client_name = :client_name',
            ExpressionAttributeValues={
                ":client_name": client_name
            },
            ScanIndexForward=False,
        )

        priority_queue_data = client_table.query(
            IndexName='current_state-user_mode-index',
            KeyConditionExpression=Key('current_state').eq(
                'waiting') & Key('user_mode').eq('priority'),
            FilterExpression='client_name = :client_name',
            ExpressionAttributeValues={
                ":client_name": client_name
            },
            ScanIndexForward=False,
        )

        free_queue_items = free_queue_data.get('Items')
        final_free_result_out = []
        priority_queue_items = priority_queue_data.get('Items')
        priority_free_result_out = []
        print (priority_queue_items)
        for index, item in enumerate(free_queue_data["Items"]):
            try:
                user_id = item["user_id"]
            except Exception as e:
                user_id = ""
            try:
                pos_in_queue = item["pos_in_queue"]
            except Exception as e:
                pos_in_queue = ""
            try:
                user_mode = item["user_mode"]
            except Exception as e:
                user_mode = ""
            try:
                current_state = item["current_state"]
            except Exception as e:
                current_state = ""

            free_result_out = {
                'user_id': user_id,
                'pos_in_queue': pos_in_queue,
                'user_mode': user_mode,
                'current_state': current_state
            }
            final_free_result_out.append(free_result_out)

        for index, item in enumerate(priority_queue_data["Items"]):
            try:
                user_id = item["user_id"]
            except Exception as e:
                user_id = ""
            try:
                pos_in_queue = item["pos_in_queue"]
            except Exception as e:
                pos_in_queue = ""

            priority_result_out = {
                'user_id': user_id,
                'pos_in_queue': pos_in_queue,
            }
            priority_free_result_out.append(priority_result_out)

        final_out_dict = {}
        final_out_dict.update({'free_queue': final_free_result_out, 'priority_queue': priority_free_result_out, 'status_code': 200,
                               'success': True})
        final_out_dict= cadtf(final_out_dict)
        # print("this is final _out",final_out_dict)
        return jsonify(final_out_dict), 200

    except Exception as e:
        traceback.print_exc()
        print ('Exception -> ', e)
        return jsonify({
            'error':str(e),
            'msg': 'database error',
            'status_code': 500,
            'success': False}), 500


@bp.route('/getConfigureData/<client_name>', methods=['GET'])
@login_required(permission='usr')
def getConfigureData(client_name):
    '''
        This endpoint returns the backend configuration for the particular client.
    '''
    try:
        client_table = dynamodb.Table('ipv_bpo_state')
        data = client_table.query(
            KeyConditionExpression=Key('client').eq(
                client_name+'_vkyc')
        )
        items = data.get('Items')
        print ('items', items[0])
        final_out_dict = {}
        data = items[0]
        for key in data.keys():
            if isinstance(data[key], decimal.Decimal):
                data[key] = float(data[key])

        final_out_dict.update({'data': data, 'status_code': 200,
                               'success': True})
        return jsonify(final_out_dict), 200

    except Exception as e:
        print ('Exception -> ', e)
        return jsonify({
            'msg': 'database error',
            'status_code': 500,
            'success': False}), 500


@bp.route('/saveConfigureData/<client_name>', methods=['POST'])
@login_required(permission='usr')
def saveConfigureData(client_name):
    '''
    This endpoint is used to save the backend cofiguration 
    '''
    content = request.json
    update_data = content["data"]
    print ("Env", client_name, update_data)
    try:
        client_table = dynamodb.Table('ipv_bpo_state')
        res = client_table.update_item(
            Key={
                'client': client_name+'_vkyc'
            },
            UpdateExpression="set videokyc_req_timer=:v, sqs_message_retention_timer=:s, agent_wait_after_videokyc_req_reject=:w, user_abandon_timer=:u, user_abandon_timer_ongoing_call=:a",
            ExpressionAttributeValues={
                ':v': update_data['videokyc_req_timer'],
                ':s': update_data['sqs_message_retention_timer'],
                ':w': update_data['agent_wait_after_videokyc_req_reject'],
                ':u': update_data['user_abandon_timer'],
                ':a': update_data['user_abandon_timer_ongoing_call']
            },
            ReturnValues="UPDATED_NEW"
        )
        print ('res', res)
        final_out_dict = {}
        final_out_dict.update({'data': update_data, 'status_code': 200,
                               'success': True})
        return jsonify(final_out_dict), 200
    except Exception as e:
        print ('e', e)
        return jsonify({
            'msg': 'Failed to update',
            'status_code': 500,
            'success': False}), 500

@bp.route('/getFreeCallList/<client_name>', methods=['POST'])
@login_required(permission='usr')
def getFreeCallList(client_name):
    content = request.json
    filters = content['filters']
    get_attr_list=["session_id","session_status","user_id","phone_number","audit_lock","audit_result","link_type_custom_info"]
    try:
        final_out = []
        paginator = dynamodb_client.get_paginator('query')
        data=paginator.paginate(
            TableName=getPOCTableName('Session Status'),
            Select='SPECIFIC_ATTRIBUTES',
            AttributesToGet=get_attr_list,
            IndexName='client_name-start_time-index',
            KeyConditions={
                    'client_name': {
                        'AttributeValueList': [
                            {
                                'S': client_name
                            }
                        ],
                        'ComparisonOperator': 'EQ'
                    },
                    'start_time': {
                        'AttributeValueList': [
                            {
                                'S': filters['start_time']
                            },
                            {
                                'S': filters['end_time']
                            }
                        ],
                        'ComparisonOperator': 'BETWEEN'
                    }
                },
            QueryFilter={
                    "link_type_custom_info" : {
                        'AttributeValueList': [
                             {
                                'S': "free"
                            }
                        ],
                        'ComparisonOperator': 'EQ'
                    }
                }
        )
        res_items = []

        for page in data:
            print(len(page['Items']))
            res_items+=page['Items']
        for index, item in enumerate(res_items):
            try:
                session_id = item["session_id"]['S']
            except Exception as e:
                session_id = ""
            try:
                link_type_custom_info = item["link_type_custom_info"]['S']
            except Exception as e:
                link_type_custom_info = ""
            try:
                session_status = item['session_status']['S']
            except Exception as e:
                session_status = ""
            try:
                user_id = item["user_id"]['S']
            except Exception as e:
                user_id = ""
            try:
                audit_result = item["audit_result"]['S']
            except Exception as e:
                try:
                    audit_result = item["audit_result"]['N']
                except Exception as e:
                    audit_result = ""
            try:
                phone_number = item["phone_number"]['S']
            except Exception as e:
                phone_number = ""
            #below added for custom session_status for rbl
            try:
                rbl_session_status = session_status_mapper[session_status]
                if rbl_session_status in ["NOT_ASSIGNED_TO_AGENT"]:
                    if link_type_custom_info == "free":
                        rbl_session_status = "NOT_ASSIGNED_TO_AGENT"
                    else:
                        try:
                            user_table = dynamodb.Table(getPOCTableName('User Status'))
                            data = user_table.query(KeyConditionExpression=Key('user_id').eq(user_id))
                            items = data.get('Items')
                            if "ekyc_success" in items[0]:
                                rbl_session_status = "VCIP_PENDING"
                            else:
                                rbl_session_status = "KYC_PENDING"
                        except Exception as e:
                            rbl_session_status = str(e)
                            print("error at ekyc status checking",str(e))
                            pass
                if rbl_session_status in ["VCIP_APPROVED"]:
                    try:
                        audit_lock = item['audit_lock']['BOOL']
                        if audit_lock:
                            rbl_session_status = "CHECKER_PENDING"
                        else:
                            pass
                    except Exception as e:
                        print("error at audit lock checking",str(e))
                        pass
                if rbl_session_status in ["VCIP_APPROVED","CHECKER_PENDING","VCIP_REJECTED"]:
                    try:
                        if int(audit_result) == 1:
                            rbl_session_status = "CHECKER_APPROVED"
                        else:
                            rbl_session_status = "CHECKER_REJECTED"
                    except Exception as e:
                        print(str(e))
                        pass    
            except Exception as e:
                print("assigning rbl_session_status error",str(e))
                rbl_session_status = ""
            result_out = {
                'session_id': session_id,
                'session_status': rbl_session_status,
                'TA_session_status': session_status,
                'phone_number':phone_number,
                'user_id': user_id}
            final_out.append(result_out)
        
        final_out_dict = {}
        final_out_dict.update({'session_data': final_out, 'status_code': 200,
                               'success': True})
        return jsonify(final_out_dict), 200
    except Exception as e:
        print ('Exception', e)
        traceback.print_exc()
        return jsonify({
            'msg': 'Failed to update',
            'status_code': 500,
            'success': False}), 500
        

@bp.route("/getSession/<sessionid>")
@login_required(permission='usr')
def getSession(sessionid):
    table =  dynamodb.Table(getPOCTableName('Session Status'))
    response = table.query(KeyConditionExpression=Key('session_id').eq(sessionid))
    if response["Count"] > 0:
        for key in response['Items'][0].keys():
            if isinstance(response['Items'][0][key], decimal.Decimal):
                response['Items'][0][key] = float(response['Items'][0][key])
        #response['Items']=[{key:(str(value) if isinstance(value,list)!=True else value) for key,value in response['Items'][i].items()} for i in range(len(response['It$
        #return response['Items'][0]
        return jsonify({'data': response['Items'][0],'status_code': 200,'success': False}), 200
    else:
        return jsonify({'msg': 'not found','status_code': 500,'success': False, "count": response["Count"]}), 500


def process_page(page_queue, res_items_lock, res_items):
    while True:
        try:
            page = page_queue.get_nowait()
        except queue.Empty:
            break
        print(len(page['Items']))
        with res_items_lock:
            res_items += page['Items']
        page_queue.task_done()


def request_wrapper(method,url,headers,data,responses,res_int):
    resp = requests.request(method,url,headers=headers,data=data)
    #print resp
    responses[res_int] = resp
    return resp
    

@bp.route('/getSessionListCount/<client_name>', methods=['POST'])
@login_required(permission='usr')
def getSessionListCount(client_name):
    print(" getSessionListCount = ", client_name)
    return get_session_list(client_name, request.json)

@bp.route('/getSessionListCountPg/<client_name>', methods=['POST'])
@login_required(permission='usr')
def getSessionListCountFromPg(client_name):
    print(" getSessionListCountFromPg = ", client_name)
    return get_session_list_pg(client_name, request.json)

@bp.route('/getSessionStatus/<client_name>', methods=['POST'])
@bp.route('/getSessionList/<client_name>', methods=['POST'])
@login_required(permission='usr')
def getSessionStatus(client_name):
    auth_token = request.headers.get('auth')
    url = "https://vcip.rblbank.com/api/v6/getSessionStatus/{}".format(client_name)
    content = request.json
    start_time = float(content["filters"]["start_time"])
    end_time = float(content["filters"]["end_time"])
    try:
        from_mtd = content["from_mtd"]
    except:
        from_mtd = False

    """
    if client_name in ["RBL"]:
        temp_data_df = sdf[(sdf["start_time"] >= start_time) & (sdf["end_time"] <= end_time)]
        temp_data_dict = temp_data_df.to_dict(orient='records')

        if start_time > 1692383340:
            pass
        elif end_time > 1692383340:
            start_time = 1692383340
            end_time = end_time
        else:
            final_resp = {
                "server":"62",
                "session_data": temp_data_dict,
                "status_code":200,
                "success":True
            }

            return jsonify(final_resp), 200
    else:
        temp_data_dict = []
    """

    temp_data_dict = []

    total_request_duration = end_time - start_time
    print("remainder from api: {}".format(total_request_duration)) 
    app = current_app

    per_thread_gap = (end_time - start_time)/2

    start_time_1 = start_time
    end_time_1 = start_time_1 + per_thread_gap
    start_time_2 = end_time_1
    end_time_2 = start_time_2 + per_thread_gap

    responses = [None,None]
    #responses = Queue()
    headers = {
                "Content-Type":"application/json",
                "auth":auth_token
            }

    payload1 = {
            "filters": {
                "start_time": str(start_time_1),
                "end_time": str(end_time_1)
            }
        }
    print("payload1: {}".format(payload1))


    payload2 = {
            "filters": {
                "start_time": str(start_time_2),
                "end_time": str(end_time_2)
            }
        }

    print("payload2: {}".format(payload2))

    def getSessionStatuswrapper(client_name,payload,referrer,response_setter,response_index):
        ress = getSessionStatusint(client_name,payload,referrer)
        #print("respo: {}".format(ress))
        #response_setter.put(ress)
        response_setter[response_index] = ress
        return ress


    thread1 = threading.Thread(target=getSessionStatuswrapper,args=(client_name,payload1,request.referrer,responses,0))
    thread2 = threading.Thread(target=getSessionStatuswrapper,args=(client_name,payload2,request.referrer,responses,1))
    #thread3 = threading.Thread(target=getSessionStatuswrapper,args=(client_name,payload3,request.referrer,responses,2))
    #thread4 = threading.Thread(target=getSessionStatuswrapper,args=(client_name,payload4,request.referrer,responses,3))

    #thread1 = Process(target=getSessionStatuswrapper,args=(client_name,payload1,request.referrer,responses,0))
    #thread2 = Process(target=getSessionStatuswrapper,args=(client_name,payload2,request.referrer,responses,1))

    thread1.start()
    thread2.start()


    thread1.join()
    thread2.join()

    #results = []
    #while not responses.empty():
    #    result = responses.get()
    #    results.append(result)


    resp1 = responses[0]
    resp2 = responses[1]
    

    final_resp = {
                "server":"62",
                "session_data": temp_data_dict + resp1["session_data"] + resp2["session_data"],
                "status_code":200,
                "success":True
            }

    return jsonify(final_resp), 200

@bp.route('/getLatestCaseStatusByCaseCreationDate_v2', methods=['POST'])
@login_required(permission='usr')
def getLatestCaseStatusByCaseCreationDate_v2():
    client_name = g.user_id
    print('request.json = ', request.json)
    return getCachedSession(client_name, request.json)

@bp.route('/getLatestCaseStatusByCaseCreationDate', methods=['POST'])
@login_required(permission='usr')
def getLatestCaseStatusByCaseCreationDate():
    client_name = g.user_id


    print('request.json = ', request.json)
    #return 'ok', 200
    #return getCachedSession(client_name, request.json)
    if request.json["session_type"] == "all" and request.json["time_type"] == "attempt_time":
        return get_all_sessions_by_attempt_date(client_name,request.json)
    if request.json["session_type"] == "unique" and request.json["time_type"] == "attempt_time":
        return get_last_case_status_by_attempt_date(client_name,request.json)
    if request.json["session_type"] == "unique" and request.json["time_type"] == "creation_time":
        return get_latest_case_status_by_case_creation_date(client_name, request.json)
    if request.json["session_type"] == "all" and request.json["time_type"] == "creation_time":
        return get_all_sessions_by_case_creation_date(client_name, request.json)


@bp.route('/get_all_reports', methods=['GET'])
@login_required(permission='usr')
def getAllReports():
    client_name = g.user_id
    query = {
            "client_name": "eq.{}".format(client_name)
            }
    data = query_table("report_generation_tracker", query)
    return jsonify({"data": json.loads(data)}), 200

@bp.route('/download_reports', methods=['POST'])
@login_required(permission='usr')
def downloadReports():
    client_name = g.user_id
    return getCachedSession(client_name, request.json, True)


@bp.route('/generate_report', methods=['POST'])
@login_required(permission='usr')
def generateReport():
    client_name = g.user_id
    request_data = request.json

    start_time = request_data.get('start_time')
    end_time = request_data.get('end_time')
    kind = request_data.get('time_type')   
    get_latest_status_for_userid_only = request_data.get('session_type') == 'unique'
    sendEmail = request_data.get('sendEmail') 
    recipients =  request_data.get('recipients')
    sendEmailLink =  request_data.get('sendEmailLink')
    
    requset_id = celery_generate_report(client_name, start_time, end_time, kind, get_latest_status_for_userid_only, sendEmail, recipients, sendEmailLink)

    row_data = {
            'report_id': str(requset_id),
            'report_state': "INITIATED",
            'client_name': client_name,
            'time_type': kind,
            'session_type': request_data.get('session_type'),
            'start_time': str(datetime.fromtimestamp(float(start_time) + 19800)),
            'end_time': str(datetime.fromtimestamp(float(end_time) + 19800)),
            'created_at': datetime.fromtimestamp(time.time() + 19800).isoformat()
            }

    sql_response = add_item("report_generation_tracker", row_data)
    return jsonify({'request_id': str(requset_id), 'status': 'Initiated', 'sql_response': sql_response}), 200


@bp.route('/getSessionListNewAbhishek/<client_name>', methods=['POST'])
@login_required(permission='usr')
def getSessionListCountNew(client_name):
    #return get_session_list_new(client_name, request.json)
    return pendingList(client_name, request.json)

@bp.route('/getPendingSessionGroupByAgent', methods=['POST'])
@login_required(permission='usr')
def getPendingSessionGroupByAgent():
    client_name = g.user_id 
    pendingList =  pendingList(client_name, request.json, True)
    final_dataframe = pd.DataFrame(pendingList)
    final_data = final_dataframe.groupby("agent_id").apply(lambda x: x.to_dict(orient="records")).to_dict(orient="records")
    return jsonify({"session_data":final_data ,"count":len(final_data),"server":"14"}),200

@bp.route('/getSessionAllNew', methods=['POST'])
@login_required(permission='usr')
def getSessionListCountNewAll():
    client_name = g.user_id 
    return get_session_list_new_all(client_name, request.json)



#@bp.route('/getSessionStatus/<client_name>', methods=['POST'])
#@bp.route('/getSessionList/<client_name>', methods=['POST'])
#@login_required(permission='usr')
def getSessionStatusint(client_name,requestc=None,referrer=None):
    '''
    This endpoint returns all the sessions that were initiated in between the provided time.
    start_time and end_time come as epoch timestamped value.

    All the related data to a session is returned in this endpoint.
    '''
    pf_file = open('session_status_api_performance.txt'+str(time.time()), 'w')

    current_time = time.time()
    pf_file.write("Start Time "+str(current_time)+"\n")
    print (client_name)
    if requestc != None:
        content = requestc
    else:
        content = request.json
    filters = content['filters']
    agent_table = dynamodb.Table(getPOCTableName('Agent'))
    ekyc_table = dynamodb.Table(EKYC_STATUS_TABLE)
    session_table = dynamodb.Table(session_status_table)
    user_table = dynamodb.Table(user_status_table)
    case_push_table = dynamodb.Table(CASE_PUSH_TABLE)

    agents_data = agent_table.query(
        IndexName='client_name-index',
        KeyConditionExpression=Key('client_name').eq(client_name)
    )

    agents_data_items = agents_data.get('Items')

    agents_name_dict = {}

    for index, item in enumerate(agents_data_items):
        try:
            agent_id = item["agent_id"]
            agent_name = item["agent_name"]
            agents_name_dict[agent_id] = agent_name
        except Exception as e:
            agents_name_dict[agent_id] = ""

    c_time = time.time()
    pf_file.write("Time taken to get all agents name "+str(c_time-current_time)+"\n")
    current_time = c_time

    try:
        final_out = []
        if referrer == "https://vcip.rblbank.com/user-status":
            get_attr_list=["vkyc_start_time","session_id","queue",'session_status',"user_id","start_time","end_time","agent_assignment_time","audit_result",'extras','audit_id','feedback',"manual_agent","expiry_reason","expiring_at_link_expiry","CUSTNAME","auditor_fdbk","ekyc_req_time","audit_end_time","PRODUCT","phone_number","agent_id","audit_lock"]
        else:
            get_attr_list=["vkyc_start_time","session_id","queue",'session_status',"user_id","start_time","end_time","agent_id","agent_assignment_time","audit_result",'auditor_name','session_type','extras','audit_id','feedback',"cack_t","link_type_custom_info","sfdc_time","sfdc_response","skygee_time","fraud_advisory_given","manual_agent","expiry_reason","expiring_at_link_expiry","CUSTNAME","phone_number","auditor_fdbk","ekyc_req_time","audit_end_time"]
        paginator = dynamodb_client.get_paginator('query')
        data=paginator.paginate(
            TableName=getPOCTableName('Session Status'),
            Select='SPECIFIC_ATTRIBUTES',
            AttributesToGet=get_attr_list,
            IndexName='client_name-start_time-index',
            KeyConditions={
                    'client_name': {
                        'AttributeValueList': [
                            {
                                'S': client_name
                            }
                        ],
                        'ComparisonOperator': 'EQ'
                    },
                    'start_time': {
                        'AttributeValueList': [
                            {
                                'S': filters['start_time']
                            },
                            {
                                'S': filters['end_time']
                            }
                        ],
                        'ComparisonOperator': 'BETWEEN'
                    }
                }

        )

        c_time = time.time()
        pf_file.write("Time taken to get all the data from db "+str(c_time-current_time)+"\n")
        current_time = c_time

        res_items = []
        res_items_lock = threading.Lock()

        for page in data:
            print(len(page['Items']))
            res_items+=page['Items']

        """
        threads = []
        page_queue = Queue.Queue()

        for page in data:
            page_queue.put(page)

        for i in range(2):
            thread = threading.Thread(target=process_page, args=(page_queue, res_items_lock, res_items))
            thread.start()
            print("thread started: {}".format(i))
            threads.append(thread)

        for thread in threads:
            thread.join()

        page_queue.join()

        """

        print("rows: {}".format(len(res_items)))
        c_time = time.time()
        print("Time taken to execute for loop "+str(c_time-current_time)+"\n")
        #c_time = time.time()
        pf_file.write("Time taken to execute for loop "+str(c_time-current_time)+"\n")
        current_time = c_time

        
        for index, item in enumerate(res_items):
            try:
                session_id = item["session_id"]['S']
            except Exception as e:
                session_id = ""
            try:
                skygee_time = item["skygee_time"]['S']
            except Exception as e:
                skygee_time = ""
            try:
                queue = item["queue"]['S']
            except Exception as e:
                queue = ""
            try:
                fraud_advisory_given = item["fraud_advisory_given"]['BOOL']
                if fraud_advisory_given:
                    fraud_advisory_given = "True"
                else:
                    fraud_advisory_given = "False"
            except Exception as e:
                fraud_advisory_given = ""
            try:
                manual_agent = item["manual_agent"]['S']
            except Exception as e:
                manual_agent = ""

            try:
                manual_agent_name = agents_name_dict[manual_agent]
            except:
                manual_agent_name = ""

            #expiring_at_link_expiry
            try:
                expiring_at_link_expiry = item["expiring_at_link_expiry"]['S']
            except Exception as e:
                expiring_at_link_expiry = None
            try:
                expiry_reason = item["expiry_reason"]['S']
            except Exception as e:
                expiry_reason = None
            try:
                session_status = item['session_status']['S']
            except Exception as e:
                session_status = ""
            
            try:
                user_id = item["user_id"]['S']
            except Exception as e:
                user_id = ""

            try:
                customer_name = item['CUSTNAME']['S']
            except Exception as e:
                customer_name = ""
                print("customer name not found!!------")
                try:
                    data_se = session_table.query(IndexName="user_id-index",KeyConditionExpression=Key('user_id').eq(user_id)).get('Items')
                    for d in data_se:
                        if d.get("CUSTNAME") != None:
                            customer_name = d.get("CUSTNAME")
                            break
                except Exception as e_c:
                    print("Customer name finding error:",str(e_c))
                if customer_name == "":
                    customer_name = "-"

            try:
                link_type_custom_info = item["link_type_custom_info"]['S']
            except Exception as e:
                link_type_custom_info = ""
            #if queue == "" and link_type_custom_info != "":
            #    queue = link_type_custom_info
            try:
                start_time = float(item["start_time"]['S'])
            except Exception as e:
                start_time = ""
            try:
                end_time = float(item["end_time"]['S'])
            except Exception as e:
                end_time = ""

            try:
                if referrer != "https://vcip.rblbank.com/user-status":
                    product = item["PRODUCT"]['S']
                else:
                    product = ""
            except Exception as e:
                product = ""
                print("priduct code node found!!-------")
                try:
                    data_se = session_table.query(IndexName="user_id-index",KeyConditionExpression=Key('user_id').eq(user_id)).get('Items')
                    for d in data_se:
                        if d.get("PRODUCT") != None:
                            product = d.get("PRODUCT")
                            break
                except:
                    pass
                if product == "":
                    product = "CC"

            try:
                sfdc_time = float(item["sfdc_time"]['S'])
            except Exception as e:
                sfdc_time = ""

            try:
                sfdc_response = float(item["sfdc_response"]['S'])
            except Exception as e:
                sfdc_response = ""

            try:
                if referrer != "https://vcip.rblbank.com/user-status":
                    phone_number = item["phone_number"]['S']
                else:
                    phone_number = ""
            except Exception as e:
                phone_number = ""
                try:
                    data_se = session_table.query(IndexName="user_id-index",KeyConditionExpression=Key('user_id').eq(user_id)).get('Items')
                    for d in data_se:
                        if d.get("phone_number") != None:
                            phone_number = d.get("phone_number")
                            break
                except:
                    pass
                if phone_number == "":
                    phone_number = "-"

            try:
                agent_id = item["agent_id"]['S']
            except Exception as e:
                agent_id = ""
            try:
                agent_assignment_time = item["agent_assignment_time"]['S']
            except Exception as e:
                agent_assignment_time = ""
            try:
                summary_data = item["summary_data"]['S']
            except Exception as e:
                summary_data = ""
            try:
                agent_screen_url = item["agent_screen_url"]['S']
            except Exception as e:
                agent_screen_url = ""
            try:
                agent_video_url = item["agent_video_url"]['S']
            except Exception as e:
                agent_video_url = ""
            try:
                user_video_url = item["user_video_url"]['S']
            except Exception as e:
                user_video_url = ""

            try:
                aadhaar_image_url = item["aadhaar_url"]['S']
            except Exception as e:
                aadhaar_image_url = ""

            try:
                pan_image_url = item["pan_url"]['S']
            except Exception as e:
                pan_image_url = ""

            try:
                selfie_image_url = item["selfie_url"]['S']
            except Exception as e:
                selfie_image_url = ""

            try:
                zip_url = item["zip_url"]['S']
            except Exception as e:
                zip_url = ""

            try:
                audit_result = item["audit_result"]['S']
            except Exception as e:
                try:
                    audit_result = item["audit_result"]['N']
                except Exception as e:
                    audit_result = ""

            try:
                summary_pdf_url = item["summary_pdf_url"]['S']
            except Exception as e:
                summary_pdf_url = ""

            try:
                auditor_name = item['auditor_name']['S']
            except:
                auditor_name = ""

            try:
                auditor_fdbk = item['auditor_fdbk']['S']
            except:
                auditor_fdbk = ""

            try:
                feedback= item['feedback']['S']
            except:
                feedback =""

            try:
                vkyc_start_time = float(item['vkyc_start_time']['S'])
            except:
                try:
                    vkyc_start_time = float(item['vkyc_start_time']['N'])
                except:
                    vkyc_start_time = ""
            try:
                audit_id = item['audit_id']['S']
            except:
                audit_id = ""

            try:
                ekyc_time = item["ekyc_req_time"]['S']
            except Exception as e:
                try:
                    #response_ekyc = ekyc_table.get_item(Key={"user_id":user_id}).get(["Item"])
                    #ekyc_time = response_ekyc["ekyc_req_time"]
                    response_ekyc = ekyc_table.query(IndexName='user_id-client_name-index', KeyConditionExpression=Key('user_id').eq(user_id) & Key('client_name').eq("ekyc")).get('Items')
                    ekyc_time = response_ekyc[0]["ekyc_req_time"]
                except Exception as e:
                    ekyc_time = ""

            
            #user_custom_data = []
            user_table = dynamodb.Table(getPOCTableName('User Status'))
            #data = user_table.get_item(Key={"user_id":user_id})
            #items = [data.get('Item')]

            """
            try:
                case_push_table_data = case_push_table.get_item(Key={"user_id":user_id}).get("Item")
                case_push_time = case_push_table_data["case_push_time"]
            except:
                case_push_time = ""
            """

            try:
                print("data oldest",get_first_session_detail(user_id))
                case_push_time = get_first_session_detail(user_id)["start_time"]
            except Exception as ecp:
                print("Exception cpt",str(ecp))
                case_push_time = ""

            #print("items: {}".format(items))
            #custom_ekyc_time = items.get("ekyc_req_time", "")
            data = user_table.query(KeyConditionExpression=Key('user_id').eq(user_id))
            items = data.get('Items')
            try:
                custom_ekyc_time = items[0].get("ekyc_req_time", "")
            except:
                custom_ekyc_time = ""
            if ekyc_time != "": 
                custom_ekyc_time = ekyc_time
            #g = items[0]
            #below added for custom session_status for rbl
            try:
                rbl_session_status = session_status_mapper[session_status]
                if rbl_session_status in ["NOT_ASSIGNED_TO_AGENT"]:
                    if queue == "free":
                        rbl_session_status = "NOT_ASSIGNED_TO_AGENT"
                    else:
                        try:
                            if "ekyc_success" in items[0]:
                                rbl_session_status = "VCIP_PENDING"
                            else:
                                rbl_session_status = "KYC_PENDING"

                            if client_name in ["RBL_uat", "RBL", "Digiremit_uat","Digiremit","Retailasset_uat","Retailasset"]:
                                if "custom_ekyc" in items[0]:
                                #if True:
                                    if "ekyc_success" in items[0]: #this condn wil wokr if customer entered through custom ekyc
                                        if (custom_ekyc_time == "") or (divmod(time.time() - float(custom_ekyc_time), 60)[0] > ekyc_expiry_time_period[client_name]):
                                            rbl_session_status = "KYC_PENDING"
                                        else:
                                            rbl_session_status = "VCIP_PENDING"
                                    else:
                                        rbl_session_status = "KYC_PENDING"
                                        ekyc_time = ""
                                elif ekyc_time != "" and (divmod(time.time() - float(ekyc_time), 60)[0] <= ekyc_expiry_time_period[client_name]): #else we'll check ekyc_time we got from sendLink
                                    rbl_session_status = "VCIP_PENDING"
                                    pass
                                else: #if ekyc_time is missing then defaults to KYC_pending
                                    rbl_session_status = "KYC_PENDING"

                        except Exception as e:
                            rbl_session_status = str(e)
                            print("error at ekyc status checking",str(e))

                if rbl_session_status in ["VCIP_APPROVED"]:
                    try:
                        audit_lock = item['audit_lock']['BOOL']
                        if audit_lock:
                            rbl_session_status = "CHECKER_PENDING"
                        else:
                            pass
                    except Exception as e:
                        print("error at audit lock checking",str(e))
                        pass
                if rbl_session_status in ["VCIP_APPROVED","CHECKER_PENDING","VCIP_REJECTED"]:
                    try:
                        if int(audit_result) == 1:
                            rbl_session_status = "CHECKER_APPROVED"
                        else:
                            rbl_session_status = "CHECKER_REJECTED"
                    except Exception as e:
                        print(str(e))
                        pass
                if client_name in ["RBL","RBL_uat","BFL","BFL_uat","Retailasset_uat","Retailasset"]:
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
                if client_name in ["BFL","BFL_uat"]:
                    if session_status == "kyc_rejected":
                        rbl_session_status = "kyc_rejected"    

                if client_name in ["RBL","RBL_uat","Digiremit_uat","Digiremit","Retailasset_uat","Retailasset"]:
                    if rbl_session_status == "CHECKER_REJECTED" and "REVISIT" in auditor_fdbk.upper():
                        if ekyc_time == "" or divmod(time.time() - float(ekyc_time), 60)[0] > ekyc_expiry_time_period[client_name]:
                            rbl_session_status = "KYC_PENDING"
                        else:
                            rbl_session_status = "VCIP_PENDING"

            except Exception as e:
                print("assigning rbl_session_status error",str(e))
                rbl_session_status = ""
            
            try:
                audit_end_time = float(item['audit_end_time']['N'])
            except:
                try:
                    if referrer != "https://vcip.rblbank.com/user-status" and audit_id != "":
                        res= dynamodb_client.query(
                                TableName="vkyc_audit_status",
                                Select='SPECIFIC_ATTRIBUTES',
                                AttributesToGet=['audit_end_time'],
                                KeyConditions={
                                        'audit_id': {
                                            'AttributeValueList': [
                                                {
                                                        'S': audit_id
                                                }
                                            ],
                                            'ComparisonOperator': 'EQ'
                                        }
                                }
                            )
                        print(res["Items"])
                        audit_end_time = float(res['Items'][0]['audit_end_time']['N'])
                        # auditor_fdbk = res['Items'][0]['feedback']['S']
                    else:
                        audit_end_time = ""
                except Exception as e:
                    print(e)
                    traceback.print_exc()
                    audit_end_time = ""

            print("END Time",audit_end_time)

            agent_item = None
            agent_name = ""

            '''
            auditor_name = ""

            try:
                if audit_id != "":
                    res = dynamodb_client.query(TableName="vkyc_audit_status",Select='SPECIFIC_ATTRIBUTES',AttributesToGet=['admin_id'],KeyConditions={'audit_id': {'AttributeValueList': [{'S': audit_id}],'ComparisonOperator': 'EQ'}})
                    print(res["Items"])
                    auditor_name = res['Items'][0]['admin_id']['S']
            except:
                pass
            '''

            try:
                if referrer != "https://vcip.rblbank.com/user-status":
                    item = agent_table.query(KeyConditionExpression=Key("agent_id").eq(agent_id)&Key('client_name').eq(client_name))['Items'][0]
                    agent_item = item
                    agent_name = item["agent_name"]
            except Exception as e:
                print("failed at getting agent name",str(e))
                pass
            call_center = ""
            try:
                if referrer != "https://vcip.rblbank.com/user-status" and agent_item:
                    call_center = agent_item["call_center"]
            except Exception as e:
                # call_center = str(e)
                print("failed at getting agent call center name",str(e))

            '''
            try:
                item = agent_table.query(KeyConditionExpression=Key("agent_id").eq(auditor_name)&Key('client_name').eq(client_name))['Items'][0]
                auditor_name = item["agent_name"]
            except Exception as e:
                print("failed at getting agent name",str(e))
            '''

            if auditor_name == "":
                auditor_id = "-"
            else:
                auditor_id = auditor_name

            try:
                auditor_name = agents_name_dict[auditor_id]
            except:
                auditor_name = "-"

            dispositions_punched = ""

            try:
                #if referrer != "https://vcip.rblbank.com/user-status":
                manual_allocation_table = dynamodb.Table(MANUAL_ALLOCATION_TABLE)
                manual_allocation_data = manual_allocation_table.query(KeyConditionExpression=Key("user_id").eq(user_id)).get('Items')

                if manual_allocation_data and len(manual_allocation_data) > 0:
                    dispositions_punched = json.loads(manual_allocation_data[0].get('disposition_punched' ))
            except:
                pass

            if client_name in ["Retailasset"]:
                if ekyc_time == "1651343400":
                    ekyc_time = ""

            result_out = {
                'agent_id': agent_id,
                'skygee_time': skygee_time,
                'manual_agent': manual_agent,
                'fraud_advisory_given':fraud_advisory_given,
                'customer_name':customer_name,
                'agent_name':agent_name,
                'call_center':call_center,
                'ekyc_time':ekyc_time,
                'session_id': session_id,
                'auditor_fdbk': auditor_fdbk,
                'link_type':link_type_custom_info,
                'queue': queue,
                'phone_number':phone_number,
                'user_id': user_id,
                'agent_assignment_time': agent_assignment_time,
                'start_time': start_time,
                'end_time': end_time,
                'session_status': rbl_session_status,
                'TA_session_status': session_status,
                'auditor_name':auditor_name,
                'auditor_id':auditor_id,
                'summary_data': summary_data,
                'agent_screen_url': agent_screen_url,
                'agent_video_url': agent_video_url,
                'user_video_url': user_video_url,
                'aadhaar_image_url': aadhaar_image_url,
                'pan_image_url': pan_image_url,
                'selfie_image_url': selfie_image_url,
                'zip_url': zip_url,
                'dispositions_punched':dispositions_punched,
                "audit_id":audit_id,
                "case_push_time":case_push_time,
                'manual_agent_name':manual_agent_name,
                'audit_result': str(audit_result),
                'audit_end_time' : audit_end_time,
                'feedback': feedback,
                'product_code': product,
                'summary_pdf_url': summary_pdf_url,
                'vkyc_start_time':vkyc_start_time,
                'sfdc_time':sfdc_time,
                'sfdc_response':sfdc_response
            }

            final_out.append(result_out)
       

        c_time = time.time()
        pf_file.write("Time taken to do the parsing and conversion "+str(c_time-current_time)+"\n")
        current_time = c_time

        temp_df = pd.DataFrame(final_out)
        if temp_df.shape[0] > 0:
            # Existing statuses to skip
            statuses_to_skip = ["CHECKER_APPROVED", "CHECKER_REJECTED", "NOT_ASSIGNED_TO_AGENT", "session_expired", "VCIP_APPROVED"]


            temp_df.sort_values("start_time",inplace=True,ascending=False)
            # Update session_status for duplicated user_id except for the statuses in statuses_to_skip
            condition = temp_df.duplicated("user_id", keep="first") & ~temp_df["session_status"].isin(statuses_to_skip)
            temp_df.loc[condition, "session_status"] = "VCIP_SESSION_INVALID"

            #temp_df.loc[temp_df.duplicated("user_id", keep="first"), "session_status"] = "VCIP_SESSION_INVALID"
            # temp_df[temp_df.duplicated("user_id","first")]["session_status"] = "VCIP_SESSION_INVALID"
            
            if client_name in ["RBL_uat", "RBL","Retailasset_uat","Retailasset","Digiremit_uat","Digiremit"]:
                temp_df.loc[temp_df["session_status"] == "NOT_ASSIGNED_TO_AGENT", "session_status"] = "VCIP_PENDING"

            temp_df.loc[temp_df["session_status"] == "CHECKER_APPROVED", "session_status"] = "Audited and Okay"
            temp_df.loc[temp_df["session_status"] == "CHECKER_REJECTED", "session_status"] = "Audited and not okay"
            temp_df.loc[temp_df["session_status"] == "session_expired", "session_status"] = "VCIP_EXPIRED"

        final_out_n = temp_df.to_dict(orient="records")
        final_out_dict = {}

        for row in final_out_n:
            for key in row.keys():
                if isinstance(row[key], decimal.Decimal):
                    row[key] = float(row[key])

        final_out_dict.update({'session_data': final_out_n, 'status_code': 200,'server':'4','success': True})

        c_time = time.time()
        pf_file.write("Time taken to update some fields "+str(c_time-current_time)+"\n")
        current_time = c_time

        if requestc != None:
            return final_out_dict

        return jsonify(final_out_dict), 200
    except Exception as e:
        print ('Exception', e)
        traceback.print_exc()
        if requestc != None:
            return {"e":str(e)}
        return jsonify({
            'msg': 'Failed to update',
            'status_code': 500,
            'success': False}), 500


@bp.route('/getAgentsData/<client_name>', methods=['GET'])
@login_required(permission='usr')
def getAgentsData(client_name):
    '''
    This endpoint returns the agent related data
    '''
    #return "ok"
    print("getAgentsData = ", client_name)
    try:
        client_table = dynamodb.Table(BPO_STATE_COUNT_TABLE)
        try:
            manual_agent_list = client_table.query(KeyConditionExpression=Key('client').eq(client_name+"_vkyc"))["Items"][0]["manual_agent_list"]
        except Exception as e:
            manual_agent_list = str(e)

        try:
            if isinstance(manual_agent_list, set):
                manual_agent_list = str(list(manual_agent_list))
        except:
            pass

        manual_agent_list = []

        client_table = dynamodb.Table(getPOCTableName('Agent'))
        data = client_table.query(
            IndexName='client_name-index',
            KeyConditionExpression=Key('client_name').eq(client_name)
        )

        items = data.get('Items')
        final_out = []
        for index, item in enumerate(data["Items"]):
            try:
                agent_id = item["agent_id"]
            except Exception as e:
                agent_id = ""
            try:
                call_center = item["call_center"]
            except Exception as e:
                call_center = "dummy"
            try:
                session_id = item["session_id"]
            except Exception as e:
                session_id = ""
            try:
                agent_mode = item['agent_mode']
            except:
                agent_mode = ""
            try:
                is_admin = bool(item["IS_ADMIN"])
            except:
                is_admin = False
            try:
                if item['IS_HALTED'] == "1":
                    agent_status = 'halted'
                else:
                    agent_status = item["agent_status"]
            except Exception as e:
                agent_status = ""
            try:
                user_id = item["user_id"]
            except Exception as e:
                user_id = "",
            try:
                last_active_timestamp = item["last_active_timestamp"]
            except Exception as e:
                last_active_timestamp = ""

            try:
                is_logged_in = str(item["login"])
            except:
                is_logged_in = "0"

            try:
                agent_role = item['agent_role']
            except:
                agent_role= "both"

            try:
                agent_name = item['agent_name']
            except:
                agent_name = ""
            try:
                ld  = item['IS_LDAP']
                login_method = "LDAP"
            except:
                login_method = "NATIVE"

            try:
                is_locked = int(item['login_lock'])
            except:
                is_locked = 0

            try:
                is_registered = int(str(item['register']))
            except Exception as e:
                is_registered = 0

            result_out = {
                'agent_id': agent_id,
                'agent_name':agent_name,
                'login_method':login_method,
                'agent_mode':agent_mode,
                'session_id': session_id,
                'agent_role':agent_role,
                'is_logged_in':is_logged_in,
                'agent_status': agent_status,
                'user_id': user_id,
                'is_admin': is_admin,
                'last_active_timestamp': last_active_timestamp,
                'is_locked' : is_locked,
                "call_center" : call_center,
                "is_registered":is_registered
            }

            final_out.append(result_out)
        final_out_dict = {}
        final_out_dict.update({'data': final_out,"outbound_agent_list":manual_agent_list, 'status_code': 200,
                               'success': True})
        return jsonify(final_out_dict), 200

    except Exception as e:
        print ('Exception -> ', e)
        return jsonify({
            'e':str(e),
            'msg': 'database error',
            'status_code': 500,
            'success': False}), 500


@bp.route('/getSettingsData/<client_name>/<environment>', methods=['GET'])
@login_required(permission='usr')
def getSettingsData(client_name, environment):
    print ("Env", environment, client_name)
    print(client_name+'_'+environment)
    try:
        client_table = dynamodb.Table('ipv_settings_data')
        data = client_table.query(
            KeyConditionExpression=Key('client_name').eq(
                client_name+'_'+environment)
        )
        items = data.get('Items')
        print (items)
        result_data = ""
        for index, item in enumerate(data["Items"]):
            try:
                table_data = item['settings_data']
                for data in table_data:
                    print("466", type(data))
                    for key in data.keys():
                        if isinstance(data[key], decimal.Decimal):
                            data[key] = float(data[key])

                result_data = table_data
            except Exception as e:
                print(e)
                result_data = ""
        print ("468", result_data)
        return jsonify({'settings_data': result_data, 'status_code': 200,
                        'success': True}), 200
    except Exception as e:
        print("472", e)
        return jsonify({
            'msg': 'Failed to update',
            'status_code': 500,
            'success': False}), 500


@bp.route('/saveSettingsData/<client_name>/<environment>', methods=['POST'])
@login_required(permission='usr')
def saveSettingsData(client_name, environment):
    content = request.json
    update_data = content["data"]
    print ("Env", environment, client_name, update_data)
    try:
        client_table = dynamodb.Table('ipv_settings_data')
        res = client_table.update_item(
            Key={
                'client_name': client_name+'_'+environment
            },
            UpdateExpression="set settings_data=:d",
            ExpressionAttributeValues={
                ':d': update_data,
            },
            ReturnValues="UPDATED_NEW"
        )
        final_out_dict = {}
        final_out_dict.update({'data': update_data, 'status_code': 200,
                               'success': True})
        return jsonify(final_out_dict), 200
    except Exception as e:
        print (e)
        return jsonify({
            'msg': 'Failed to update',
            'status_code': 500,
            'success': False}), 500


def get_table_data(table_object, primary_key_name, primary_key_value, sort_key_name=None, sort_key_value=None):
    if sort_key_name:
        data = table_object.query(KeyConditionExpression=Key(primary_key_name).eq(
            primary_key_value) & Key(sort_key_name).eq(sort_key_value))
    else:
        data = table_object.query(KeyConditionExpression=Key(
            primary_key_name).eq(primary_key_value))
    return data


@bp.route('/admin/generate_token', methods=["POST"])
def generate_token():

    data = request.get_json()
    print (data)
    try:
        login_username = data['username']
    except Exception as e:
        print("Exception = ", str(e))

        return jsonify({
            'msg': 'please provide valid username and password',
            "status_code": 400,
            "success": True}), 400

    try:
        login_password = data['password']
    except Exception as e:
        print("Exception = ", str(e))

        return jsonify({
            'msg': 'please provide valid username and password',
            "status_code": 400,
            "success": True}), 400
    print (login_username, login_password)
    client_credentials_table = dynamodb.Table(CLIENT_TABLE)

    try:
        client_data = client_credentials_table.query(
            KeyConditionExpression=Key('ClientID').eq(login_username))["Items"][0]
        print (client_data)
    except Exception as e:
        print("Exception dfgdf = ", str(e))
        print("Login error {}".format(e))
        return jsonify({
            'msg': 'username/password did not match',
            "status_code": 401,
            "success": True}), 401

    if client_data:
        if client_data['Secret'] != login_password:
            return jsonify({
                'msg': 'username/password did not match',
                "status_code": 401,
                "success": True}), 401

        else:

            response = create_token(login_username)
            print("new token generated")

            return jsonify({
                "Token": response,
                "status_code": 200,
                "success": True}), 200
    else:
        # wrong username
        return jsonify({
            'msg': 'username/password did not match',
            "status_code": 401,
            "success": True}), 401


@bp.route('/admin/register', methods=['POST'])
@login_required(permission='usr')
def admin_register():
    '''
    This endpoint is used to register a new Admin id.
    If the passed id already exists, then we change its register flag to 1.
    Else, we make a new entry in the agent table,with the register flag as 1.
    '''
    client_name = g.user_id
    print("client_name", client_name)

    data = request.get_json()

    try:
        admin_id = data['admin_id']
    except:
        return jsonify({"msg": "missing parameter 'agent_id'", "status": 500}), 500

    try:
        admin_access = data['admin_access']
    except:
        admin_access = 'super_admin'

    is_ldap = False
    try:
        is_ldap = data["is_ldap"]
        admin_password = "Agfc#$%&ghjxpq"
    except:
        is_ldap = False
    try:
        admin_password = data['password']
        if not is_ldap:
            pat=re.compile("[A-Za-z@#$%^&+=_]{6,}")
            if re.search(pat,admin_password):
                pass
            else:
                print("password doesnt match regex [A-Za-z@#$%^&+=_]{6,}")
                return jsonify({"msg": "password doesnt match with required format", "status": 503}), 503
    except:
        return jsonify({"msg": "missing parameter 'password'", "status": 500}), 500
    is_ldap = False
    try:
        is_ldap = data["is_ldap"]
        admin_password = "Agfc#$%&ghjxpq"
    except:
        is_ldap = False

    try:
        agent_table = dynamodb.Table(agent_status_table)
    except Exception as e:
        return jsonify({
            'msg': 'database error',
            'status_code': 500,
            'success': False}), 500

    try:
        print("table name is {}".format(agent_table))
        response = agent_table.query(KeyConditionExpression=Key(
            'agent_id').eq(admin_id) & Key('client_name').eq(client_name))
    except Exception as e:
        print("resopnse data error", e)
        pass

    items = response.get('Items')
    if not items:
        try:
            new_admin_data = {
                'agent_id': admin_id,
                'client_name': client_name,
                'register': 1,
                'secret': admin_password,
                'login': 0,
                'IS_ADMIN': 1,
                'admin_access': admin_access
            }
            if is_ldap:
                new_admin_data.update({"IS_LDAP":True})
            response = agent_table.put_item(Item=new_admin_data)

            return jsonify({
                "msg": "Admin Registered Successfully",
                'status_code': 200,
                'success': False}), 200

        except Exception as e:
            return jsonify({
                'msg': 'Failed to write to database',

                'status_code': 500,
                'success': False}), 500
    else:
        item = items[0]

        if item["register"] == 0:

            try:
                response = agent_table.update_item(
                    Key={
                        'agent_id': admin_id,
                        'client_name': client_name
                    },
                    UpdateExpression="set register = :r, IS_ADMIN = :ih",
                    ExpressionAttributeValues={':r': 1, ':ih': 1},
                    ReturnValues="ALL_NEW"
                )
            except Exception as e:
                return jsonify({
                    'msg': 'Failed to update',

                    'status_code': 500,
                    'success': False}), 500

            return jsonify({
                "msg": "Admin Re-Registered Successfully",
                'status_code': 200,
                'success': False}), 200

        return jsonify({
            "msg": "All ready Registered Admin",
            'status_code': 200,
            'success': True}), 200


@bp.route('/admin/deregister/<admin_id>', methods=['POST'])
@login_required(permission='usr')
def admin_deregister(admin_id):
    '''
        This endpoint is used to deregister the admin.
        We simply set the register flag to 0.
    '''
    client_name = g.user_id

    try:
        agent_table = dynamodb.Table(agent_status_table)
    except Exception as e:
        return jsonify({
            'msg': 'database error',
            'status_code': 500,
            'success': False}), 500

    try:
        response = agent_table.query(KeyConditionExpression=Key(
            'agent_id').eq(admin_id) & Key('client_name').eq(client_name))
    except Exception as e:
        return jsonify({
            'msg': 'Failed to search in database',
            'status_code': 500,
            'success': False}), 500

    items = response.get('Items')
    if not items:
        return jsonify({
            "msg": "Agent not Present",
            'status_code': 404,
            'success': False}), 404
    else:
        item = items[0]

        # To logout agent if its login at the time of registration
        admin_logout(admin_id)

        if item["register"] == 1:
            try:
                response = agent_table.update_item(
                    Key={
                        'agent_id': admin_id,
                        'client_name': client_name
                    },
                    UpdateExpression="set register = :r, login = :r",
                    ExpressionAttributeValues={':r': 0},
                    ReturnValues="ALL_NEW"
                )
            except Exception as e:
                return jsonify({
                    'msg': 'Failed to update',

                    'status_code': 500,
                    'success': False}), 500
            return jsonify({
                "msg": "Agent De-Registered Successfully",
                'status_code': 200,
                'success': True}), 200
        return jsonify({
            "msg": "Agent All ready De-Registered",
            'status_code': 200,
            'success': True}), 200

@bp.route('/admin/login', methods=['POST'])
# @login_required(permission='usr')
def admin_login():
    '''
        This endpoint is used to login an admin into the admin panel.
        The request data contains the admin_username and admin_password.
        client name comes in the request data.
    '''
    data = request.get_json()

    print("data", data)
    admin_id = data['admin_id']
    try:
        isEncrypted = int(data['isEncrypted'])
    except Exception as e:
        isEncrypted = 0
    admin_password =  data['admin_password'] if not isEncrypted  else decrypt_string(data['admin_password'])
    #admin_password = data['admin_password']
    client_name = data['domain_name']

    '''
        Authorization token is fetched from the headers and if not found, we create a new one.
    '''
    try:
        auth_token = request.headers['auth']
    except:
        auth_token = create_token(client_name)

    try:
        agent_table = dynamodb.Table(agent_status_table)
    except Exception as e:
        return jsonify({
            'msg': 'database error',
            'status_code': 500,
            'success': False}), 500

    try:
        response = agent_table.query(KeyConditionExpression=Key(
            'agent_id').eq(admin_id) & Key('client_name').eq(client_name))
    except Exception as e:
        print(e)
        return jsonify({
            'msg': 'Failed to search in database',

            'status_code': 500,
            'success': False}), 500

    items = response.get('Items')
    if not items:
        return jsonify({
            "msg": "Agent not Present",
            'status_code': 404,
            'success': False}), 404
    else:
        item = items[0]
        if "IS_ADMIN" not in item:
            return jsonify({"msg": "not admin",'status_code': 404,'success': False}), 404
        try:
            if str(item["IS_ADMIN"]) == "0":
                return jsonify({"msg": "not admin",'status_code': 404,'success': False}), 404
        except Exception as e:
            return jsonify({"e":str(e)}),404
            pass
        ################### maintain logs ##########################
        if client_name in ['FINO','think_dev']:
            try:
                agent_logs_table = dynamodb.Table(kwikid_agent_logs_table)
            except Exception as e:
                print(e)
                return jsonify({
                    'msg': 'database error',
                    'status_code': 500,
                    'success': False}), 500


            try:
                log=dict()
                log['client_name']= client_name
                log['agent_id']=admin_id
                try:
                    log['agent_name'] = item['agent_name']
                except:
                    log['agent_name']= ""

                try:
                    log['agent_role'] = item['agent_role']
                except:
                    log['agent_role'] = ""

                try:
                    log['IS_ADMIN'] = int(item['IS_ADMIN'])
                except:
                    log['IS_ADMIN'] = ""
                log['action'] = "login"
                log['portal'] = "admin"
                log['updated_timestamp'] =str(int(time.time()))
            
                response = agent_logs_table.put_item(Item=log)

            except Exception as e:
                print("Failed to updated agent action logs : ",str(e))
                pass
        ############################################################


        if client_name in ["FINO","think_dev","GHFL_VKYC_prod"]:
            try:
                is_first_login = item['is_first_login']
            except:
                is_first_login = 1
        else:
            is_first_login = 0 #for other client who donot need the contition

        if item['secret'] != admin_password:
            if client_name in ['FINO',"think_dev"]: #for login lock and login_attemps
                if 'login_attempts' not in item:
                    # response = update_table(agent_table, 'agent_id', admin_id, "set login_attempts = :la", {':la': 1 },'client_name', client_name)
                    agent_table.update_item(Key={'agent_id': admin_id, 'client_name': client_name},UpdateExpression="set login_attempts = :la", ExpressionAttributeValues={':la': 1}, ReturnValues="ALL_NEW")
                    #send_logs
                    return jsonify({"msg": "wrong password", 'status': 500}), 500
                
                elif item['login_attempts'] < 3 :
                    login_attempts = item['login_attempts'] + 1
                    #update table

                    # response = update_table(agent_table, 'agent_id', admin_id, "set login_attempts = :la", {':la': login_attempts },'client_name', client_name)
                    agent_table.update_item(Key={'agent_id': admin_id, 'client_name': client_name},UpdateExpression="set login_attempts = :la", ExpressionAttributeValues={':la': login_attempts}, ReturnValues="ALL_NEW")
                    
                    #send_logs
                    return jsonify({"msg": "wrong password", 'status': 500}), 500
                else:
                    #lock agent account
                    # response = update_table(agent_table, 'agent_id', admin_id, "set login_lock = :ll", {':ll': 1 },'client_name', client_name)
                    agent_table.update_item(Key={'agent_id': admin_id, 'client_name': client_name},UpdateExpression="set login_lock = :ll", ExpressionAttributeValues={':ll': 1}, ReturnValues="ALL_NEW")

                    return jsonify({"msg": "maximum attempts for login exceeded ,your account has been locked , please contact admin" ,"status" : 401 }),401
            else:
                if item['secret'] != admin_password:
                    if "IS_LDAP" in item:
                        resp = rbl_esb_details({"UserId":admin_id,"LoginPwd":admin_password,"ChlId":"ECH"})
                        print("resp from LDAP:",resp)
                        if resp:
                            pass
                        else:
                            if 'login_attempts' not in item:
                                agent_table.update_item(Key={'agent_id': admin_id, 'client_name': client_name},UpdateExpression="set login_attempts = :la", ExpressionAttributeValues={':la': 1}, ReturnValues="ALL_NEW")
                            elif item['login_attempts'] < 3 :
                                login_attempts = item['login_attempts'] + 1
                                agent_table.update_item(Key={'agent_id': admin_id, 'client_name': client_name},UpdateExpression="set login_attempts = :la", ExpressionAttributeValues={':la': login_attempts}, ReturnValues="ALL_NEW")
                            else:
                                agent_table.update_item(Key={'agent_id': admin_id, 'client_name': client_name},UpdateExpression="set login_lock = :ll", ExpressionAttributeValues={':ll': 1}, ReturnValues="ALL_NEW")
                                return jsonify({"msg": "maximum attempts for login exceeded ,your account has been locked , please contact admin" ,"status" : 401}),401

                            return jsonify({"msg": "wrong password", 'status': 500}), 500
                    else:
                        if 'login_attempts' not in item:
                            agent_table.update_item(Key={'agent_id': admin_id, 'client_name': client_name},UpdateExpression="set login_attempts =:la", ExpressionAttributeValues={':la': 1}, ReturnValues="ALL_NEW")
                        elif item['login_attempts'] < 3 :
                            login_attempts = item['login_attempts'] + 1
                            agent_table.update_item(Key={'agent_id': admin_id, 'client_name': client_name},UpdateExpression="set login_attempts =:la", ExpressionAttributeValues={':la': login_attempts}, ReturnValues="ALL_NEW")
                        else:
                            agent_table.update_item(Key={'agent_id': admin_id, 'client_name': client_name},UpdateExpression="set login_lock = :ll", ExpressionAttributeValues={':ll': 1}, ReturnValues="ALL_NEW")
                            return jsonify({"msg": "maximum attempts for login exceeded ,your account has been locked , please contact admin" ,"status" : 401}),401
                        return jsonify({"msg": "wrong password", 'status': 500}), 500

        if item["register"] == 0:
            return jsonify({
                "msg": "Agent De-Registered, can't login",
                'status_code': 404,
                'success': False}), 404

        is_pass_outdated=0
        
        try:
            last_pass_updated = item['last_pass_updated']
        except:
            last_pass_updated = None

        if client_name in ['FINO','think_dev',"Digiremit","RBL"]:
            if 'login_lock' in item and item['login_lock']!=None:
                if item['login_lock'] == 1 :
                    return jsonify({
                        "msg": "account is locked please contact admin",
                        "status": 401
                        }),500
                else:
                    pass
            if 'last_pass_updated' in item and item['last_pass_updated']!=None:
                if int(item['last_pass_updated']) <= int(time.time()) - 45*24*60*60:
                    is_pass_outdated = 1
        ##RBL give access to only specific tabs based on admin access/role
        try:
            admin_access=item['admin_access']
        except:
            admin_access='super_admin'
        if item["login"] == 0:

            try:
                if client_name in ['FINO','think_dev']: 
                    response = agent_table.update_item(Key={'agent_id': admin_id, 'client_name': client_name},
                                                 UpdateExpression="set agent_status = :r, login = :l, login_attempts = :la , is_first_login = :ifl", ExpressionAttributeValues={':r': 'waiting', ':l': 1, ':la': 0 ,':ifl' : is_first_login}, ReturnValues="ALL_NEW")
                else:
                    response = agent_table.update_item(Key={'agent_id': admin_id, 'client_name': client_name},
                                                   UpdateExpression="set agent_status = :r, login = :l", ExpressionAttributeValues={':r': 'waiting', ':l': 1}, ReturnValues="ALL_NEW")
            except Exception as e:
                print(e)
                return jsonify({
                    'msg': 'Failed to update',
                    'status_code': 500,
                    'success': False}), 500

            response = {
                "msg": "Admin Logged-In Successfully",
                'status_code': 200,
                'token': auth_token,
                'is_first_login':int(is_first_login),
                'is_pass_outdated':is_pass_outdated,
                'last_pass_updated':last_pass_updated,
                'admin_access':admin_access,
                'success': True
            }

            print("response ", response)
            return jsonify(response), 200

        response = {
            "msg": "Admin All ready Logged-In",
            'token': auth_token,
            'status_code': 200,
            'is_first_login': int(is_first_login),
            'is_pass_outdated':is_pass_outdated,
            'last_pass_updated':last_pass_updated,
            'admin_access':admin_access,
            'success': True}

        print("response = ", response)
        return jsonify(response), 200

@bp.route('/admin/logout/<admin_id>', methods=['POST'])
@login_required(permission='usr')
def admin_logout(admin_id):
    '''
    This endpoint is used to logout the admin from the admin portal.
    We simply set the login flag value to 0.
    '''
    print("request headers !!!", request.headers)
    client_name = g.user_id

    try:
        agent_table = dynamodb.Table(agent_status_table)
    except Exception as e:
        return jsonify({
            'msg': 'database error',
            'status_code': 500,
            'success': False}), 500

    try:
        response = agent_table.query(KeyConditionExpression=Key(
            'agent_id').eq(admin_id) & Key('client_name').eq(client_name))
    except Exception as e:
        return jsonify({
            'msg': 'Failed to search in database',
            'status_code': 500,
            'success': False}), 500

    items = response.get('Items')
    if not items:
        return jsonify({
            "msg": "Admin not Present",
            'status_code': 404,
            'success': False}), 404
    else:
        item = items[0]

        ################### maintain logs ##########################
        if client_name in ['FINO','think_dev']:
            try:
                agent_logs_table = dynamodb.Table(kwikid_agent_logs_table)
            except Exception as e:
                return jsonify({
                    'msg': 'database error',
                    'status_code': 500,
                    'success': False}), 500


            try:
                log=dict()
                log['agent_id']=admin_id
                try:
                    log['agent_name'] = item['agent_name']
                except:
                    log['agent_name']= ""

                try:
                    log['agent_role'] = item['agent_role']
                except:
                    log['agent_role'] = ""

                try:
                    log['IS_ADMIN'] = int(item['IS_ADMIN'])
                except:
                    log['IS_ADMIN'] = ""
                log['action'] = "logout"
                log['portal'] = "admin"
                log['updated_timestamp'] =str(int(time.time()))
            
                response = agent_logs_table.put_item(Item=log)

            except Exception as e:
                print("Failed to updated agent action logs : ",str(e))
                pass
        ############################################################

        try:
            task_id = item["task_id"]
        except Exception as e:
            task_id = ""
        try:
            agent_status = item["agent_status"]
        except Exception as e:
            agent_status = ""
        isMicrosoft = False
        if "isMicrosoft" in item:
            isMicrosoft = True
        

        if item["register"] == 0:
            return jsonify({
                "msg": "Admin De-Registered, can't process",
                'status_code': 404,
                'success': False}), 404

        if item["login"] == 1:
            try:
                response = agent_table.update_item(Key={'agent_id': admin_id, 'client_name': client_name},
                                                   UpdateExpression="REMOVE agent_status set login = :r", ExpressionAttributeValues={':r': 0}, ReturnValues="ALL_NEW")
            except Exception as e:
                print("Exceptoin = "+str(e))
                return jsonify({
                    'msg': 'Failed to update',
                    'status_code': 500,
                    'success': False}), 500
            return jsonify({
                "msg": "Agent Logged-Out Successfully",
                "isMicrosoft":isMicrosoft,
                'status_code': 200,
                'success': True}), 200
    return jsonify({
        "msg": "Agent All ready Logged-Out",
        'isMicrosoft':isMicrosoft,
        'status_code': 200,
        'success': True}), 200





@bp.route('/admin/get_audit_report/<audit_id>')
@login_required(permission='usr')
def get_audit_report(audit_id):
    '''
        This endpoint is used to get the audit report of an audit id.
        All the audit related data, like 
            - check_list
            - audit_result
            - feedback
            - audit start time
            - audit end time
            - admin id 
        are sent back as the response.
    '''
    audit_table = dynamodb.Table(audit_status_table)

    try:    
        audit_data = audit_table.query(KeyConditionExpression=Key('audit_id').eq(audit_id))['Items'][0]
    except Exception as e:
        return jsonify({"msg":"No data available for {}".format(audit_id),"status":500}),500


    print(audit_data.keys())
    try:
        check_list_data = audit_data['checklist']
    except:
        check_list_data = None
    try:
        audit_result =  str(audit_data['audit_result'])
    except:
        audit_result = None
    try:    
        feedback =  str(audit_data['feedback'])
    except:
        feedback = None

    try:
        audit_init_time = str(audit_data['audit_init_time'])
    except:
        audit_init_time = None

    try:
        audit_end_time = str(audit_data['audit_end_time'])
    except:
        audit_end_time = None
    
    auditor =  audit_data['admin_id']

    return jsonify({"auditor":auditor,"feedback":feedback,"checklist_data":check_list_data,"audit_result":audit_result,"audit_init_time":audit_init_time,"audit_end_time":audit_end_time,"status":200}),200



@bp.route('/get_agent_sessions/<agent_id>',methods=['POST'])
@login_required(permission='usr')
def get_agent_sessions(agent_id):
    '''
        This endpoint returns all the sessions performed during the provided timestamp range , taken by the particular agent_id .
    '''
    if not agent_id:
        return jsonify({"msg":"Empty agent_id","status":500}),500
    
    print("get_agent_sessions Called for {}".format(agent_id))
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg":"No request data fround","status":500}),500

    try:
        start_time = request_data['start_time']
        end_time = request_data['end_time']
    except:
        return jsonify({"msg":"missing start_time/end_time keyword","status":500}),500

    paginator = dynamodb_client.get_paginator('query')

    data=paginator.paginate(
        TableName=getPOCTableName('Session Status'),
        IndexName="agent_id-start_time-index",
        KeyConditions={
                'agent_id': {
                    'AttributeValueList': [
                        {
                            'S': agent_id
                        }
                    ],
                    'ComparisonOperator': 'EQ'
                },
                'start_time': {
                    'AttributeValueList': [
                        {
                            'S': start_time
                        },
                        {
                            'S': end_time
                        }
                    ],
                    'ComparisonOperator': 'BETWEEN'
                }
            }
    )

    res_items = []

    for page in data:
        print(len(page['Items']))
        res_items+=page['Items']

    sessions = []
    for session_data in res_items:
        session_dict={}
        session_dict['session_id']=session_data['session_id']['S']
        session_dict['user_id']=session_data['user_id']['S']
        session_dict['mobile_number']=session_data['phone_number']['S']
        session_dict['session_status']=session_data['session_status']['S']
        try:
            session_dict['audit_status']=session_data['audit_result']['S']
        except:
            try:
                session_dict['audit_status']=session_data['audit_result']['N']
            except:
                session_dict['audit_status']="Un-audited"

        sessions.append(session_dict)

    return jsonify({"sessions":sessions,"status":200}),200



@bp.route('/delete_session/<session_id>')
@login_required(permission="usr")
def delete_session_entry(session_id):
    '''
    This endpoint is used to delete a session entry from the database.
    '''
    session_table = dynamodb.Table(session_status_table)
    try:
        #deleting entry
        session_table.delete_item(Key={"session_id":session_id})
    except Exception as e:
        return jsonify({"msg":"error","error":e}),500
    return jsonify({"msg":"done","status":200}),200

@bp.route('/config/get/<get_key>', methods = ['POST'] )
def get_frontend_config_all(get_key):
    '''
    This endpoint is used to get the frontend configuration for the client.
    '''
    client_name = None
    config_key = request.json['config_key']

    mapping_file = "CONFIG/{0}/mapping.json".format(get_key.upper())
    print("mapping file: {}".format(mapping_file))
    #return mapping_file
    #r = requests.get(url=mapping_file)
    data = get_s3_file(s3,S3_BUCKET,mapping_file)
    mapping = json.loads(data)
    mapping =mapping['mapping']
    for key,value in mapping.items():
        if config_key == key:
            client_name = key
            break
        if config_key in value:
            client_name = key
            break
    if client_name is None:
        return jsonify({"msg":'invalid config_key', "status": 200,"dta":json.dumps(data),"mf":str(mapping_file),"bucket":str(S3_BUCKET)}),200

    agent_config_url ="CONFIG/{0}/{1}/{2}Config.json".format(get_key.upper(),client_name,get_key.lower()) 
    try:
        #r = requests.get(url=agent_config_url)
        data=get_s3_file(s3,S3_BUCKET,agent_config_url)
        return jsonify({"config":json.loads(data),"status":200}),200
    except Exception as e:
        return jsonify({"msg":"url error","status":500,"error":str(e),"u":agent_config_url}),500

#@bp.route('/config/get/admin', methods = ['POST'])
# @login_required(permission='usr')
def get_frontend_config_admin():
    # client_name = g.user_id
    '''
    This endpoint is used to get the admin portal frontend configurations.
    '''
    client_name = None
    config_key = request.json['config_key']

    mapping_file = "https://kwikid.s3.ap-south-1.amazonaws.com/CONFIG/ADMIN/mapping.json"
    r = requests.get(url=mapping_file)
    data = r.content
    mapping = json.loads(data)
    mapping =mapping['mapping']
    for key,value in mapping.items():
        if config_key == key:
            client_name = key
            break
        if config_key in value:
            client_name = key
            break
    if client_name is None:
        return jsonify({"msg":'invalid config_key', "status": 200}),200

    admin_config_url ="https://kwikid.s3.ap-south-1.amazonaws.com/CONFIG/ADMIN/{}/adminConfig.json".format(client_name.lower())
    try:
        r = requests.get(url=admin_config_url)
        data=r.content
        print(type(data))
        return jsonify({"config":json.loads(data),"status":200}),200
    except Exception as e:
        return jsonify({"msg":"url error","status":500,"error":e}),500


#@bp.route('/config/get/user', methods = ['POST'])
# @login_required(permission='usr')
def get_frontend_config_user():
    '''
    This endpoint is used to get the frontend configuration for the user portal.
    '''
    client_name = None
    config_key = request.json['config_key']

    mapping_file = "https://kwikid.s3.ap-south-1.amazonaws.com/CONFIG/USER/mapping.json"
    r = requests.get(url=mapping_file)
    data = r.content
    mapping = json.loads(data)
    mapping =mapping['mapping']
    for key,value in mapping.items():
        if config_key == key:
            client_name = key
            break
        if config_key in value:
            client_name = key
            break
    if client_name is None:
        return jsonify({"msg":'invalid config_key', "status": 200}),200
    user_config_url =" https://kwikid.s3.ap-south-1.amazonaws.com/CONFIG/USER/{}/userConfig.json".format(client_name)
    try:
        r = requests.get(url=user_config_url)
        data=r.content
        print(type(data))
        return jsonify({"config":json.loads(data),"status":200}),200
    except Exception as e:
        return jsonify({"msg":"url error","status":500,"error":str(e)}),500

@bp.route('/config/set/agent',methods=['POST'])
@login_required(permission='usr')
def set_frontend_config_agent():
    '''
    Set agent portal frontend configurations.

    The whole configuration is sent in the request data as JSON, which is then written and then dumped into S3.
    '''
    try:
        client_name = g.user_id
        config_data = request.get_json()['config']

        bucket = "rbl-uat-kwikid.vkyc"
        agent_config_key = "CONFIG/AGENT/{}/agentConfig.json".format(client_name)
        bytesIO= BytesIO()
        bytesIO.write(json.dumps(config_data))
        bytesIO.seek(0)
        s3.put_object(Bucket=bucket,Body=bytesIO.read(),Key=agent_config_key)#,ExtraArgs={"ACL":"public-read"})
        return jsonify({"success":True,"status":200})

    except Exception as e:
        print(e)
        traceback.print_exc()
        return str(e) 


@bp.route('/config/set/user',methods=['POST'])
@login_required(permission='usr')
def set_frontend_config_user():
    '''
    Set user portal frontend configurations.

    The whole configuration is sent in the request data as JSON, which is then written and then dumped into S3.
    '''
    client_name = g.user_id
    config_data = request.get_json()['config']
    
    bucket = "rbl-uat-kwikid.vkyc"
    user_config_key = "CONFIG/USER/{}/userConfig.json".format(client_name)
    bytesIO= BytesIO()
    bytesIO.write(json.dumps(config_data))
    bytesIO.seek(0)
    s3.put_object(Bucket=bucket,Body=bytesIO.read(),Key=user_config_key)#,ExtraArgs={'ACL': "public-read"})
    
    return jsonify({"success":True,"status":200})
    
@bp.route('/config/set/admin',methods=['POST'])
@login_required(permission='usr')
def set_frontend_config_admin():
    '''
    Set admin portal frontend configurations.

    The whole configuration is sent in the request data as JSON, which is then written and then dumped into S3.
    '''
    client_name = g.user_id
    config_data = request.get_json()['config']
    
    bucket = "kwikid"
    admin_config_key = "CONFIG/ADMIN/{}/adminConfig.json".format(client_name)
    bytesIO= BytesIO()
    bytesIO.write(json.dumps(config_data))
    bytesIO.seek(0)
    s3.put_object(Bucket=bucket,Body=bytesIO.read(),Key=admin_config_key)#,ExtraArgs={'ACL': "public-read"})
    
    return jsonify({"success":True,"status":200})

@bp.route('/create_client_credentials/<client_name>', methods = ['POST'])
def create_client_credentials(client_name):
    '''
    This endpoint is used to create credentials for a new client.
    '''
    client_name = client_name
    password = client_name
    try:
        print("\n\nCreating credentials")
        status=createCredentials(client_name, password)
        if status=="exist":
            response = jsonify({"message": "client already exist",
                                "data": []})
            response.status_code = 500
            return response

        print("map client data")
        try:
            config_data = request.get_json()['config']
            map_result=map_client_details(client_name,config_data)
            if map_result == 0:
                response = jsonify({"message": "Issue while saving config data"
                            })
                response.status_code = 500
                return response

        except:
            pass 

        print("\n\nCreating entry in ipv_bpo_state")
        make_entry_in_bpo_state_table(client_name)
        print("\n\nMaking Queue")
        make_queue(client_name)
        print("\n\n creating Agents")
        number_of_agents = 20
        try:
            data = request.json
            if data['number_of_agents']:
                number_of_agents = int(data['number_of_agents'])
        except:
            pass
        print(number_of_agents)
        agents = createAgents(client_name, number_of_agents)
        response = jsonify({"message": "credentials created successfully",
                            "data": agents})
        response.status_code = 200
        return response
    except:
        response = jsonify({"message": "issue while creating credentials",
                            "data": []})
        response.status_code = 200
        return response



def update_table(table_name,primary_key_name,primary_key_value,update_dict):
    table = dynamodb.Table(table_name)        
    print("in save function")
    update_expression = 'SET {}'.format(','.join('#{0}=:{1}'.format(k,k) for k in update_dict))
    expression_attribute_values = {':{0}'.format(k): v for k, v in update_dict.items()}
    expression_attribute_names = {'#{0}'.format(k): k for k in update_dict}
    #print(update_expression)
    #print(expression_attribute_values)
    #print(expression_attribute_names)

    response = table.update_item(Key={primary_key_name: primary_key_value,},
                                 UpdateExpression=update_expression,
                                 ExpressionAttributeValues=expression_attribute_values,
                                 ExpressionAttributeNames=expression_attribute_names,
                                 ReturnValues='UPDATED_NEW',)
    print(response)
    return response

def upload_s3_file(s3_obj,bucket,key,file_object,to_encrypt = False):
    '''
    We are passing the boto3 s3 object here to differentiate between Think's/Client's s3
    Rather than a file path, this file upload function accepts a file object(a BytesIO buffer)
    '''
    s3_obj.put_object(Body=file_object, Bucket=bucket, Key=key)
    try:
        s3_obj.put_object_acl(ACL="public-read",Bucket=bucket,Key=key)
    except Exception as e:
        print("error while setting the file to public mode",e)
        pass


def fetch_row(table_name,primary_key_name,primary_key_value):
    table = dynamodb.Table(table_name)
    response = table.query(KeyConditionExpression=Key(primary_key_name).eq(primary_key_value))
    if response["Count"] == 1:
        for key in response['Items'][0].keys():
            if isinstance(response['Items'][0][key], decimal.Decimal):
                response['Items'][0][key] = float(response['Items'][0][key])
        return response['Items'][0]
    else:
        raise Exception("not found")

#use eval to convert to dict
@bp.route("/backendConfig/set", methods=['POST'])
@login_required(permission='usr')
def backend_configure():
    client_name = g.user_id
    content = request.json
    print(content)
    #content = request.form
    #content = content.to_dict(flat=True)
    #print(content)
    
    #agent_config_json = json.loads(content.get("agent_config",None))
    #user_config_json = json.loads(content.get("user_config",None))
    #bucket_name= "kwikid"

    #if agent_config_json:
    #    key = "Agent/{client_name}/agentConfig.json".format(client_name=client_name)
    #    buff = BytesIO()
    #    buff.write(json.dumps(agent_config_json).encode('utf-8'))
    #    buff.seek(0)
    #    upload_s3_file(s3,bucket_name,key,buff.read().decode('utf-8'))
    #if user_config_json:
    #    key = "User/{client_name}/userConfig.json".format(client_name=client_name)
    #    buff = BytesIO()
    #    buff.write(json.dumps(user_config_json).encode('utf-8'))
    #    buff.seek(0)
    #    upload_s3_file(s3,bucket_name,key,buff.read().decode('utf-8'))
    

    #try:
    #    update_dict = {'link_url':content['link_url'],'scheduled_link_expiry':content['scheduled_link_expiry'],'VKYC_link_expiry':content['VKYC_link_expiry'],'VPD_link_expiry':content['VPD_link_expiry'],'link_type':content['link_type'],'user_abandon_timer':content['user_abandon_timer'],'send_sms_schedule':content['send_sms_schedule'],'send_sms_reminder':content['send_sms_reminder']}
    #except Exception as e:
    #    return jsonify({'e':str(e),'status_code':500,'success':False}),500

    '''
        SMS content check
    '''
    normal_sms_content = content.get("normal_sms",None)
    reminder_sms_content = content.get("reminder_sms",None)

    normal_sms_regex_pattern = r"{\w+}" #Must contain a link_url enclosed in {}
    reminder_sms_regex_pattern = r"{\w+}" #Must contain a datetime enclosed in {}

    sms_content_dict = dict()

    if normal_sms_content:
        normal_sms_parameters = re.findall(normal_sms_regex_pattern,normal_sms_content)
        print(len(normal_sms_parameters))
        if len(normal_sms_parameters) >= 1:
            sms_content_dict.update({"normal_sms":normal_sms_content})
        else:
            return jsonify({"message":"Improper content for normal sms,the string must contain the {link_url} substring in the content",'status_code':500,'success':False}),500

    if reminder_sms_content:
        reminder_sms_parameters = re.findall(reminder_sms_regex_pattern,reminder_sms_content)
        print(len(reminder_sms_parameters))
        if len(reminder_sms_parameters) >= 1:
            sms_content_dict.update({"reminder_sms":reminder_sms_content})
        else:
            return jsonify({"message":"Improper content for reminder sms,the string must contain the {reminder_time} substring in the content",'status_code':500,'success':False}),500
    
    print(json.dumps(sms_content_dict))
    ipv_config_list = ['link_url',
                       'scheduled_link_expiry',
                       'VKYC_link_expiry',
                       'VPD_link_expiry',
                       'link_type',
                       'user_abandon_timer',
                       'send_sms_schedule',
                       'send_sms_reminder']
    try:
        update_dict = {k:content[k] for k in ipv_config_list if content[k] is not None}
    except Exception as e:
        return jsonify({'e':str(e),'status_code':500,'success':False}),500
    if len(sms_content_dict.keys())!=0:
        update_dict.update({"normal_sms":sms_content_dict["normal_sms"]})
        update_dict.update({"reminder_sms":sms_content_dict["reminder_sms"]})
    configs_modified = {}
    configs_modified.update(update_dict)

    try:
        update_table('ipv_bpo_state','client',client_name+'_vkyc',update_dict)
    except Exception as e:
        return jsonify({'e':str(e),'status_code':500,'success':False}),500
    cred_config_list = ['webhook_req_type','webhook_url','webhook_enable']

    try:
        update_dict = {k:content[k] for k in cred_config_list if content[k] is not None}
        if update_dict["webhook_req_type"] != "json":
            del update_dict["webhook_req_type"]
        else:
            pass
        update_table('kwik_id_client_credentials','ClientID',client_name,update_dict)
    except Exception as e:
        return jsonify({'e':str(e),'status_code':500,'success':False}),500
    configs_modified.update(update_dict)
    

    return jsonify({'modifications':configs_modified,'status_code':200,'success':True}),200

@bp.route("/backendConfig/get")
@login_required(permission='usr')
def get_backend_configure():
    '''
    This endpoint returns the backend configuration for the client.
    '''
    client_name = g.user_id      #"dummytest"   #g.user_id
    try:
        response_bpo_state = fetch_row('ipv_bpo_state','client',client_name+'_vkyc')
    except Exception as e:
        return jsonify({'e':str(e),'status_code':500,'success':False}),500
    final_dict = response_bpo_state
    try:
        response_client_credentials = fetch_row('kwik_id_client_credentials','ClientID',client_name)
    except Exception as e:
        return jsonify({'e':str(e),'status_code':500,'success':False}),500#

    final_dict.update(response_client_credentials)
    configurable_list = ['link_url',
                         'scheduled_link_expiry',
                         'VKYC_link_expiry',
                         'VPD_link_expiry',
                         'link_type',
                         'user_abandon_timer',
                         'normal_sms',
                         'reminder_sms',
                         'send_sms_schedule',
                         'send_sms_reminder',
                         'webhook_req_type',
                         'webhook_url',
                         'webhook_enable']
    final_dict_send = {k: final_dict[k] for k in final_dict if k in configurable_list}
    final_dict_send_def = {'link_url':None,
                           'scheduled_link_expiry':None,
                           'VKYC_link_expiry':None,
                           'VPD_link_expiry':None,
                           'link_type':None,
                           'user_abandon_timer':None,
                           'normal_sms':None,
                           'reminder_sms':None,
                           'send_sms_schedule':None,
                           'send_sms_reminder':None,
                           'webhook_req_type':None,
                           'webhook_url':None,
                           'webhook_enable':None}
    final_dict_send_def.update(final_dict_send)
    return jsonify({"data":final_dict_send_def,'status_code':200,'success':True})

def createCredentials(client_name,password,credits=100):
    data={}
    table=dynamodb.Table(CLIENT_TABLE)
    try:
      table_data=table.query(KeyConditionExpression=Key('ClientID').eq(client_name))['Items'][0]
      return "exist"
    except:
      data['ClientID']=client_name
      data['Secret']=password
      data['credits']=credits
      table.put_item(Item=data)
      return "created"



def make_entry_in_bpo_state_table(client_name):
    table=dynamodb.Table(BPO_STATE_COUNT_TABLE)
    template_data={
              "active_agent_count": 0,
              "agent_abandon_timer": 10,
              "agent_wait_after_videokyc_req_reject": 30,
              "average_bpo_time": 60,
              "client": "{}_vkyc".format(client_name),
              "client_approved": 0,
              "clientshandled_free": 0,
              "clientshandled_priority": 0,
              "register_agent_count": 0,
              "sqs_message_retention_timer": 720,
              "unavailable_count": 0,
              "user_abandon_timer": 5,
              "user_abandon_timer_ongoing_call": 40,
              "videokyc_req_timer": 30,
              "waiting_agent_count": 0}

    table.put_item(Item=template_data)

def make_queue(client_name):
    queues=['{}_free_vkyc','{}_priority_vkyc',"{}_schedule_vkyc","{}_concurrent_video_audit_queue"]
    for queue in queues:
      if not 'concurrent_video_audit_queue' in queue:
        sqs.create_queue(QueueName=queue.format(client_name))
      else:
        sqs.create_queue(QueueName=queue.format(client_name.lower()))


def createAgents(client_name, number_of_agents):
    headers = {
        'auth': getToken(client_name),
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "e73d03e5-6405-4e09-aef8-12ebe7699bf5"
    }

    agents_data = []

    for i in range(number_of_agents):
        data = {
            "agent_id": "{}_agent_{}",
            "password": "{}_agent_{}",
            "agent_mode": "free"
        }

        data['agent_id'] = data['agent_id'].format(client_name, i + 1)
        data['password'] = data['password'].format(client_name, i + 1)

        agents_data.append(data)
        payload = json.dumps(data)
        response = requests.request("POST", URL + 'register', data=payload, headers=headers)

        # register agent
        admin_reg_url = "https://uat.vkyc.getkwikid.com:8070/admin/register"
        admin_data = {
            "admin_id": "{}_admin".format(client_name),
            "password": "1234"
        }
        payload = json.dumps(admin_data)
        response = requests.request("POST", admin_reg_url, data=payload, headers=headers)

        print(response.text)
    return agents_data

def map_client_details(client_name,config_data):

    mapping_file = "https://kwikid.s3.ap-south-1.amazonaws.com/CONFIG/AGENT/mapping.json"
    r = requests.get(url=mapping_file)
    data = r.content
    mapping = json.loads(data)
    mapping_data = mapping['mapping']
    #add new entry
    mapping_data[client_name]=[client_name]
    mapping['mapping'] = mapping_data
    print(mapping)

    try:
        bucket = "kwikid"
        agent_config_key = "CONFIG/AGENT/mapping.json"
        
        bytesIO = BytesIO()
        bytesIO.write(json.dumps(mapping,indent = 2))
        bytesIO.seek(0)
        s3.put_object(Bucket=bucket, Body=bytesIO.read(), Key=agent_config_key)

        agent_config_key = "CONFIG/AGENT/{}/agentConfig.json".format(client_name)
        bytesIO = BytesIO()
        bytesIO.write(json.dumps(config_data,indent = 2))
        bytesIO.seek(0)
        s3.put_object(Bucket=bucket, Body=bytesIO.read(), Key=agent_config_key)
        
        return 1
    except:
        return 0



def getToken(client_name):
    table = dynamodb.Table(CLIENT_TABLE)
    table_data = table.query(KeyConditionExpression=Key('ClientID').eq(client_name))['Items'][0]

    data = {}
    data['username'] = table_data['ClientID']
    data['password'] = table_data['Secret']

    headers = {
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "e73d03e5-6405-4e09-aef8-12ebe7699bf5"
    }

    payload = json.dumps(data)
    response = requests.request("POST", URL + 'generate_token', data=payload, headers=headers)

    return json.loads(response.text)['Token']

@bp.route('/custom/manualagentupdate', methods=['POST'])
@login_required(permission='usr')
def manualagentupdate():
    client_name = g.user_id
    content = request.json
    manual_agent_list = content["manual_agent_list"]
    client_table = dynamodb.Table(BPO_STATE_COUNT_TABLE)
    try:
        client_table.update_item(Key={"client":client_name+"_vkyc"},UpdateExpression="set manual_agent_list= :manual_agent_list",ExpressionAttributeValues={':manual_agent_list':manual_agent_list},ReturnValues="UPDATED_NEW")
    except Exception as e:
        return(jsonify({'msg': str(e),'status':500}), 500)
    return (jsonify({'msg': "ok",'status':200}), 200)

@bp.route('/custom/bulk/manualassign', methods=['POST'])
@login_required(permission='usr')
def custom_bulk_assign():
    client_name = g.user_id
    content = request.json
    return bulk_manualassign_detached(client_name, content)


@bp.route('/custom/manualassign', methods=['POST'])
@login_required(permission='usr')
def manualassign():
    client_name = g.user_id
    content = request.json
    if client_name == "Retailasset":
        content = content['assigned_users'][0]
        content["assigned_agent"] = request.json["assigned_agent"]
        content["admin_id"] = request.json["admin_id"]
    try:
        user_id = content["user_id"]
        try:
            user_name = content["customer_name"]
        except:
            user_name = content["user_name"]
        try:
            phone_number = content["mobile_number"]
        except:
            phone_number = content["phone_number"]
        try:
            manual_agent = content["assigned_agent"]
        except:
            manual_agent = content["manual_agent"]
        try:
            manual_agent_name = content["agent_name"]
        except:
            manual_agent_name = None
        #last_call_time = content["last_call_time"]
        #custom_msg = content["custom_msg"]
        admin_id = content["admin_id"]
        #ekyc_req_time = content["ekyc_req_time"]
        case_push_time = content.get("case_push_time", str(content.get("start_time_raw","")))
        #creation_time = content["creation_time"]
        try:
            latest_row_sess_id = content["session_id"]
        except:
            latest_row_sess_id = content["latest_row_sess_id"]
    except Exception as e:
        print(e)
        return jsonify(success=False, msg=str(e), from_= "here"), 500
    manual_allocation_table = dynamodb.Table(MANUAL_ALLOCATION_TABLE)
    time_frame_expiry_detected = False
    case_push_table = dynamodb.Table(CASE_PUSH_TABLE)
    agent_table = dynamodb.Table(agent_status_table)
    session_table =  dynamodb.Table(getPOCTableName('Session Status'))
    till_now_df = pd.read_pickle("to_assign_{}.pkl".format(client_name))

    try:
        print("data oldest",get_first_session_detail(user_id))
        case_push_time = get_first_session_detail(user_id)["start_time"]
    except Exception as ecp:
        print("Exception cpt",str(ecp))
        case_push_time = ""

    try:
        if case_push_time == "":
            case_push_time=case_push_table.query(KeyConditionExpression=Key('user_id').eq(user_id))['Items'][0]["case_push_time"]
        if divmod((time.time() - float(case_push_time)),60)[0] > time_frame_expiry_period[client_name]:
            try:
                data = session_table.query(IndexName="user_id-index",KeyConditionExpression=Key('user_id').eq(user_id)).get('Items')
                for i in data:
                    session_table.update_item(Key={'session_id': i['session_id']},
                                          UpdateExpression="set time_frame_expired = :t",
                                          ExpressionAttributeValues={':t': "yes"},
                                          ReturnValues="UPDATED_NEW")
                    till_now_df.loc[till_now_df["session_id"]==i["session_id"], "time_frame_expired"] = "yes"
            except Exception as e:
                print("time_frame_expired flag set error",str(e))
            return(jsonify({'msg': 'time_frame_expired','status':500}), 500)
    except Exception as e:
        print("case_push_time check failed",str(e))
    #response_ekyc = ekyc_table.query(IndexName='user_id-client_name-index', KeyConditionExpression=Key('user_id').eq(user_id) & Key('client_name').eq("ekyc")).get('Items')
    #if len(response_ekyc) == 0:
    #    print("ekyc data unavailable",user_id)
    #    return(jsonify({'msg': 'Ekyc data Expired/unavailable','status':500}), 500)
    #elif len(response_ekyc)>1:
    #    print("ekyc data multiple rows exist",user_id)
    #    return(jsonify({'msg': 'Ekyc data Expired/unavailable','status':500}), 500)
    #if divmod((time.time() - float(response_ekyc[0]["ekyc_req_time"])),60)[0] > 4320:
    #    print("ekyc data expired",user_id)
    #    return(jsonify({'msg': 'Ekyc data Expired/unavailable','status':500}), 500)
    #session_table =  dynamodb.Table(getPOCTableName('Session Status'))
    PRODUCT = "CC"
    data = session_table.query(IndexName="user_id-index",KeyConditionExpression=Key('user_id').eq(user_id)).get('Items')
    for i in data:
        if i["session_id"] == latest_row_sess_id:
            PRODUCT = i.get("PRODUCT", "CC")
        if i["session_status"] == "kyc_result_approved":
            try:
                audit_result = int(i["audit_result"])
                if audit_result == 1:
                    print("checker approval case found",user_id,i["session_id"])
                    return(jsonify({'msg': 'case has already been approved by agent/auditor please refresh the screen','status':500}), 500)
            except Exception as e:
                print("agent approval case found",user_id,i["session_id"])
                return(jsonify({'msg': 'case has already been approved by agent/auditor please refresh the screen','status':500}), 500)
    data = manual_allocation_table.query(
            KeyConditionExpression=Key('user_id').eq(user_id)).get('Items')
    print("data from manual assign table",data)
    try:
        item = agent_table.query(KeyConditionExpression=Key("agent_id").eq(manual_agent)&Key('client_name').eq(client_name))['Items'][0]
        agent_name = item["agent_name"]
    except Exception as e:
        print("failed at getting agent name",str(e))
        agent_name = ""
        pass


    phog("auto_assign",client_name,user_id, latest_row_sess_id,title="",extras={"manual_agent": manual_agent, "case_push_time": case_push_time})
    try:
        session_data_for_table = {
                "client_name":client_name,
                "user_id":  user_id,
                "session_id":  latest_row_sess_id,
                "manual_agent": manual_agent
                }
        insert("kwikid_vkyc_session_status",session_data_for_table)
    except Exception as e:
        print("Exception while postgres insert", e)


    if len(data) != 0:
        try:
            res = session_table.update_item(Key={'session_id': latest_row_sess_id},
                                            UpdateExpression="set manual_agent =:ma",
                                            ExpressionAttributeValues={":ma":manual_agent},
                                            ReturnValues="UPDATED_NEW")
            print("setting manual_agent on session in case missed",res)
            manual_allocation_table.update_item(Key={'user_id': user_id},
                                                UpdateExpression="set agent_id=:ma, start_time =:st,case_push_time =:case_push_time,sess_id=:sess_id,product_code=:pc",
                                                ExpressionAttributeValues={":st":"yes",":case_push_time":case_push_time,":ma": manual_agent,":sess_id": latest_row_sess_id,":pc": PRODUCT},
                                                ReturnValues="UPDATED_NEW")
            till_now_df.loc[till_now_df["session_id"]==latest_row_sess_id, "manual_agent"] = manual_agent
            till_now_df.loc[till_now_df["session_id"]==latest_row_sess_id, "manual_agent_name"] = agent_name
            pd.to_pickle(till_now_df,'./to_assign_{}.pkl'.format(client_name))
            upload_file_s3_local(S3_BUCKET,"./to_assign_{}.pkl".format(client_name),"/videokyc/userassign/{}/to_assign.pkl".format(client_name))
            #pd.to_pickle(till_now_df,'./userslist_{}.pkl'.format(client_name))
            #upload_file_s3_local(S3_BUCKET,"./{}/userslist.pkl".format(client_name),"/videokyc/userassign/{}/userslist.pkl".format(client_name))
            return jsonify(msg="manual agent overridden from {} to {}".format(data[0]["agent_id"], manual_agent),assigned_manual_agent=manual_agent,success=True,status_code=200),200
            #return jsonify({"msg":"already manual agent assigned please refresh screen to get latest data",
             #               "assigned_manual_agent":data[0]["agent_id"],"success":False,"status_code":500}),500
        except Exception as e:
            print("error at setting manual_agent on session in case missed",str(e))
            return jsonify({"msg":str(e),"success":False,"here":"here","status_code":500}),500
    else:
        new_item = {
                "user_id":user_id,
                "user_name":user_name,
                "phone_number":phone_number,
                "agent_id":manual_agent,
                #"last_call_time":last_call_time,
                #"creation_time":creation_time,
                "latest_row_sess_id":latest_row_sess_id,
                "start_time":"yes",
                "call_counter":"0",
                "product_code": PRODUCT,
                "case_push_time":case_push_time,
                "message_punched":json.dumps([]),
                "disposition_punched":json.dumps([]),
                "client_name":client_name
                }
        res = session_table.update_item(Key={'session_id': latest_row_sess_id},
                                        UpdateExpression="set manual_agent =:ma",
                                        ExpressionAttributeValues={":ma":manual_agent},
                                        ReturnValues="UPDATED_NEW")
        manual_allocation_table.put_item(Item=new_item)
        return jsonify({"msg":"manual allocation success","assigned_manual_agent":manual_agent,
                        "success":True,"status_code":200}),200

#now add message,manual over ride
@bp.route('/custom/punchInMessage', methods=['POST'])
def punchInMessage():
    content = request.json
    try:
        agent_id = content["agent_id"]
        user_id = content["user_id"]
        message = content["message"]
    except Exception as e:
        print(e)
        return jsonify(success=False,msg=str(e)), 400
    agnts_cron_data = "dummy"   #json.loads(download_file_s3(S3_BUCKET,"agntsCrnDmp/{}.json".format(client_name)))
    try:
        #agent_name = agnts_cron_data[agent_id]["agent_name"]
        agent_name = "dummy"
    except Exception as e:
        print("punch in message failed at getting agent name",str(e))
        try:
            agents_dump_cron()
            agnts_cron_data = json.loads(download_file_s3(S3_BUCKET,"agntsCrnDmp/{}.json".format(client_name)))
            agent_name = agnts_cron_data[agent_id]["agent_name"]
        except Exception as e:
            agent_name = ""
            print("punch in message failed at re getting agent name",str(e))
    manual_allocation_table = dynamodb.Table(MANUAL_ALLOCATION_TABLE)
    data = manual_allocation_table.query(KeyConditionExpression=Key("user_id").eq(user_id)).get('Items')
    if len(data) == 0:
        return jsonify({"msg":"not manually assigned yet",
                        "success":False,"status_code":500}),500
    else:
        prev_message_punched = json.loads(data[0]["message_punched"])
        item = {
                "Time":str(datetime.now()),
                "id":agent_id,
                "name":agent_name,
                "message":message}

        prev_message_punched.append(item)
        manual_allocation_table.update_item(Key={'user_id': user_id},
                                            UpdateExpression="set message_punched =:mp",
                                            ExpressionAttributeValues={":mp":json.dumps(prev_message_punched)},
                                            ReturnValues="UPDATED_NEW")
        return jsonify({"msg":"message punched in successfully","success":True,"status_code":200}),200

@bp.route('/custom/getvoicedispose', methods=['POST'])
def getvoicedispose():
    content = request.json
    user_id = content["user_id"]
    manual_allocation_table = dynamodb.Table(MANUAL_ALLOCATION_TABLE)
    data = manual_allocation_table.query(KeyConditionExpression=Key("user_id").eq(user_id)).get('Items')
    if len(data) == 0:
        return jsonify({"msg":"not manually assigned yet","success":False,"status_code":500}),500
    else:
        return jsonify({"disposition_punched":json.loads(data[0]["disposition_punched"]),"success":True,"status_code":200}),200
    
@bp.route('/custom/voicedispose', methods=['POST'])
def voicedispose():
    content = request.json
    return voicedispose_detached(content)


@bp.route('/custom/manualOverride', methods=['POST'])
@login_required(permission='usr')
def manualOverride():
    client_name = g.user_id
    content = request.json
    user_id = content["user_id"]
    manual_agent = content["manual_agent"]
    case_push_time = content["case_push_time"]
    latest_row_sess_id = content["session_id"]
    ekyc_table = dynamodb.Table(EKYC_STATUS_TABLE)
    case_push_table = dynamodb.Table(CASE_PUSH_TABLE)
    session_table =  dynamodb.Table(getPOCTableName('Session Status'))
    try:
        if case_push_time == "":
            case_push_time=case_push_table.query(KeyConditionExpression=Key('user_id').eq(user_id))['Items'][0]["case_push_time"]
        if divmod((time.time() - float(case_push_time)),60)[0] > time_frame_expiry_period[client_name]:
            try:
                data = session_table.query(IndexName="user_id-index",KeyConditionExpression=Key('user_id').eq(user_id)).get('Items')
                for i in data:
                    session_table.update_item(Key={'session_id': i['session_id']},
                                          UpdateExpression="set time_frame_expired = :t",
                                          ExpressionAttributeValues={':t': "yes"},
                                          ReturnValues="UPDATED_NEW")
            except Exception as e:
                print("time_frame_expired flag set error",str(e))
            return(jsonify({'msg': 'time_frame_expired','status':500}), 500)
    except Exception as e:
        print("case_push_time check failed",str(e))
    #response_ekyc = ekyc_table.query(IndexName='user_id-client_name-index', KeyConditionExpression=Key('user_id').eq(user_id) & Key('client_name').eq
#("ekyc")).get('Items')
    #if len(response_ekyc) == 0:
    #    print("ekyc data unavailable",user_id)
    #    return(jsonify({'msg': 'Ekyc data Expired/unavailable','status':500}), 500)
    #elif len(response_ekyc)>1:
    #    print("ekyc data multiple rows exist",user_id)
    #    return(jsonify({'msg': 'Ekyc data Expired/unavailable','status':500}), 500)
    #if divmod((time.time() - float(response_ekyc[0]["ekyc_req_time"])),60)[0] > 4320:
    #    print("ekyc data expired",user_id)
    #    return(jsonify({'msg': 'Ekyc data Expired/unavailable','status':500}), 500)
    #session_table =  dynamodb.Table(getPOCTableName('Session Status'))
    data = session_table.query(IndexName="user_id-index",KeyConditionExpression=Key('user_id').eq(user_id)).get('Items')
    for i in data:
        if i["session_status"] == "kyc_result_approved":
            try:
                audit_result = int(i["audit_result"])
                if audit_result == 1:
                    print("checker approval case found",user_id,i["session_id"])
                    return(jsonify({'msg': 'case has already been approved by agent/auditor please refresh the screen','status':500}), 500)
            except Exception as e:
                print("agent approval case found",user_id,i["session_id"])
                return(jsonify({'msg': 'case has already been approved by agent/auditor please refresh the screen','status':500}), 500)
    manual_allocation_table = dynamodb.Table(MANUAL_ALLOCATION_TABLE)
    #session_table =  dynamodb.Table(getPOCTableName('Session Status'))
    session_table.update_item(Key={'session_id': latest_row_sess_id},
                              UpdateExpression="set manual_agent =:ma",
                              ExpressionAttributeValues={":ma":manual_agent},
                              ReturnValues="UPDATED_NEW")
    manual_allocation_table.update_item(Key={'user_id': user_id},
                                        UpdateExpression="set agent_id =:ma,latest_row_sess_id =:lrsid",
                                        ExpressionAttributeValues={":ma":manual_agent,":lrsid":latest_row_sess_id},
                                        ReturnValues="UPDATED_NEW")
    return jsonify({"msg":"override successful to {}".format(manual_agent),"success":True,"status_code":200}),200




@bp.route('/getUserData/<client_name>', methods=['POST'])
@bp.route('/getUserList/<client_name>', methods=['POST'])
@login_required(permission='usr')
def getUserList(client_name):
    '''
    This endpoint returns all the sessions that were initiated in between the provided time.
    start_time and end_time come as epoch timestamped value.

    All the related data to a session is returned in this endpoint.
    '''
    print (client_name)
    content = request.json

    filters = content["filters"]

    manual_allocation_table = dynamodb.Table(MANUAL_ALLOCATION_TABLE)
    session_table = dynamodb.Table(session_status_table)
    def mapper(x):
        #x = x.encode("utf-8")
       # print(type(x))
        y=session_status_mapper.get(x.get("session_status"))
        try:
            #print(x.session_status)
            #y=x["session_status"]
            if y == "VCIP_SESSION_INVALID" and x.get("feedback") and json.loads(x["feedback"])["type"] == "Reschedule":
                return "VCIP_RESCHEDULED"

        except Exception as e:
            print(e)
            pass
        return y
    def mapper1(x):
        try:
            return json.loads(x)["type"]
            pass
        except:
            return "NA"
            pass
    def ekycexp(x):
        try:
            if divmod(time.time() - float(x), 60)[0] > 257400:
                return True
            pass
        except:
            pass
        return False

    try:
        #download_file_s3_local(S3_BUCKET,"userslist.pkl","/videokyc/userassign/{}/userslist.pkl".format(client_name))
        till_now_df = pd.read_pickle("to_assign_{}.pkl".format(client_name))
        print(till_now_df.dtypes)
        till_now_df["ekyc_time"] = till_now_df["ekyc_time"].fillna("")
        till_now_df['ekyc_time'] = pd.to_numeric(till_now_df['ekyc_time'])
        till_now_df["no_vcip_attempts"] = ""
        till_now_df["dispositions_punched"] = ""
        till_now_df["no_dialer_attempts"] = ""
        till_now_df["TA_session_status"] = ""
        till_now_df["ekyc_exp_diff"] = ""
        till_now_df["type"] = till_now_df["feedback"].apply(lambda x: mapper1(x))
        #till_now_df["session_status"] = till_now_df["session_status"].apply(lambda x: )
        #till_now_df['session_status'].mask(till_now_df['type'] == 'Reschedule', "VCIP_RESCHEDULED", inplace=True)
        till_now_df["session_status"] = till_now_df["session_status"].apply(lambda x: session_status_mapper.get(x, x))
        #till_now_df.fillna("", inplace=True)
        #till_now_df["session_status"] = till_now_df[["session_status","ekyc_time"]].apply(lambda x: "KYC_PENDING" if ekycexp(x["ekyc_time"]) else x["session_status"])
        till_now_df.loc[till_now_df["type"] == "Reschedule", "session_status"] = "VCIP_RESCHEDULED"
        till_now_df.loc[till_now_df["session_status"] == "NOT_ASSIGNED_TO_AGENT", "session_status"] = "VCIP_PENDING"
        #till_now_df["ekyc_exp_diff"] = till_now_df["ekyc_time"].apply(lambda x: divmod(time.time() - x, 60)[0])
        till_now_df.loc[divmod(time.time() - till_now_df["ekyc_time"], 60)[0] > ekyc_expiry_time_period[client_name], "session_status"] = "KYC_PENDING"
        till_now_df["ekyc_time"] = till_now_df["ekyc_time"].fillna("")
        till_now_df.loc[till_now_df["ekyc_time"] == "", "session_status"] = "KYC_PENDING"
        #till_now_df['session_status'].mask(till_now_df['ekyc_time'] == "", "KYC_PENDING", inplace=True)
        till_now_df.loc[till_now_df["session_status"] == "CHECKER_APPROVED", "session_status"] = "Audited and Okay"
        till_now_df.loc[till_now_df["session_status"] == "CHECKER_REJECTED", "session_status"] = "Audited and not okay"
        till_now_df = till_now_df[till_now_df["case_push_time"] != ""]
        till_now_df = till_now_df[till_now_df["time_frame_expired"] == "no"]
        till_now_df.drop(columns=["type", "ekyc_exp_diff"], errors="ignore", inplace=True)
        till_now_df.reset_index(inplace=True, drop=True)

        print(till_now_df.head())
        print(till_now_df.shape)

        #return jsonify(success=True)
        
        for i in range(till_now_df.shape[0]):
            continue

            user_id = till_now_df['user_id'][i]
            session_status = till_now_df['session_status'][i]
            feedback = till_now_df['feedback'][i]

            #print(user_id)
            #continue

            try:
                rbl_session_status = session_status_mapper[session_status]
                #print(rbl_session_status, session_status)
                continue
                
                if rbl_session_status == "VCIP_SESSION_INVALID":
                    if feedback != "":
                        try:
                            if json.loads(feedback)["type"] == "Reschedule":
                                rbl_session_status = "VCIP_RESCHEDULED"
                        except Exception as e:
                            print("error at checking feedback",str(e))

                        # if expiring_at_link_expiry:
                        #     if expiry_reason is None:
                        #         rbl_session_status = "VCIP_EXPIRED"

            except Exception as e:
                print("assigning rbl_session_status error",str(e))
                rbl_session_status = ""

            #till_now_df['session_status'][i] = rbl_session_status
            #till_now_df['TA_session_status'][i] = session_status

            if client_name in ["RBL_uat","RBL"]:
                continue
            
            if user_id:

                vcip_data = session_table.query(IndexName="user_id-index",KeyConditionExpression=Key('user_id').eq(user_id)).get('Items')
                print(vcip_data)
                till_now_df['no_vcip_attempts'][i] = len(vcip_data)
                #continue

                data = manual_allocation_table.query(KeyConditionExpression=Key("user_id").eq(user_id)).get('Items')
                
                if data:
                    till_now_df['dispositions_punched'][i] = json.loads(data[0].get("disposition_punched", "[]")) 
                    # prev_message_punched = json.loads(data[0].get("message_punched", "[]"))
                    till_now_df['no_dialer_attempts'][i] = data[0].get("call_counter", "0")


        #till_now_df = till_now_df.drop(["session_status"], axis = 1)
        #return jsonify(success=True)
        till_now_df = till_now_df.fillna("")
        till_now_data = till_now_df.to_dict('records')

        #return jsonify(success=True)

        #till_now_df = till_now_df.drop(["session_status"], axis = 1)
        final_out_dict = {}
        client_table = dynamodb.Table(BPO_STATE_COUNT_TABLE)
        try:
            manual_agent_list = client_table.query(KeyConditionExpression=Key('client').eq(client_name+"_vkyc"))["Items"][0]["manual_agent_list"]
        except Exception as e:
            manual_agent_list = ['BFL_uat_agent_1']

        filtered_records = list(filter(lambda record: float(filters["start_time"])  <= record['start_time'] <= float(filters["end_time"]), till_now_data))

        final_out_dict.update({'session_data': filtered_records, 'status_code': 200,"manual_agent_list":manual_agent_list,'success': True})
        return jsonify(final_out_dict), 200
    except Exception as e:
        print ('Exception', e)
        traceback.print_exc()
        return jsonify({
            'e': traceback.format_exc(),
            'msg': 'Failed to update',
            'status_code': 500,
            'success': False}), 500
    """
    content = request.json
    filters = content['filters']
    agent_table = dynamodb.Table(getPOCTableName('Agent'))
    #agnts_cron_data = json.loads(download_file_s3(S3_BUCKET,"agntsCrnDmp/{}.json".format(client_name)))
    #get_attr_list = ["session_id"]
    get_attr_list=["session_id",'feedback',"queue",'session_status',"user_id","phone_number","start_time","audit_result","ekyc_req_time",'vkyc_start_time','CUSTNAME','manual_agent',"expiring_at_link_expiry","expiry_reason",'audit_lock','auditor_fdbk']
    try:
        final_out = []

        paginator = dynamodb_client.get_paginator('query')
        data=paginator.paginate(
            TableName=getPOCTableName('Session Status'),
            Select='SPECIFIC_ATTRIBUTES',
            AttributesToGet=get_attr_list,
            IndexName='client_name-start_time-index',
            KeyConditions={
                    'client_name': {
                        'AttributeValueList': [
                            {
                                'S': client_name
                            }
                        ],
                        'ComparisonOperator': 'EQ'
                    },
                    'start_time': {
                        'AttributeValueList': [
                            {
                                'S': filters['start_time']
                            },
                            {
                                'S': filters['end_time']
                            }
                        ],
                        'ComparisonOperator': 'BETWEEN'
                    }
                }
        )

        res_items = []

        for page in data:
            print(len(page['Items']))
            res_items+=page['Items']


        for index, item in enumerate(res_items):
            try:
                session_id = item["session_id"]['S']
            except Exception as e:
                session_id = ""
            try:
                mobile_number = item['phone_number']['S']
            except:
                try:
                    mobile_number = item['user_id']['S'].split('_')[(-1)]
                    if len(mobile_number) != 10:
                        mobile_number = ""
                except Exception as e:
                    mobile_number = ""
            try:
                customer_name = item['CUSTNAME']['S']
            except Exception as e:
                customer_name = ""
            try:
                manual_agent = item["manual_agent"]['S']
            except Exception as e:
                manual_agent = ""
            #expiring_at_link_expiry
            try:
                expiring_at_link_expiry = item["expiring_at_link_expiry"]['S']
            except Exception as e:
                expiring_at_link_expiry = None
            try:
                expiry_reason = item["expiry_reason"]['S']
            except Exception as e:
                expiry_reason = None
            try:
                session_status = item['session_status']['S']
            except Exception as e:
                session_status = ""
            try:
                user_id = item["user_id"]['S']
            except Exception as e:
                user_id = ""
            try:
                start_time = float(item["start_time"]['S'])
            except Exception as e:
                start_time = ""
            try:
                vkyc_start_time = float(item['vkyc_start_time']['S'])
            except:
                try:
                    vkyc_start_time = float(item['vkyc_start_time']['N'])
                except:
                    vkyc_start_time = ""
            try:
                queue = item["queue"]['S']
            except Exception as e:
                queue = ""
            try:
                audit_result = item["audit_result"]['S']
            except Exception as e:
                try:
                    audit_result = item["audit_result"]['N']
                except Exception as e:
                    audit_result = ""
            try:
                feedback= item['feedback']['S']
            except:
                feedback =""
            try:
                auditor_feedback= item['auditor_fdbk']['S']
            except:
                auditor_feedback =""
            try:
                rbl_session_status = session_status_mapper[session_status]
                if rbl_session_status in ["NOT_ASSIGNED_TO_AGENT"]:
                    if queue == "free":
                        rbl_session_status = "NOT_ASSIGNED_TO_AGENT"
                    else:
                        try:
                            rbl_session_status = "VCIP_PENDING"
                        except Exception as e:
                            rbl_session_status = str(e)
                            print("error at ekyc status checking",str(e))
                            pass
                if rbl_session_status in ["VCIP_APPROVED"]:
                    try:
                        audit_lock = item['audit_lock']['BOOL']
                        if audit_lock:
                            rbl_session_status = "CHECKER_PENDING"
                        else:
                            pass
                    except Exception as e:
                        print("error at audit lock checking",str(e))
                        pass
                #CHECKER_REJECTED instead of VCIP_SESSION_INVALID
                if rbl_session_status in ["VCIP_APPROVED","CHECKER_PENDING","VCIP_REJECTED","VCIP_SESSION_INVALID"]:
                    try:
                        if int(audit_result) == 1:
                            rbl_session_status = "CHECKER_APPROVED"
                        else:
                            rbl_session_status = "CHECKER_REJECTED"
                    except Exception as e:
                        print(str(e))
                        pass
                if rbl_session_status == "VCIP_SESSION_INVALID":
                    if feedback != "":
                        try:
                            if json.loads(feedback)["type"] == "Reschedule":
                                rbl_session_status = "VCIP_RESCHEDULED"
                        except Exception as e:
                            print("error at checking feedback",str(e))
                if client_name in ["RBL","RBL_uat","BFL","BFL_uat"]:
                    if rbl_session_status == "VCIP_SESSION_INVALID":
                        if expiring_at_link_expiry:
                            if expiry_reason is None:
                                rbl_session_status = "VCIP_EXPIRED"
                #if client_name in ["BFL","BFL_uat"]:
                #    if session_status == "kyc_rejected":
                #        rbl_session_status = "kyc_rejected"

            except Exception as e:
                print("assigning rbl_session_status error",str(e))
                rbl_session_status = ""
            try:
                ekyc_time = item["ekyc_req_time"]['S']
            except Exception as e:
                ekyc_time = ""
                


            result_out = {
                'manual_agent': manual_agent,
                'customer_name':customer_name,
                'session_id': session_id,
                'phone_number':mobile_number,
                'user_id': user_id,
                'start_time': start_time,
                'session_status': rbl_session_status,
                'TA_session_status': session_status,
                'vkyc_start_time':vkyc_start_time,
                'ekyc_time':ekyc_time,
                'feedback':feedback,
                'auditor_feedback':auditor_feedback
            }

            final_out.append(result_out)

        final_out_dict = {}
        final_out_dict.update({'session_data': final_out, 'status_code': 200,
                               'success': True})
        return jsonify(final_out_dict), 200
    except Exception as e:
        print ('Exception', e)
        traceback.print_exc()
        return jsonify({

    """
@bp.route("/getAllUserSessionv2/<client_name>/<userid>", methods=["GET", "POST"])
@login_required(permission='usr')
def getAllUserSessionv2(client_name, userid):
    if request.method == "GET":
        return globalSearch(client_name, userid, 'user_id')
    if request.method == "POST":
        request_data = request.json
        if request_data["type"] == "user_id":
            return globalSearch(client_name, userid, 'user_id')
        if request_data["type"] == "session_id":
            return globalSearch(client_name, userid, 'session_id')
    

@bp.route("/getAllUserSession/<client_name>/<userid>", methods=["GET", "POST"])
@login_required(permission='usr')
def getAllUserSession(client_name, userid):
    print("getAllUserSession = ", client_name, userid)

    session_table = dynamodb.Table(getPOCTableName('Session Status'))

    if request.method == "GET":
        all_session_list = session_table.query(
            IndexName='user_id-index',
            KeyConditionExpression=Key('user_id').eq(userid),
            FilterExpression='client_name = :client_name',
            ExpressionAttributeValues={
                ":client_name": client_name
            },
            ScanIndexForward=False
        )
    if request.method == "POST":
        request_data = request.json
        print("request_data = ", request_data["type"])

        if request_data["type"] == "user_id":
            all_session_list = session_table.query(
                IndexName='user_id-index',
                KeyConditionExpression=Key('user_id').eq(userid),
                FilterExpression='client_name = :client_name',
                ExpressionAttributeValues={
                    ":client_name": client_name
                },
                ScanIndexForward=False
            )
        if request_data["type"] == "session_id":
            all_session_list = session_table.query(
                KeyConditionExpression=Key('session_id').eq(userid)
            )

    if all_session_list["Count"] > 0:

        final_data_list = format_json_array(all_session_list['Items'])
        
        ekyc_time = get_ekyc_req_time(final_data_list[0], {})
        
        for list_item in final_data_list:
          list_item["ekyc_time"] = ekyc_time
          

        # final_session_data_list = format_json_array(ta_to_rbl_session_status(final_data_list,client_name))

        final_session_data_list = ta_to_rbl_session_status(final_data_list,client_name)

        return jsonify(
                {'session_list': final_session_data_list, 'final_data_list_raw': final_data_list,  'session_count': len(all_session_list['Items']), 'status_code': 200, 'success': True}), 200
    else:
        return jsonify(
            {'msg': 'not found', 'status_code': 500, 'success': False}), 500

@bp.route("/getFirstSessionData/<userid>", methods=["GET", "POST"])
# @login_required(permission='usr')
def getFirstSessionData(userid):
    try:
        user_old_data = get_first_session_detail(userid)
    except Exception as old_err:
        print("Old data error:",str(old_err))
    return jsonify({'old_data':user_old_data}),200
