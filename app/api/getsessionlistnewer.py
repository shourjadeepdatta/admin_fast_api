from flask import jsonify
from boto3.dynamodb.conditions import Key, Attr
from multiprocessing.pool import ThreadPool
from itertools import chain
import pandas as pd
import traceback
import datetime
import time
import json
import numpy as np

from app.config import dynamodb, dynamodb_client, MANUAL_ALLOCATION_TABLE, user_status_table, EKYC_STATUS_TABLE, session_status_mapper, ekyc_expiry_time_period
from app.helpers import getPOCTableName, convert_dynamodb_item_to_json, get_latest_session_detail, get_first_session_detail
import app.api.data_processor as data_processor

import pytz

import app.api.postgres as postgres

import app.api.abhishek as abhishek

def get_session_data(client_name, content):
  get_attr_list=["vkyc_start_time","session_id","queue",'session_status',"user_id","start_time","end_time","agent_assignment_time","audit_result",'extras','audit_id','feedback',"manual_agent","expiry_reason","expiring_at_link_expiry","CUSTNAME","auditor_fdbk","ekyc_req_time","audit_end_time","PRODUCT","phone_number","agent_id","audit_lock", "auditor_name", "fraud_advisory_given"]
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
                          'S': content['start_time']
                      },
                      {
                          'S': content['end_time']
                      }
                  ],
                  'ComparisonOperator': 'BETWEEN'
              }
          }

      )

  res_items = []
  for page in data:
      res_items+=page['Items']
      
  return res_items

def get_agent_data(client_name):
  agent_table = dynamodb.Table(getPOCTableName('Agent'))

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
        agents_name_dict[agent_id] = {"agent_name": agent_name, "call_center": item.get("call_center")}
    except Exception as e:
        agents_name_dict[agent_id] = {"e": str(e)}
        
  return agents_data_items

def chunks(lst, n):
  """Yield successive n-sized chunks from lst."""
  for i in range(0, len(lst), n):
      yield lst[i:i + n]

def get_manual_allocation_info(session_list):
  session_data_new = []

  try:
      for session_data in session_list:
        manual_allocation_data = dynamodb_client.query(
                      TableName=MANUAL_ALLOCATION_TABLE,
                      Select='SPECIFIC_ATTRIBUTES',
                      AttributesToGet=['disposition_punched'],
                      KeyConditions={
                              'user_id': {
                                  'AttributeValueList': [
                                      {
                                              'S': session_data.get('user_id', {}).get('S')
                                      }
                                  ],
                                  'ComparisonOperator': 'EQ'
                              }
                      }
                  )
        manual_allocation_data = manual_allocation_data.get('Items')

        if manual_allocation_data and len(manual_allocation_data) > 0:
          session_data['dispositions_punched'] = {'object': json.loads(convert_dynamodb_item_to_json(manual_allocation_data[0]).get('disposition_punched'))}

        session_data_new.append(session_data)

  except Exception as e:
      user_response = {"e": str(e), "stack": traceback.format_exc(), 'server': 4}
      session_data['manual_allocation_response'] = user_response
      session_data_new.append(session_data)

  return session_data_new

def get_user_info_threaded(session_ids):
  
  def get_user_info(session_list):
    session_data_new = []
    
    try:
      for session_data in session_list:
        data = dynamodb_client.query(
                      TableName=user_status_table,
                      Select='SPECIFIC_ATTRIBUTES',
                      AttributesToGet=['user_id', 'ekyc_req_time', 'ekyc_success', 'product_code'],
                      KeyConditions={
                              'user_id': {
                                  'AttributeValueList': [
                                      {
                                              'S': session_data.get('user_id', {}).get('S')
                                      }
                                  ],
                                  'ComparisonOperator': 'EQ'
                              }
                      }
                  )

        user_response = data.get('Items')[0]
        session_data['user_response'] = {'object': convert_dynamodb_item_to_json(user_response)}
        session_data_new.append(session_data)
    
    except Exception as e:
      user_response = {"e": e, "stack": traceback.format_exc(), 'server': 4, "sl": user_response}
      session_data['user_response'] = user_response
      session_data_new.append(session_data)


    return session_data_new

  pool = ThreadPool(10)
  updated_session_list = pool.map(get_user_info, session_ids)
  pool.close()
  pool.join()
  return updated_session_list

def get_manual_allocation_info_threaded(session_ids):
  pool = ThreadPool(10)
  updated_session_list = pool.map(get_manual_allocation_info, session_ids)
  pool.close()
  pool.join()
  return updated_session_list

def get_ekyc_req_time(session_data, user_data):
  ekyc_time = user_data.get("ekyc_req_time")
  
  if not bool(ekyc_time):
      ekyc_time = session_data.get("ekyc_req_time")
  
  if not bool(ekyc_time):
      try:
          response_ekyc = ekyc_table.query(
                  IndexName='user_id-client_name-index', 
                  KeyConditionExpression=Key('user_id').eq(session_data.get("user_id")) & Key('client_name').eq("ekyc")).get('Items')

          ekyc_time = response_ekyc[0]["ekyc_req_time"]
      except Exception as e:
          ekyc_time = ""
          
  return ekyc_time
              
def check_if_ekyc_pending(client_name, session_data, user_data, ekyc_time):
  is_ekyc_pending = True
  #check_expiry_time = client_name in ["RBL_uat", "RBL", "Digiremit_uat","Digiremit","Retailasset_uat","Retailasset"]

  is_ekyc_pending = not "ekyc_success" in user_data
  
  #if check_expiry_time:
  is_ekyc_pending = (ekyc_time == "") or (divmod(time.time() - float(ekyc_time), 60)[0] > ekyc_expiry_time_period[client_name])
      
  return is_ekyc_pending

def alter_invalid_status(client_name, rbl_session_status, session_data):
  if client_name in ["RBL","RBL_uat","BFL","BFL_uat","Retailasset_uat","Retailasset"]:
      if session_data.get('feedback') != "":
          try:
              if json.loads(session_data.get('feedback'))["type"] == "Reschedule":
                  rbl_session_status = "VCIP_RESCHEDULED"
          except Exception as e:
              print("error at checking feedback",str(e))
              
      if session_data.get('expiring_at_link_expiry') and session_data.get('expiry_reason') is None:
              rbl_session_status = "VCIP_EXPIRED"
              
  return rbl_session_status
              
def alter_checker_status(client_name, rbl_session_status, ekyc_time):
  if client_name in ["RBL","RBL_uat","Digiremit_uat","Digiremit","Retailasset_uat","Retailasset"]:
      if ekyc_time == "" or divmod(time.time() - float(ekyc_time), 60)[0] > ekyc_expiry_time_period[client_name]:
          rbl_session_status = "KYC_PENDING"
      else:
          rbl_session_status = "VCIP_PENDING"
          
  return rbl_session_status


def get_case_push_time(user_id):
  return get_first_session_detail(user_id).get("start_time")





def get_key(o,k):
    try:
        return o[k]
    except:
        return ""


def get_all_sessions_by_case_creation_date(client_name,request_json):
    request_json = {
                "required_keys": request_json.get("required_keys"),
                "filters":request_json,
                "start_time":request_json["start_time"],
                "end_time":request_json["end_time"],
                "session_type":"unique_user"
            }
    return get_session_list_new(client_name, request_json, skip_session_status_filter = True,kind="by_creation_date",get_latest_status_for_userid_only=False)


def get_all_sessions_by_attempt_date(client_name,request_json):
    request_json = {
                "required_keys": request_json.get("required_keys"),
                "filters":request_json,
                "start_time":request_json["start_time"],
                "end_time":request_json["end_time"],
                "session_type":"unique_user"
            }
    return get_session_list_new(client_name, request_json, skip_session_status_filter = True,kind="by_attempt_date",get_latest_status_for_userid_only=False)


def get_last_case_status_by_attempt_date(client_name,request_json):
    request_json = {
                "required_keys": request_json.get("required_keys"),
                "filters":request_json,
                "start_time":request_json["start_time"],
                "end_time":request_json["end_time"],
                "session_type":"unique_user"
            }
    return get_session_list_new(client_name, request_json, skip_session_status_filter = True,kind="by_attempt_date",get_latest_status_for_userid_only=True)

def get_latest_case_status_by_case_creation_date(client_name,request_json):
    request_json = {
                "required_keys": request_json.get("required_keys"),
                "filters":request_json,
                "start_time":request_json["start_time"],
                "end_time":request_json["end_time"],
                "session_type":"unique_user"
            }
    return get_session_list_new(client_name, request_json, skip_session_status_filter = True,kind="by_creation_date",get_latest_status_for_userid_only=True)

def get_session_list_new(client_name, request_json, skip_session_status_filter = False,kind="by_creation_date",get_latest_status_for_userid_only=True):
  #try:
  if True:
    content = request_json.get('filters')
    session_type = request_json.get('session_type', "")
    
    #res_items = get_session_data(client_name, content)
    #merged_df = abhishek.get_session_user_data_from_dynamo(res_items)


    #call_function("2023-09-22 00:00:00","2023-10-22 23:59:59")
    db_start_time = str(datetime.datetime.fromtimestamp(float(content["start_time"])+19800))
    db_end_time = str(datetime.datetime.fromtimestamp(float(content["end_time"])+19800))

    if kind == "by_creation_date":
        print("by created at!!!!!!!!!!!!!!!!!!")
        function_name = "retrieve_userid_and_sessionid_for_daterange"
    else:
        function_name = "retrieve_sessionid_for_daterange"

    res_items = postgres.call_function(db_start_time,db_end_time,client_name,function_name)

    merged_df = abhishek.get_session_user_data_from_dynamo(res_items)
    #merged_df["user_id"] = merged_df["user_id"].apply(lambda x:eval(x))

    final_data = data_processor.process_session_data(client_name, merged_df, skip_session_status_filter, get_latest_status_for_userid_only, False)

    if bool(request_json.get('required_keys')):
        final_dataframe = pd.DataFrame(final_data)
        columns_to_keep = request_json.get('required_keys')
        columns_to_keep = [col for col in columns_to_keep if col in final_dataframe.columns]
        df_filtered = final_dataframe[columns_to_keep]
        final_data = df_filtered.to_dict(orient="records")


    return jsonify({"session_data":final_data ,"count":len(final_data),"server":"62"}),200

    columns = [
        "phone_number_x",
        "feedback",
        "audit_end_time",
        "created_at",
        "agent_id_x",
        "end_time_x",
        "start_time",
        "session_id_x",
        "vkyc_start_time",
        "user_id",
        "session_status",
        "auditor_name",
        "disposition_punched",
        "CUSTNAME",
        "ekyc_req_time_x",
        "manual_agent",
        "queue",
        "audit_result",
        "audit_lock",
        "auditor_fdbk",
        "PRODUCT",
    ]

    res_items = merged_df[[col for col in columns if col in merged_df.columns]]
    # Add missing columns as empty
    for col in columns:
        if col not in res_items.columns:
            res_items[col] = None

    #res_items = merged_df[["phone_number_x","feedback","audit_end_time","created_at","agent_id_x","end_time_x","start_time","session_id_x","vkyc_start_time","user_id","session_status","auditor_name","disposition_punched","CUSTNAME","ekyc_req_time_x","manual_agent","queue","audit_result","audit_lock","auditor_fdbk","PRODUCT"]]#.to_dict(orient="records")
    #res_items = merged_df
    
    #res_items["manual_agent_x"] = res_items["manual_agent"].fillna({"manual_agent":{"S":""}})

    #res_items.loc[pd.isna(res_items["manual_agent"]),"manual_agent"] = [{"S":""}]
    #res_items.loc[pd.isna(res_items["disposition_punched"]),"disposition_punched"] = [{"S":""}]
    res_items.loc[pd.isna(res_items["audit_end_time"]),"audit_end_time"] = [{"N":""}]
    res_items.loc[pd.isna(res_items["ekyc_req_time_x"]),"ekyc_req_time_x"] = [{"S":""}]
    res_items.loc[pd.isna(res_items["feedback"]),"feedback"] = [{"S":""}]
    res_items.loc[pd.isna(res_items["audit_result"]),"audit_result"] = [{"N":""}]
    res_items.loc[pd.isna(res_items["auditor_fdbk"]),"auditor_fdbk"] = [{"S":""}]
    res_items.loc[pd.isna(res_items["auditor_name"]),"auditor_name"] = [{"S":""}]
    res_items.loc[pd.isna(res_items["end_time_x"]),"end_time_x"] = [{"S":""}]
    res_items.loc[pd.isna(res_items["audit_lock"]),"audit_lock"] = [{"BOOL":False}]
    res_items.loc[pd.isna(res_items["vkyc_start_time"]),"vkyc_start_time"] = [{"S":""}]
    res_items.loc[pd.isna(res_items["phone_number_x"]),"phone_number_x"] = [{"S":""}]
    res_items.loc[pd.isna(res_items["agent_id_x"]),"agent_id_x"] = [{"S":""}]
    res_items.loc[pd.isna(res_items["manual_agent"]),"manual_agent"] = [{"S":""}]

    res_items["vcip_agent_id"] = res_items["agent_id_x"]
    res_items["manual_agent_id"] = res_items["manual_agent"]
    del res_items["agent_id_x"]
    del res_items["manual_agent"]

    print("columnssssss: {}".format(res_items.columns))
    print("res-items len: ini{}".format(len(res_items)))

    '''
    res_items.loc[pd.isna(res_items["agent_id"]),"agent_id"] = [{"S":""}]
    res_items.loc[pd.isna(res_items["agent_name"]),"agent_name"] = [{"S":""}]
    '''

    #res_items.loc[pd.isna(res_items["CUSTNAME"]),"CUSTNAME"] = [{"S":""}]
    #res_items.loc[pd.isna(res_items["queue"]),"queue"] = [{"S":""}]
    #res_items["disposition_punched"] = res_items["disposition_punched"].fillna({"disposition_punched":{"S":""}})
    #res_items["ekyc_req_time"] = res_items["ekyc_req_time_x"].fillna({"ekyc_req_time":{"S":""}})
  

    
    #return jsonify(res_items.to_dict(orient="records")),200
    #res_items["user_id"] = res_items["user_id"].apply(lambda x:x["S"])
    res_items["ekyc_req_time"] = res_items["ekyc_req_time_x"].apply(lambda x:x.get("S",x.get("N","")))
    res_items["audit_result"] = res_items["audit_result"].apply(lambda x:x.get("N"))
    res_items["audit_end_time"] = res_items["audit_end_time"].apply(lambda x:x.get("N"))
    res_items["audit_lock"] = res_items["audit_lock"].apply(lambda x:x.get("BOOL",""))
    res_items["auditor_fdbk"] = res_items["auditor_fdbk"].apply(lambda x:x.get("S",""))
    res_items["auditor_name"] = res_items["auditor_name"].apply(lambda x:x.get("S",""))
    res_items["manual_agent_id"] = res_items["manual_agent_id"].apply(lambda x:get_key(x,"S"))
    res_items["vcip_agent_id"] = res_items["vcip_agent_id"].apply(lambda x:get_key(x,"S"))
    res_items["session_status"] = res_items["session_status"].apply(lambda x:x.get("S",""))
    res_items["end_time"] = res_items["end_time_x"].apply(lambda x:x.get("S",""))
    res_items["disposition_punched"] = res_items["disposition_punched"].apply(lambda x:get_key(x,"S"))
    res_items["CUSTNAME"] = res_items["CUSTNAME"].apply(lambda x:get_key(x,"S"))
    res_items["PRODUCT"] = res_items["PRODUCT"].apply(lambda x:get_key(x,"S"))
    res_items["queue"] = res_items["queue"].apply(lambda x:get_key(x,"S"))
    #res_items["session_id"] = res_items["session_id_x"].apply(lambda x:get_key(x,"S"))
    res_items["start_time"] = res_items["start_time"].apply(lambda x:x.get("S",""))
    res_items["vkyc_start_time"] = res_items["vkyc_start_time"].apply(lambda x:x.get("S",""))
    res_items["dispositions_punched"] = res_items["disposition_punched"].apply(lambda x: [] if x == "" else json.loads(x))
    res_items["feedback"] = res_items["feedback"].apply(lambda x:x.get("S",""))
    res_items["phone_number_x"] = res_items["phone_number_x"].apply(lambda x:x.get("S",""))

    res_items["session_id"] = res_items["session_id_x"]

    '''
    res_items["agent_id"] = res_items["agent_id"].apply(lambda x:x.get("S",""))
    res_items["agent_name"] = res_items["agent_name"].apply(lambda x:x.get("S",""))
    '''

    print("res-items len after pdna: {}".format(len(res_items)))
    #return jsonify(res_items),200
    res_items["rbl_session_status"] = res_items["session_status"].apply(lambda x:session_status_mapper[x])
    del res_items["end_time_x"]
    del res_items["ekyc_req_time_x"]
    del res_items["session_id_x"]

    agents_name_dict = get_agent_data(client_name)
    
    agents_df = pd.DataFrame(agents_name_dict)
    agents_df = agents_df[["agent_id","agent_name"]]

    #agents_df = agents_df.rename(columns={"agent_id":"db_agent_id","agent_name":"db_agent_name"})
    #print(agents_df.columns)
    
    res_items = res_items.merge(agents_df,how='left',left_on="vcip_agent_id",right_on="agent_id",suffixes=("","_vcip"))
    res_items = res_items.merge(agents_df,how='left',left_on='manual_agent_id',right_on='agent_id',suffixes=("","_manual"))
    res_items = res_items.merge(agents_df,how='left',left_on='auditor_name',right_on='agent_id',suffixes=("","_auditor"))

    print("res-items len afteragentdfmerge: {}".format(len(res_items)))
    print("columns after agentdf merge: {}".format(res_items.columns))

    ekyc_expiry_timer = ekyc_expiry_time_period[client_name]

    res_items["ekyc_status_calc"] = res_items["ekyc_req_time"].apply(lambda x: "KYC_PENDING" if x == "" else "VCIP_PENDING")
    res_items["ekyc_status_calc"] = res_items["ekyc_req_time"].apply(lambda x: "KYC_PENDING" if (x == "" or divmod(time.time() - float(x), 60)[0] > ekyc_expiry_timer) else "VCIP_PENDING")


    res_items["rbl_session_status_new"] = res_items["rbl_session_status"]
    res_items.loc[(res_items["rbl_session_status"] == "NOT_ASSIGNED_TO_AGENT") & (res_items["queue"] != "free"),"rbl_session_status_new"] = res_items["ekyc_status_calc"]
    res_items.loc[res_items["audit_result"] == "1","rbl_session_status_new"] = "CHECKER_APPROVED"
    res_items.loc[res_items["audit_result"] == "0","rbl_session_status_new"] = "CHECKER_REJECTED"
    res_items.loc[(res_items["audit_lock"] == True) & (res_items["audit_result"] == "") & (res_items["rbl_session_status_new"] == "VCIP_APPROVED"),"rbl_session_status_new"] = "CHECKER_PENDING"

    #res_items['rbl_session_status'] = res_items.apply(lambda row: row['ekyc_status_calc'] if row['rbl_session_status'] in ['VCIP_SESSION_INVALID', 'user_abandoned'] else row['rbl_session_status'],axis=1)
    print("res-items len after bs: {}".format(len(res_items)))

    res_items["auditor_fdbk"] = res_items["auditor_fdbk"].apply(lambda x:x.upper())
    #only_auditor_rejects = res_items[res_items["audit_result"] == "0"]
    res_items.loc[(res_items["audit_result"] == "0") & (res_items["auditor_fdbk"].str.contains("REVISIT")),"rbl_session_status_new"] = res_items["ekyc_status_calc"] 

    res_items["rbl_session_status"] = res_items["rbl_session_status_new"]

    res_items["ekyc_time"] = res_items["ekyc_req_time"]

    del res_items["rbl_session_status_new"]

    print("res-items after del status new: {}".format(len(res_items)))

    res_items['rbl_session_status'] = res_items.apply(lambda row: row['ekyc_status_calc'] if row['rbl_session_status'] in ['VCIP_SESSION_INVALID', 'user_abandoned','NOT_ASSIGNED_TO_AGENT'] else row['rbl_session_status'],axis=1)

    print("columnsssssssssssssssssss",res_items.columns)

    res_items['s_start_time'] = pd.to_datetime(res_items['start_time'], unit='s')

    res_items.sort_values(by='s_start_time', ascending=False,inplace=True)

    print("res-items before status skip: {}".format(len(res_items)))

    #res_items = res_items.duplicated(subset='user_id', keep='first')
    if skip_session_status_filter:
        print("skipping session status filter!!!!")
        pass
    else:
        users_to_drop = res_items[(res_items["rbl_session_status"] == "VCIP_APPROVED") & (res_items["audit_result"] == "")]["user_id"]
        res_items = res_items[~res_items["user_id"].isin(users_to_drop)]
        users_to_drop = res_items[res_items["rbl_session_status"] == "CHECKER_APPROVED"]["user_id"]
        res_items = res_items[~res_items["user_id"].isin(users_to_drop)]


    print("res-items before after skip: {}".format(len(res_items)))

    '''
    duplicate_mask = res_items.duplicated(subset='user_id', keep='first')

    res_items.loc[duplicate_mask, 'session_status'] = "VCIP_SESSION_INVALID"

    res_items.reset_index(inplace=True, drop=True)
    '''
    #res_items.to_csv("userstatus.csv",index=False)
    if get_latest_status_for_userid_only:
        res_items.drop_duplicates(subset=["user_id"],keep='first',inplace=True)
    else:
        print("skipping drop duplicates of userid!!!")
        pass
    #status_to_skip = ['VCIP_APPROVED','CHECKER_APPROVED','CHECKER_PENDING','CHECKER_REJECTED','VCIP_REJECTED','CHECKER_PENDING','VCIP_EXPIRED']
    #res_items = res_items[~res_items['rbl_session_status'].isin(status_to_skip)]

    nnow = datetime.datetime.now(pytz.FixedOffset(330)) - datetime.timedelta(seconds=3600*24*30)
    print(res_items["created_at"].head())
    print(pd.to_datetime(res_items["created_at"].head()))
    print(nnow)
    res_items.loc[(res_items["rbl_session_status"].isin(["KYC_PENDING","VCIP_PENDING"])) & (pd.to_datetime(res_items["created_at"]) < nnow), "rbl_session_status"] = "VCIP_EXPIRED" 

    status_to_keep = ["KYC_PENDING","VCIP_PENDING"]
    if skip_session_status_filter:
        print("skipping session status filter 2!!!!")
        pass
    else:
        res_items = res_items[res_items["rbl_session_status"].isin(status_to_keep)]

    print("res-items before after skip 2: {}".format(len(res_items)))

    #res_items = res_items.reset_index(drop=True)
    res_items.fillna("",inplace=True) 
    res_items.loc[res_items["rbl_session_status"] == "CHECKER_APPROVED","rbl_session_status"] = "Audited and Okay"
    res_items.loc[res_items["rbl_session_status"] == "CHECKER_REJECTED","rbl_session_status"] = "Audited and not Okay"
 
    

    #del res_items["phone_number"]
    res_items["phone_number"] = res_items["phone_number_x"]
    #del res_items["phone_number_x"]

    res_items = res_items.to_dict(orient="records")
    print("final return length: {}".format(len(res_items)))

    return jsonify({"session_data":res_items,"count":len(res_items),"server":"62"}),200
   
