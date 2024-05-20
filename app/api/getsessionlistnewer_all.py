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


def get_session_list_new(client_name, request_json):
  #try:
  if True:
    content = request_json.get('filters')
    session_type = request_json.get('session_type', "")
    
    #res_items = get_session_data(client_name, content)
    #merged_df = abhishek.get_session_user_data_from_dynamo(res_items)


    #call_function("2023-09-22 00:00:00","2023-10-22 23:59:59")
    db_start_time = str(datetime.datetime.fromtimestamp(float(content["start_time"])+19800))
    db_end_time = str(datetime.datetime.fromtimestamp(float(content["end_time"])+19800))
    res_items = postgres.call_function(db_start_time,db_end_time,client_name)

    merged_df = abhishek.get_session_user_data_from_dynamo(res_items)
    merged_df["user_id"] = merged_df["user_id"].apply(lambda x:eval(x))
    res_items = merged_df[["end_time_x","start_time","session_id_x","user_id","session_status","auditor_name","vkyc_start_time","disposition_punched","CUSTNAME","ekyc_req_time_x","manual_agent","queue","audit_result","audit_lock","auditor_fdbk"]]#.to_dict(orient="records")
    #res_items = merged_df
    
    #res_items["manual_agent_x"] = res_items["manual_agent"].fillna({"manual_agent":{"S":""}})

    #res_items.loc[pd.isna(res_items["manual_agent"]),"manual_agent"] = [{"S":""}]
    #res_items.loc[pd.isna(res_items["disposition_punched"]),"disposition_punched"] = [{"S":""}]
    res_items.loc[pd.isna(res_items["ekyc_req_time_x"]),"ekyc_req_time_x"] = [{"S":""}]
    res_items.loc[pd.isna(res_items["audit_result"]),"audit_result"] = [{"N":""}]
    res_items.loc[pd.isna(res_items["auditor_fdbk"]),"auditor_fdbk"] = [{"S":""}]
    res_items.loc[pd.isna(res_items["auditor_name"]),"auditor_name"] = [{"S":""}]
    res_items.loc[pd.isna(res_items["end_time_x"]),"end_time_x"] = [{"S":""}]
    res_items.loc[pd.isna(res_items["audit_lock"]),"audit_lock"] = [{"BOOL":False}]
    res_items.loc[pd.isna(res_items["vkyc_start_time"]),"vkyc_start_time"] = [{"S":""}]

    '''
    res_items.loc[pd.isna(res_items["agent_id"]),"agent_id"] = [{"S":""}]
    res_items.loc[pd.isna(res_items["agent_name"]),"agent_name"] = [{"S":""}]
    '''

    #res_items.loc[pd.isna(res_items["CUSTNAME"]),"CUSTNAME"] = [{"S":""}]
    #res_items.loc[pd.isna(res_items["queue"]),"queue"] = [{"S":""}]
    #res_items["disposition_punched"] = res_items["disposition_punched"].fillna({"disposition_punched":{"S":""}})
    #res_items["ekyc_req_time"] = res_items["ekyc_req_time_x"].fillna({"ekyc_req_time":{"S":""}})
  

    
    #return jsonify(res_items.to_dict(orient="records")),200
    res_items["user_id"] = res_items["user_id"].apply(lambda x:x["S"])
    res_items["ekyc_req_time"] = res_items["ekyc_req_time_x"].apply(lambda x:x.get("S",x.get("N","")))
    res_items["audit_result"] = res_items["audit_result"].apply(lambda x:x.get("N"))
    res_items["audit_lock"] = res_items["audit_lock"].apply(lambda x:x.get("BOOL",""))
    res_items["auditor_fdbk"] = res_items["auditor_fdbk"].apply(lambda x:x.get("S",""))
    res_items["manual_agent"] = res_items["manual_agent"].apply(lambda x:get_key(x,"S"))
    res_items["session_status"] = res_items["session_status"].apply(lambda x:x.get("S",""))
    res_items["auditor_name"] = res_items["auditor_name"].apply(lambda x:x.get("S",""))
    res_items["end_time"] = res_items["end_time_x"].apply(lambda x:x.get("S",""))
    res_items["disposition_punched"] = res_items["disposition_punched"].apply(lambda x:get_key(x,"S"))
    res_items["CUSTNAME"] = res_items["CUSTNAME"].apply(lambda x:get_key(x,"S"))
    res_items["queue"] = res_items["queue"].apply(lambda x:get_key(x,"S"))
    res_items["session_id"] = res_items["session_id_x"].apply(lambda x:get_key(x,"S"))
    res_items["start_time"] = res_items["start_time"].apply(lambda x:x.get("S",""))

    res_items["dispositions_punched"] = res_items["disposition_punched"].apply(lambda x: [] if x == "" else json.loads(x))


    '''
    res_items["agent_id"] = res_items["agent_id"].apply(lambda x:x.get("S",""))
    res_items["agent_name"] = res_items["agent_name"].apply(lambda x:x.get("S",""))
    '''


    #return jsonify(res_items),200
    res_items["rbl_session_status"] = res_items["session_status"].apply(lambda x:session_status_mapper[x])
    del res_items["end_time_x"]
    del res_items["ekyc_req_time_x"]

    agents_name_dict = get_agent_data(client_name)
    
    agents_df = pd.DataFrame(agents_name_dict)
    agents_df = agents_df[["agent_id","agent_name"]]
    res_items = res_items.merge(agents_df,how='left',left_on='manual_agent',right_on='agent_id')


    res_items.loc[pd.isna(res_items["agent_id"]),"agent_id"] = ""
    res_items.loc[pd.isna(res_items["agent_name"]),"agent_name"] = ""

    res_items["manual_agent_name"] = res_items["agent_name"]
    del res_items["agent_name"]

    #res_items["agent_id"] = res_items["agent_id"].apply(lambda x:x.get("S",""))
    #res_items["agent_name"] = res_items["agent_name"].apply(lambda x:x.get("S",""))

    ekyc_expiry_timer = ekyc_expiry_time_period[client_name]

    res_items["ekyc_status_calc"] = res_items["ekyc_req_time"].apply(lambda x: "KYC_PENDING" if x == "" else "VCIP_PENDING")
    res_items["ekyc_status_calc"] = res_items["ekyc_req_time"].apply(lambda x: "KYC_PENDING" if (x == "" or divmod(time.time() - float(x), 60)[0] > ekyc_expiry_timer) else "VCIP_PENDING")
    
    res_items["rbl_session_status_new"] = res_items["rbl_session_status"]
    res_items.loc[(res_items["rbl_session_status"] == "NOT_ASSIGNED_TO_AGENT") & (res_items["queue"] != "free"),"rbl_session_status_new"] = res_items["ekyc_status_calc"]
    res_items.loc[res_items["audit_result"] == "1","rbl_session_status_new"] = "CHECKER_APPROVED"
    res_items.loc[res_items["audit_result"] == "0","rbl_session_status_new"] = "CHECKER_REJECTED"
    res_items.loc[(res_items["audit_lock"] == True) & (res_items["audit_result"] == "") & (res_items["rbl_session_status_new"] == "VCIP_APPROVED"),"rbl_session_status_new"] = "CHECKER_PENDING"

    #res_items['rbl_session_status'] = res_items.apply(lambda row: row['ekyc_status_calc'] if row['rbl_session_status'] in ['VCIP_SESSION_INVALID', 'user_abandoned'] else row['rbl_session_status'],axis=1)


    res_items["auditor_fdbk"] = res_items["auditor_fdbk"].apply(lambda x:x.upper())
    #only_auditor_rejects = res_items[res_items["audit_result"] == "0"]
    res_items.loc[(res_items["audit_result"] == "0") & (res_items["auditor_fdbk"].str.contains("REVISIT")),"rbl_session_status_new"] = res_items["ekyc_status_calc"] 

    res_items["rbl_session_status"] = res_items["rbl_session_status_new"]

    res_items["ekyc_time"] = res_items["ekyc_req_time"]

    del res_items["rbl_session_status_new"]

    res_items['rbl_session_status'] = res_items.apply(lambda row: row['ekyc_status_calc'] if row['rbl_session_status'] in ['VCIP_SESSION_INVALID', 'user_abandoned','NOT_ASSIGNED_TO_AGENT'] else row['rbl_session_status'],axis=1)

    print("columnsssssssssssssssssss",res_items.columns)

    res_items['s_start_time'] = pd.to_datetime(res_items['start_time'], unit='s')

    res_items.sort_values(by='s_start_time', ascending=False,inplace=True)

    #res_items.sort_values(by='start_time', ascending=False,inplace=True)

    #res_items = res_items.duplicated(subset='user_id', keep='first')
    
    '''
    duplicate_mask = res_items.duplicated(subset='user_id', keep='first')

    res_items.loc[duplicate_mask, 'session_status'] = "VCIP_SESSION_INVALID"

    res_items.reset_index(inplace=True, drop=True)
    '''

    res_items.drop_duplicates(subset=["user_id"],keep='first',inplace=True)

    #status_to_skip = ['VCIP_APPROVED','CHECKER_APPROVED','CHECKER_PENDING','CHECKER_REJECTED','VCIP_REJECTED','CHECKER_PENDING']
    #res_items = res_items[~res_items['rbl_session_status'].isin(status_to_skip)]

    status_to_keep = ["KYC_PENDING","VCIP_PENDING"]
    #res_items = res_items[res_items["rbl_session_status"].isin(status_to_keep)]


    #res_items = res_items.reset_index(drop=True)

    res_items = res_items.to_dict(orient="records")


    return jsonify({"session_data":res_items,"count":len(res_items)}),200
  
  

    #ekyc_table = dynamodb.Table(EKYC_STATUS_TABLE)


    #json_session_list = []
    #error_object = {}
    for session in one_d_session_list:
        session_data = convert_dynamodb_item_to_json(session)
        
        user_response = session_data.get('user_response', {})
        try:
            session_extras = (
              json.loads(session_data.get('extras')) 
              if bool(session_data.get('extras')) and (isinstance(session_data.get('extras'), str) or type(session_data.get('extras')) == unicode)
              else session_data.get('extras') 
            )
        except:
            session_extras = str(type(session_data.get('extras')))
    
                 
        # Alter Session status start ---------------


        rbl_session_status = session_status_mapper[session_data.get('session_status')]
        
        ekyc_time = get_ekyc_req_time(session_data, user_response)

        if rbl_session_status in ["NOT_ASSIGNED_TO_AGENT"]:
            if session_data.get('queue') == "free":
                rbl_session_status = "NOT_ASSIGNED_TO_AGENT"
            else:
                rbl_session_status = "KYC_PENDING" if check_if_ekyc_pending(client_name, session_data, user_response, ekyc_time) else "VCIP_PENDING"


        session_data['rbl_session_status_2'] = rbl_session_status

        if 'audit_result' in session_data and int(session_data.get('audit_result')) == 1:
            rbl_session_status = "CHECKER_APPROVED"
        elif 'audit_result' in session_data and int(session_data.get('audit_result')) == 0:
            rbl_session_status = "CHECKER_REJECTED"                  
        elif rbl_session_status in ["VCIP_APPROVED"] and bool(session_data.get('audit_lock')):
            rbl_session_status = "CHECKER_PENDING"                    

        # if rbl_session_status == "VCIP_SESSION_INVALID":
            # rbl_session_status = alter_invalid_status(client_name, rbl_session_status, session_data)

        if client_name in ["BFL","BFL_uat"]:
            if session_data.get('session_status') == "kyc_rejected":
                rbl_session_status = "kyc_rejected"
                        
        if rbl_session_status == "CHECKER_REJECTED" and "REVISIT" in session_data.get('auditor_fdbk').upper():
            rbl_session_status = alter_checker_status(client_name, rbl_session_status, ekyc_time)


        if client_name in ["Retailasset"] and ekyc_time == "1651343400":
          ekyc_time = ""
        
        session_data['TA_session_status'] = session_data.get("session_status")
        session_data['session_status'] = rbl_session_status
        session_data['rbl_session_status_1'] = rbl_session_status
        session_data['check_if_ekyc_pending_out'] = check_if_ekyc_pending(client_name, session_data, user_response, ekyc_time)
        session_data['session_extras'] = str(session_extras)
        

        
        session_data['PRODUCT'] = session_data.get("PRODUCT")
        session_data['CUSTNAME'] = session_data.get("CUSTNAME") if bool(session_data.get("CUSTNAME")) and isinstance(session_data.get("CUSTNAME"), str) else "" 
        if not bool(session_data.get("CUSTNAME")) and isinstance(session_extras, dict):
            session_data['CUSTNAME'] = (
              session_extras.get("CUSTNAME") 
              if bool(session_extras.get("CUSTNAME")) 
              else session_extras.get("name") 
                if bool(session_extras.get("name")) 
                else session_extras.get("user_info", {}).get("name", "")
            )
        
        
        
        error_object = {"sample session_extras": "1", "isinstance": isinstance(session_data.get('extras'), str), "type": str(type(session_data.get('extras'))), "session_data": session_data, "session_extras": session_extras}
        
        
        get_first_session_detail_out = {"sample": "1"} # this is for debugging
        if not bool(session_data['PRODUCT']) or session_data['PRODUCT'] in ("NA") or session_data['CUSTNAME'] in ("DEfaultName"):
          get_first_session_detail_out = get_first_session_detail(session_data.get("user_id"))
          session_data['get_first_session_detail_out'] = get_first_session_detail_out
          
          try:
              get_first_session_detail_extras = (
                json.loads(get_first_session_detail_out.get('extras')) 
                if bool(get_first_session_detail_out.get('extras')) 
                else {"sample": "2"}
                ) # this else condition is for debugging
          except:
              get_first_session_detail_extras = str(type(get_first_session_detail_out.get('extras')))
              
          session_data['get_first_session_detail_extras'] = get_first_session_detail_extras
          session_data['PRODUCT'] = get_first_session_detail_out.get("PRODUCT")
              
          
        if not bool(session_data['PRODUCT']) or session_data['PRODUCT'] in ("NA"):
          try:
            session_data['PRODUCT'] = get_first_session_detail_extras.get("product_code")
          except Exception as e:
            session_data['PRODUCT_exception'] = str(e)
            
        
        if (not bool(session_data['CUSTNAME']) or session_data['CUSTNAME'] in ("DEfaultName")) and bool(get_first_session_detail_out):
          session_data['CUSTNAME'] = get_first_session_detail_out.get("CUSTNAME")
          


        session_data['phone_number'] = session_data.get("phone_number") if bool(session_data.get("phone_number")) else user_response.get('phone_number')
        if not bool(session_data['phone_number']):
          session_data['phone_number'] = get_first_session_detail_out.get("phone_number")
         
         
        session_data['manual_agent_name'] = agents_name_dict.get(session_data.get('manual_agent', ""), {}).get("agent_name")
        session_data['agent_name'] = agents_name_dict.get(session_data.get('agent_id', ""), {}).get("agent_name")
        session_data['ekyc_time'] = ekyc_time
        session_data['auditor_id'] = session_data.get('auditor_name', "")
        session_data['auditor_name_redable'] = agents_name_dict.get(session_data.get('auditor_name', ""), {}).get("agent_name")
        session_data['customer_name'] = session_data['CUSTNAME'] # this key is redundent, it is kept becasuse consumers are expecting this key
        session_data['fraud_advisory_given'] = "True" if bool(session_data.get('auditor_name', '')) else "False"
        session_data['call_center'] = agents_name_dict.get(session_data.get('agent_id', ""), {}).get("call_center")
        session_data['end_time'] = session_data.get('end_time', "")
          

        # Alter Session status end ---------------



        json_session_list.append(session_data)

  #except Exception as e:
  #  return jsonify({'exception': str(e), "stack": traceback.format_exc(), "server": 4, "error_object": error_object}), 500


  # return jsonify({'session_count': len(res_items), 'session_data': json_session_list, 'server': 4}), 200


  temp_df = pd.DataFrame(json_session_list)
  if temp_df.shape[0] > 0:
          # Replace NaN with empty string or any other value
          temp_df.fillna("", inplace=True)


          # To get the latest_session_status for all the user
          unique_users_df = pd.DataFrame(temp_df['user_id'].unique(), columns=['user_id'])
          unique_users_df['latest_session_status'] = unique_users_df['user_id'].apply(get_latest_session_detail)
          temp_df = pd.merge(temp_df, unique_users_df, on='user_id', how='left')
          
          
          # Existing statuses to skip
          statuses_to_skip = ["CHECKER_APPROVED", "NOT_ASSIGNED_TO_AGENT", "session_expired", "VCIP_APPROVED", "VCIP_EXPIRED"]
          statuses_to_mark_invalid = ["kyc_result_approved", "waiting"]
          
          temp_df.sort_values("start_time",inplace=True,ascending=False)

          # Update session_status for duplicated user_id except for the statuses in statuses_to_skip
          condition = (
              temp_df.duplicated("user_id", keep="first") &
              ~temp_df["session_status"].isin(statuses_to_skip)
          )
          
          temp_df.loc[condition, "session_status"] = "VCIP_SESSION_INVALID"

          

              
          if session_type and session_type in ("unique_user"):
            # Find user_ids that have session with "TA_session_status" != "kyc_result_approved"
            users_without_approved_TA = temp_df.loc[temp_df["TA_session_status"] != "kyc_result_approved", 'user_id'].unique()

            # Find user_ids that have session with "latest_session_status" == "kyc_result_approved"
            users_with_approved_latest = temp_df.loc[temp_df["latest_session_status"] == "kyc_result_approved", 'user_id'].unique()

            # Find intersection of both lists: these are the users who have neither session type
            users_to_remove = set(users_without_approved_TA).intersection(users_with_approved_latest)

            # Remove rows corresponding to these user_ids
            temp_df = temp_df[~temp_df['user_id'].isin(users_to_remove)]


          # Additional condition if the column exists
          if 'latest_session_status' in temp_df.columns:

              condition = (
                  (temp_df["TA_session_status"] != temp_df["latest_session_status"]) &
                  temp_df["latest_session_status"].isin(statuses_to_mark_invalid)
              )
              temp_df.loc[condition, "session_status"] = "VCIP_SESSION_INVALID"
                
                
              
              

          if client_name in ["RBL_uat", "RBL","Retailasset_uat","Retailasset","Digiremit_uat","Digiremit"]:
              temp_df.loc[temp_df["session_status"] == "NOT_ASSIGNED_TO_AGENT", "session_status"] = "VCIP_PENDING"

          temp_df.loc[temp_df["session_status"] == "CHECKER_APPROVED", "session_status"] = "Audited and Okay"
          temp_df.loc[temp_df["session_status"] == "CHECKER_REJECTED", "session_status"] = "Audited and not okay"
          temp_df.loc[temp_df["session_status"] == "session_expired", "session_status"] = "VCIP_EXPIRED"
          
          
          if session_type and session_type in ("report_generation"):
            unique_users_df['case_push_time'] = unique_users_df['user_id'].apply(get_case_push_time)
            temp_df = pd.merge(temp_df, unique_users_df, on='user_id', how='left')
            
            

          final_out_n = temp_df.to_dict(orient="records")
  else:
      final_out_n = json_session_list

  return jsonify({'session_count': len(res_items), 'session_data': final_out_n, 'server': 4}), 200

