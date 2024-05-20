import datetime
import json
import logging
import multiprocessing
import time

import boto3
import pandas as pd
import requests
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from flask import (Blueprint, Flask, current_app, g, jsonify, make_response,
                   request)
from app.config import session_status_mapper
from app.config import dynamodb, dynamodb_client, MANUAL_ALLOCATION_TABLE, user_status_table, EKYC_STATUS_TABLE, session_status_mapper, ekyc_expiry_time_period
from app.helpers import getPOCTableName, convert_dynamodb_item_to_json, get_latest_session_detail, get_first_session_detail
from auth.decorators import login_required
from boto3.dynamodb.conditions import Key, Attr
bp = Blueprint("get_session_list", __name__)

# creating a new logger for testing
logger = logging.getLogger("debug_logger")
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(filename)s:%(lineno)d - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


dynamodb_client = boto3.client("dynamodb",region_name="ap-south-1")

# Custom serializer
def custom_serializer(data):
    serializer = TypeSerializer()
    serialized_data = serializer.serialize(data)
    return serialized_data

def custom_deserializer(data):
    try:
        if data == "":
            return None
        deserializer = TypeDeserializer()
        deserialized_data = deserializer.deserialize(data)
        return deserialized_data
    except Exception as e:
        logger.debug(data)
        logger.debug(str(e))
        return ""

def fetch_sessions_with_dispositions(client_name, start_date, end_date):
    """Fetch sessions between two dates grouped by user id"""
    url = "https://kbipvd.thinkanalytics.in:6356/postgres/rpc/get_grouped_sessions_with_dispositions_with_time"

    payload = json.dumps({
        "start_date": str(start_date),
        "end_date": str(end_date)
    })
    
    logger.debug(payload)
    
    headers = {
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoicmJscHJvZCJ9.iPQPLTcg0qU-pFcmTdI_rVEhHA_MX6WDY-3ep6tXFCo',
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    data = response.json()
    
    return data

def build_data_frame(data):
    logger.debug(data[0])
    df = pd.DataFrame(data)
      
    # split data frame into two with sessions and dispostions
    session_df = df[["user_id", "session_ids", "session_status", "session_created_at", "user_created_at"]]
    session_df = session_df.drop_duplicates()
    session_df['session_created_at'] = pd.to_datetime(session_df['session_created_at'])
    session_df = session_df.rename(columns={"session_ids": "session_id"})
    
    disposition_df = df[["user_id", "dispositions", "disposition_created_at", "disposition_agent_id"]]
    disposition_df = disposition_df.dropna(subset=['disposition_created_at', 'dispositions'])  
    disposition_df['dispositions'] = df.apply(lambda row: {'disposition': row['dispositions'], 'disposition_created_at': row['disposition_created_at'], 'disposition_agent_id': row['disposition_agent_id']}, axis=1)
    disposition_df = disposition_df[["user_id", "dispositions"]]
    
    return disposition_df, session_df

def generate_merged_unique_df(sessions_df, disposition_df):
    # only get the latest values for the distinct user id based on creation time
    # Group by 'user_id' and get the index of rows with the latest 'created_at' timestamp
    grouped_df = disposition_df.groupby('user_id').agg(list)
    logger.debug("Grouped df")
    logger.debug(grouped_df.head())
    
    # now the same operation needs to be done for the sessions_df
    latest_indices = sessions_df.groupby('user_id')['session_created_at'].idxmax()
    # Use the indices to filter the DataFrame
    sessions_filtered_df = sessions_df.loc[latest_indices]
    
    # merging the two dataframes using user_id as the common key and renaming the created_at
    # to dispostion_created at and session_created at
    # Merge DataFrames on 'user_id'
    merged_df = pd.merge(
        sessions_filtered_df, 
        grouped_df, 
        on='user_id', how='left'
    )
    merged_df = merged_df.fillna(value="")
    logger.debug("Length of df from SQL {}".format(len(merged_df)))
    # Rename the columns
    # merged_df = merged_df.rename(columns={'created_at_x': 'session_created_at', 'created_at_y': 'disposition_created_at'})

    return merged_df

def generated_merged_all_df(sessions_df, disposition_df):
    # for type all we are only returning the count for the dispositions then
    # the disposition data wont be required so first converting that
    grouped_df = disposition_df.groupby('user_id').agg(list)

    # Merge session_df with grouped_df based on 'user_id'
    merged_df = pd.merge(sessions_df, grouped_df, on='user_id', how='left')

    merged_df = merged_df.fillna(value="")
    logger.debug(len(merged_df))

    return merged_df

def do_dynamo_pull(session_list):
    get_attr_list = ["vkyc_start_time","session_id","queue",'session_status',"user_id","start_time","end_time","agent_assignment_time","audit_result",'extras','audit_id','feedback',"manual_agent","expiry_reason","expiring_at_link_expiry","CUSTNAME","auditor_fdbk","ekyc_req_time","audit_end_time","PRODUCT","phone_number","agent_id","audit_lock", "auditor_name", "fraud_advisory_given"]
    data = dynamodb_client.batch_get_item(
        RequestItems={
            "kwikid_vkyc_session_status":{
                "Keys":session_list,
                'AttributesToGet': get_attr_list,
            }
        }
    )
    bat = data["Responses"]["kwikid_vkyc_session_status"]
    return bat

def chunks(lst, chunk_size):
    """Breaks the sessions list in chunks"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]
        
def get_session_data(session_list):
    # creating a session serialized list for querying the
    # sessions
    session_list = [{
        "session_id": custom_serializer(session_id)
    } for session_id in session_list]
    chunked_session_ids = list(chunks(session_list, 100))
    
    # running batch get items on pool
    pool = multiprocessing.Pool(5)
    results = pool.map(do_dynamo_pull, chunked_session_ids)
    pool.close()
    pool.join()
    
    # the result will be a 2d array flattening that array
    sessions = []
    for session_data in results:
        sessions += session_data
     
    # creating a data frame and formatting the data
    df = pd.DataFrame(sessions)
    df = df.fillna(value="")
    df_formatted = pd.DataFrame(columns=df.columns)
    for column in df.columns:
        df_formatted[column] = df[column].apply(custom_deserializer)
    df_formatted = df_formatted.fillna(value="")

    logger.debug("Length of sessions df {}".format(len(df_formatted)))
    logger.debug(df_formatted.head())
    return df_formatted

def do_dynamo_pull_usertable(user_list):
        # print("called")
        data = dynamodb_client.batch_get_item(
                        RequestItems={
                            "kwikid_vkyc_user_status":{
                                "Keys":user_list
                            }
                        }
                    )
        bat = data["Responses"]["kwikid_vkyc_user_status"]
        return bat



def do_dynamo_pull_manualallocationtable(user_list):
        # print("called")
        data = dynamodb_client.batch_get_item(
                        RequestItems={
                            "kwik_id_user_manual_allocation":{
                                "Keys":user_list,
                                "AttributesToGet":['disposition_punched',"user_id"]
                            }
                        }
                    )
        bat = data["Responses"]["kwik_id_user_manual_allocation"]
        return bat

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

def get_session_and_user_data(session_list, client_name="RBL"):
    # create chunks of the sessions and query those sessions
    session_list = [{"session_id": custom_serializer(session)} for session in session_list]
    chunked_session_ids = list(chunks(session_list, 100))

    # query the sesssion from dynamodb
    pool = multiprocessing.Pool(5)
    results = pool.map(do_dynamo_pull, chunked_session_ids)
    pool.close()
    pool.join()

    # from the sessions queried find the users to be
    # queried and merged in the df
    user_id_list = []
    for result in results:
        user_id_list = user_id_list + result

    # from the sessions get the user ids
    session_df = pd.DataFrame(user_id_list)
    session_df_og = session_df
    session_df["user_id"] = session_df["user_id"].apply(lambda x: str(x))
    
    # dont drop the duplicates here
    session_df = session_df.drop_duplicates("user_id") 
    
    # from the user list fetcht the users
    user_list_gen = session_df["user_id"].apply(lambda x: eval(x)).apply(lambda x: {"user_id":x})
    user_list_gen = user_list_gen.values.tolist()
    
    # querying all the users from dynamodb
    chunked_user_ids = list(chunks(user_list_gen, 100))
    pool = multiprocessing.Pool(5)
    results = pool.map(do_dynamo_pull_usertable, chunked_user_ids)
    pool.close()
    pool.join()
    
    # merging the user responses
    user_data = []
    for result in results:
        user_data = user_data + result
    
    user_df = pd.DataFrame(user_data)
    pool = multiprocessing.Pool(5)
    results = pool.map(do_dynamo_pull_manualallocationtable, chunked_user_ids)
    pool.close()
    pool.join()

    user_data = []
    for result in results:
        user_data = user_data + result

    disposition_df = pd.DataFrame(user_data)
    session_df_og["user_id"] = session_df_og["user_id"].astype(str)
    user_df["user_id"] = user_df["user_id"].astype(str)
    disposition_df["user_id"] = disposition_df["user_id"].astype(str)

    merged_df = session_df_og.merge(user_df,how='left',left_on='user_id',right_on='user_id')
    merged_df = merged_df.merge(disposition_df,how='left',left_on='user_id',right_on='user_id')
    return merged_df
    
def process_attempt_time_data(data, session_type, required_keys, queries, client_name = "RBL"):
    # firstly create a data frame 
    disposition_df, sessions_df = build_data_frame(data)
    
    # the i will process the session type as that will determine the no of rows actually to be filtered from
    if session_type == "unique":
        merged_df = generate_merged_unique_df(sessions_df, disposition_df)
    elif session_type == "all":
        merged_df = generated_merged_all_df(sessions_df, disposition_df)
        logger.debug("Length of initial data {}".format(len(merged_df))) 
    else:
        raise ValueError("The session type passed was invalid should be all/unique")

    # run batch get item to get the session id data
    sessions_list = merged_df["session_id"].tolist()
    session_df = get_session_data(sessions_list)

    merged_df = pd.merge(merged_df, session_df, on="session_id", how="left")
    logger.debug("Length after merging session data {}".format(len(merged_df)))
    merged_df["session_status"] = merged_df["session_status_y"]
    merged_df["user_id"] = merged_df["user_id_x"]
    merged_df = merged_df.drop(columns=["session_status_x", "session_status_y", "user_id_x", "user_id_y"])

    agents_name_dict = get_agent_data(client_name)
    agents_df = pd.DataFrame(agents_name_dict)
    agents_df = agents_df[["agent_id","agent_name"]]

    merged_df = merged_df.merge(agents_df,how='left',left_on='manual_agent',right_on='agent_id')

    merged_df["agent_id"] = merged_df["agent_id_x"]
    merged_df = merged_df.drop(columns=["agent_id_x", "agent_id_y"])
    merged_df["rbl_session_status"] = merged_df["session_status"].apply(lambda x:session_status_mapper[x])
    columns_to_typecast = merged_df.columns.tolist()
    columns_to_typecast.remove("dispositions")
    logger.debug(columns_to_typecast)
    merged_df[columns_to_typecast] = merged_df[columns_to_typecast].astype(str)

    # then run the qureies I am assuming that the queries will be only the sql data
    for query_key, query_value in queries.items():
        # query operator fro now is always in should be customized later
        # to add on the new opreations
        query_values = [query_value] if isinstance(query_value, str) else query_value
        merged_df = merged_df[merged_df[query_key].isin(query_value)]
        
    return merged_df

def fetch_user_and_sessions(client_name, start_time, end_time):
    payload = {
        "start_date": start_time,
        "end_date": end_time,
        "client_name_r": client_name
    }
    
    payload = json.dumps(payload)
    
    headers = {
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoicmJscHJvZCJ9.iPQPLTcg0qU-pFcmTdI_rVEhHA_MX6WDY-3ep6tXFCo',
        'Content-Type': 'application/json'
    }

    res = requests.request(
        "POST", 
        "https://kbipvd.thinkanalytics.in:6356/postgres/rpc/retrieve_userid_and_sessionid_for_daterange",
        headers=headers,
        verify=False,
        data=payload
    )

    data = res.json()
    logger.debug(data[0])
    
    return res.json()

def generate_user_merged_unique_df(user_df):
    # drop duplicates from the user df as they wont be used
    # further
    user_df = user_df.drop_duplicates(subset="user_id") 
    return user_df

def generated_user_merged_all_df(user_df):
    #sessions_list = user_df["session_id"].tolist()
    #logger.debug(sessions_list)

    # formatting the data recieved
    #merged_df = get_session_and_user_data(sessions_list)
    #merged_df = merged_df.fillna(value="")
    #df_formatted = pd.DataFrame(columns=merged_df.columns)
    #for column in merged_df.columns:
        #   df_formatted[column] = merged_df[column].apply(custom_deserializer)
    #df_formatted = df_formatted.fillna(value="")

    # logger.debug(df_formatted.head())

    #return df_formatted
    return user_df
    
def process_creation_time_data(data, session_type, required_keys, queries, client_name="RBL", start_time = None, end_time = None):
    users_df = pd.DataFrame(data)
    
    # the i will process the session type as that will determine the no of rows actually to be filtered from
    if session_type == "unique":
        merged_df = generate_user_merged_unique_df(users_df)
    elif session_type == "all":
        merged_df = generated_user_merged_all_df(users_df)
    else:
        raise ValueError("The session type passed was invalid should be all/unique")

    # run batch get item to get the session id data
    # merged_df["session_id"] = merged_df["session_id_y"]
    sessions_list = merged_df["session_id"].tolist()
    session_df = get_session_data(sessions_list)

    merged_df = pd.merge(merged_df, session_df, on="session_id", how="left")
    logger.debug(merged_df.columns)
    logger.debug("Length after merging session data {}".format(len(merged_df)))
    #merged_df["session_status"] = merged_df["session_status_y"]
    merged_df["user_id"] = merged_df["user_id_x"]
    merged_df = merged_df.drop(columns=["user_id_x", "user_id_y"])
    #logger.debug(merged_df.columns)

    # alternate sessions data
    alternate_sessions_data = fetch_sessions_with_dispositions(client_name, start_time, end_time)
    disposition_df, alt_session_df = build_data_frame(alternate_sessions_data) 
    dispositions_df = disposition_df.groupby("user_id").agg(list)
    dispositions_df = dispositions_df.fillna(value="")

    logger.debug("disposition df")
    logger.debug(disposition_df.head())

    agents_name_dict = get_agent_data(client_name)
    agents_df = pd.DataFrame(agents_name_dict)
    agents_df = agents_df[["agent_id","agent_name"]]

    merged_df = merged_df.merge(agents_df,how='left',left_on='manual_agent',right_on='agent_id')

    merged_df["agent_id"] = merged_df["agent_id_x"]
    merged_df = merged_df.drop(columns=["agent_id_x", "agent_id_y"])
    merged_df["rbl_session_status"] = merged_df["session_status"].apply(lambda x:session_status_mapper[x])

    merged_df = merged_df.merge(disposition_df, how="left", on="user_id")
    merged_df = merged_df.fillna(value="") 
    columns_to_typecast = merged_df.columns.tolist()
    logger.debug(columns_to_typecast)
    columns_to_typecast.remove("dispositions")
    logger.debug(columns_to_typecast)
    merged_df[columns_to_typecast] = merged_df[columns_to_typecast].astype(str)
    
    # then run the qureies I am assuming that the queries will be only the sql data
    for query_key, query_value in queries.items():
        # query operator fro now is always in should be customized later
        # to add on the new opreations
        query_values = [query_value] if isinstance(query_value, str) else query_value
        merged_df = merged_df[merged_df[query_key].isin(query_value)]
    
    # maipulating merged data patch
    merged_df["rbl_session_status"] = merged_df["session_status"].apply(lambda x:session_status_mapper[x])
    # run batch get item to get the session id data
            
    return merged_df
    
@bp.route("/sessions", methods=["POST"])
@login_required(permission="usr")
def get_session_list():
    # preparing argument
    # client_name = g.user_id
    client_name = g.user_id
    data = request.json

    # setdefault for start time end time here these values will 
    # be used in case the data is not porvided
    data.setdefault("start_time", int(time.time()))
    data.setdefault("end_time", int(time.time()) - 24*60*60)
    
    start_time = str(datetime.datetime.fromtimestamp(float(data["start_time"]) + 19800))
    end_time = str(datetime.datetime.fromtimestamp(float(data["end_time"]) + 19800))
    
    # query all the session that happenend in the give time frame 
    # group them by user_ids i.e all user that attempted any sessions
    # in the given time frame fetch this from postgres
    # based on the time type
    time_type = data.get("time_type", "attempt_time")
    if time_type == "attempt_time": 
        sessions_data = fetch_sessions_with_dispositions(client_name, start_time, end_time)
        processed_data =  process_attempt_time_data(
            sessions_data,
            data.get("session_type", "all"),
            required_keys=data.get("required_keys", []),
            queries=data.get("query", {})
        )
    elif time_type == "creation_time":
        users_data = fetch_user_and_sessions(client_name, start_time, end_time)
        # define for the creation time filter
        processed_data = process_creation_time_data(
            users_data,
            data.get("session_type", "all"),
            required_keys=data.get("required_keys", []),
            queries=data.get("query", {}),
            client_name=client_name,
            start_time=start_time,
            end_time=end_time
        )
    else:
        return jsonify({"msg": "Invalid time type shoould be attempt_time/creation_time"}), 400
    
    
    try:
        # based on response_type return response
        response_type = data.get("response_type", "json")
        if response_type == "json":
            return jsonify({"data": processed_data.to_dict(orient="records"), "count": len(processed_data)}), 200
        elif response_type == "csv":
            csv_data = processed_data.to_csv(index=False)

            # Create a response with the CSV data
            response = make_response(csv_data)
            response.headers["Content-Disposition"] = "attachment; filename=sessions.csv"
            response.headers["Content-Type"] = "text/csv"
            return response
        else:
            raise ValueError("The response type is invalid it should be json/csv")
    except ValueError as e:
        logger.debug(str(e))
        return jsonify({"msg": str(e)}), 400
    except Exception as e:
        logger.debug(str(e))
        return jsonify({"msg": "Internal Server Error"}), 500


