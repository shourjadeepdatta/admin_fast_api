import pandas as pd
import numpy as np
import boto3
import json
from multiprocessing.pool import ThreadPool

import multiprocessing

from itertools import chain
import traceback

import app.api.postgres as postgres

import time

#a = time.time()

#df = pd.read_csv("session_month.csv")

#df = postgres.call_function("2023-08-22 00:00:00","2023-10-22 23:59:59")


dynamodb_client = boto3.client("dynamodb",region_name="ap-south-1")

def do_dynamo_pull(session_list):
        # print("called")
    get_attr_list = ["vkyc_start_time","session_id","queue",'session_status',"user_id","start_time","end_time","agent_assignment_time","audit_result",'extras','audit_id','feedback',"manual_agent","expiry_reason","expiring_at_link_expiry","CUSTNAME","auditor_fdbk","ekyc_req_time","audit_end_time","PRODUCT","phone_number","agent_id","audit_lock", "auditor_name", "fraud_advisory_given","auditor_id"]
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


def chunks(lst, n):
      """Yield successive n-sized chunks from lst."""
      for i in range(0, len(lst), n):
          yield lst[i:i + n]

def get_session_data_from_dynamo(df):
    dynamodb_client = boto3.client("dynamodb",region_name="ap-south-1")
    session_to_fetch = df["session_id"].apply(lambda x: {"session_id": {"S": x}}).values.tolist()
    # running batch get for the session to be fetched
    
    # breaking into chunks
    chunked_session_ids = list(chunks(session_to_fetch, 100))
    
    # running btach get items on pool
    pool = multiprocessing.Pool(5)
    results = pool.map(do_dynamo_pull, chunked_session_ids)
    pool.close()
    pool.join()
    
    print("No of sessions", len(results))
    print("No of session data in one chunk", len(results[0]))
    
    # the result will be a 2d array flattening that array
    sessions = []
    for session_data in results:
        sessions += session_data
        
    df = pd.DataFrame(sessions)
    print(df.columns)
    print(len(df))
    print(df.head())
    
    return df


def get_manual_allocation_data_from_dynamo(df):
    dynamodb_client = boto3.client("dynamodb",region_name="ap-south-1")
    df["session_id"] = df["session_id"].apply(lambda x: {"session_id":{"S":x}})
    session_list_gen = df["session_id"].values.tolist()

    chunked_session_ids = list(chunks(session_list_gen, 100))

    pool = multiprocessing.Pool(4)
    results = pool.map(do_dynamo_pull_manualallocationtable, chunked_session_ids)
    pool.close()
    pool.join()

    user_data = []

    for result in results:
        user_data = user_data + result


    
def get_session_user_data_from_dynamo(df):
    dynamodb_client = boto3.client("dynamodb",region_name="ap-south-1")
    absogdf = df
    print(df.columns)
    df["session_id"] = df["session_id"].apply(lambda x: {"session_id":{"S":x}})
    print("received : {} for processing from postgres".format(len(df)))
    #df["session_id"] = df["session_id"].apply(eval).apply(lambda x: {"session_id":x})
    print(df.head())

    session_list_gen = df["session_id"].values.tolist()

    chunked_session_ids = list(chunks(session_list_gen, 100))


    pool = multiprocessing.Pool(4)
    results = pool.map(do_dynamo_pull, chunked_session_ids)
    pool.close()
    pool.join()

    print(len(results))
    print(len(results[0]))


    user_id_list = []

    for result in results:
        #print(type(result))
        #print(len(result))
        user_id_list = user_id_list + result

    print(len(user_id_list))
    session_df = pd.DataFrame(user_id_list)

    session_df_og = session_df

    session_df["user_id"] = session_df["user_id"].apply(lambda x: str(x))

    print("len session_df: {}".format(len(session_df)))
    #session_df = session_df.drop_duplicates("user_id")
    print("session df  --- > ", session_df["user_id"]) 


    #user_list_gen = session_df["user_id"].drop_duplicates(keep='first').apply(process_user_id)# Define a function to safely evaluate expressions


    #session_df = session_df[session_df.apply(lambda x: bool(x) and x["user_id"] is not None)]

    #session_df['user_id'] = session_df['user_id'].replace('nan', np.nan)

    # Filter out rows where 'user_id' is NaN or not a dictionary
    #session_df = session_df[session_df['user_id'].apply(lambda x: isinstance(x, dict) and pd.notna(x))]

    session_df['user_id'] = session_df['user_id'].replace('nan', np.nan)   
    session_df = session_df.dropna(subset=["user_id"])

    print("session df  --- > ", session_df["user_id"]) 
    user_list_gen = session_df["user_id"].drop_duplicates(keep='first').apply(lambda x:eval(x)).apply(lambda x: {"user_id":x})



    #user_list_gen = session_df["user_id"].apply(lambda x:eval(x)).apply(lambda x: {"user_id":x}) 

    print("len: {}".format(len(user_list_gen)))

    print(user_list_gen.head())

    user_list_gen = user_list_gen.values.tolist()

    print("unique_users: {}".format(len(user_list_gen)))

    chunked_session_ids = list(chunks(user_list_gen, 100))

    #a = time.time()

    pool = multiprocessing.Pool(4)
    results = pool.map(do_dynamo_pull_usertable, chunked_session_ids)
    pool.close()
    pool.join()

    user_data = []

    for result in results:
        user_data = user_data + result

    user_df = pd.DataFrame(user_data)
    print(user_df.columns)

    print('user_df', len(user_df))
    print(user_df.head())
    print(session_df_og.head())

    #pool = multiprocessing.Pool(4)
    #results = pool.map(do_dynamo_pull_manualallocationtable, chunked_session_ids)
    #pool.close()
    #pool.join()

    user_data = []

    #for result in results:
    #    user_data = user_data + result

    disposition_df = pd.DataFrame(user_data)
    print(disposition_df.columns)

    print(len(disposition_df))
    print('disposition_df', disposition_df.head())


    #print("done: {}".format(time.time() - a))

    session_df["user_id"] = session_df["user_id"].astype(str)
    user_df["user_id"] = user_df["user_id"].astype(str)
    if len(disposition_df) > 0:
        disposition_df["user_id"] = disposition_df["user_id"].astype(str)

    #session_df_og[session_df_og["CUSTNAME"].isna()].to_csv("missingdata")

    #empty_names = session_df_og[session_df_og["CUSTNAME"].isna()].to_dict()

    print("session_df len:{}".format(len(session_df)))


    merged_df = session_df.merge(user_df,how='left',left_on='user_id',right_on='user_id')
    
    if len(disposition_df) > 0:
        merged_df = merged_df.merge(disposition_df,how='left',left_on='user_id',right_on='user_id')

    #del absogdf["session_id"]
    print("absogdf columns: {}".format(absogdf.columns))
    print(absogdf.head())
    user_id_flag = 0
    session_id_x_flag = 0
    try:
        merged_df["user_id"] = merged_df["user_id"].apply(lambda x:eval(x)["S"])
        user_id_flag = 1
        merged_df["session_id_x"] = merged_df["session_id_x"].apply(lambda x:x["S"])
        session_id_x_flag = 1
    except Exception as e:
        if session_id_x_flag == 0:
            merged_df["session_id_x"] = ""
        else:
            merged_df["user_id"] = ""

    # Columns to potentially select
    columns_to_select = ["session_id", "user_id", "created_at", "dispositions"]
    existing_columns = [col for col in columns_to_select if col in absogdf.columns]
    non_null_columns = [col for col in existing_columns if not absogdf[col].isnull().all()]
    absogdf_selected = absogdf[non_null_columns]

    absogdf["session_id"] = absogdf["session_id"].apply(lambda x:x["session_id"]["S"])
    print(merged_df.columns)
    #print(merged_df.head()[["session_id_x","session_id_y","user_id"]])
    merged_df = merged_df.merge(absogdf,how='left',left_on='session_id_x',right_on='session_id',suffixes=("","r"))
    #print(merged_df.head()[["user_id","created_at"]])
    #del absogdf["session_id"]
    #merged_df = merged_df.merge(absogdf,how='left',left_on='user_id',right_on='user_id')
    #merged_df[merged_df["CUSTNAME"].isna()].to_csv("missing.csv")
    #print(merged_df[merged_df["CUSTNAME"].isna()].to_dict(orient="records")[0]["form_data"]["S"])
    #merged_df.loc[merged_df["CUSTNAME"].isna(),"CUSTNAME_N"] = merged_df[merged_df["CUSTNAME"].isna()].apply(lambda x: eval(x["form_data"]["S"])["user_info"]["Appname"])
    #merged_df["CUSTNAMES"] = merged_df.apply(lambda x: str(json.loads(x["form_data"]["S"]).get("user_info",{})),axis=1)

    print("final length before return")
    print(len(merged_df))
    print(merged_df.head())
    print("final columns::::::::::")
    print(merged_df.columns)
    print("final merged df count: {}".format(len(merged_df)))


    #import sys
    #sys.exit(0)

    return merged_df
