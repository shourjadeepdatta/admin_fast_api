import requests
import json
import datetime

import pandas as pd

headers = {
            "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoicmJscHJvZCJ9.iPQPLTcg0qU-pFcmTdI_rVEhHA_MX6WDY-3ep6tXFCo",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }

payload = {
            "user_id":"wow",
            "agent_id":"who",
            "session_id":"moo",
            "session_status":"what",
            "client_name":"foo"
        }

#payload = json.dumps(payload)

#res = requests.request("POST","https://kbipvd.thinkanalytics.in:6356/postgres/kwikid_vkyc_session_status",data=payload,headers=headers)

BASE_URL = "https://kbipvd.thinkanalytics.in:6356/postgres/"

#print(res.text)

def call_function(start_time,end_time,client_name,function_name="retrieve_userid_and_sessionid_for_daterange"):

    payload = {
            "start_date":start_time,
                "end_date":end_time,
                "client_name_r":client_name
            }
    payload = json.dumps(payload)

    with open("fncall","w") as fncall:
        fncall.write(payload)

    res = requests.request("POST",BASE_URL+"/rpc/{}".format(function_name),headers=headers,verify=False,data=payload)
    #print(res.text)
    print(res.headers)
    d = res.json()
    print(len(d))
    df = pd.DataFrame(d)
    print(df.head())
    print(len(df))
    #with open("fnres","w") as fnres:
    #    fnres.write(res.text)
    #df.to_csv("wow.csv",index=False)
    return df

def call_function_raw(start_time,end_time,client_name,function_name="retrieve_userid_and_sessionid_for_daterange"):
    payload = {
            "start_date":start_time,
                "end_date":end_time,
                "client_name_r":client_name
            }
    payload = json.dumps(payload)

    with open("fncall","w") as fncall:
        fncall.write(payload)

    res = requests.request("POST",BASE_URL+"/rpc/{}".format(function_name),headers=headers,verify=False,data=payload)
    print(res.headers)
    d = res.json()
    return d


def call_function_dynamic(payload={},function_name="retrieve_userid_and_sessionid_for_daterange"):
    payload = json.dumps(payload)

    with open("fncall","w") as fncall:
        fncall.write(payload)

    res = requests.request("POST",BASE_URL+"/rpc/{}".format(function_name),headers=headers,verify=False,data=payload)
    d = res.json()
    print('call_function_dynamic = ', d)
    df = pd.DataFrame(d)
    return df

#call_function("2023-09-22 00:00:00","2023-10-22 23:59:59")

def insert(table_name,row):
    try:
        try:
            if row.has_key("start_time"):
                row["start_time"] = str(datetime.datetime.fromtimestamp(float(row["start_time"])))
        except Exception as e:
            print("error while converting start_time: {}".format(e))
            pass

        try:
            if row.has_key("created_at"):
                row["created_at"] = str(datetime.datetime.fromtimestamp(float(row["created_at"])))
        except Exception as e:
            print("error while converting start_time: {}".format(e))
            pass

        with open("pgreq","w") as pgreq:
            pgreq.write(str(row))
        row = json.dumps(row)
        res = requests.request("POST",BASE_URL + table_name,data=row,headers=headers,verify=False)
        print(res.text)
        with open("pgres","w") as pgres:
            pgres.write(str(res.text))
    except Exception as e:
        with open("pgerr","w") as pgerr:
            pgerr.write(str(e))
        print("error while writing to postgres:{}".format(e))


def add_item(table_name, row_data):
    try:
        row_data = json.dumps(row_data)
        res = requests.request("POST", BASE_URL + table_name, data=row_data, headers=headers, verify=False)
        print(res.content)
    except Exception as e:
        print("Exception while add_item = ", e)


def query_table(table_name, params):
    try:
        res = requests.get(BASE_URL + table_name, headers=headers, params=params, verify=False)
        return res.content
    except:
        return False
