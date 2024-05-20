import json
import time
from flask import jsonify
import app.api.postgres as postgres
import datetime

import pandas as pd
import app.api.abhishek as abhishek
import app.api.data_processor as data_processor

def getSessionData(client_name, request_json, direct=False):
    start_time = time.time()
    request_json = {
                "filters":request_json,
                "start_time":request_json["start_time"],
                "end_time":request_json["end_time"]
            }
    content = request_json.get('filters')
    print("content = ", content)

    if direct:
        db_start_time = request_json.get('start_time') 
        db_end_time = request_json.get('end_time') 
    else:
        db_start_time = str(datetime.datetime.fromtimestamp(float(content["start_time"])+19800))
        db_end_time = str(datetime.datetime.fromtimestamp(float(content["end_time"])+19800))
    print("db_start_time = ", db_start_time)
    print("db_end_time = ", db_end_time)

    res_items = postgres.call_function_raw(db_start_time,db_end_time,client_name,'get_cached_session_data')
    print("pendingList = ", len(res_items))
    out = []
    if len(res_items) > 0:
        for item in res_items:
            out += json.loads(item.get('session_data'))
        
        out = pd.DataFrame(out)
        out = out.drop_duplicates(subset=["session_id"])
        out = out.to_json(orient="records")
        out = json.loads(out)


    return jsonify({"db_start_time": db_start_time, "db_end_time": db_end_time,"session_data":out ,"count":len(out),"server":"4"}),200
