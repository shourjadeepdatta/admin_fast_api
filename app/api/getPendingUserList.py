
import app.api.postgres as postgres
from flask import jsonify
import pandas as pd
import traceback
import datetime
import time
import json
import numpy as np
import app.api.abhishek as abhishek
import app.api.data_processor as data_processor


def pendingList(client_name, request_json, isRaw=False):

    content = request_json.get('filters')

    db_start_time = str(datetime.datetime.fromtimestamp(float(content["start_time"])+19800))
    db_end_time = str(datetime.datetime.fromtimestamp(float(content["end_time"])+19800))

    res_items = postgres.call_function(db_start_time,db_end_time,client_name,'retrieve_pending_userid')
    #print("globalSearch = ", res_items)
    merged_df = abhishek.get_session_user_data_from_dynamo(res_items)
    #print("merged_df = ", merged_df)
    final_data = data_processor.process_session_data(client_name, merged_df, False, False, False)


    if bool(request_json.get('required_keys')) and False:
        final_dataframe = pd.DataFrame(final_data)
        columns_to_keep = request_json.get('required_keys')
        columns_to_keep = [col for col in columns_to_keep if col in final_dataframe.columns]
        df_filtered = final_dataframe[columns_to_keep]
        final_data = df_filtered.to_dict(orient="records")

    if isRaw:
        return final_data

    #print("final_data = ", final_data)
    return jsonify({"session_data":final_data ,"count":len(final_data),"server":"4"}),200
