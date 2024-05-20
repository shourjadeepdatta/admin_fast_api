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


def globalSearch(client_name, current_id, id_type):
    payload = {
            "client_name_r": client_name,
            "current_id": current_id,
            "id_type": id_type 
            }
    res_items = postgres.call_function_dynamic(payload,"search_all_for_userid_or_sessionid")
    print("globalSearch = ", len(res_items))
    #try:
    if True:
        merged_df = abhishek.get_session_user_data_from_dynamo(res_items)
        #print("merged_df = ", merged_df)
        final_data = data_processor.process_session_data(client_name, merged_df)

        print("final_data = ", final_data)
        return jsonify({"session_list":final_data,"session_count":len(final_data),"server":"62"}),200
    #except Exception as e:
    #    return jsonify({"session_list":str(e), "server":"62"}), 500

