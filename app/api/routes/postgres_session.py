from flask import jsonify
import requests
from timeit import default_timer as timer
import traceback
import json
import datetime

def epoch_to_datetime(epoch_str):
    # Convert the epoch string to an integer
    epoch_int = int(epoch_str)
    # Convert the epoch time to a datetime object
    return datetime.datetime.fromtimestamp(epoch_int).strftime('%Y-%m-%d %H:%M:%S')

# # Example usage
# epoch_time = "1633036800"
# print(epoch_to_datetime(epoch_time))


BASE_URL = "https://kbipvd.thinkanalytics.in:6356/postgres/"

headers = {
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoicmJscHJvZCJ9.iPQPLTcg0qU-pFcmTdI_rVEhHA_MX6WDY-3ep6tXFCo",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}

# 1714501800 1may

# 1715279400 10may

# 1715970540 17 may

request={
        "filters": {
        "start_time": "1714501800",
        "end_time": "1715279400"
    },
    "session_type": "all"
}


def get_session_list_pg(client_name, request_json):
    get_attr_list=["vkyc_start_time",
               "session_id",    
               "queue",
               'session_status',
               "user_id",
               "start_time",
               "end_time",
               "agent_assignment_time",
               "audit_result",
               'extras',
               'audit_id',
               'feedback',
               "manual_agent",
               "expiry_reason",
               "expiring_at_link_expiry",
               "CUSTNAME",
               "auditor_fdbk",
               "ekyc_req_time",
               "audit_end_time",
               "PRODUCT",
               "phone_number",
               "agent_id",
               "audit_lock", 
               "auditor_name", 
               "fraud_advisory_given"]
    try:
        print("request_json = ", request_json)
        return get_data_from_postgres("kwikid_vkyc_session_status_uat", client_name,get_attr_list, request_json['filters'],request_json)
    except Exception as e:
        return jsonify({'exception': str(e)}), 500

def get_data_from_postgres(table_name, client_name,attr_list, payload,request_json,function_name="get_session_list_by_time_range"):
    try: 
        start=timer()
        print("payload = ", payload)
        payload_json = {
                'start_time_arg':  payload.get("start_time"),
                'end_time_arg':  payload.get("end_time"),
                'client_name_arg': client_name,
                # "attr_list": attr_list,
                # "table_name": table_name 
            }
        payload_json = json.dumps(payload_json)
        # raise Exception("payload_json")
        print("payload2 = ", payload_json)
        response = requests.request("POST",
                BASE_URL+"/rpc/{}".format(function_name),
                headers=headers,
                verify=False,
                data=payload_json,
            )
        end = timer()
        print("Response from Postgres:", response.status_code, len(response.json()))
        print("@@@@@@@@@@ TIME-TAKEN = ", end - start)

        # with open('response.txt', 'w') as file:
        #     file.write(response)

        # with open('response_text.txt', 'w') as file:
        #     file.write(response.text)

        # with open('response_json.txt', 'w') as file:
        #     file.write(response.json())

        # "start_time": "1715653800",
        # "end_time": "1715661000"

        # return response.json()
        # return jsonify({"server": 4, "session_count": len(response.json()), "session_data": response.json()}), response.status_code
        return {'server': 4,'session_count': len(response.json()), 'session_data': response.text}, response.status_code

    except Exception as e:
        traceback.print_exc()
        print("Error while fetching data from Postgres:", e)
        return (
            jsonify(
                {"msg": "Error while fetching data from Postgres", "success": False, "status_code": 500, "request_json": request_json, "payload": payload, "payload_json": payload_json, "e": str(e)}
            ),
            500,
        )
        # print({"msg": "Error while fetching data from Postgres", "success": False, "status_code": 500,'request_json': request_json, 'payload': payload, 'payload_json': payload_json})


get_session_list_pg("RBL", request)
