
import datetime
import app.api.data_processor as data_processor
import app.api.postgres as postgres
import app.api.abhishek as abhishek



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
    print("")



client_name  = "RBL"
request_json = {
    "filters": {
        "start_time": 1701023400,
        "end_time": 1701109799
    }
}
skip_session_status_filter = True
kind = "by_creation_date"

get_session_list_new(client_name, request_json, skip_session_status_filter, kind)
