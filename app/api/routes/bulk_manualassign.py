from app.config import dynamodb, MANUAL_ALLOCATION_TABLE, CASE_PUSH_TABLE, agent_status_table, time_frame_expiry_period, S3_BUCKET
from app.helpers import get_first_session_detail, getPOCTableName, phog, upload_file_s3_local, update_table
from boto3.dynamodb.conditions import Key
from flask import jsonify  # type: ignore
from app.api.postgres import insert
import pandas as pd
import time
import json


def manual_assign(content, client_name):
    data_to_be_changed_in_pickle = {}
    # try:
    if True:
        user_id = content["user_id"]
        user_name = content.get("customer_name", content.get("customer_name", content.get("CUSTNAME")))
        phone_number = content.get("mobile_number", content.get("phone_number", content.get("phone_number_x")))
        try:
            manual_agent = content["assigned_agent"]
        except:
            manual_agent = content["manual_agent"]
        try:
            manual_agent_name = content["agent_name"]
        except:
            manual_agent_name = None
        # last_call_time = content["last_call_time"]
        # custom_msg = content["custom_msg"]
        admin_id = content["admin_id"]

        try:
            print("data oldest", get_first_session_detail(user_id))
            case_push_time = get_first_session_detail(user_id)["start_time"]
        except Exception as ecp:
            print("Exception cpt", str(ecp))
            case_push_time = ""

        # ekyc_req_time = content["ekyc_req_time"]
        # case_push_time = content.get("case_push_time", str(content.get("start_time_raw","")))
        # creation_time = content["creation_time"]
        try:
            latest_row_sess_id = content["session_id"]
        except:
            latest_row_sess_id = content["latest_row_sess_id"]
    # except Exception as e:
    #    print(e)

    latest_row_sess_id = content["session_id"]

    # return jsonify(success=False, msg=str(e), from_= "here"), 500
    manual_allocation_table = dynamodb.Table(MANUAL_ALLOCATION_TABLE)
    time_frame_expiry_detected = False
    case_push_table = dynamodb.Table(CASE_PUSH_TABLE)
    agent_table = dynamodb.Table(agent_status_table)
    session_table = dynamodb.Table(getPOCTableName("Session Status"))
    try:
        if case_push_time == "":
            case_push_time = case_push_table.query(KeyConditionExpression=Key("user_id").eq(user_id))["Items"][0]["case_push_time"]
        if divmod((time.time() - float(case_push_time)), 60)[0] > time_frame_expiry_period[client_name]:
            try:
                data = session_table.query(IndexName="user_id-index", KeyConditionExpression=Key("user_id").eq(user_id)).get("Items")
                for i in data:
                    update_table(session_table, 'session_id', i["session_id"], "set time_frame_expired = :t", {":t": "yes"})
                    
                    # session_table.update_item(Key={"session_id": i["session_id"]}, UpdateExpression="set time_frame_expired = :t", ExpressionAttributeValues={":t": "yes"}, ReturnValues="UPDATED_NEW")


                    data_to_be_changed_in_pickle[i["session_id"]].update({"time_frame_expired": "yes"})
            except Exception as e:
                print("time_frame_expired flag set error", str(e))
            # return(jsonify({'msg': 'time_frame_expired','status':500}), 500)
    except Exception as e:
        print("case_push_time check failed", str(e))

    PRODUCT = "CC"
    data = session_table.query(IndexName="user_id-index", KeyConditionExpression=Key("user_id").eq(user_id)).get("Items")
    for i in data:
        if i["session_id"] == latest_row_sess_id:
            PRODUCT = i.get("PRODUCT", "CC")
        if i["session_status"] == "kyc_result_approved":
            try:
                audit_result = int(i["audit_result"])
                if audit_result == 1:
                    print("checker approval case found", user_id, i["session_id"])
                    return (jsonify({"msg": "case has already been approved by agent/auditor please refresh the screen", "status": 500}), 500)
            except Exception as e:
                print("agent approval case found", user_id, i["session_id"])
                # return(jsonify({'msg': 'case has already been approved by agent/auditor please refresh the screen','status':500}), 500)
    data = manual_allocation_table.query(KeyConditionExpression=Key("user_id").eq(user_id)).get("Items")
    print("data from manual assign table", data)
    try:
        item = agent_table.query(KeyConditionExpression=Key("agent_id").eq(manual_agent) & Key("client_name").eq(client_name))["Items"][0]
        agent_name = item["agent_name"]
    except Exception as e:
        print("failed at getting agent name", str(e))
        agent_name = ""
        pass

    phog("manual_assign", client_name, user_id, latest_row_sess_id, title="", extras={"manual_agent": manual_agent, "case_push_time": case_push_time})
    try:
        session_data_for_table = {"client_name": client_name, "user_id": user_id, "session_id": latest_row_sess_id, "manual_agent": manual_agent}
        insert("kwikid_vkyc_session_status", session_data_for_table)
    except Exception as e:
        print("Exception while postgres insert", e)

    if len(data) != 0:
        try:
            res=update_table(session_table, 'session_id', latest_row_sess_id, "set manual_agent =:ma", {":ma": manual_agent})
            # res = session_table.update_item(
            #     Key={"session_id": latest_row_sess_id}, UpdateExpression="set manual_agent =:ma", ExpressionAttributeValues={":ma": manual_agent}, ReturnValues="UPDATED_NEW"
            # )
            
            print("setting manual_agent on session in case missed", res)
            update_table(manual_allocation_table, 'user_id', user_id, "set agent_id=:ma, start_time =:st,case_push_time =:case_push_time,sess_id=:sess_id,product_code=:pc", {":ma": manual_agent,":st": "yes", ":case_push_time": case_push_time, ":sess_id": latest_row_sess_id, ":pc": PRODUCT})
            # manual_allocation_table.update_item(
            #     Key={"user_id": user_id},
            #     UpdateExpression="set agent_id=:ma, start_time =:st,case_push_time =:case_push_time,sess_id=:sess_id,product_code=:pc",
            #     ExpressionAttributeValues={":st": "yes", ":case_push_time": case_push_time, ":ma": manual_agent, ":sess_id": latest_row_sess_id, ":pc": PRODUCT},
            #     ReturnValues="UPDATED_NEW",
            # )

            data_to_be_changed_in_pickle[latest_row_sess_id].update({"manual_agent": manual_agent})
            data_to_be_changed_in_pickle[latest_row_sess_id].update({"manual_agent_name": manual_agent_name})
            # pd.to_pickle(till_now_df,'./to_assign_{}.pkl'.format(client_name))
            # upload_file_s3_local(S3_BUCKET,"./to_assign_{}.pkl".format(client_name),"/videokyc/userassign/{}/to_assign.pkl".format(client_name))
            # pd.to_pickle(till_now_df,'./userslist_{}.pkl'.format(client_name))
            # upload_file_s3_local(S3_BUCKET,"./{}/userslist.pkl".format(client_name),"/videokyc/userassign/{}/userslist.pkl".format(client_name))
            print("manual agent overridden from {} to {}".format(data[0]["agent_id"], manual_agent))
        except Exception as e:
            print("error at setting manual_agent on session in case missed", str(e))
    else:
        new_item = {
            "user_id": user_id,
            "user_name": user_name,
            "phone_number": phone_number,
            "agent_id": manual_agent,
            "latest_row_sess_id": latest_row_sess_id,
            "start_time": "yes",
            "call_counter": "0",
            "product_code": PRODUCT,
            "case_push_time": case_push_time,
            "message_punched": json.dumps([]),
            "disposition_punched": json.dumps([]),
            "client_name": client_name,
        }
        res = update_table(session_table, 'session_id', latest_row_sess_id, "set manual_agent =:ma", {":ma": manual_agent})
        # res = session_table.update_item(Key={"session_id": latest_row_sess_id}, UpdateExpression="set manual_agent =:ma", ExpressionAttributeValues={":ma": manual_agent}, ReturnValues="UPDATED_NEW")
        manual_allocation_table.put_item(Item=new_item)
        print("manual allocation success")
    return data_to_be_changed_in_pickle


def api(client_name, content):

    try:
        admin_id = content["admin_id"]
    except BaseException:
        pass

    try:
        assigned_agent = content["assigned_agent"]
    except BaseException:
        return jsonify(message="Agent to be assigned missing"), 400

    try:
        assigned_users = content["assigned_users"]
    except BaseException:
        return jsonify(message="Users to be assigned not provided"), 400

    if type(assigned_users) is not list:
        return jsonify(message="Assigned users provided is not in the format of list"), 400

    """
    headers = {}
    headers["Content-Type"] = "application/json"
    headers["auth"] = request.headers['auth']

    url = "http://localhost:5568/custom/manualassign"

    failed_users = []
    failed_api_resp = []

    for user in assigned_users:
        user["admin_id"] = admin_id
        user["assigned_agent"] = assigned_agent
        user.pop("manual_agent",None)
        resp = requests.post(url=url,headers=headers,data=json.dumps(user))

        if resp.status_code != 200:
            failed_users.append(user)
            failed_api_resp.append(resp.text)

    return jsonify({"msg":"manual allocation success","server":"62","assigned_manual_agent":assigned_agent,
                        "success":True,"failed_users":failed_users,"failed_api_resp": failed_api_resp,"status_code":200}),200
    """
    till_now_df = pd.read_pickle("to_assign_{}.pkl".format(client_name))

    sessions = {}

    print("assigned users")
    print(assigned_users)

    for user in assigned_users:
        if not bool(user):
            continue

        user["admin_id"] = admin_id
        user["assigned_agent"] = assigned_agent
        user.pop("manual_agent", None)
        back_data = manual_assign(user, client_name)
        try:
            sessions.update(back_data)
        except:
            pass

    for session in sessions:
        for change in session.keys():
            till_now_df.loc[session, change] = session[change]

    pd.to_pickle(till_now_df, "./to_assign_{}.pkl".format(client_name))
    upload_file_s3_local(S3_BUCKET, "./to_assign_{}.pkl".format(client_name), "/videokyc/userassign/{}/to_assign.pkl".format(client_name))

    return jsonify({"msg": "manual allocation success", "status_code": 200}), 200
