from app.config import dynamodb, MANUAL_ALLOCATION_TABLE, S3_BUCKET
from app.helpers import download_file_s3,update_table
from boto3.dynamodb.conditions import Key
from flask import jsonify  # type: ignore
from app.api.postgres import insert
from datetime import datetime
import json


def api(content, client_name=None):
    agent_id = content["agent_id"]
    user_id = content["user_id"]
    disposition = content["disposition"]
    agnts_cron_data = "dummy"  # json.loads(download_file_s3(S3_BUCKET,"agntsCrnDmp/{}.json".format(client_name)))
    scheduled_time = content.get("scheduled_time", "")
    try:
        if "Call back" in disposition:
            disposition = disposition + "-" + str(datetime.fromtimestamp(float(scheduled_time)))
    except Exception as e:
        disposition = disposition + "-" + str(e)
        pass
    try:
        insert("kwikid_vkyc_dialer_dispositions", {"agent_id": agent_id, "user_id": str(user_id), "disposition": str(disposition), "client_name": "RBL"})
    except Exception as e:
        print(e)

    try:
        # agent_name = agnts_cron_data[agent_id]["agent_name"]
        agent_name = "dummy"
    except Exception as e:
        print("punch in message failed at getting agent name", str(e))
        try:
            # agents_dump_cron()
            agnts_cron_data = json.loads(download_file_s3(S3_BUCKET, "agntsCrnDmp/{}.json".format(client_name)))
            agent_name = agnts_cron_data[agent_id]["agent_name"]
        except Exception as e:
            agent_name = ""
            print("punch in message failed at re getting agent name", str(e))
    manual_allocation_table = dynamodb.Table(MANUAL_ALLOCATION_TABLE)
    data = manual_allocation_table.query(KeyConditionExpression=Key("user_id").eq(user_id)).get("Items")
    if len(data) == 0:
        return jsonify({"msg": "not manually assigned yet", "success": False, "status_code": 500}), 500
    else:
        try:
            prev_disposition = json.loads(data[0]["disposition_punched"])
        except Exception as e:
            prev_disposition = []
        item = {"Time": str(datetime.now()), "id": agent_id, "name": agent_name, "disposition": disposition, "scheduled_time": scheduled_time}

        prev_disposition.append(item)
        update_table(manual_allocation_table, 'user_id', user_id, "set disposition_punched =:dp", {":dp": json.dumps(prev_disposition)})

        # manual_allocation_table.update_item(
        #     Key={"user_id": user_id}, UpdateExpression="set disposition_punched =:dp", ExpressionAttributeValues={":dp": json.dumps(prev_disposition)}, ReturnValues="UPDATED_NEW"
        # )
        return jsonify({"msg": "disposition punched in successfully", "success": True, "status_code": 200}), 200
