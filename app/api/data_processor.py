import datetime
import pytz
import time
import json
import pandas as pd
from app.config import dynamodb, dynamodb_client, MANUAL_ALLOCATION_TABLE, user_status_table, EKYC_STATUS_TABLE, session_status_mapper, ekyc_expiry_time_period
from app.helpers import getPOCTableName, convert_dynamodb_item_to_json, get_latest_session_detail, get_first_session_detail, get_ekyc_detail
from boto3.dynamodb.conditions import Key, Attr
from multiprocessing.pool import ThreadPool


def get_key(o, k):
    try:
        return o[k]
    except Exception as e:
        return ""


def get_agent_data(client_name):
    agent_table = dynamodb.Table(getPOCTableName("Agent"))

    agents_data = agent_table.query(
        IndexName="client_name-index",
        KeyConditionExpression=Key("client_name").eq(client_name),
    )
    agents_data_items = agents_data.get("Items")
    agents_name_dict = {}
    for index, item in enumerate(agents_data_items):
        try:
            agent_id = item["agent_id"]
            agent_name = item["agent_name"]
            agents_name_dict[agent_id] = {
                "agent_name": agent_name,
                "call_center": item.get("call_center"),
            }
        except Exception as e:
            agents_name_dict[agent_id] = {"e": str(e)}

    return agents_data_items


def process_session_data(
    client_name,
    merged_df,
    skip_session_status_filter=True,
    get_latest_status_for_userid_only=False,
    deep_search=True,
):
    tracker_t = time.time()
    tracker_t_store = {}
    columns = [
        "phone_number_x",
        "phone_number_y",
        "feedback",
        "audit_end_time",
        "created_at",
        "agent_id",
        "agent_id_x",
        "end_time",
        "end_time_x",
        "start_time",
        "session_id_x",
        "vkyc_start_time",
        "user_id",
        "session_status",
        "auditor_name",
        "disposition_punched",
        "CUSTNAME",
        "CUSTNAME_x",
        "CUSTNAME_y",
        "ekyc_req_time",
        "ekyc_req_time_x",
        "ekyc_req_time_y",
        "manual_agent",
        "queue",
        "audit_result",
        "audit_lock",
        "auditor_fdbk",
        "PRODUCT",
        "dispositions",
        "extras",
        "form_data",
    ]

    default_values = {
        "audit_end_time": {"N": ""},
        "ekyc_req_time_x": {"S": ""},
        "feedback": {"S": ""},
        "audit_result": {"N": ""},
        "auditor_fdbk": {"S": ""},
        "auditor_name": {"S": ""},
        "end_time_x": {"S": ""},
        "audit_lock": {"BOOL": False},
        "vkyc_start_time": {"S": ""},
        "phone_number_x": {"S": ""},
        "agent_id_x": {"S": ""},
        "manual_agent": {"S": ""},
        "agent_id": {"S": ""},
        "phone_number_y": {"S": ""},
    }


    res_items = merged_df.copy()
    for col in columns:
        if col not in res_items.columns:
            res_items[col] = None

    for col, default_val in default_values.items():
        res_items[col] = res_items[col].apply(lambda x: default_val if pd.isna(x) else x)

    res_items.rename(columns={"agent_id_x": "vcip_agent_id", "manual_agent": "manual_agent_id"}, inplace=True)
    columns_to_delete = ["agent_id_x", "manual_agent"]
    for col in columns_to_delete:
        if col in res_items.columns:
            del res_items[col]

    # Initialize additional columns with empty string
    res_items["first_session_detail"] = ""
    res_items["ekyc_detail"] = ""

    # res_items.to_csv("output_check.csv", encoding="utf-8", index=False)

    print("columnssssss: {}".format(res_items.columns))
    print("res-items len: ini{}".format(len(res_items)))

    tracker_t_store["first"] = time.time() - tracker_t

    def extract_value(d, key, default=""):
        if pd.isna(d):
            return default
        return d.get(key, default)
    

    mask = res_items['end_time'].isna()
    # Splitting the DataFrame for end_time
    end_res_items_to_process = res_items[mask]
    end_res_items_not_to_process = res_items[~mask]

    if len(end_res_items_to_process) > 0:    
        # Apply logic only to rows where end_time is NaN
        end_res_items_to_process['end_time'] = end_res_items_to_process.apply(lambda row: extract_value(row["end_time_x"], "S"), axis=1)
    end_res_items_not_to_process["end_time"] =  end_res_items_not_to_process["end_time"].apply(lambda x: get_key(x, "S"))
    # Concatenate the results
    res_items = pd.concat([end_res_items_to_process, end_res_items_not_to_process])




    mask = res_items['phone_number_x'].isna()
    # Splitting the DataFrame for end_time
    phone_res_items_to_process = res_items[mask]
    phone_res_items_not_to_process = res_items[~mask]
    # Apply logic only to rows where end_time is NaN
    print('phone_res_items_to_process = ', phone_res_items_to_process)
    print('row["phone_number_y"] = ',  res_items["phone_number_y"])
    if len(phone_res_items_to_process) > 0:
        phone_res_items_to_process['phone_number_x'] = phone_res_items_to_process.apply(lambda row: extract_value(row["phone_number_y"], "S"), axis=1)
    phone_res_items_not_to_process["phone_number_x"] =  phone_res_items_not_to_process["phone_number_x"].apply(lambda x: get_key(x, "S"))
    # Concatenate the results
    res_items = pd.concat([phone_res_items_to_process, phone_res_items_not_to_process])

    # Optimizing the extraction of values
    #res_items["end_time"] = res_items.apply(lambda row: extract_value(row["end_time_x"], "S") if pd.isna(row["end_time"]) else extract_value(row["end_time"], "S"), axis=1)
    #res_items["phone_number_x"] = res_items.apply(
    #    lambda row: extract_value(row["phone_number_y"], "S") if pd.isna(row["phone_number_x"]) or not extract_value(row["phone_number_x"], "S") else extract_value(row["phone_number_x"], "S"), axis=1
    #)

    # Simplify the extraction for other columns
    for col, key in [
        ("audit_result", "N"),
        ("audit_end_time", "N"),
        ("audit_lock", "BOOL"),
        ("auditor_fdbk", "S"),
        ("auditor_name", "S"),
        ("agent_id", "S"),
        ("manual_agent_id", "S"),
        ("vcip_agent_id", "S"),
        ("session_status", "S"),
        ("disposition_punched", "S"),
    ]:
        res_items[col] = res_items[col].apply(lambda x: extract_value(x, key))

    tracker_t_store["second"] = time.time() - tracker_t


    #res_items.to_csv("ekyc_text.csv", encoding="utf-8")

    def get_ekyc_time(row):
        ekyc_time = ""
        ekyc_detail = row["ekyc_detail"]

        if not pd.isna(row["ekyc_req_time_x"]) and (bool(row["ekyc_req_time_x"].get("S", "")) or bool(row["ekyc_req_time_x"].get("N", ""))):
            ekyc_time = row["ekyc_req_time_x"].get("S", row["ekyc_req_time_x"].get("N", ""))

        if not ekyc_time:
            ekyc_detail = get_ekyc_detail(row["user_id"])
            if bool(ekyc_detail):
                ekyc_time = ekyc_detail.get("ekyc_req_time")
            else:
                ekyc_time = ""

        return pd.Series([ekyc_time, ekyc_detail], index=["ekyc_req_time", "ekyc_detail"])

    
    print("starting for ekyc")



    # Create a mask for your condition
    mask = res_items['ekyc_req_time_y'].isna()
    # Splitting the DataFrame
    ekyc_res_items_to_process = res_items[mask]
    ekyc_res_items_not_to_process = res_items[~mask]
    print("ekyc_res_items_to_proces =  ", len(ekyc_res_items_to_process))
    print("ekyc_res_items_not_to_process   =  ", len(ekyc_res_items_not_to_process))


    if len(ekyc_res_items_to_process) > 0:
        # Apply function only to rows that need processing
        processed = ekyc_res_items_to_process.apply(get_ekyc_time, axis=1)
        # Assigning the results back
        ekyc_res_items_to_process[["ekyc_req_time", "ekyc_detail"]] = processed

    ekyc_res_items_not_to_process["ekyc_req_time"] =  ekyc_res_items_not_to_process["ekyc_req_time_y"].apply(lambda x: get_key(x, "S"))
    # Concatenate the results
    res_items = pd.concat([ekyc_res_items_to_process, ekyc_res_items_not_to_process])


    #res_items[["ekyc_req_time", "ekyc_detail"]] = res_items.apply(get_ekyc_time, axis=1)

    tracker_t_store["third"] = time.time() - tracker_t
    #tracker_t_store["ekyc_5"] = len(ekyc_res_items_to_process)
    #tracker_t_store["ekyc_6"] = len(ekyc_res_items_not_to_process)

    def get_custname(row):
        custname = ""
        first_session_detail = row["first_session_detail"]

        if not pd.isna(row["CUSTNAME"]) and bool(row["CUSTNAME"].get("S", "")):
            custname = get_key(row["CUSTNAME"], "S")

        if not bool(custname) and not pd.isna(row["CUSTNAME_x"]) and bool(row["CUSTNAME_x"].get("S", "")):
            custname = get_key(row["CUSTNAME_x"], "S")


        if not bool(custname) and not pd.isna(row["CUSTNAME_y"]) and bool(row["CUSTNAME_y"].get("S", "")):
            custname = get_key(row["CUSTNAME_y"], "S")


        if not bool(custname) and not pd.isna(row["extras"]):
            if not pd.isna(row["extras"]) and bool(row["extras"].get("S", row["extras"].get("M", ""))):
                custname = json.loads(row["extras"].get("S", row["extras"].get("M", "")))
                if bool(custname):
                    custname = custname.get("CUSTNAME", custname.get("user_info", {}).get("name"))


        if not bool(custname) and deep_search:
            first_session_detail = get_first_session_detail(row["user_id"])
            custname = first_session_detail.get("CUSTNAME")


        return pd.Series([custname, first_session_detail], index=["CUSTNAME", "first_session_detail"])

    print("starting cust")

    # Create a mask for your condition
    mask = res_items['CUSTNAME'].isna()
    # Splitting the DataFrame
    custname_res_items_to_process = res_items[mask]
    custname_res_items_not_to_process = res_items[~mask]
    print("custname_res_items_to_proces =  ", len(custname_res_items_to_process))
    print("custname_res_items_not_to_process   =  ", len(custname_res_items_not_to_process))
    # Apply function only to rows that need processing
    if len(custname_res_items_to_process) > 0:
        processed = custname_res_items_to_process.apply(get_custname, axis=1)
        # Assigning the results back
        custname_res_items_to_process[["CUSTNAME", "first_session_detail"]] = processed
    custname_res_items_not_to_process["CUSTNAME"] = custname_res_items_not_to_process["CUSTNAME"].apply(lambda x: get_key(x, "S"))
    # Concatenate the results
    res_items = pd.concat([custname_res_items_to_process, custname_res_items_not_to_process])

    #res_items[["CUSTNAME", "first_session_detail"]] = res_items.apply(get_custname, axis=1)

    tracker_t_store["fourth"] = time.time() - tracker_t

    print("start product")

    def get_product(row):
        product = ""
        first_session_detail = row["first_session_detail"] 

        if row.get("first_session_detail"):
            product = row["first_session_detail"].get("PRODUCT")

        if not product and not pd.isna(row["extras"]):
            if not pd.isna(row["extras"]) and bool(row["extras"].get("S", row["extras"].get("M", ""))):
                try:
                    product = json.loads(row["extras"].get("S", row["extras"].get("M", "")))
                    product = product.get("PRODUCT", "")
                except:
                    product = ""
        #if not product and deep_search:
        #    first_session_detail = get_first_session_detail(row["user_id"])
        #    product = first_session_detail.get("PRODUCT")
        #else:
        #    product = ""

        return pd.Series([product, first_session_detail], index=["PRODUCT", "first_session_detail"])


    
    # Create a mask for your condition
    mask = res_items['PRODUCT'].isna()
    # Splitting the DataFrame
    product_res_items_to_process = res_items[mask]
    product_res_items_not_to_process = res_items[~mask]
    print("product_res_items_to_proces =  ", len(product_res_items_to_process))
    print("prouct_res_items_not_to_process   =  ", len(product_res_items_not_to_process))

    if len(product_res_items_to_process) > 0:
        # Apply function only to rows that need processing
        processed = product_res_items_to_process.apply(get_product, axis=1)
        # Assigning the results back
        product_res_items_to_process[["PRODUCT", "first_session_detail"]] = processed
    product_res_items_not_to_process["PRODUCT"] = product_res_items_not_to_process["PRODUCT"].apply(lambda x: get_key(x, "S"))
    # Concatenate the results
    res_items = pd.concat([product_res_items_to_process, product_res_items_not_to_process])



    #res_items[["PRODUCT", "first_session_detail"]] = res_items.apply(get_product, axis=1)

    tracker_t_store["fifth"] = time.time() - tracker_t

    res_items["queue"] = res_items["queue"].apply(lambda x: get_key(x, "S"))
    res_items["start_time"] = res_items["start_time"].apply(lambda x: x.get("S", ""))
    res_items["vkyc_start_time"] = res_items["vkyc_start_time"].apply(lambda x: x.get("S", ""))
    res_items["dispositions_punched"] = res_items["disposition_punched"].apply(lambda x: [] if x == "" else json.loads(x))
    res_items["feedback"] = res_items["feedback"].apply(lambda x: x.get("S", ""))
    res_items["session_id"] = res_items["session_id_x"]

    """
    res_items["agent_id"] = res_items["agent_id"].apply(lambda x:x.get("S",""))
    res_items["agent_name"] = res_items["agent_name"].apply(lambda x:x.get("S",""))
    """

    print("res-items len after pdna: {}".format(len(res_items)))
    # return jsonify(res_items),200

    res_items["rbl_session_status"] = res_items["session_status"].apply(lambda x: session_status_mapper[x])
    del res_items["end_time_x"]
    del res_items["ekyc_req_time_x"]
    del res_items["session_id_x"]

    agents_name_dict = get_agent_data(client_name)

    agents_df = pd.DataFrame(agents_name_dict)
    agents_df = agents_df[["agent_id", "agent_name"]]

    # agents_df = agents_df.rename(columns={"agent_id":"db_agent_id","agent_name":"db_agent_name"})
    # print(agents_df.columns)

    tracker_t_store["sixth"] = time.time() - tracker_t

    res_items = res_items.merge(agents_df, how="left", left_on="vcip_agent_id", right_on="agent_id", suffixes=("", "_vcip"))
    res_items = res_items.merge(agents_df, how="left", left_on="manual_agent_id", right_on="agent_id", suffixes=("", "_manual"))
    res_items = res_items.merge(agents_df, how="left", left_on="auditor_name", right_on="agent_id", suffixes=("", "_auditor"))

    print("res-items len afteragentdfmerge: {}".format(len(res_items)))
    print("columns after agentdf merge: {}".format(res_items.columns))

    ekyc_expiry_timer = ekyc_expiry_time_period[client_name]

    res_items["ekyc_status_calc"] = res_items["ekyc_req_time"].apply(lambda x: "KYC_PENDING" if x == "" else "VCIP_PENDING")
    res_items["ekyc_status_calc"] = res_items["ekyc_req_time"].apply(lambda x: "KYC_PENDING" if (x == "" or divmod(time.time() - float(x), 60)[0] > ekyc_expiry_timer) else "VCIP_PENDING")

    res_items["rbl_session_status_new"] = res_items["rbl_session_status"]
    res_items.loc[(res_items["rbl_session_status"] == "NOT_ASSIGNED_TO_AGENT") & (res_items["queue"] != "free"), "rbl_session_status_new"] = res_items["ekyc_status_calc"]
    res_items.loc[res_items["audit_result"] == "1", "rbl_session_status_new"] = "CHECKER_APPROVED"
    res_items.loc[res_items["audit_result"] == "0", "rbl_session_status_new"] = "CHECKER_REJECTED"
    res_items.loc[(res_items["audit_lock"] == True) & (res_items["audit_result"] == "") & (res_items["rbl_session_status_new"] == "VCIP_APPROVED"), "rbl_session_status_new"] = "CHECKER_PENDING"

    # res_items['rbl_session_status'] = res_items.apply(lambda row: row['ekyc_status_calc'] if row['rbl_session_status'] in ['VCIP_SESSION_INVALID', 'user_abandoned'] else row['rbl_session_status'],axis=1)
    print("res-items len after bs: {}".format(len(res_items)))

    res_items["auditor_fdbk"] = res_items["auditor_fdbk"].apply(lambda x: x.upper())
    # only_auditor_rejects = res_items[res_items["audit_result"] == "0"]
    res_items.loc[(res_items["audit_result"] == "0") & (res_items["auditor_fdbk"].str.contains("REVISIT")), "rbl_session_status_new"] = res_items["ekyc_status_calc"]

    res_items["rbl_session_status"] = res_items["rbl_session_status_new"]

    res_items["ekyc_time"] = res_items["ekyc_req_time"]

    del res_items["rbl_session_status_new"]

    print("res-items after del status new: {}".format(len(res_items)))

    res_items["rbl_session_status"] = res_items.apply(
        lambda row: row["ekyc_status_calc"] if row["rbl_session_status"] in ["VCIP_SESSION_INVALID", "user_abandoned", "NOT_ASSIGNED_TO_AGENT", "KYC_PENDING", "VCIP_PENDING"] else row["rbl_session_status"], axis=1
    )

    print("columnsssssssssssssssssss", res_items.columns)

    res_items["s_start_time"] = pd.to_datetime(res_items["start_time"], unit="s")

    res_items.sort_values(by="s_start_time", ascending=False, inplace=True)

    tracker_t_store["seventh"] = time.time() - tracker_t

    print("res-items before status skip: {}".format(len(res_items)))

    # res_items = res_items.duplicated(subset='user_id', keep='first')

    if skip_session_status_filter:
        print("skipping session status filter!!!!")
        pass
    else:
        users_to_drop = res_items[(res_items["rbl_session_status"] == "VCIP_APPROVED") & (res_items["audit_result"] == "")]["user_id"]
        res_items = res_items[~res_items["user_id"].isin(users_to_drop)]
        users_to_drop = res_items[res_items["rbl_session_status"] == "CHECKER_APPROVED"]["user_id"]
        res_items = res_items[~res_items["user_id"].isin(users_to_drop)]

    print("res-items before after skip: {}".format(len(res_items)))

    """
    duplicate_mask = res_items.duplicated(subset='user_id', keep='first')

    res_items.loc[duplicate_mask, 'session_status'] = "VCIP_SESSION_INVALID"

    res_items.reset_index(inplace=True, drop=True)
    """
    # res_items.to_csv("userstatus.csv",index=False)
    if get_latest_status_for_userid_only:
        res_items.drop_duplicates(subset=["user_id"], keep="first", inplace=True)
    else:
        print("skipping drop duplicates of userid!!!")
        pass
    # status_to_skip = ['VCIP_APPROVED','CHECKER_APPROVED','CHECKER_PENDING','CHECKER_REJECTED','VCIP_REJECTED','CHECKER_PENDING','VCIP_EXPIRED']
    # res_items = res_items[~res_items['rbl_session_status'].isin(status_to_skip)]

    nnow = datetime.datetime.now(pytz.FixedOffset(330)) - datetime.timedelta(seconds=3600 * 24 * 30)
    print(res_items["created_at"].head())
    print(pd.to_datetime(res_items["created_at"].head()))
    print(nnow)
    # nnow = nnow.replace(tzinfo=None)
    res_items["created_at"] = pd.to_datetime(res_items["created_at"], utc=True).dt.tz_convert(nnow.tzinfo)

    res_items["created_at"] = pd.to_datetime(res_items["created_at"], utc=True).dt.tz_convert(nnow.tzinfo)

    res_items.loc[(res_items["rbl_session_status"].isin(["KYC_PENDING", "VCIP_PENDING"])) & (pd.to_datetime(res_items["created_at"]) < nnow), "rbl_session_status"] = "VCIP_EXPIRED"

    status_to_keep = ["KYC_PENDING", "VCIP_PENDING"]

    if skip_session_status_filter:
        print("skipping session status filter 2!!!!")
        pass
    else:
        res_items = res_items[res_items["rbl_session_status"].isin(status_to_keep)]

    print("res-items before after skip 2: {}".format(len(res_items)))

    # res_items = res_items.reset_index(drop=True)
    res_items.fillna("", inplace=True)
    res_items.loc[res_items["rbl_session_status"] == "CHECKER_APPROVED", "rbl_session_status"] = "Audited and Okay"
    res_items.loc[res_items["rbl_session_status"] == "CHECKER_REJECTED", "rbl_session_status"] = "Audited and not Okay"

    tracker_t_store["eighth"] = time.time() - tracker_t

    # del res_items["phone_number"]


    print("fdadfsadf phone mask = ", res_items['phone_number_x'])
    #pmask = res_items['phone_number_x'].isna()

    pmask = res_items['phone_number_x'].isnull() | ( res_items['phone_number_x'] == 'NA') | ( res_items['phone_number_x'] == '')

    phone_res_items_to_process = res_items[pmask]
    phone_res_items_not_to_process = res_items[~pmask]

    def get_phone(row):
        phone = "" 
        print("phone ---- > ", row["phone_number_x"], row["phone_number_y"])
        if not phone and not pd.isna(row["phone_number_y"]) and bool(row["phone_number_y"].get("S", "")):
            phone = get_key(row["phone_number_y"], "S")

        return pd.Series([phone], index=["phone_number"])

    print("phone_res_items_to_process = ", phone_res_items_to_process)
    if len(phone_res_items_to_process) > 0:
        processed = phone_res_items_to_process.apply(get_phone, axis=1)
        phone_res_items_to_process["phone_number_x"] = processed

    res_items = pd.concat([phone_res_items_to_process, phone_res_items_not_to_process])

    print(" 1  phone mask = ", res_items['phone_number_x'])
    res_items["phone_number"] = res_items["phone_number_x"]
    print(" 1  phone mask = ", res_items['phone_number'])
    res_items["dispositions_punched"] = res_items["dispositions"]
    # del res_items["phone_number_x"]

    res_items = res_items.to_dict(orient="records")
    print("final return length: {}".format(len(res_items)), time.time() - tracker_t, tracker_t_store)

    return res_items
