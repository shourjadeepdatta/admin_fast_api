import requests
import json
import traceback


columns_to_put = {
    "kwikid_vkyc_agent": [
        "login_lock",
        "login_attempts",
        "is_ldap",
        "register",
        "login",
        "is_admin",
        "previous_audit_end_time",
        "task_id",
        "agent_role",
        "session_id",
        "agent_mode",
        "agent_id",
        "agent_last_active_timestamp",
        "is_halted",
        "client_name",
        "secret",
        "user_id",
        "agent_status",
    ],
    "kwik_id_user_manual_allocation": [
        "agent_id",
        "call_counter",
        "latest_row_sess_id",
        "disposition_punched",
        "client_name",
        "product_code",
        "start_time",
        "user_id",
        "sess_id",
        "phone_number",
        "case_push_time",
        "message_punched",
        "user_name",
    ],
    "kwikid_vkyc_link_status": ["phone_number", "user_id", "link_status", "client_name", "session_id", "link_id", "link_expiry_time", "link_initiation_time", "link_url", "link_type"],
    "kwikid_vkyc_session_status": [
        "audit_lock",
        "number_of_videos_uploaded",
        "fraud_advisory_given",
        "is_vido_uploaded",
        "agent_assignment_time",
        "summary_json_url",
        "pan_url",
        "session_id",
        "vkyc_start_time",
        "custname",
        "agent_screen_url",
        "client_name",
        "summary_data",
        "captured_images",
        "location",
        "app_version_number",
        "queue_mode",
        "zip_url",
        "audit_init_time",
        "link_type_custom_info",
        "phone_number",
        "session_status",
        "start_time",
        "product",
        "selfie_request_time",
        "user_id",
        "summary_pdf_url",
        "link_id",
        "selfie_url",
        "agent_video_url",
        "kwikid_support",
        "manual_audit_lock_remove_time",
        "ekyc_req_time",
        "case_push_time",
        "queue",
        "pan_request_time",
        "extras",
        "end_time",
        "feedback",
        "agent_id",
        "email_id",
        "user_video_url",
        "manual_agent",
    ],
    "kwikid_vkyc_user_status": [
        "from_skygee",
        "prev_client_handled_count",
        "ekyc_success",
        "form_data",
        "client_name",
        "current_state",
        "ekyc_req_time",
        "init_timestamp",
        "user_id",
        "last_active_timestamp",
        "user_mode",
        "app_version_number",
        "session_id",
        "phone_number",
        "agent_id",
        "case_push_time",
        "cpuniquerefno",
        "location",
    ],
    "Kwikid_vkyc_ekyc": ["ekyc_redirection_url", "user_id", "cpuniquerefno", "client_name", "ekyc_link", "ekyc_success", "ekyc_data", "custom_ekyc", "ekyc_req_time"],
}


BASE_URL = "https://kbipvd.thinkanalytics.in:6356/postgres/"

headers = {
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoicmJscHJvZCJ9.iPQPLTcg0qU-pFcmTdI_rVEhHA_MX6WDY-3ep6tXFCo",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}


def update_postgres_table(table_name, primary_key_name, primary_key_value, update_expression_string, expression_attribute_value, sort_key_name=None, sort_key_value=None):
    try:
        final_dictionary = form_dictionary(update_expression_string, expression_attribute_value)
    except Exception as e:
        traceback.print_exc()
        print("Error while generating dictionary for postgres row updation", e, update_expression_string, expression_attribute_value)

    if table_name in columns_to_put:
        table_columns = columns_to_put[table_name]
        for key in final_dictionary.keys():
            if key not in table_columns:
                del final_dictionary[key]

        params = {
            primary_key_name: "eq." + primary_key_value,
        }

        if sort_key_name and sort_key_value:
            params[sort_key_name] = "eq." + sort_key_value

        row = json.dumps(final_dictionary)

        try:
            res = requests.patch(BASE_URL + table_name + "_uat", params=params, data=row, headers=headers, verify=False)
            print("postgres update response", table_name, row, params, res.text, res.status_code)
        except Exception as e:
            print(e)


def extract_keys_to_set_null(input_string):
    parts = [part.strip() for part in input_string.split(",")]
    extracted_substrings = []
    for part in parts:
        extracted_substrings.append(part.strip())  # Strip whitespace from each part

    return extracted_substrings


def extract_keys_to_update(input_string):
    parts = [part.strip() for part in input_string.split(",")]
    extracted_substrings = []
    for part in parts:
        # Split each part based on '=' and get the first part
        key_substring = part.split("=")[0].strip()
        value_substring = part.split("=")[1].strip()
        extracted_substrings.append({"key": key_substring, "val": value_substring})
        # Initialize a list to store the extracted substrings
    return extracted_substrings


def form_dictionary(update_expression_string, expression_attribute_value):
    update = []
    remove = []
    first_split_og = update_expression_string.strip().split("REMOVE")
    first_split = []
    for fs in first_split_og:
        if bool(fs):
            first_split.append(fs)

    print("first_split", first_split)

    if len(first_split) == 2:
        remove = extract_keys_to_set_null(first_split[-1])

        second_split = first_split[0].split("set")
        print("second", second_split)
        update = extract_keys_to_update(second_split[-1])
    elif "set" in update_expression_string:
        print("set", update_expression_string, first_split)
        value = update_expression_string.split("set")
        update = extract_keys_to_update(value[-1])
    elif "REMOVE" in update_expression_string:
        print("REMOVE", update_expression_string, first_split)
        remove = extract_keys_to_set_null(first_split[-1])

    final_dict = {}
    for item in update:
        final_dict[item.get("key")] = expression_attribute_value.get(item.get("val"))

    for item in remove:
        final_dict[item] = None

    print("attr", type(expression_attribute_value))
    print("update", update)
    print("remove", remove)
    print("final_dict", final_dict)
    return final_dict


def insert(table_name, row):
    try:
        if table_name in columns_to_put:
            table_columns = columns_to_put[table_name]
            for key in row.keys():
                if key not in table_columns:
                    del row[key]

            row = json.dumps(row)
            res = requests.request("POST", BASE_URL + table_name + "_uat", data=row, headers=headers, verify=False)
            print("postgres insert response", table_name, row, res.text, res.status_code)
    except Exception as e:
        print("error while writing to postgres:{}".format(e))

def put_item_in_postgres(table_name, row):
    insert(table_name=table_name, row=row)
