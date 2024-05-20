import requests
import json
import time
from datetime import datetime
import base64
from dicttoxml import dicttoxml
#from dict2xml import dict2xml
import xmltodict
from app.config import session_status_mapper,ekyc_expiry_time_period
import pandas as pd

#url_esb_details = "http://10.100.114.26:7080/rbl/esb/ldapdetails"
url_esb_ech = "https://iibuat.rblbank.com:7081/rbl/esb/echldapservice"          #"http://10.100.114.26:7080/rbl/esb/echldapservice"

#url_esb_enquiry = "https://apideveloper.rblbank.com/test/sb/v1/rbl/esb/ldapenquiry"

#url_esb_details = "https://apideveloper.rblbank.com/test/sb/v1/rbl/esb/ldapdetails"

url_esb_enquiry = "https://iibuat.rblbank.com:7081/rbl/esb/ldapenquiry"

# url_esb_details = "https://iibuat.rblbank.com:7081/rbl/esb/ldapdetails"

url_esb_details = "https://iib.ratnakarbank.in:5443/rbl/esb/ldapdetails"

headers_ldap = headers = {
  'Content-Type': 'application/xml'
}

def rbl_esb_details(data):
    #data=request.get_json()
    #data = {}
    #data["UserId"] = "21703"
    #data["LoginPwd"] = "Apr@2021"
    base_data = {
            "LdapDtls":
                { "Request":
                    {
                            "UserId":"{0}".format(data["UserId"]),
                            "ChlId": "{0}".format(data["ChlId"]),
                            "LoginPwd": "{0}".format(str(base64.b64encode(data["LoginPwd"].encode("utf-8"))))
                    }
                }
                }
    payload = dicttoxml(base_data,root=False)
    #payload = json.dumps(base_data)
    response = requests.request("POST", url_esb_details, headers=headers_ldap, data=payload,verify = False)
    #print(response.text)
    resp = xmltodict.parse(response.text,dict_constructor=dict)
    print(resp)
    if "LdapDtls" in resp:
        if resp["LdapDtls"]["Response"]["Status"] == "SUCCESS":
            return resp
    return None
    #print(xmltodict.parse(response.text,dict_constructor=dict))
    #return True

def ta_to_rbl_session_status(sessions,client_name):
    for data in sessions:
        session_status = data['session_status']
        rbl_session_status = session_status_mapper[session_status]
        client_name = data['client_name']

        if client_name in ["RBL_uat", "RBL", "Digiremit_uat","Digiremit","Retailasset_uat","Retailasset"] and rbl_session_status in ["NOT_ASSIGNED_TO_AGENT"]:
            ekyc_req_time = data.get('ekyc_req_time','')
            if ekyc_req_time == "":
                rbl_session_status = "KYC_PENDING"
            else:
                if (divmod(time.time() - float(ekyc_req_time), 60)[0] <= ekyc_expiry_time_period[client_name]):
                    rbl_session_status = "VCIP_PENDING"
                else:
                    rbl_session_status = "KYC_PENDING"

        if rbl_session_status in ["VCIP_APPROVED"]:
            try:
                audit_lock = data['audit_lock']
                if audit_lock:
                    rbl_session_status = "CHECKER_PENDING"
            except:
                pass
        if rbl_session_status in ["VCIP_APPROVED","CHECKER_PENDING"]:
            try:
                audit_result = data['audit_result']
                if int(audit_result) == 1:
                    rbl_session_status = "CHECKER_APPROVED"
                else:
                     rbl_session_status = "CHECKER_REJECTED"
            except Exception as e:
                pass

        if client_name in ["RBL","RBL_uat","BFL","BFL_uat","Retailasset_uat","Retailasset"]:
            feedback = data.get('feedback','')
            expiring_at_link_expiry = data.get('expiring_at_link_expiry',None)
            expiry_reason = data.get('expiry_reason',None)
            if rbl_session_status == "VCIP_SESSION_INVALID":
                if feedback != "":
                    try:
                        if json.loads(feedback)["type"] == "Reschedule":
                            rbl_session_status = "VCIP_RESCHEDULED"
                    except Exception as e:
                        pass
                if expiring_at_link_expiry:
                    if expiry_reason is None:
                        rbl_session_status = "VCIP_EXPIRED"

        data['session_status'] = rbl_session_status

    temp_df = pd.DataFrame(sessions)

    print("sessions c89ae364-4c68-4025-9596-4e24028e3f4d - ", filter(lambda x: x['session_id'] == 'c89ae364-4c68-4025-9596-4e24028e3f4d', sessions))

    if temp_df.shape[0] > 0:
        # Existing statuses to skip
        statuses_to_skip = ["CHECKER_APPROVED", "NOT_ASSIGNED_TO_AGENT", "session_expired", "VCIP_APPROVED"]

        temp_df.sort_values("start_time",inplace=True,ascending=False)
       
        # Update session_status for duplicated user_id except for the statuses in statuses_to_skip
        condition = temp_df.duplicated("user_id", keep="first") & ~temp_df["session_status"].isin(statuses_to_skip)
        temp_df.loc[condition, "session_status"] = "VCIP_SESSION_INVALID"


        #temp_df.loc[temp_df.duplicated("user_id", keep="first"), "session_status"] = "VCIP_SESSION_INVALID"
        # temp_df[temp_df.duplicated("user_id","first")]["session_status"] = "VCIP_SESSION_INVALID"

        if client_name in ["RBL_uat", "RBL","Retailasset_uat","Retailasset"]:
            temp_df.loc[temp_df["session_status"] == "NOT_ASSIGNED_TO_AGENT", "session_status"] = "VCIP_PENDING"

        temp_df.loc[temp_df["session_status"] == "CHECKER_APPROVED", "session_status"] = "Audited and Okay"
        temp_df.loc[temp_df["session_status"] == "CHECKER_REJECTED", "session_status"] = "Audited and not okay"
        temp_df.fillna("", inplace=True)

    final_out_n = temp_df.to_dict(orient="records")

    return final_out_n

#rbl_esb_details({"UserId":"21703","LoginPwd":"Apr@2021","ChlId":"ECH"})
