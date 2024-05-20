import json
import time
import requests
from datetime import datetime, timedelta



def get_token():
    login_url = "https://vcip.rblbank.com/api/admin/v7/admin/login" 
    payload = {
        "admin_id": "RBL_admin",
        "admin_password": "1234",
        "domain_name": "RBL"
    } 
    headers = {
        'content-type': "application/json"
    }
    response = requests.request("POST", login_url, headers=headers, data=json.dumps(payload))
    print("get_token = ", response.text)
    print("get_token = ", response.json().get("token"))
    return response.json().get("token")






# For unique session list for last 30 days
end_time = datetime.now().replace(hour=23, minute=59, second=0, microsecond=0)
start_time = (end_time - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)

start_time_epoch = int(time.mktime(start_time.timetuple()))
end_time_epoch = int(time.mktime(end_time.timetuple()))

payload = {
    "time_type": "creation_time",
    "start_time": start_time_epoch,
    "end_time":end_time_epoch,
    "session_type": "unique",
    "sendEmail": True,
    "recipients": [
        "shourjadeep.datta@think360.ai",
        "tejesh.more@think360.ai",
        "atharva.dave@think360.ai",
        "shivam.pandey@think360.ai",
        "ravi.lakhwani@think360.ai",
        "support@getkwikid.com",
        "ravi.lakhwani@think360.ai",
        "jaspreet.sandhu@rblbank.com",
        "rajkumar.choubey@globiva.com",
        "neelesh.kumar@rblbank.com",
        "ishan.dwivedi@rblbank.com",
        "riyaan.khan@rblbank.com",
        "VideoKyc_Support@rblbank.com",
        "balraj.boora@rblbank.com"   
    ]
}


report_maker_url = "https://vcip.rblbank.com/api/admin/v7/generate_report"
headers = {
    "auth": get_token(),
    'content-type': "application/json"
}
print("payload = ", payload)
response = requests.request("POST", report_maker_url, headers=headers, data=json.dumps(payload))
print("Final resposne = ", response)
