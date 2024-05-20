
from flask import Flask,request, Blueprint
from celery import Celery
from app.api.routes.getsessionlist import bp  as get_session_list_bp

bp = Blueprint('bp', __name__)
app = Flask(__name__)


import time

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import utils

from base64 import b64encode
import json

xridlist = []

app_celery_new = Celery('celery',
                  broker='amqp://abhiace:abhi123$@ec2-13-233-10-152.ap-south-1.compute.amazonaws.com/kwikid_vkyc_rbl'
                  )


def celery_generate_report(client_name, start_time, end_time, kind, get_latest_status_for_userid_only, sendEmail, recipients, sendEmailLink):
    print("celery_generate_report  called")
    request_id = app_celery_new.send_task(name='generate_report.task',
                             args=[client_name, start_time, end_time, kind, get_latest_status_for_userid_only, sendEmail, recipients, sendEmailLink],
                             queue='celery_report_maker')
    print("celery_generate_report request_id = ", request_id)
    return request_id 


# with open('pkey.pem','r') as pkey:
#     private_key = pkey.read()
#     private_key = serialization.load_pem_private_key(
#                          private_key,
#                                   password=None,
#                                           backend=default_backend()
#                                                )

def get_signature(message):
    chosen_hash = hashes.SHA1()
    hasher = hashes.Hash(chosen_hash,default_backend())
    hasher.update(message)
    digest = hasher.finalize()
                                                    #print "digest: {}".format(b64encode(digest))
    signature = private_key.sign(
                    digest,
                    padding.PSS(
                    mgf=padding.MGF1(hashes.SHA1()),
                        salt_length=20
                    ),
                utils.Prehashed(chosen_hash)
                 )


      # print signature
    return b64encode(signature),b64encode(digest)

    pass


from app import routes
app.register_blueprint(bp)
app.register_blueprint(get_session_list_bp)


@app.after_request
def after_request(response):
    try:
        if str(request.headers['xrid']) in xridlist:
            return response
        d = response.get_data()
        iat = int(time.time())-15
        exp = iat+900
        sig,response.headers['dig'] = get_signature(str(d).replace("\n","").replace("\r","").replace("\t","").replace("  ","").replace("\", \"","\",\"").replace("\": \"","\":\"").replace(", \"",",\"").replace(": t",":t")+str(request.headers['xrid']))#+str(iat)+str(exp))
        response.headers['iat'] = str(iat)
        response.headers['exp'] = str(exp)
        response.headers['sig'] = sig + "." + b64encode(str(exp))
        response.headers['xrid'] = request.headers['xrid']

        xridlist.append(str(request.headers['xrid']))
        #response.headers['xs'] = len(xridlist)
        return response
    except Exception as e:
        print ("error : {}".format(e))
        #raise e
        print ("Error while generating signature:")
        return response
