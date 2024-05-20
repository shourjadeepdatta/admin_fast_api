from flask import jsonify, g, request
import jwt
from jwt import DecodeError, ExpiredSignature
from datetime import datetime, timedelta
import calendar
from functools import wraps
import os

#####################################################################
# update following variable to switch to UAT/PRODUCTION server

isQA = True

#####################################################################

# env=os.environ.get("ENV")
# cname=os.environ.get("CLIENT_NAME")

SECRET_KEY = 'gmh2837kdsk0aa:.sds'


def create_token(api_key):
    payload = {
        'sub': api_key,
        'aud': "usr",
        'iat': calendar.timegm(datetime.utcnow().timetuple()),
        'exp': calendar.timegm((datetime.utcnow() + timedelta(days=(365))).timetuple()),
        'iss': 'ipv_api_IonicApp',
        'role': 'admin'
    }
    print (calendar.timegm(datetime.utcnow().timetuple()))
    print (calendar.timegm((datetime.utcnow() + timedelta(minutes=(30))).timetuple()))
    token = jwt.encode(payload, SECRET_KEY, algorithm='HS256')
    return token.decode('unicode_escape')


def parse_token(req, permission):
    token = req.headers.get('auth')
    #print ("auth-->{}".format(token))
    return jwt.decode(token, SECRET_KEY, algorithms='HS256', audience=permission)


def login_required(*args1, **kwargs1):
    def login_decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            #print ("request header--{}".format(request.headers))
            if not request.headers.get('auth'):
                response = jsonify(message='Missing authorization header')
                print("No header passed")
                response.status_code = 401
                return response
            try:
                payload = parse_token(request,kwargs1['permission'])
                #if payload("role") != "admin":
                    #pass
                    #print("auth role not admin")
                    #response = jsonify(message='invalid role')
                    #response.status_code = 401
                    #return response

            except DecodeError as d:
                response = jsonify(message='Token is invalid')
                print("DecodeError",d)
                response.status_code = 401
                return response
            except ExpiredSignature as es:
                response = jsonify(message='Token has expired')
                print("expired signature",es)
                response.status_code = 401
                return response
            except jwt.InvalidAudienceError:
                response = jsonify(message='Insufficient access level')
                print("insufficient access")
                response.status_code = 401
                return response
            except jwt.InvalidIssuedAtError:
                response = jsonify(message='Issued at time doesnt look right')
                print("invalid issue time")
                response.status_code
                return response
            except jwt.InvalidIssuerError:
                response = jsonify(message='Doesnt look like we issued this token')
                print("token missing")
                response.status_code
                return response
            
            # if env.lower()=="prod":
            #     g.user_id = cname.lower()
            # else:
            #     g.user_id="{}_{}".format(cname.lower(),env.lower())
            
            g.user_id = payload['sub']
            return f(*args, **kwargs)
        return decorated_function
    return login_decorator



#print(create_token("BFL_uat"))
