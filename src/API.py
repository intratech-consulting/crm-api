import requests
from datetime import datetime
import xml.etree.ElementTree as ET
import jwt
import sys, os

if os.path.isdir('/app'):
    sys.path.append('/app')
else:
    local_dir = os.path.dirname(os.path.realpath(__file__))
    sys.path.append(local_dir)
import config.secrets as secrets
from xml_parser import *
from logger import init_logger

##############################
## Authentication API Calls ##
##############################

# Get the access token and domain name
def authenticate():
    with open(secrets.KEY_FILE) as file:
        private_key = file.read()

    claimSet = {
        'iss': secrets.ISSUER,
        'exp': int(datetime.now().timestamp()) + 300,
        'aud': 'https://login.salesforce.com',
        'sub': secrets.SUBJECT
    }

    assertion = jwt.encode(claimSet, private_key, algorithm='RS256', headers={'alg': 'RS256'})

    req = requests.post('https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/oauth2/token',
                        data={
                            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                            'assertion': assertion
                        })

    response = req.json()
    secrets.ACCESS_TOKEN = response['access_token']
    logger.info("Authenticated successfully")

########################
## Consumer API Calls ##
########################

# Add an user api call
def add_user(payload):
    url = secrets.DOMAIN_NAME + 'sobjects/user__c'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    response = requests.post(url, headers=headers, data=payload)
    return response.json().get('id', None)

# Update an user api call
def update_user(id, payload):
    print("update user: " + id)
    url = secrets.DOMAIN_NAME + f'sobjects/user__c/{id}'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    requests.patch(url, headers=headers, data=payload)

# Delete an user api call
def delete_user(user_id):
    url = secrets.DOMAIN_NAME + f'sobjects/user__c/{user_id}'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    requests.delete(url, headers=headers)

# Add a company api call
def add_company(payload):
    url = secrets.DOMAIN_NAME + f'sobjects/Company__c'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    response = requests.post(url, headers=headers, data=payload)
    return response.json().get('id', None)

# Update a company api call
def update_company(id, payload):
    url = secrets.DOMAIN_NAME + f'sobjects/Company__c/{id}'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    requests.patch(url, headers=headers, data=payload)

# Delete a company api call
def delete_company(company_id):
    url = secrets.DOMAIN_NAME + f'sobjects/company__c/{company_id}'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    requests.delete(url, headers=headers)

# Add an event api call
def add_event(payload):
    url = secrets.DOMAIN_NAME + f'sobjects/event__c'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    response = requests.post(url, headers=headers, data=payload)
    return response.json().get('id', None)

# Update an event api call
def update_event(id, payload):
    url = secrets.DOMAIN_NAME + f'sobjects/event__c/{id}'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    requests.patch(url, headers=headers, data=payload)

# Delete an event api call
def delete_event(event_id):
    url = secrets.DOMAIN_NAME + f'sobjects/event__c/{event_id}'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    requests.delete(url, headers=headers)

# Add an attendance
def add_attendance(payload):
    url = secrets.DOMAIN_NAME + f'sobjects/attendance__c'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    response = requests.post(url, headers=headers, data=payload)
    return response.json().get('id', None)

# Update an attendance
def update_attendance(id, payload):
    url = secrets.DOMAIN_NAME + f'sobjects/attendance__c/{id}'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    requests.patch(url, headers=headers, data=payload)

# Delete an attendance api call
def delete_attendance(attendance_id):
    url = secrets.DOMAIN_NAME + f'sobjects/attendance__c/{attendance_id}'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    requests.delete(url, headers=headers)

# Add a product
def add_product(payload):
    url = secrets.DOMAIN_NAME + f'sobjects/product__c'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    response = requests.post(url, headers=headers, data=payload)
    return response.json().get("id", None)

# Update a product
def update_product(id, payload):
    url = secrets.DOMAIN_NAME + f'sobjects/product__c/{id}'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    requests.patch(url, headers=headers, data=payload)

# Add an order
def add_order(payload):
    url = secrets.DOMAIN_NAME + f'sobjects/order__c'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    requests.post(url, headers=headers, data=payload)

def update_order(id, payload):
    url = secrets.DOMAIN_NAME + f'sobjects/order__c/{id}'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    requests.patch(url, headers=headers, data=payload)

# Get order from user to change amount
def get_order_user(user_id, product_id):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+Id,amount__c+FROM+order__c+WHERE+user_id__c=\'{user_id}\'AND+product_id__c=\'{product_id}\''
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json().get("records", [])

    if data:
        return data[0]['Id'], data[0]['amount__c']
    else:
        return None, None
    
# Get order from company to change amount
def get_order_company(company_id, product_id):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+Id,amount__c+FROM+order__c+WHERE+company_id__c=\'{company_id}\'AND+product_id__c=\'{product_id}\''
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json().get("records", [])

    if data:
        return data[0]['Id'], data[0]['amount__c']
    else:
        return None, None

# Create a custom logger
logger = init_logger("__API__")