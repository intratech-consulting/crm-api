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

# Get order to change amount
def get_order(user_id, product_id):
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

#########################
## Publisher API Calls ##
#########################

# Get an user by id api call
def get_new_user(user_id=None):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+Id,first_name__c,last_name__c,email__c,telephone__c,birthday__c,country__c,state__c,city__c,zip__c,street__c,house_number__c,company_email__c,company_id__c,source__c,user_role__c,invoice__c,calendar_link__c+FROM+user__c+WHERE+Id+=+\'{user_id}\''
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json().get("records", [])
    if data:
        return create_xml_user(data)
    
# Get updated user fields from changedSalesforce data
def get_updated_user(id=None):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+first_name__c,last_name__c,email__c,telephone__c,birthday__c,country__c,state__c,city__c,zip__c,street__c,house_number__c,company_email__c,company_id__c,source__c,user_role__c,invoice__c,calendar_link__c+FROM+changed_object__c+WHERE+Id=\'{id}\''
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json().get("records", [])
    if data:
        logger.debug(f"Updated user fields: {data}")
        return [key for obj in data for key, value in obj.items() if isinstance(value, bool) and value]

# Get a company by id api call
def get_new_company(company_id=None):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+Id,Name,email__c,telephone__c,country__c,state__c,city__c,zip__c,street__c,house_number__c,type__c,invoice__c+FROM+Company__c+WHERE+Id+=+\'{company_id}\''
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json().get("records", [])
    if data:
        return create_xml_company(data)
    
# Get updated company fields from changedSalesforce data
def get_updated_company(id=None):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+name__c,email__c,telephone__c,country__c,state__c,city__c,zip__c,street__c,house_number__c,type__c,invoice__c+FROM+changed_object__c+WHERE+Id=\'{id}\''
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json().get("records", [])
    if data:
        return [key for obj in data for key, value in obj.items() if isinstance(value, bool) and value]
    
# Get a event by id api call
def get_new_event(event_id=None):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+Id,date__c,start_time__c,end_time__c,location__c,user_id__c,company_id__c,max_registrations__c,available_seats__c,description__c+FROM+event__c+WHERE+Id+=+\'{event_id}\''
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json().get("records", [])
    if data:
        logger.debug(f"New event: {data}")
        return create_xml_event(data)
    
# Get updated event fields from changedSalesforce data
def get_updated_event(id=None):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+date__c,start_time__c,end_time__c,location__c,user_id__c,company_id__c,max_registrations__c,available_seats__c,description__c+FROM+changed_object__c+WHERE+Id=\'{id}\''
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json().get("records", [])
    if data:
        return [key for obj in data for key, value in obj.items() if isinstance(value, bool) and value]
    
# Get the attendances
def get_new_attendance(attendance_id=None):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+Id,user_id__c,event_id__c+FROM+attendance__c+WHERE+Id+=\'{attendance_id}\''
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json().get("records", [])
    if data:
        return create_xml_attendance(data)
    
# Get updated attendance fields from changedSalesforce data
def get_updated_attendance(id=None):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+user_id__c,event_id__c+FROM+changed_object__c+WHERE+Id=\'{id}\''
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json().get("records", [])
    if data:
        return [key for obj in data for key, value in obj.items() if isinstance(value, bool) and value]

# Get changedSalesforce data
def get_changed_data():
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+Id,Name,object_type__c,crud__c+FROM+changed_object__c'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json().get("records", [])
    if data:
        return create_xml_changed_data(data)

# Get updated values
def get_updated_values(query=None):
    url = secrets.DOMAIN_NAME + query
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json().get("records", [])
    if data:
        return data[0]

# Delete Change Object api call
def delete_change_object(id=None):
    url = secrets.DOMAIN_NAME + f'sobjects/changed_object__c/{id}'
    headers = {
        'Authorization': 'Bearer ' + secrets.ACCESS_TOKEN
    }
    requests.delete(url, headers=headers)

# Create a custom logger
logger = init_logger("__API__")