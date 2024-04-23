import requests
from datetime import datetime
import xml.etree.ElementTree as ET
import jwt
import time
import logging

KEY_FILE = 'salesforce.key' #Key file
ISSUER = '3MVG9k02hQhyUgQBC9hiaTcCgbbdMVPx9heQhKpTslb68bY7kICgeRxzAKW7qwDxbo6uYZgMzU1GG9MVVefyU' #Consumer Key
SUBJECT = 'admin@ehb.be' #Subject
DOMAIN_NAME = 'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com'
ACCESS_TOKEN = ''

logger = logging.getLogger(__name__)

# Create a file handler
handler = logging.FileHandler('api.log')
handler.setLevel(logging.INFO)

# Create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)

# Get the access token and domain name
def authenticate():
    with open(KEY_FILE) as file:
        private_key = file.read()

    claimSet = {
        'iss': ISSUER,
        'exp': int(datetime.now().timestamp()) + 300,
        'aud': 'https://login.salesforce.com',
        'sub': SUBJECT
    }

    assertion = jwt.encode(claimSet, private_key, algorithm='RS256', headers={'alg': 'RS256'})

    req = requests.post('https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/oauth2/token', data={
        'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        'assertion': assertion
    })

    response = req.json()
    global ACCESS_TOKEN
    ACCESS_TOKEN = response['access_token']


# Get users api call
def get_users():
    url = DOMAIN_NAME + '/services/data/v60.0/query?q=SELECT+user_id__c,first_name__c,last_name__c,email__c,telephone__c,birthday__c,country__c,state__c,city__c,zip__c,street__c,house_number__c,company_email__c,company_id__c,source__c,user_role__c,invoice__c+FROM+user__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN
    }
    payload = {}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        data = response.json().get("records", [])
        
        root = ET.Element("Users")

        # Makes json data into xml data
        for record in data:
            user_element = ET.SubElement(root, "user__c")
            for field, value in record.items():
                if field == "attributes":
                    continue
                field_element = ET.SubElement(user_element, field)
                field_element.text = str(value)

        xml_string = ET.tostring(root, encoding="unicode", method="xml")
        logger.info("get users: " + xml_string)
        return xml_string
    except Exception as e:
        print("Error fetching users from Salesforce:", e)
        return None

# Add an user api call
def add_user(user_id, first_name, last_name, email, telephone, birthday, country, state, city, zip, street, house_number, company_email = "", company_id = "", source = "", user_role = "", invoice = ""):
    url = DOMAIN_NAME + '/services/data/v60.0/sobjects/Portal_user__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = f'''
        <user__c>
            <user_id__c>{user_id}</user_id__c>
            <first_name__c>{first_name}</first_name__c>
            <last_name__c>{last_name}</last_name__c>
            <email__c>{email}</email__c>
            <telephone__c>{telephone}</telephone__c>
            <birthday__c>{birthday}</birthday__c>
            <country__c>{country}</country__c>
            <state__c>{state}</state__c>
            <city__c>{city}</city__c>
            <zip__c>{zip}</zip__c>
            <street__c>{street}</street__c>
            <house_number__c>{house_number}</house_number__c>
            <company_email__c>{company_email}</company_email__c>
            <company_id__c>{company_id}</company_id__c>
            <source__c>{source}</source__c>
            <user_role__c>{user_role}</user_role__c>
            <invoice__c>{invoice}</invoice__c>
        </user__c>
    '''

    response = requests.request("POST", url, headers=headers, data=payload)
    logger.info("add user" + response.text)

# Get companies api call
def get_companies():
    url = DOMAIN_NAME + '/services/data/v60.0/query?q=SELECT+id__c,name__c,email__c,telephone__c,country__c,state__c,city__c,street__c,house_number__c,type__c,invoice__c+FROM+Company__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN
    }
    payload = {}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        data = response.json().get("records", [])
        
        root = ET.Element("Companies")

        # Makes json data into xml data
        for record in data:
            company_element = ET.SubElement(root, "Company__c")
            for field, value in record.items():
                if field == "attributes":
                    continue
                field_element = ET.SubElement(company_element, field)
                field_element.text = str(value)

        xml_string = ET.tostring(root, encoding="unicode", method="xml")
        return xml_string
    except Exception as e:
        print("Error fetching companies from Salesforce:", e)
        return None

# Add a company api call
def add_company(id, name, email, telephone, country, state, city, zip, street, house_number, type, invoice):
    url = DOMAIN_NAME + '/services/data/v60.0/sobjects/Company__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = f'''
        <Company__c>
            <id__c>{id}</id__c>
            <name>{name}</name>
            <email__c>{email}</email__c>
            <telephone__c>{telephone}</telephone__c>
            <country__c>{country}</country__c>
            <state__c>{state}</state__c>
            <city__c>{city}</city__c>
            <zip__c>{zip}</zip__c>
            <street__c>{street}</street__c>
            <house_number__c>{house_number}</house_number__c>
            <type__c>{type}</type__c>
            <invoice__c>{invoice}</invoice__c>
        </Company__c>
    '''

    response = requests.request("POST", url, headers=headers, data=payload)
    logger.info("add company" + response.text)
    print(response.text)

# Get talks api call
def get_talk():
    url = DOMAIN_NAME + '/services/data/v60.0/query?q=SELECT+Id,Name,Date__c+FROM+Talk__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN
    }
    payload = {}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        data = response.json().get("records", [])
        
        root = ET.Element("Talks")

        # Makes json data into xml data
        for record in data:
            talk_element = ET.SubElement(root, "Talk__c")
            for field, value in record.items():
                if field == "attributes":
                    continue
                field_element = ET.SubElement(talk_element, field)
                field_element.text = str(value)

        xml_string = ET.tostring(root, encoding="unicode", method="xml")
        return xml_string
    except Exception as e:
        print("Error fetching talks from Salesforce:", e)
        return None

# Add a talk api call
def add_talk(id, date, start_time, end_time, user_id, available_seats, description):
    url = DOMAIN_NAME + '/services/data/v60.0/sobjects/event__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = f'''
        <event__c>
            <id__c>{id}</id__c>
            <date__c>{date}</date__c>
            <start_time__c>{datetime.strptime(start_time, '%H:%M').strftime('%H:%M:%S')}</start_time__c>
            <end_time__c>{datetime.strptime(end_time, '%H:%M').strftime('%H:%M:%S')}</end_time__c>
            <user_id__c>{user_id}</user_id__c>
            <available_seats__c>{available_seats}</available_seats__c>
            <description__c>{description}</description__c>
        </event__c>
    '''

    response = requests.request("POST", url, headers=headers, data=payload)
    logger.info("add talk" + response.text)

# Get the attendances
def get_attendance():
    url = DOMAIN_NAME + '/services/data/v60.0/query?q=SELECT+Id,Name,Talk__c,Portal_user__c+FROM+talkAttendance__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN
    }
    payload = {}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        data = response.json().get("records", [])
        
        root = ET.Element("Attendance")

        # Makes json data into xml data
        for record in data:
            attendance_element = ET.SubElement(root, "Attendance__c")
            for field, value in record.items():
                if field == "attributes":
                    continue
                field_element = ET.SubElement(attendance_element, field)
                field_element.text = str(value)

        xml_string = ET.tostring(root, encoding="unicode", method="xml")
        return xml_string
    except Exception as e:
        print("Error fetching attendance from Salesforce:", e)
        return None

# Add an attendance
def add_attendance(User = None, Talk = None):
    url = DOMAIN_NAME + '/services/data/v60.0/sobjects/talkAttendance__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = f'''
        <TalkAttendance__c>
            <Portal_user__c>{User}</Portal_user__c>
            <Talk__c>{Talk}</Talk__c>
        </TalkAttendance__c>
    '''

    response = requests.request("POST", url, headers=headers, data=payload)
    logger.info("add attendance" + response.text)