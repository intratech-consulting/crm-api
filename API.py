import subprocess
import re
import requests
from datetime import datetime
import xml.etree.ElementTree as ET
import jwt

KEY_FILE = 'salesforce.key' #Key file
ISSUER = '3MVG9k02hQhyUgQBC9hiaTcCgbbdMVPx9heQhKpTslb68bY7kICgeRxzAKW7qwDxbo6uYZgMzU1GG9MVVefyU' #Consumer Key
SUBJECT = 'admin@ehb.be' #Subject
DOMAIN_NAME = 'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/'

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
    print(assertion)

    req = requests.post('https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/oauth2/token', data={
        'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        'assertion': assertion
    })

    response = req.json()
    global ACCESS_TOKEN
    ACCESS_TOKEN = response['access_token']
    print(ACCESS_TOKEN)


# Get users api call
def get_users():
    url = DOMAIN_NAME + '/services/data/v60.0/query?q=SELECT+Id,Name,First_name__c,Last_name__c,Email__c,Company__c,Company_email__c,Signup_source__c+FROM+Portal_user__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN
    }
    payload = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json().get("records", [])
    root = ET.Element("Portal_Users")

    # Makes json data into xml data
    for record in data:
        user_element = ET.SubElement(root, "Portal_user__c")
        for field, value in record.items():
            if field == "attributes":
                continue
            field_element = ET.SubElement(user_element, field)
            field_element.text = str(value)

    xml_string = ET.tostring(root, encoding="unicode", method="xml")
    print(xml_string)

# Add an user api call
def add_user(FirstName = None, LastName = None, Email = None, Company = None, CompanyEmail = None, Source = None):
    url = DOMAIN_NAME + '/services/data/v60.0/sobjects/Portal_user__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = f'''
        <Portal_user__c>
            <First_name__c>{FirstName}</First_name__c>
            <Last_name__c>{LastName}</Last_name__c>
            <Email__c>{Email}</Email__c>
            <Company__c>{Company}</Company__c>
            <Company_email__c>{CompanyEmail}</Company_email__c>
            <Signup_source__c>{Source}</Signup_source__c>
        </Portal_user__c>
    '''

    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)

# Get companies api call
def get_companies():
    url = DOMAIN_NAME + '/services/data/v60.0/query?q=SELECT+Id,Name,Phone__c,Street_name__c,House_number__c,Zip_Postal_code__c,State_Province__c+FROM+Company__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN
    }
    payload = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json().get("records", [])
    root = ET.Element("Companies")

    # Makes json data into xml data
    for record in data:
        user_element = ET.SubElement(root, "Company__c")
        for field, value in record.items():
            if field == "attributes":
                continue
            field_element = ET.SubElement(user_element, field)
            field_element.text = str(value)

    xml_string = ET.tostring(root, encoding="unicode", method="xml")
    print(xml_string)

# Add a company api call
def add_company(Name = None, Type = None, PhoneNo = None, Email = None, Street = None, HouseNumber = None, zip = None, Province = None):
    url = DOMAIN_NAME + '/services/data/v60.0/sobjects/Company__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = f'''
        <Company__c>
            <Name>{Name}</Name>
            <Type__c>{Type}</Type__c>
            <Phone__c>{PhoneNo}</Phone__c>
            <Contact_email__c>{Email}</Contact_email__c>
            <Street_name__c>{Street}</Street_name__c>
            <House_number__c>{HouseNumber}</House_number__c>
            <Zip_Postal_code__c>{zip}</Zip_Postal_code__c>
            <State_Province__c>{Province}</State_Province__c>
        </Company__c>
    '''

    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)

# Get talks api call
def get_talk():
    url = DOMAIN_NAME + '/services/data/v60.0/query?q=SELECT+Id,Name,Date__c+FROM+Talk__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN
    }
    payload = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json().get("records", [])
    root = ET.Element("Talks")

    # Makes json data into xml data
    for record in data:
        user_element = ET.SubElement(root, "Talk__c")
        for field, value in record.items():
            if field == "attributes":
                continue
            field_element = ET.SubElement(user_element, field)
            field_element.text = str(value)

    xml_string = ET.tostring(root, encoding="unicode", method="xml")
    print(xml_string)

# Add a talk api call
def add_talk(Name = None, Date = None):
    url = DOMAIN_NAME + '/services/data/v60.0/sobjects/Talk__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = f'''
        <Talk__c>
            <Name>{Name}</Name>
            <Date__c>{Date}</Date__c>
        </Talk__c>
    '''

    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)

# Get the attendances
def get_attendance():
    url = DOMAIN_NAME + '/services/data/v60.0/query?q=SELECT+Id,Name,Talk__c,Portal_user__c+FROM+talkAttendance__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN
    }
    payload = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json().get("records", [])
    root = ET.Element("Attendance")

    # Makes json data into xml data
    for record in data:
        user_element = ET.SubElement(root, "Attendance__c")
        for field, value in record.items():
            if field == "attributes":
                continue
            field_element = ET.SubElement(user_element, field)
            field_element.text = str(value)

    xml_string = ET.tostring(root, encoding="unicode", method="xml")
    print(xml_string)

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
    print(response.text)


if __name__ == '__main__':
    authenticate()
