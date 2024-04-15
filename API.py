import subprocess
import re
import requests
from datetime import datetime
import xml.etree.ElementTree as ET

ACCESS_TOKEN = ''
DOMAIN_NAME = ''

# Get the access token and domain name
def authenticate():
    global ACCESS_TOKEN, DOMAIN_NAME
    COMMAND = 'sf org display --target-org admin@ehb.be'
    response = subprocess.run(COMMAND, shell=True, check=True, capture_output=True, text=True)
    output = response.stdout

    access_token_match = re.search(r'Access Token\s+(.*)', output)
    if access_token_match:
        ACCESS_TOKEN = access_token_match.group(1).strip()

    domain_match = re.search(r'Instance Url\s+(.*)', output)
    if domain_match:
        DOMAIN_NAME = domain_match.group(1).strip()

    print("Domain Name:", DOMAIN_NAME)
    print("Access Token:", ACCESS_TOKEN)

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
