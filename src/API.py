import requests
from datetime import datetime
import xml.etree.ElementTree as ET
import jwt
import sys

sys.path.append('/app')
import config.secrets as secrets


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
    global ACCESS_TOKEN
    ACCESS_TOKEN = response['access_token']


# Get an user by id api call
def get_user(user_id=None):
    url = secrets.DOMAIN_NAME + "/services/data/v60.0/query?q=SELECT+Id,first_name__c,last_name__c,email__c,telephone__c,birthday__c,country__c,state__c,city__c,zip__c,street__c,house_number__c,company_email__c,company_id__c,source__c,user_role__c,invoice__c,calendar_link__c+FROM+user__c+WHERE+Id+=+'" + user_id + "'"
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN
    }
    payload = {}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        data = response.json().get("records", [])
        if data:
            user_data = data[0]
            root = ET.Element("user")

            address_element = None  # Initialize address_element to None

            address_fields = ["country__c", "state__c", "city__c", "zip__c", "street__c", "house_number__c"]

            for field, value in user_data.items():
                if field == "attributes":
                    field_element = ET.SubElement(root, "routing_key")
                    field_element.text = "user.crm"
                elif field == "Id":
                    field_element = ET.SubElement(root, "user_id")
                    field_element.text = str(value)
                elif field == "birthday__c":
                    field_element = ET.SubElement(root, "birthday")
                    field_element.text = str(value)
                    address_element = ET.SubElement(root, "address")
                elif field in address_fields and address_element is not None:
                    sub_field = field.split("__")[0]
                    sub_field_element = ET.SubElement(address_element, sub_field)
                    sub_field_element.text = str(value)
                else:
                    field_name = field.split("__")[0]
                    field_element = ET.SubElement(root, field_name)
                    field_element.text = str(value)

            xml_string = ET.tostring(root, encoding="unicode", method="xml")
            # logger.info("get users: " + xml_string)
            return xml_string
        else:
            print("No user found with this id: " + user_id)
            return None
    except Exception as e:
        print("Error fetching users from Salesforce:", e)
        return None


# Add an user api call
def add_user(user_id, first_name, last_name, email, telephone, birthday, country, state, city, zip, street,
             house_number, company_email="", company_id="", source="", user_role="Customer", invoice="Yes", calendar_link=""):

    required_fields = {
        'user_id': user_id,
        'first_name': first_name,
        'last_name': last_name,
        'email': email,
    }

    #check_required_fields(required_fields, user_id=user_id, first_name=first_name, last_name=last_name, email=email)

    url = secrets.DOMAIN_NAME + '/services/data/v60.0/sobjects/user__c'
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
        <calendar_link__c>{calendar_link}</calendar_link__c>
    </user__c>
    '''

    response = requests.request("POST", url, headers=headers, data=payload)
    print(response)
    # logger.info("add user" + response.text)


# Get a company by id api call
def get_company(company_id=None):
    url = secrets.DOMAIN_NAME + "/services/data/v60.0/query?q=SELECT+Id,Name,email__c,telephone__c,country__c,state__c,city__c,zip__c,street__c,house_number__c,type__c,invoice__c+FROM+Company__c+WHERE+Id+=+'" + company_id + "'"
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN
    }
    payload = {}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        data = response.json().get("records", [])
        if data:
            company_data = data[0]
            root = ET.Element("company")

            address_element = None  # Initialize address_element to None

            address_fields = ["country__c", "state__c", "city__c", "zip__c", "street__c", "house_number__c"]

            for field, value in company_data.items():
                if field == "attributes":
                    field_element = ET.SubElement(root, "routing_key")
                    field_element.text = "company.crm"
                elif field == "telephone__c":
                    field_element = ET.SubElement(root, "telephone")
                    field_element.text = str(value)
                    field_element = ET.SubElement(root, "logo")
                    field_element.text = ""
                    address_element = ET.SubElement(root, "address")
                elif field in address_fields and address_element is not None:
                    sub_field = field.split("__")[0]
                    sub_field_element = ET.SubElement(address_element, sub_field)
                    sub_field_element.text = str(value)
                else:
                    field_name = field.split("__")[0]
                    field_element = ET.SubElement(root, str(field_name).lower())
                    field_element.text = str(value)

            xml_string = ET.tostring(root, encoding="unicode", method="xml")
            # logger.info("get company: " + xml_string)
            return xml_string
        else:
            print("No company found with this id: " + company_id)
            return None
    except Exception as e:
            print("Error fetching companies from Salesforce:", e)
            return None


# Add a company api call
def add_company(id, name, email, telephone, country, state, city, zip, street, house_number, type, invoice):
    required_fields = {
        'id': id,
        'name': name,
        'email': email
    }

    #check_required_fields(required_fields, id=id, name=first, email=email)

    url = secrets.DOMAIN_NAME + '/services/data/v60.0/sobjects/Company__c'
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
    # logger.info("add company" + response.text)
    print(response.text)


# Get a event by id api call
def get_event(event_id=None):
    url = secrets.DOMAIN_NAME + "/services/data/v60.0/query?q=SELECT+Id,date__c,start_time__c,end_time__c,location__c,user_id__c,company_id__c,max_registrations__c,available_seats__c,description__c+FROM+event__c+WHERE+Id+=+'" + event_id + "'"
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN
    }
    payload = {}
    print(url)
    print(ACCESS_TOKEN)
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        data = response.json().get("records", [])
        if data:
            event_data = data[0]
            root = ET.Element("event")

            speaker_element = None  # Initialize speaker_element to None
            speaker_fields = ["user_id__c", "company_id__c"]

            for field, value in event_data.items():
                print(value, field)
                if field == "attributes":
                    field_element = ET.SubElement(root, "routing_key")
                    field_element.text = "event.crm"
                elif field == "location__c":
                    field_element = ET.SubElement(root, "location")
                    field_element.text = str(value)
                    speaker_element = ET.SubElement(root, "speaker")
                elif field in speaker_fields and speaker_element is not None:
                    sub_field = field.split("__")[0]
                    sub_field_element = ET.SubElement(speaker_element, sub_field)
                    sub_field_element.text = str(value)
                elif field == "max_registrations__c" or field == "available_seats__c":
                    field_element = ET.SubElement(root, field.split("__")[0])
                    field_element.text = str(int(value))
                else:
                    field_name = field.split("__")[0]
                    field_element = ET.SubElement(root, str(field_name).lower())
                    field_element.text = str(value)

            xml_string = ET.tostring(root, encoding="unicode", method="xml")
            print(xml_string)
            # logger.info("get company: " + xml_string)
            return xml_string
        else:
            print("No event found with this id: " + event_id)
            return None
    except Exception as e:
        print("Error fetching event from Salesforce:", e)
        return None


# Add an event api call
def add_event(id, date, start_time, end_time, location, user_id, company_id, max_registrations, available_seats, description):
    required_fields = {
        'date': date,
        'start_time': start_time,
        'end_time': end_time,
        'available_seats': available_seats
    }
    print("inside api")
    #check_required_fields(required_fields, date=date, start_time=start_time, end_time=end_time, available_seats=available_seats)

    url = secrets.DOMAIN_NAME + '/services/data/v60.0/sobjects/event__c'
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
            <location__c>{location}</location__c>
            <user_id__c>{user_id}</user_id__c>
            <company_id__c>{company_id}</company_id__c>
            <max_registrations__c>{str(int(max_registrations))}</max_registrations__c>
            <available_seats__c>{str(int(available_seats))}</available_seats__c>
            <description__c>{description}</description__c>
        </event__c>
    '''
    print(payload)
    response = requests.request("POST", url, headers=headers, data=payload)
    # logger.info("add talk" + response.text)
    print(response)


# Get the attendances
def get_attendance():
    url = secrets.DOMAIN_NAME + '/services/data/v60.0/query?q=SELECT+Id,Name,Talk__c,Portal_user__c+FROM+talkAttendance__c'
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
def add_attendance(user_id=None, event_id=None):
    url = secrets.DOMAIN_NAME + '/services/data/v60.0/sobjects/attendance__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = f'''
        <attendance__c>
            <user_id__c>{user_id}</user_id__c>
            <event_id__c>{event_id}</event_id__c>
        </attendance__c>
    '''

    response = requests.request("POST", url, headers=headers, data=payload)
    # logger.info("add attendance" + response.text)


# Add a product
def add_product(name):
    url = secrets.DOMAIN_NAME + '/services/data/v60.0/sobjects/product__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = f'''
        <product__c>
            <Name>{name}</Name>
        </product__c>
    '''

    response = requests.request("POST", url, headers=headers, data=payload)
    # logger.info("add product" + response.text)
    return response.json().get("id", None)




# Returns product id if exists
def product_exists(id):
    print('IN HERE', id)
    url = secrets.DOMAIN_NAME + f'/services/data/v60.0/query?q=SELECT+Id+FROM+product__c+WHERE+Id=\'{id}\''
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = {}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("records", [])

        if data:
            return True
        else:
            return False
    except Exception as e:
        print("Error fetching product from Salesforce:", e)
        return False


# Add an order
def add_order(user_id, product, amount):
    url = secrets.DOMAIN_NAME + '/services/data/v60.0/sobjects/order__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = f'''
        <order__c>
            <user_id__c>{user_id}</user_id__c>
            <product__c>{product}</product__c>
            <amount__c>{amount}</amount__c>
        </order__c>
    '''

    response = requests.request("POST", url, headers=headers, data=payload)
    # logger.info("add order" + response.text)


def update_order(order_id, amount):
    url = secrets.DOMAIN_NAME + f'/services/data/v60.0/sobjects/order__c/{order_id}'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = f'''
        <order__c>
            <amount__c>{amount}</amount__c>
        </order__c>
    '''
    response = requests.request("PATCH", url, headers=headers, data=payload)
    # logger.info("add order" + response.text)


# Get order to change amount
def get_order(user_id, product):
    url = secrets.DOMAIN_NAME + f'/services/data/v60.0/query?q=SELECT+Id,amount__c+FROM+order__c+WHERE+user_id__c=\'{user_id}\'AND+product__c=\'{product}\''
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = {}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("records", [])
        print(data)

        if data:
            return data[0]['Id'], data[0]['amount__c']
        else:
            return None, None
    except Exception as e:
        print("Error fetching order from Salesforce:", e)
        return None, None


# Get changedSalesforce data
def get_changed_data():
    url = secrets.DOMAIN_NAME + '/services/data/v60.0/query?q=SELECT+Id,Name,object_type__c+FROM+changed_object__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = {}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("records", [])

        root = ET.Element("ChangedData")

        # Makes json data into xml data
        for record in data:
            changed_element = ET.SubElement(root, "ChangedObject__c")
            for field, value in record.items():
                if field == "attributes":
                    continue
                field_element = ET.SubElement(changed_element, field)
                field_element.text = str(value)

        xml_string = ET.tostring(root, encoding="unicode", method="xml")
        return xml_string
    except Exception as e:
        print("Error fetching changed data from Salesforce:", e)
        return None


def check_required_fields(required_fields, **kwargs):
    for field_name in required_fields:
        if field_name not in kwargs or not kwargs[field_name] or kwargs[field_name].isspace():
            raise ValueError(f"{field_name} cannot be empty or just spaces")


# Delete Change Object api call
def delete_change_object(id=None):
    url = secrets.DOMAIN_NAME + f'/services/data/v60.0/sobjects/changed_object__c/{id}'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN
    }
    payload = {}

    try:
        response = requests.delete(url, headers=headers)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
    except Exception as e:
        print("Error deleting user from Salesforce:", e)
        return None

authenticate()
get_company('a03Qy000004cOQUIA2')