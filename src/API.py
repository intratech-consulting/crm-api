import logging
import colorlog
import requests
from datetime import datetime
import xml.etree.ElementTree as ET
import jwt
import sys

sys.path.append('/app')
import config.secrets as secrets

def initialize_logger(logger):
    # Set level for the logger
    logger.setLevel(logging.DEBUG)

    # Create a color formatter
    formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s:%(levelname)s:%(name)s:%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'green',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'red,bg_white',
        },
    )

    # Create a stream handler and set the formatter
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

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
    logger.info("Authenticated successfully")


# Get an user by id api call
def get_new_user(user_id=None):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+Id,first_name__c,last_name__c,email__c,telephone__c,birthday__c,country__c,state__c,city__c,zip__c,street__c,house_number__c,company_email__c,company_id__c,source__c,user_role__c,invoice__c,calendar_link__c+FROM+user__c+WHERE+Id+=+\'{user_id}\''
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
                    field_element = ET.SubElement(root, "crud_operation")
                    field_element.text = "create"
                elif field == "Id":
                    field_element = ET.SubElement(root, "id")
                    field_element.text = "" if value == None else str(value)
                elif field == "birthday__c":
                    field_element = ET.SubElement(root, "birthday")
                    field_element.text = "" if value == None else str(value).lower()
                    address_element = ET.SubElement(root, "address")
                elif field == "house_number__c" or field == "zip__c":
                    sub_field = field.split("__")[0]
                    sub_field_element = ET.SubElement(address_element, sub_field)
                    sub_field_element.text = "" if value == None else str(int(value))
                elif field in address_fields and address_element is not None:
                    sub_field = field.split("__")[0]
                    sub_field_element = ET.SubElement(address_element, sub_field)
                    sub_field_element.text = "" if value == None else str(value).lower()
                else:
                    field_name = field.split("__")[0]
                    field_element = ET.SubElement(root, field_name)
                    field_element.text = "" if value == None else str(value).lower()

            xml_string = ET.tostring(root, encoding="unicode", method="xml")
            logger.debug(f"Response: {response};\nData: {data};\nXML: {xml_string}")
            return xml_string
        else:
            logger.error(f"No user found with this id: {user_id}")
            return None
    except Exception as e:
        logger.error(f"Error fetching users from Salesforce: {e}")
        return None


# Add an user api call
def add_user(id, first_name, last_name, email, telephone, birthday, country, state, city, zip, street,
             house_number, company_email="", company_id="", source="", user_role="Customer", invoice="Yes", calendar_link=""):
    url = secrets.DOMAIN_NAME + 'sobjects/user__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }

    payload = '''
    <user__c>
        {}
    </user__c>
    '''.format(
        ''.join([
            f'<{field}__c>{value}</{field}__c>'
            for field, value in {
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "telephone": telephone,
                "birthday": birthday,
                "country": country,
                "state": state,
                "city": city,
                "zip": zip,
                "street": street,
                "house_number": house_number,
                "company_email": company_email,
                "company_id": company_id,
                "source": source,
                "user_role": user_role,
                "invoice": invoice,
                "calendar_link": calendar_link,
            }.items() if value != '' and value != None
        ])
    )

    logger.debug(f"Payload: {payload};")

    response = requests.post(url, headers=headers, data=payload)
    logger.debug(f"Response: {response};")
    return response.json().get('id', None)

# Update an user api call
def update_user(id, first_name, last_name, email, telephone, birthday, country, state, city, zip, street,
             house_number, company_email, company_id, source, user_role, invoice, calendar_link):
    print("update user: " + id)
    url = secrets.DOMAIN_NAME + f'sobjects/user__c/{id}'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = '''
    <user__c>
        {}
    </user__c>
    '''.format(
        ''.join([
            f'<{field}__c>{value}</{field}__c>'
            for field, value in {
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "telephone": telephone,
                "birthday": birthday,
                "country": country,
                "state": state,
                "city": city,
                "zip": zip,
                "street": street,
                "house_number": house_number,
                "company_email": company_email,
                "company_id": company_id,
                "source": source,
                "user_role": user_role,
                "invoice": invoice,
                "calendar_link": calendar_link,
            }.items() if value != ''
        ])
    )

    response = requests.patch(url, headers=headers, data=payload)
    logger.debug(f"Response: {response};")

# Delete an user api call
def delete_user(user_id):
    url = secrets.DOMAIN_NAME + f'sobjects/user__c/{user_id}'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }

    payload = {}

    response = requests.delete(url, headers=headers, data=payload)
    logger.debug(f"Response: {response};")


# Get a company by id api call
def get_new_company(company_id=None):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+Id,Name,email__c,telephone__c,country__c,state__c,city__c,zip__c,street__c,house_number__c,type__c,invoice__c+FROM+Company__c+WHERE+Id+=+\'{company_id}\''
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
                    field_element = ET.SubElement(root, "crud_operation")
                    field_element.text = "create"
                elif field == "Id":
                    field_element = ET.SubElement(root, "id")
                    field_element.text = "" if value == None else str(value)
                elif field == "telephone__c":
                    field_element = ET.SubElement(root, "telephone")
                    field_element.text = "" if value == None else str(value).lower()
                    field_element = ET.SubElement(root, "logo")
                    field_element.text = ""
                    address_element = ET.SubElement(root, "address")
                elif field == "house_number__c":
                    field_element = ET.SubElement(address_element, "house_number")
                    field_element.text = "" if value == None else str(int(value))
                elif field in address_fields and address_element is not None:
                    sub_field = field.split("__")[0]
                    sub_field_element = ET.SubElement(address_element, sub_field)
                    sub_field_element.text = "" if value == None else str(value).lower()
                else:
                    field_name = field.split("__")[0]
                    field_element = ET.SubElement(root, str(field_name).lower())
                    field_element.text = "" if value == None else str(value).lower()

            xml_string = ET.tostring(root, encoding="unicode", method="xml")
            logger.debug(f"Response: {response};\nData: {data};\nXML: {xml_string}")
            return xml_string
        else:
            logger.error(f"No company found with this id: {company_id}")
            return None
    except Exception as e:
            logger.error(f"Error fetching companies from Salesforce: {e}")
            return None


# Add a company api call
def add_company(id, name, email, telephone, country, state, city, zip, street, house_number, type, invoice):
    url = secrets.DOMAIN_NAME + f'sobjects/Company__c'
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

    response = requests.post(url, headers=headers, data=payload)
    logger.debug(f"Response: {response};")
    return response.json().get('id', None)

# Update a company api call
def update_company(id, name, email, telephone, country, state, city, zip, street, house_number, type, invoice):
    url = secrets.DOMAIN_NAME + f'sobjects/Company__c/{id}'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = '''
    <Company__c>
        {}
    </Company__c>
    '''.format(
        ''.join([
            f'<{field}__c>{value}</{field}__c>'
            for field, value in {
                "name": name,
                "email": email,
                "telephone": telephone,
                "country": country,
                "state": state,
                "city": city,
                "zip": zip,
                "street": street,
                "house_number": house_number,
                "type": type,
                "invoice": invoice,
            }.items() if value != ''
        ])
    )


    response = requests.patch(url, headers=headers, data=payload)
    logger.debug(f"Response: {response};")



# Delete a company api call
def delete_company(company_id):
    url = secrets.DOMAIN_NAME + f'sobjects/company__c/{company_id}'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }

    payload = {}

    response = requests.delete(url, headers=headers, data=payload)
    logger.debug(f"Response: {response};")


# Get a event by id api call
def get_new_event(event_id=None):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+Id,date__c,start_time__c,end_time__c,location__c,user_id__c,company_id__c,max_registrations__c,available_seats__c,description__c+FROM+event__c+WHERE+Id+=+\'{event_id}\''
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN
    }
    payload = {}
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
                    field_element = ET.SubElement(root, "crud_operation")
                    field_element.text = "create"
                elif field == "location__c":
                    field_element = ET.SubElement(root, "location")
                    field_element.text = "" if value == None else str(value).lower()
                    speaker_element = ET.SubElement(root, "speaker")
                elif field in speaker_fields and speaker_element is not None:
                    sub_field = field.split("__")[0]
                    sub_field_element = ET.SubElement(speaker_element, sub_field)
                    sub_field_element.text = "" if value == None else str(value).lower()
                elif field == "max_registrations__c" or field == "available_seats__c":
                    field_element = ET.SubElement(root, field.split("__")[0])
                    field_element.text = "" if value == None else str(int(value))
                else:
                    field_name = field.split("__")[0]
                    field_element = ET.SubElement(root, str(field_name).lower())
                    field_element.text = "" if value == None else str(value).lower()

            xml_string = ET.tostring(root, encoding="unicode", method="xml")
            logger.debug(f"Response: {response};\nData: {data};\nXML: {xml_string}")
            return xml_string
        else:
            logger.error(f"No event found with this id: {event_id}")
            return None
    except Exception as e:
        logger.error(f"Error fetching event from Salesforce: {e}")
        return None


# Add an event api call
def add_event(id, date, start_time, end_time, location, user_id, company_id, max_registrations, available_seats, description):
    url = secrets.DOMAIN_NAME + f'sobjects/event__c'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }

    payload = '''
    <event__c>
        {}
    </event__c>
    '''.format(
        ''.join([
            f'<{field}__c>{value}</{field}__c>'
            for field, value in {
                "date": date,
                "start_time": "" if start_time == None else datetime.strptime(start_time, '%H:%M:%S').strftime('%H:%M:%S'),
                "end_time": "" if end_time == None else datetime.strptime(end_time, '%H:%M:%S').strftime('%H:%M:%S'),
                "location": location,
                "user_id": user_id,
                "company_id": company_id,
                "max_registrations": max_registrations,
                "available_seats": available_seats,
                "description": description,
            }.items() if value != ''
        ])
    )

    response = requests.post(url, headers=headers, data=payload)
    logger.debug(f"Response: {response};")
    return response.json().get('id', None)

# Update an event api call
def update_event(id, date, start_time, end_time, location, user_id, company_id, max_registrations, available_seats, description):
    url = secrets.DOMAIN_NAME + f'sobjects/event__c/{id}'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = '''
    <event__c>
        {}
    </event__c>
    '''.format(
        ''.join([
            f'<{field}__c>{value}</{field}__c>'
            for field, value in {
                "date": date,
                "start_time": datetime.strptime(start_time, '%H:%M:%S').strftime('%H:%M:%S') if start_time else "",
                "end_time": datetime.strptime(end_time, '%H:%M:%S').strftime('%H:%M:%S') if end_time else "",
                "location": location,
                "user_id": user_id,
                "company_id": company_id,
                "max_registrations": max_registrations,
                "available_seats": available_seats,
                "description": description,
            }.items() if value != ''
        ])
    )
    response = requests.patch(url, headers=headers, data=payload)
    logger.debug(f"Response: {response};")

# Delete an event api call
def delete_event(event_id):
    url = secrets.DOMAIN_NAME + f'sobjects/event__c/{event_id}'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }

    payload = {}

    response = requests.delete(url, headers=headers, data=payload)
    logger.debug(f"Response: {response};")

# Get the attendances
def get_new_attendance(attendance_id=None):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+Id,user_id__c,event_id__c+FROM+attendance__c+WHERE+Id+=\'{attendance_id}\''
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN
    }
    payload = {}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        data = response.json().get("records", [])
        if data:
            event_data = data[0]
            root = ET.Element("attendance")

            for field, value in event_data.items():
                print(value, field)
                if field == "attributes":
                    field_element = ET.SubElement(root, "routing_key")
                    field_element.text = "attendance.crm"
                    field_element = ET.SubElement(root, "crud_operation")
                    field_element.text = "create"
                else:
                    field_name = field.split("__")[0]
                    field_element = ET.SubElement(root, str(field_name).lower())
                    field_element.text = "" if value == None else str(value).lower()

            xml_string = ET.tostring(root, encoding="unicode", method="xml")
            logger.debug(f"Response: {response};\nData: {data};\nXML: {xml_string}")
            return xml_string
        else:
            logger.error(f"No attendance found with this id: {attendance_id}")
            return None
    except Exception as e:
        logger.error(f"Error fetching attendance from Salesforce: {e}")
        return None


# Add an attendance
def add_attendance(user_id=None, event_id=None):
    url = secrets.DOMAIN_NAME + f'sobjects/attendance__c'
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

    response = requests.post(url, headers=headers, data=payload)
    logger.debug(f"Response: {response};")
    return response.json().get('id', None)

# Update an attendance
def update_attendance(id=None, user_id=None, event_id=None):
    url = secrets.DOMAIN_NAME + f'sobjects/attendance__c/{id}'
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

    response = requests.patch(url, headers=headers, data=payload)
    logger.debug(f"Response: {response};")

# Delete an attendance api call
def delete_attendance(attendance_id):
    url = secrets.DOMAIN_NAME + f'sobjects/attendance__c/{attendance_id}'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }

    payload = {}

    response = requests.delete(url, headers=headers, data=payload)
    logger.debug(f"Response: {response};")


# Add a product
def add_product(name):
    url = secrets.DOMAIN_NAME + f'sobjects/product__c'
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
    logger.debug(f"Response: {response};")
    return response.json().get("id", None)




# Returns product id if exists
def product_exists(id):
    print('IN HERE', id)
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+Id+FROM+product__c+WHERE+Id=\'{id}\''
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
    url = secrets.DOMAIN_NAME + f'sobjects/order__c'
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

    response = requests.post(url, headers=headers, data=payload)
    logger.debug(f"Response: {response};")


def update_order(order_id, amount):
    url = secrets.DOMAIN_NAME + f'sobjects/order__c/{order_id}'
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
    logger.debug(f"Response: {response};")


# Get order to change amount
def get_order(user_id, product):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+Id,amount__c+FROM+order__c+WHERE+user_id__c=\'{user_id}\'AND+product__c=\'{product}\''
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
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+Id,Name,object_type__c,crud__c+FROM+changed_object__c'
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
        logger.debug(f"Response: {response};\nData: {data};\nXML: {xml_string}")
        return xml_string
    except Exception as e:
        print("Error fetching changed data from Salesforce:", e)
        return None

# Get updated user fields from changedSalesforce data
def get_updated_user(id=None):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+first_name__c,last_name__c,email__c,telephone__c,birthday__c,country__c,state__c,city__c,zip__c,street__c,house_number__c,company_email__c,company_id__c,source__c,user_role__c,invoice__c,calendar_link__c+FROM+changed_object__c+WHERE+Id=\'{id}\''
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = {}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("records", [])
        changed_data = [key for obj in data for key, value in obj.items() if isinstance(value, bool) and value]
        logger.debug(f"Response: {response};\nData: {data}")
        return changed_data
    except Exception as e:
        print("Error fetching changed data from Salesforce:", e)
        return None

# Get updated company fields from changedSalesforce data
def get_updated_company(id=None):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+name__c,email__c,telephone__c,country__c,state__c,city__c,zip__c,street__c,house_number__c,type__c,invoice__c+FROM+changed_object__c+WHERE+Id=\'{id}\''
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = {}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("records", [])
        changed_data = [key for obj in data for key, value in obj.items() if isinstance(value, bool) and value]
        logger.debug(f"Response: {response};\nData: {data}")
        return changed_data
    except Exception as e:
        print("Error fetching changed data from Salesforce:", e)
        return None

# Get updated event fields from changedSalesforce data
def get_updated_event(id=None):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+date__c,start_time__c,end_time__c,location__c,user_id__c,company_id__c,max_registrations__c,available_seats__c,description__c+FROM+changed_object__c+WHERE+Id=\'{id}\''
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = {}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("records", [])
        changed_data = [key for obj in data for key, value in obj.items() if isinstance(value, bool) and value]
        logger.debug(f"Response: {response};\nData: {data}")
        return changed_data
    except Exception as e:
        print("Error fetching changed data from Salesforce:", e)
        return None

# Get updated attendance fields from changedSalesforce data
def get_updated_attendance(id=None):
    url = secrets.DOMAIN_NAME + f'query?q=SELECT+user_id__c,event_id__c+FROM+changed_object__c+WHERE+Id=\'{id}\''
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN,
        'Content-Type': 'application/xml'
    }
    payload = {}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("records", [])
        changed_data = [key for obj in data for key, value in obj.items() if isinstance(value, bool) and value]
        logger.debug(f"Response: {response};\nData: {data}")
        return changed_data
    except Exception as e:
        print("Error fetching changed data from Salesforce:", e)
        return None

# Get updated values
def get_updated_values(query=None):
    url = secrets.DOMAIN_NAME + query
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN
    }
    payload = {}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        data = response.json().get("records", [])
        logger.debug(f"Response: {response};\nData: {data}")
        return data[0]
    except Exception as e:
        print("Error fetching users from Salesforce:", e)
        return None

# Delete Change Object api call
def delete_change_object(id=None):
    url = secrets.DOMAIN_NAME + f'sobjects/changed_object__c/{id}'
    headers = {
        'Authorization': 'Bearer ' + ACCESS_TOKEN
    }
    payload = {}

    try:
        response = requests.delete(url, headers=headers)
        response.raise_for_status()
        logger.debug(f"Response: {response}")
    except Exception as e:
        print("Error deleting user from Salesforce:", e)
        return None
    
# Create a custom logger
logger = colorlog.getLogger(__name__)
initialize_logger(logger)