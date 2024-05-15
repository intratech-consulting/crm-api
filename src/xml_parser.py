from datetime import datetime
import sys
import xml.etree.ElementTree as ET

sys.path.append('/app')
import config.secrets as secrets
from uuidapi import *

TEAM = 'crm'

def read_xml_user(variables, root):
    for child in root:
        if child.tag == "routing_key" or child.tag == "crud_operation":
            continue
        elif child.tag == "address":
            for address_field in child:
                variables[address_field.tag] = address_field.text
        elif child.tag == "id" or child.tag == "company_id":
            if(child.text is not None):
                variables[child.tag] = get_service_id(child.text, TEAM)
            else:
                variables[child.tag] = None
        else:
            variables[child.tag] = child.text

def write_xml_user(id, first_name, last_name, email, telephone, birthday, country, state, city, zip, street, house_number, company_email, company_id, source, user_role, invoice, calendar_link):
    return '''
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

def create_xml_user(data):
    user_data = data[0]
    root = ET.Element("user")

    address_element = None
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

    return ET.tostring(root, encoding="unicode", method="xml")

def read_xml_company(variables, root):
    for child in root:
        if child.tag == "routing_key" or child.tag == "crud_operation":
            continue
        elif child.tag == "id":
            variables[child.tag] = get_service_id(child.text, TEAM)
        elif child.tag == "address":
            for address_field in child:
                variables[address_field.tag] = address_field.text
        elif child.tag == "logo":
            pass
        else:
            variables[child.tag] = child.text

def write_xml_company(id, name, email, telephone, country, state, city, zip, street, house_number, type, invoice):
    return '''
        <company__c>
            {}
        </company__c>
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
                }.items() if value != '' and value != None
            ])
        )

def create_xml_company(data):
    company_data = data[0]
    root = ET.Element("company")

    address_element = None
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
            field_element = ET.SubElement(root, str(field_name).lower())
            field_element.text = "" if value == None else str(value).lower()

    return ET.tostring(root, encoding="unicode", method="xml")

def read_xml_event(variables, root):
    for child in root:
        if child.tag == "routing_key" or child.tag == "crud_operation":
            continue
        elif child.tag == "id":
            variables[child.tag] = get_service_id(child.text, TEAM)
        elif child.tag == "speaker":
            for speaker_field in child:
                variables[speaker_field.tag] = get_service_id(speaker_field.text, TEAM)
        else:
            variables[child.tag] = child.text

def write_xml_event(id, date, start_time, end_time, location, user_id, company_id, max_registrations, available_seats, description):
    return '''
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
            }.items() if value != '' and value != None
        ])
    )

def create_xml_event(data):
    event_data = data[0]
    root = ET.Element("event")

    speaker_element = None
    speaker_fields = ["user_id__c", "company_id__c"]

    for field, value in event_data.items():
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

    return ET.tostring(root, encoding="unicode", method="xml")

def read_xml_attendance(variables, root):
    for child in root:
        if child.tag == "routing_key" or child.tag == 'crud_operation':
            continue
        else:
            variables[child.tag] = get_service_id(child.text, TEAM)

def write_xml_attendance(id, user_id, event_id):
    return '''
    <attendance__c>
        {}
    </attendance__c>
    '''.format(
        ''.join([
            f'<{field}__c>{value}</{field}__c>'
            for field, value in {
                "user_id": user_id,
                "event_id": event_id,
            }.items() if value != '' and value != None
        ])
    )

def create_xml_attendance(data):
    event_data = data[0]
    root = ET.Element("attendance")

    for field, value in event_data.items():
        if field == "attributes":
            field_element = ET.SubElement(root, "routing_key")
            field_element.text = "attendance.crm"
            field_element = ET.SubElement(root, "crud_operation")
            field_element.text = "create"
        else:
            field_name = field.split("__")[0]
            field_element = ET.SubElement(root, str(field_name).lower())
            field_element.text = "" if value == None else str(value).lower()

    return ET.tostring(root, encoding="unicode", method="xml")

def read_xml_product(variables, root):
    for child in root:
        if child.tag == "routing_key" or child.tag == 'crud_operation':
            continue
        elif child.tag == "id":
            variables[child.tag] = get_service_id(child.text, TEAM)
        elif child.tag == "name":
            variables[child.tag] = child.text

def write_xml_product(id, name):
    return '''
    <product__c>
        {}
    </product__c>
    '''.format(
        ''.join([
            f'<{field}>{value}</{field}>'
            for field, value in {
                "Name": name,
            }.items() if value != '' and value != None
        ])
    )

def read_xml_order(variables, root):
    for child in root:
        if child.tag == "user_id":
            variables[child.tag] =  get_service_id(child.text.strip(), TEAM)
        elif child.tag == "products":
            products = []
            for product in child.findall('product'):
                product_details = {}
                for product_field in product:
                    if product_field.tag == "product_id":
                        product_details["product_id"] = get_service_id(product_field.text.strip(), TEAM)
                    elif product_field.tag == "amount":
                        product_details["amount"] = product_field.text.strip()
                products.append(product_details)
            variables["products"] = products

def write_xml_order(user_id, product_id, amount):
    return '''
    <order__c>
        {}
    </order__c>
    '''.format(
        ''.join([
            f'<{field}__c>{value}</{field}__c>'
            for field, value in {
                "user_id": user_id,
                "product_id": product_id,
                "amount": amount,
            }.items() if value != '' and value != None
        ])
    )

def write_xml_existing_order(amount):
    return '''
    <order__c>
        {}
    </order__c>
    '''.format(
        ''.join([
            f'<{field}__c>{value}</{field}__c>'
            for field, value in {
                "amount": amount,
            }.items() if value != '' and value != None
        ])
    )