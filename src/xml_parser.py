import os
from datetime import datetime
import sys
import xml.etree.ElementTree as ET

if os.path.isdir('/app'):
    sys.path.append('/app')
else:
    local_dir = os.path.dirname(os.path.realpath(__file__))
    sys.path.append(local_dir)
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
            if (child.text is not None):
                variables[child.tag] = get_service_id(child.text, TEAM)
            else:
                variables[child.tag] = None
        else:
            variables[child.tag] = child.text


def write_xml_user(id, first_name, last_name, email, telephone, birthday, country, state, city, zip, street,
                   house_number, company_email, company_id, source, user_role, invoice, calendar_link):
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
            field_element.text = "" if value == None else create_master_uuid(str(value), TEAM)
        elif field == "company_id__c":
            field_element = ET.SubElement(root, "company_id")
            field_element.text = "" if value == None else get_master_uuid(str(value), TEAM)
        elif field == "birthday__c":
            field_element = ET.SubElement(root, "birthday")
            field_element.text = "" if value == None else str(value)
            address_element = ET.SubElement(root, "address")
        elif field == "house_number__c" or field == "zip__c":
            sub_field = field.split("__")[0]
            sub_field_element = ET.SubElement(address_element, sub_field)
            sub_field_element.text = "" if value == None else str(int(value))
        elif field in address_fields and address_element is not None:
            sub_field = field.split("__")[0]
            sub_field_element = ET.SubElement(address_element, sub_field)
            sub_field_element.text = "" if value == None else str(value)
        else:
            field_name = field.split("__")[0]
            field_element = ET.SubElement(root, field_name)
            field_element.text = "" if value == None else str(value)

    return ET.tostring(root, encoding="unicode", method="xml")


def update_xml_user(rc, crud, id, updated_values):
    master_id = get_master_uuid(id, TEAM)
    if crud == 'delete':
        delete_service_id(master_id, TEAM)
    first_name__c = updated_values.get('first_name__c', '')
    last_name__c = updated_values.get('last_name__c', '')
    email__c = updated_values.get('email__c', '')
    telephone__c = updated_values.get('telephone__c', '')
    birthday__c = updated_values.get('birthday__c', '')
    country__c = updated_values.get('country__c', '')
    state__c = updated_values.get('state__c', '')
    city__c = updated_values.get('city__c', '')
    zip__c = updated_values.get('zip__c', '')
    if zip__c != '':
        zip__c = str(int(zip__c))
    street__c = updated_values.get('street__c', '')
    house_number__c = updated_values.get('house_number__c', '')
    if house_number__c != '':
        house_number__c = str(int(house_number__c))
    company_email__c = updated_values.get('company_email__c', '')
    company_id__c = updated_values.get('company_id__c', '')
    if company_id__c != '':
        company_id__c = get_master_uuid(company_id__c, TEAM)
    source__c = updated_values.get('source__c', '')
    user_role__c = updated_values.get('user_role__c', '')
    invoice__c = updated_values.get('invoice__c', '')
    calendar_link__c = updated_values.get('calendar_link__c', '')

    return f'''
    <user>
        <routing_key>{rc}</routing_key>
        <crud_operation>{crud}</crud_operation>
        <id>{master_id}</id>
        <first_name>{first_name__c}</first_name>
        <last_name>{last_name__c}</last_name>
        <email>{email__c}</email>
        <telephone>{telephone__c}</telephone>
        <birthday>{birthday__c}</birthday>
        <address>
            <country>{country__c}</country>
            <state>{state__c}</state>
            <city>{city__c}</city>
            <zip>{zip__c}</zip>
            <street>{street__c}</street>
            <house_number>{house_number__c}</house_number>
        </address>
        <company_email>{company_email__c}</company_email>
        <company_id>{company_id__c}</company_id>
        <source>{source__c}</source>
        <user_role>{user_role__c}</user_role>
        <invoice>{invoice__c}</invoice>
        <calendar_link>{calendar_link__c}</calendar_link>
    </user>'''



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
            field_element.text = "" if value == None else create_master_uuid(str(value), TEAM)
        elif field == "Name":
            field_element = ET.SubElement(root, "name")
            field_element.text = "" if value == None else str(value)
        elif field == "telephone__c":
            field_element = ET.SubElement(root, "telephone")
            field_element.text = "" if value == None else str(value)
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
            sub_field_element.text = "" if value == None else str(value)
        else:
            field_name = field.split("__")[0]
            field_element = ET.SubElement(root, str(field_name))
            field_element.text = "" if value == None else str(value)

    return ET.tostring(root, encoding="unicode", method="xml")


def update_xml_company(rc, crud, id, updated_values):
    master_id = get_master_uuid(id, TEAM)
    if crud == 'delete':
        delete_service_id(master_id, TEAM)
    name__c = updated_values.get('Name', '')
    email__c = updated_values.get('email__c', '')
    telephone__c = updated_values.get('telephone__c', '')
    country__c = updated_values.get('country__c', '')
    state__c = updated_values.get('state__c', '')
    city__c = updated_values.get('city__c', '')
    zip__c = updated_values.get('zip__c', '')
    if zip__c != '':
        zip__c = str(int(zip__c))
    street__c = updated_values.get('street__c', '')
    house_number__c = updated_values.get('house_number__c', '')
    if house_number__c != '':
        house_number__c = str(int(house_number__c))
    type__c = updated_values.get('type__c', '')
    invoice__c = updated_values.get('invoice__c', '')

    return f'''
    <company>
        <routing_key>{rc}</routing_key>
        <crud_operation>{crud}</crud_operation>
        <id>{master_id}</id>
        <name>{name__c}</name>
        <email>{email__c}</email>
        <telephone>{telephone__c}</telephone>
        <logo></logo>
        <address>
            <country>{country__c}</country>
            <state>{state__c}</state>
            <city>{city__c}</city>
            <zip>{zip__c}</zip>
            <street>{street__c}</street>
            <house_number>{house_number__c}</house_number>
        </address>
        <type>{type__c}</type>
        <invoice>{invoice__c}</invoice>
    </company>'''


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


def write_xml_event(id, title, date, start_time, end_time, location, user_id, company_id, max_registrations, available_seats,
                    description):
    return '''
    <event__c>
        {}
    </event__c>
    '''.format(
        ''.join([
            f'<{field}__c>{value}</{field}__c>'
            for field, value in {
                "title": title,
                "date": date,
                "start_time": "" if start_time == None else datetime.strptime(start_time, '%H:%M:%S').strftime(
                    '%H:%M:%S'),
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
        elif field == "Id":
            field_element = ET.SubElement(root, "id")
            field_element.text = "" if value == None else create_master_uuid(str(value), TEAM)
        elif field == "start_time__c" or field == "end_time__c":
            field_element = ET.SubElement(root, field.split("__")[0])
            field_element.text = "" if value == None else str(value).split(".")[0]
        elif field == "location__c":
            field_element = ET.SubElement(root, "location")
            field_element.text = "" if value == None else str(value)
            speaker_element = ET.SubElement(root, "speaker")
        elif field in speaker_fields and speaker_element is not None:
            sub_field = field.split("__")[0]
            sub_field_element = ET.SubElement(speaker_element, sub_field)
            sub_field_element.text = "" if value == None else get_master_uuid(str(value), TEAM)
        elif field == "max_registrations__c" or field == "available_seats__c":
            field_element = ET.SubElement(root, field.split("__")[0])
            field_element.text = "" if value == None else str(int(value))
        else:
            field_name = field.split("__")[0]
            field_element = ET.SubElement(root, str(field_name))
            field_element.text = "" if value == None else str(value)

    return ET.tostring(root, encoding="unicode", method="xml")


def update_xml_event(rc, crud, id, updated_values):
    master_id = get_master_uuid(id, TEAM)
    if crud == 'delete':
        delete_service_id(master_id, TEAM)
    title__c = updated_values.get('title__c', '')
    date__c = updated_values.get('date__c', '')
    start_time__c = updated_values.get('start_time__c', '')
    if start_time__c != '':
        start_time__c = start_time__c.split(".")[0]
    end_time__c = updated_values.get('end_time__c', '')
    if end_time__c != '':
        end_time__c = end_time__c.split(".")[0]
    location__c = updated_values.get('location__c', '')
    user_id__c = updated_values.get('user_id__c', '')
    if user_id__c != '':
        user_id__c = get_master_uuid(user_id__c, TEAM)
    company_id__c = updated_values.get('company_id__c', '')
    if company_id__c != '':
        company_id__c = get_master_uuid(company_id__c, TEAM)
    max_registrations__c = updated_values.get('max_registrations__c', '')
    if max_registrations__c != '':
        max_registrations__c = str(int(max_registrations__c))
    available_seats__c = updated_values.get('available_seats__c', '')
    if available_seats__c != '':
        available_seats__c = str(int(available_seats__c))
    description__c = updated_values.get('description__c', '')
    return f'''
    <event>
        <routing_key>{rc}</routing_key>
        <crud_operation>{crud}</crud_operation>
        <id>{master_id}</id>
        <title>{title__c}</title>
        <date>{date__c}</date>
        <start_time>{start_time__c}</start_time>
        <end_time>{end_time__c}</end_time>
        <location>{location__c}</location>
        <speaker>
            <user_id>{user_id__c}</user_id>
            <company_id>{company_id__c}</company_id>
        </speaker>
        <max_registrations>{max_registrations__c}</max_registrations>
        <available_seats>{available_seats__c}</available_seats>
        <description>{description__c}</description>
    </event>'''


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
        elif field == "Id":
            field_element = ET.SubElement(root, "id")
            field_element.text = "" if value == None else create_master_uuid(str(value), TEAM)
        else:
            field_name = field.split("__")[0]
            field_element = ET.SubElement(root, str(field_name).lower())
            field_element.text = "" if value == None else get_master_uuid(str(value), TEAM)

    return ET.tostring(root, encoding="unicode", method="xml")


def update_xml_attendance(rc, crud, id, updated_values):
    master_id = get_master_uuid(id, TEAM)
    if crud == 'delete':
        delete_service_id(master_id, TEAM)
    user_id = updated_values.get('user_id__c', '')
    if user_id != '':
        user_id = get_master_uuid(user_id, TEAM)
    event_id = updated_values.get('event_id__c', '')
    if event_id != '':
        event_id = get_master_uuid(event_id, TEAM)

    return f'''
    <attendance>
        <routing_key>{rc}</routing_key>
        <crud_operation>{crud}</crud_operation>
        <id>{master_id}</id>
        <user_id>{user_id}</user_id>
        <event_id>{event_id}</event_id>
    </attendance>'''


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
            variables[child.tag] = get_service_id(child.text.strip(), TEAM)
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


def read_xml_changed_data(variables, root):
    changed_object = []
    for child in root:
        details = {}
        for field in child:
            if field.tag == "Id":
                details["changed_object_id"] = field.text
            elif field.tag == "Name":
                details["changed_object_name"] = field.text
            elif field.tag == "object_type__c":
                details["object_type"] = field.text
            elif field.tag == "crud__c":
                details["crud_operation"] = field.text
        changed_object.append(details)
    variables["changed_data"] = changed_object


def create_xml_changed_data(data):
    root = ET.Element("ChangedData")

    for record in data:
        changed_element = ET.SubElement(root, "ChangedObject__c")
        for field, value in record.items():
            if field == "attributes":
                continue
            field_element = ET.SubElement(changed_element, field)
            field_element.text = str(value)

    return ET.tostring(root, encoding="unicode", method="xml")
