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


def create_xml_user(user, routing_key, crud_operation):
    user_element = ET.Element("user")
    
    routing_key_element = ET.SubElement(user_element, "routing_key")
    routing_key_element.text = routing_key
    
    crud_operation_element = ET.SubElement(user_element, "crud_operation")
    crud_operation_element.text = crud_operation

    id_element = ET.SubElement(user_element, "id")
    uuid = user.get("id", "")
    if uuid and crud_operation != "create":
        uuid = get_master_uuid(uuid, TEAM)
    if uuid and crud_operation == "create":
        uuid = create_master_uuid(uuid, TEAM)
    id_element.text = uuid

    first_name_element = ET.SubElement(user_element, "first_name")
    first_name_element.text = user.get("first_name", "")

    last_name_element = ET.SubElement(user_element, "last_name")
    last_name_element.text = user.get("last_name", "")

    email_element = ET.SubElement(user_element, "email")
    email_element.text = user.get("email", "")

    telephone_element = ET.SubElement(user_element, "telephone")
    telephone_element.text = user.get("telephone", "")

    birthday_element = ET.SubElement(user_element, "birthday")
    birthday = user.get("birthday", "")
    if birthday:
        birthday = datetime.fromtimestamp(int(birthday) / 1000).strftime('%Y-%m-%d')
    birthday_element.text = birthday

    address_element = ET.SubElement(user_element, "address")

    country_element = ET.SubElement(address_element, "country")
    country_element.text = user.get("country", "")

    state_element = ET.SubElement(address_element, "state")
    state_element.text = user.get("state", "")

    city_element = ET.SubElement(address_element, "city")
    city_element.text = user.get("city", "")

    zip_element = ET.SubElement(address_element, "zip")
    zip = user.get("zip", "")
    if zip:
        zip = str(int(zip))
    zip_element.text = zip

    street_element = ET.SubElement(address_element, "street")
    street_element.text = user.get("street", "")

    house_number_element = ET.SubElement(address_element, "house_number")
    house_number = user.get("house_number", "")
    if house_number:
        house_number = str(int(house_number))
    house_number_element.text = house_number

    company_email_element = ET.SubElement(user_element, "company_email")
    company_email_element.text = user.get("company_email", "")

    company_id_element = ET.SubElement(user_element, "company_id")
    ucid = user.get("company_id", "")
    if ucid:
        ucid = get_master_uuid(ucid, TEAM)
    company_id_element.text = ucid

    source_element = ET.SubElement(user_element, "source")
    source_element.text = user.get("source", "")

    user_role_element = ET.SubElement(user_element, "user_role")
    user_role_element.text = user.get("user_role", "")

    invoice_element = ET.SubElement(user_element, "invoice")
    invoice_element.text = user.get("invoice", "")

    calendar_link_element = ET.SubElement(user_element, "calendar_link")
    calendar_link_element.text = user.get("calendar_link", "")

    return ET.tostring(user_element, encoding="utf-8").decode("utf-8")


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


def create_xml_company(company, routing_key, crud_operation):
    company_element = ET.Element("company")
    
    routing_key_element = ET.SubElement(company_element, "routing_key")
    routing_key_element.text = routing_key
    
    crud_operation_element = ET.SubElement(company_element, "crud_operation")
    crud_operation_element.text = crud_operation

    id_element = ET.SubElement(company_element, "id")
    ucid = company.get("id", "")
    if ucid and crud_operation != "create":
        ucid = get_master_uuid(ucid, TEAM)
    if ucid and crud_operation == "create":
        ucid = create_master_uuid(ucid, TEAM)
    id_element.text = ucid

    name_element = ET.SubElement(company_element, "name")
    name_element.text = company.get("name", "")

    email_element = ET.SubElement(company_element, "email")
    email_element.text = company.get("email", "")

    telephone_element = ET.SubElement(company_element, "telephone")
    telephone_element.text = company.get("telephone", "")

    logo_element = ET.SubElement(company_element, "logo")
    logo_element.text = company.get("logo", "")

    address_element = ET.SubElement(company_element, "address")

    country_element = ET.SubElement(address_element, "country")
    country_element.text = company.get("country", "")

    state_element = ET.SubElement(address_element, "state")
    state_element.text = company.get("state", "")

    city_element = ET.SubElement(address_element, "city")
    city_element.text = company.get("city", "")

    zip_element = ET.SubElement(address_element, "zip")
    zip = company.get("zip", "")
    if zip:
        zip = str(int(zip))
    zip_element.text = zip

    street_element = ET.SubElement(address_element, "street")
    street_element.text = company.get("street", "")

    house_number_element = ET.SubElement(address_element, "house_number")
    house_number = company.get("house_number", "")
    if house_number:
        house_number = str(int(house_number))
    house_number_element.text = house_number

    type_element = ET.SubElement(company_element, "type")
    type_element.text = company.get("type", "")

    invoice_element = ET.SubElement(company_element, "invoice")
    invoice_element.text = company.get("invoice", "")

    return ET.tostring(company_element, encoding="utf-8").decode("utf-8")


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

def write_xml_event(id, title, date, start_time, end_time, location, user_id, company_id, max_registrations, available_seats, description):
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


def create_xml_event(event, routing_key, crud_operation):
    event_element = ET.Element("event")
    
    routing_key_element = ET.SubElement(event_element, "routing_key")
    routing_key_element.text = routing_key
    
    crud_operation_element = ET.SubElement(event_element, "crud_operation")
    crud_operation_element.text = crud_operation

    id_element = ET.SubElement(event_element, "id")
    ueid = event.get("id", "")
    if ueid and crud_operation != "create":
        ueid = get_master_uuid(ueid, TEAM)
    if ueid and crud_operation == "create":
        ueid = create_master_uuid(ueid, TEAM)
    id_element.text = ueid

    title_element = ET.SubElement(event_element, "title")
    title_element.text = event.get("title", "")

    date_element = ET.SubElement(event_element, "date")
    date = event.get("date", "")
    if date:
        date = datetime.fromtimestamp(int(date) / 1000).strftime('%Y-%m-%d')
    date_element.text = date

    start_time_element = ET.SubElement(event_element, "start_time")
    start_time = event.get("start_time", "")
    if start_time:
        start_time = datetime.fromtimestamp(int(start_time) / 1000).strftime('%H:%M:%S')
    start_time_element.text = start_time

    end_time_element = ET.SubElement(event_element, "end_time")
    end_time = event.get("end_time", "")
    if end_time:
        end_time = datetime.fromtimestamp(int(end_time) / 1000).strftime('%H:%M:%S')
    end_time_element.text = end_time

    location_element = ET.SubElement(event_element, "location")
    location_element.text = event.get("location", "")

    speaker_element = ET.SubElement(event_element, "speaker")

    user_id_element = ET.SubElement(speaker_element, "user_id")
    uuid = event.get("speaker", "")
    if uuid:
        uuid = get_master_uuid(uuid, TEAM)
    user_id_element.text = uuid

    company_id_element = ET.SubElement(speaker_element, "company_id")
    ucid = event.get("company_id", "")
    if ucid:
        ucid = get_master_uuid(ucid, TEAM)
    company_id_element.text = ucid

    max_registrations_element = ET.SubElement(event_element, "max_registrations")
    max_registrations = event.get("max_registrations", "")
    if max_registrations:
        max_registrations = str(int(max_registrations))
    max_registrations_element.text = max_registrations

    available_seats_element = ET.SubElement(event_element, "available_seats")
    available_seats = event.get("available_seats", "")
    if available_seats:
        available_seats = str(int(available_seats))
    available_seats_element.text = available_seats

    description_element = ET.SubElement(event_element, "description")
    description_element.text = event.get("description", "")

    return ET.tostring(event_element, encoding="utf-8").decode("utf-8")


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


def create_xml_attendance(attendance, routing_key, crud_operation):
    attendance_element = ET.Element("attendance")
    
    routing_key_element = ET.SubElement(attendance_element, "routing_key")
    routing_key_element.text = routing_key
    
    crud_operation_element = ET.SubElement(attendance_element, "crud_operation")
    crud_operation_element.text = crud_operation

    id_element = ET.SubElement(attendance_element, "id")
    uaid = attendance.get("id", "")
    if uaid and crud_operation != "create":
        uaid = get_master_uuid(uaid, TEAM)
    if uaid and crud_operation == "create":
        uaid = create_master_uuid(uaid, TEAM)
    id_element.text = uaid

    user_id_element = ET.SubElement(attendance_element, "user_id")
    uuid = attendance.get("user_id", "")
    if uuid:
        uuid = get_master_uuid(uuid, TEAM)
    user_id_element.text = uuid

    event_id_element = ET.SubElement(attendance_element, "event_id")
    ueid = attendance.get("event_id", "")
    if ueid:
        ueid = get_master_uuid(ueid, TEAM)
    event_id_element.text = ueid

    return ET.tostring(attendance_element, encoding="utf-8").decode("utf-8")


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


def get_changed_values(object):
    excluded_fields = {"OwnerId", "Name", "RecordTypeId", "CreatedDate", "CreatedById", "LastModifiedDate", "LastModifiedById"}
    result = {}
    result["id"] = object["ChangeEventHeader"]["recordIds"][0]

    for key, value in object.items():
        if key not in excluded_fields and value is not None and key != "ChangeEventHeader":
            if key.endswith('__c'):
                key = key[:-3]
            result[key] = value
    
    return result