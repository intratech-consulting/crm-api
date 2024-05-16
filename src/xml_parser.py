import os
import sys
import xml.etree.ElementTree as ET

if os.path.isdir('/app'):
    sys.path.append('/app')
else:
    local_dir = os.path.dirname(os.path.realpath(__file__))
    sys.path.append(local_dir)
from uuidapi import get_service_id, add_service_id

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
                print('I am doing the get_service_id')
                variables[child.tag] = get_service_id(child.text, TEAM)
                print('I have done the get_service_id')
            else:
                variables[child.tag] = None
                print('I have set the variable to None')
        else:
            variables[child.tag] = child.text
            print('I have set the variable to the child.text')


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


def read_xml_attendance(variables, root):
    for child in root:
        if child.tag == "routing_key" or child.tag == 'crud_operation':
            continue
        else:
            variables[child.tag] = get_service_id(child.text, TEAM)
