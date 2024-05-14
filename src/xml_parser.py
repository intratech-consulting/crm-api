import sys

sys.path.append('/app')
import config.secrets as secrets
import src.API as API
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
            variables[child.tag] = child.text

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