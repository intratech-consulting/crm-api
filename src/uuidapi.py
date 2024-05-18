import os

import requests
import json
import sys

if os.path.isdir('/app'):
    sys.path.append('/app')
else:
    local_dir = os.path.dirname(os.path.realpath(__file__))
    sys.path.append(local_dir)
import config.secrets as secrets

headers = {
    "Content-Type": "application/json"
}


def create_master_uuid(service_id, service_name):
    url = f"http://{secrets.HOST}:6000/createMasterUuid"
    payload = {
        "ServiceId": service_id,
        "Service": service_name
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200 and response.json().get("success") or response.status_code == 201 and response.json().get("success"):
        return response.json().get("MasterUuid")
    else:
        return None


def get_service_id(master_uuid, service_name):
    url = f"http://{secrets.HOST}:6000/getServiceId"
    payload = {
        "MASTERUUID": master_uuid,
        "Service": service_name
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        return response.json()[service_name]
    else:
        return None


def get_master_uuid(service_id, service_name):
    url = f"http://{secrets.HOST}:6000/getMasterUuid"
    payload = {
        "ServiceId": service_id,
        "Service": service_name
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        return response.json().get("UUID")
    else:
        return None


def add_service_id(master_uuid, service_id, service):
    url = f"http://{secrets.HOST}:6000/addServiceId"
    payload = {
        "MasterUuid": master_uuid,
        "Service": service,
        "ServiceId": service_id
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 201:
        return response.json()
    else:
        return None


def delete_service_id(master_uuid, service):
    url = f"http://{secrets.HOST}:6000/updateServiceId"
    payload = {
        "MASTERUUID": master_uuid,
        "NewServiceId": None,
        "Service": service
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        return response.json()
    else:
        return None