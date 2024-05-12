import requests
import json
import sys

sys.path.append('/app')
import config.secrets as secrets

headers = {
    "Content-Type": "application/json"
}


def get_service_id(service_name, master_uuid):
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


def add_service_id(master_uuid, service, service_id):
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
        print(response.content)
        return response.json()
    else:
        return None


def delete_service_id(master_uuid, service):
    url = f"http://{secrets.HOST}:6000/updateServiceId"
    payload = {
        "MASTERUUID": master_uuid,
        "NewServiceId": "",
        "Service": service
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        return response.json()
    else:
        return None