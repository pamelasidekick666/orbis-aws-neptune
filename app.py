import json
import requests
import sys
from process_sh_bo_vertices_edges import *
from tracker import *
from process_dir_vertices_edges import *
from dedupe import *
import pandas as pd
import boto3
import os
import numpy as np


bvdid = input("Enter bvdid: ")


# AWS credentials and region
aws_access_key_id = {your_aws_access_key_id}
aws_secret_access_key = {your_aws_secret_access_key}
region_name = {your_aws_region}

# S3 bucket information
bucket_name = {your_aws_s3_bucket_name}

# neptune information
neptune_endpoint = {your_neptune_endpoint}
port = {your_neptune_port}

# tracker for node id and edge id
tracker = None
tracker_s3_path = {your_tracker_s3_path}


def check_files_in_bucket(bucket_name):
    global tracker
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            s3.download_file(tracker_s3_path)
            tracker_df = pd.read_csv("tracker.csv")
            tracker = Tracker(tracker_df["node"][0], tracker_df["edge"][0])
        else:
            tracker = Tracker(0, 0)
    except Exception as e:
        print(f"Tracker error: {e}")


check_files_in_bucket(bucket_name)


def fetch_shareholder_bo(bvdid):
    try:
        query = {"WHERE": [{"BvDID": [bvdid]}], "SELECT": ["NAME", "BVD_ID_NUMBER", "ADDRESS_LINE1", "ADDRESS_LINE2", "ADDRESS_LINE3", "ADDRESS_LINE4", "POSTCODE", "CITY", "COUNTRY_ISO_CODE", "PHONE_NUMBER", "EMAIL", "INCORPORATION_DATE", "ENTITY_TYPE", "INDUSTRY_CLASSIFICATION", "INDUSTRY_PRIMARY_LABEL", "INDUSTRY_SECONDARY_LABEL", {"BENFICIAL_OWNERS": {"SELECT": ["BO_STATUS", "BO_DEFINITION_10_10_COUNT", "BO_DEFINITION_10_50_COUNT",  "BO_DEFINITION_25_25_COUNT", "BO_BIRTHDATE", "BO_GENDER", "BO_ADDRESS", "BO_CITY", "BO_POSTCODE", "BO_BVD_ID_NUMBER", "BO_COUNTRY_ISO_CODE", "BO_PHONE_NUMBER", "BO_EMAIL", "BO_CURRENT_PREVIOUS_MANAGER", "BO_ENTITY_TYPE", "BO_FIRST_NAME", "BO_LAST_NAME", "BO_NAME", "BO_UCI"]}}, {
            "SHAREHOLDERS": {"SELECT": ["SH_BVD_ID_NUMBER", "SH_ORBISID", "SH_TICKER", "SH_COUNTRY_ISO_CODE", "SH_DIRECT_PCT", "SH_TOTAL_PCT", "SH_INFORMATION_SOURCE", "SH_INFORMATION_DATE", "SH_CURRENT_PREVIOUS_MANAGER", "SH_ENTITY_TYPE", "SH_LAST_NAME", "SH_LEI", "SH_NAME", "SH_STATE_PROVINCE", "SH_CITY", "SH_UCI", "SH_FIRST_NAME", "SH_NACE_CORE_LABEL", "SH_NACE_CORE_CODE", "SH_NAICS_CORE_CODE", "SH_NAICS_CORE_LABEL", "SH_USSIC_CORE_CODE", "SH_USSIC_CORE_LABEL", "SH_ADDRESS_LINE1", "SH_ADDRESS_LINE2", "SH_ADDRESS_LINE3", "SH_ADDRESS_LINE4", "SH_POSTCODE", "SH_PHONE_NUMBER"]}}]}
        headers = {
            "Content-Type": "application/json",
            "ApiToken": {your_orbis_api_token},
        }
        response = requests.post(
            "https://api.bvdinfo.com/v1/orbis/Companies/data",
            headers=headers,
            json=query,
        )
        result = response.json()
        return process_shareholder_bo(result, tracker)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None


def fetch_directors(bvdid):
    try:
        query = {"WHERE": [{"FromCompanies": {"BvDID": bvdid}}], "SELECT": ["CONTACTS_HEADER_BvdId", "CONTACTS_HEADER_OfficialIndividualID", "CONTACTS_HEADER_OfficialIndividualIDType", "CONTACTS_HEADER_Personorcompany", "CONTACTS_HEADER_Gender", "CONTACTS_HEADER_Age", "CONTACTS_HEADER_PLACEOFBIRTH", "CONTACTS_HEADER_MultipleNationalitiesLabel", "CONTACTS_HEADER_HomeEmail",  "CONTACTS_HEADER_HomeCountry", "CONTACTS_HEADER_Birthdate", "CONTACTS_HEADER_FirstName", "CONTACTS_HEADER_MiddleName", "CONTACTS_HEADER_LastName", "CONTACTS_HEADER_FullName", "CONTACTS_HEADER_IdDirector", "CONTACTS_HEADER_NationalityCountryLabel", "CONTACTS_HEADER_HomeAddressLine1",
                                                                            "CONTACTS_HEADER_HomeAddressLine2", "CONTACTS_HEADER_HomeAddressLine3", "CONTACTS_HEADER_HomeAddressLine4", "CONTACTS_HEADER_HomeAddressLine5", "CONTACTS_HEADER_HomePhone", "CONTACTS_HEADER_HomePostcode", "CONTACTS_HEADER_HomeCity", "CONTACTS_HEADER_HomeProvinceOrState", "CONTACTS_COUNTERS_Membership", {"MEMBERSHIP_DATA": {"FILTERS": "Filter.Name=ContactsFilter;ContactsFilter.CurrentPreviousQueryString=0;ContactsFilter.Currents=True", "SELECT": ["CONTACTS_MEMBERSHIP_IdCompany", "CONTACTS_MEMBERSHIP_NameCompany", "CONTACTS_MEMBERSHIP_BeginningNominationDate", "CONTACTS_MEMBERSHIP_CurrentOrPreviousStr", "CONTACTS_MEMBERSHIP_EndExpirationDate", "CONTACTS_MEMBERSHIP_Function", "CONTACTS_MEMBERSHIP_FullAddressCompany", "CONTACTS_MEMBERSHIP_CountryCompany", "CONTACTS_MEMBERSHIP_USSicLabel", "CONTACTS_MEMBERSHIP_WorkPhone", "CONTACTS_MEMBERSHIP_WorkEmail"]}}]}
        headers = {
            "Content-Type": "application/json",
            "ApiToken": {your_orbis_api_token},
        }
        response = requests.post(
            "https://api.bvdinfo.com/v1/orbis/contacts/data",
            headers=headers,
            json=query,
        )
        result = response.json()
        return process_dir(result, tracker)
    except Exception as e:
        print(f"An error occurred: {str(e)}")


def check_neptune_job(job_id):
    url = f"https://{neptune_endpoint}:{port}/loader/{job_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        print(response.json()["payload"]["overallStatus"]["status"])

    except requests.exceptions.RequestException as e:
        # Handle exceptions here
        print("Load failed:", e)


def neptune_loader(file):
    try:
        data = {
            "source": f"s3://{bucket_name}/{file}",
            "format": "csv",
            "iamRoleArn": {your_iam_role},
            "region": {your_aws_region},
            "failOnError": "FALSE",
            "parallelism": "MEDIUM",
            "queueRequest": "TRUE"
        }
        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post(
            f"https://{neptune_endpoint}:{port}/loader",
            headers=headers,
            json=data,
        )
        response.raise_for_status()
        if response.json()["status"] == "200 OK":
            load_job_id = response.json()["payload"]["loadId"]
            print(f"Load job ID is {load_job_id}")
            check_neptune_job(load_job_id)

    except requests.exceptions.RequestException as e:
        print(f"Request to Neptune loader failed: {str(e)}")


# Only unpack if not None
shareholder_bo = fetch_shareholder_bo(bvdid)
if shareholder_bo is not None:
    bo_node_df, bo_edge_df = shareholder_bo
else:
    bo_node_df = pd.DataFrame()
    bo_edge_df = pd.DataFrame()


director = fetch_directors(bvdid)
if director is not None:
    dir_node_df, dir_edge_df = director
else:
    dir_node_df = pd.DataFrame()
    dir_edge_df = pd.DataFrame()


node_df = pd.concat([bo_node_df, dir_node_df], ignore_index=True)
edge_df = pd.concat([bo_edge_df, dir_edge_df], ignore_index=True)

if node_df.empty or edge_df.empty:
    print("No vertices or edges.")
    sys.exit()


node_df.replace({0: None, "": None}, inplace=True)
edge_df.replace({0: None, "": None}, inplace=True)

node_df.to_csv("node.csv")
edge_df.to_csv("edge.csv")

# Deduplication based on these fields
node_df, edge_df = dedupe(node_df, edge_df, "bvd_id:String")
node_df, edge_df = dedupe(node_df, edge_df, "uci:String")

address_node_df = node_df[node_df['~label'] == 'Address']
remaining_node_df = node_df[node_df['~label'] != 'Address']
address_node_df, edge_df = dedupe(address_node_df, edge_df, "address:String")

node_df = pd.concat([remaining_node_df, address_node_df], ignore_index=True)
node_df = node_df.sort_values(by="id").reset_index(drop=True)

# To record the last IDs for tracker initialization in the next run
last_node_id = node_df['id'].iloc[-1]
last_edge_id = edge_df['id'].iloc[-1]
data = {'node': [last_node_id], 'edge': [last_edge_id]}
tracker_df = pd.DataFrame(data)
tracker_df.to_csv('tracker.csv', index=False, encoding="utf-8")

# Clean up dataframes
node_df = node_df.drop('id', axis=1)
edge_df = edge_df.drop('id', axis=1)
node_df.replace({0: None, "": None, np.nan: None}, inplace=True)
edge_df.replace({0: None, "": None, np.nan: None}, inplace=True)

node_df.to_csv('node.csv', index=False, encoding="utf-8")
edge_df.to_csv('edge.csv', index=False, encoding="utf-8")


# Upload file to S3
current_timestamp = datetime.now().strftime("%Y%m%d%H%M")
try:
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key, region_name=region_name)
    object_key_node = f"node_{current_timestamp}.csv"
    object_key_edge = f"edge_{current_timestamp}.csv"
    s3.upload_file('node.csv', bucket_name,
                   object_key_node)
    s3.upload_file('edge.csv', bucket_name,
                   object_key_edge)
    s3.upload_file('tracker.csv', 'temp-yc', "tracker.csv")
except Exception as e:
    print(f"Error uploading file to S3: {e}")

# Delete the local file
try:
    os.remove("node.csv")
    os.remove("edge.csv")
    os.remove("tracker.csv")
except Exception as e:
    print(f"Error deleting local file: {e}")

neptune_loader(object_key_node)
neptune_loader(object_key_edge)
