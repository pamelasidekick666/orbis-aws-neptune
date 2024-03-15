import pandas as pd
from datetime import datetime
import numpy as np

edge_columns = ["~id", "id", "~label", "~from", "~to", "relationship:String"]
node_columns = ["~id", "id", "~label", "name:String",
                       "gender:String", "bvd_id:String", "uci:String", "is_manager_type:String", "entity_type:String", "first_name:String", "last_name:String", "address:String",
                       "postcode:String", "city:String", "country_iso:String", "country:String", "phone:String", "email:String", "node_type:String", "industry_classification:String", "industry_primary_label:String", "industry_secondary_label:String", "current_previous_membership_count:Int"]
edge_df = pd.DataFrame(columns=edge_columns)
node_df = pd.DataFrame(columns=node_columns)


def add_edge(from_id, to_id, relationship, tracker):
    global edge_df
    new_row = {
        "~id": "e" + str(tracker.edge_id),
        "id": tracker.edge_id,
        "~label": relationship,
        "~from": from_id,
        "~to": to_id,
        "relationship:String": relationship
    }
    temp = pd.DataFrame([new_row])
    edge_df = pd.concat([edge_df, temp], ignore_index=True)
    tracker.edge_id = 1


def extract_phone(result, prefix, from_id, tracker):
    global node_df
    tracker.vertex_id = 1
    new_row = {
        "~id": "v" + str(tracker.vertex_id),
        "id": tracker.vertex_id,
        "~label": 'Phone',
        "node_type:String": "Phone",
        "name:String": result['CONTACTS_HEADER_HomePhone'],
        "country_iso:String": "",
        "city:String": "",
        "postcode:String": "",
        "bvd_id:String": "",
        "address:String": "",
        "phone:String": result['CONTACTS_HEADER_HomePhone'],
        "email:String": "",
        "entity_type:String": "",
        "industry_classification:String": "",
        "industry_primary_label:String": "",
        "industry_secondary_label:String": "",
        "uci:String": "",
        "gender:String": "",
        "is_manager_type:String": "",
        "first_name:String": "",
        "last_name:String": ""
    }
    temp = pd.DataFrame([new_row])
    node_df = pd.concat([node_df, temp], ignore_index=True)
    add_edge(from_id, "v" + str(tracker.vertex_id), "Phone", tracker)


def extract_email(result, prefix, from_id, tracker):
    global node_df
    tracker.vertex_id = 1
    new_row = {
        "~id": "v" + str(tracker.vertex_id),
        "id": tracker.vertex_id,
        "~label": 'Email',
        "node_type:String": "Email",
        "name:String": result['CONTACTS_HEADER_HomeEmail'],
        "country_iso:String": "",
        "city:String": "",
        "postcode:String": "",
        "bvd_id:String": "",
        "address:String": "",
        "phone:String": "",
        "email:String": result['CONTACTS_HEADER_HomeEmail'],
        "entity_type:String": "",
        "industry_classification:String": "",
        "industry_primary_label:String": "",
        "industry_secondary_label:String": "",
        "uci:String": "",
        "gender:String": "",
        "is_manager_type:String": "",
        "first_name:String": "",
        "last_name:String": ""
    }
    temp = pd.DataFrame([new_row])
    node_df = pd.concat([node_df, temp], ignore_index=True)
    add_edge(from_id, "v" + str(tracker.vertex_id), "Email", tracker)


def extract_address(result, prefix, from_id, tracker):
    global node_df
    tracker.vertex_id = 1
    if prefix == "membership":
        address = result["CONTACTS_MEMBERSHIP_FullAddressCompany"]
        new_row = {
            "~id": "v" + str(tracker.vertex_id),
            "id": tracker.vertex_id,
            "~label": 'Address',
            "node_type:String": "Address",
            "name:String": address,
            "country_iso:String": "",
            "country:String": result['CONTACTS_MEMBERSHIP_CountryCompany'] if result['CONTACTS_MEMBERSHIP_CountryCompany'] is not None else None,
            "city:String": None,
            "postcode:String": None,
            "bvd_id:String": "",
            "address:String": address,
            "phone:String": "",
            "email:String": "",
            "entity_type:String": "",
            "industry_classification:String": "",
            "industry_primary_label:String": "",
            "industry_secondary_label:String": "",
            "uci:String": "",
            "gender:String": "",
            "is_manager_type:String": "",
            "first_name:String": "",
            "last_name:String": ""
        }
        temp = pd.DataFrame([new_row])
        node_df = pd.concat([node_df, temp], ignore_index=True)
        add_edge(from_id, "v" + str(tracker.vertex_id), "Address", tracker)
    else:
        address_string1 = (
            result["CONTACTS_HEADER_HomeAddressLine1"] if result["CONTACTS_HEADER_HomeAddressLine1"] is not None else "")
        address_string2 = (
            result["CONTACTS_HEADER_HomeAddressLine2"] if result["CONTACTS_HEADER_HomeAddressLine2"] is not None else "")
        address_string3 = (
            result["CONTACTS_HEADER_HomeAddressLine3"] if result["CONTACTS_HEADER_HomeAddressLine3"] is not None else "")
        address_string4 = (
            result["CONTACTS_HEADER_HomeAddressLine4"] if result["CONTACTS_HEADER_HomeAddressLine4"] is not None else "")
        address_string5 = (
            result["CONTACTS_HEADER_HomeAddressLine5"] if result["CONTACTS_HEADER_HomeAddressLine5"] is not None else "")
        address = (
            address_string1
            + " "
            + address_string2
            + " "
            + address_string3
            + " "
            + address_string4
            + " "
            + address_string5
        ).strip()
        new_row = {
            "~id": "v" + str(tracker.vertex_id),
            "id": tracker.vertex_id,
            "~label": 'Address',
            "node_type:String": "Address",
            "name:String": address,
            "country_iso:String": "",
            "country:String": result['CONTACTS_HEADER_HomeCountry'] if result['CONTACTS_HEADER_HomeCountry'] is not None else "",
            "city:String": result['CONTACTS_HEADER_HomeCity'] if result['CONTACTS_HEADER_HomeCity'] is not None else "",
            "postcode:String": result['CONTACTS_HEADER_HomePostcode'] if result['CONTACTS_HEADER_HomePostcode'] is not None else "",
            "bvd_id:String": "",
            "address:String": address,
            "phone:String": "",
            "email:String": "",
            "entity_type:String": "",
            "industry_classification:String": "",
            "industry_primary_label:String": "",
            "industry_secondary_label:String": "",
            "uci:String": "",
            "gender:String": "",
            "is_manager_type:String": "",
            "first_name:String": "",
            "last_name:String": ""
        }
        temp = pd.DataFrame([new_row])
        node_df = pd.concat([node_df, temp], ignore_index=True)
        add_edge(from_id, "v" + str(tracker.vertex_id), "Address", tracker)


def extract_entities(result, prefix, tracker):
    global node_df
    if prefix == "CONTACTS_HEADER_":
        if result['CONTACTS_HEADER_HomeAddressLine1'] is not None:
            address_string1 = (
                result["CONTACTS_HEADER_HomeAddressLine1"] if result["CONTACTS_HEADER_HomeAddressLine1"] is not None else "")
            address_string2 = (
                result["CONTACTS_HEADER_HomeAddressLine2"] if result["CONTACTS_HEADER_HomeAddressLine2"] is not None else "")
            address_string3 = (
                result["CONTACTS_HEADER_HomeAddressLine3"] if result["CONTACTS_HEADER_HomeAddressLine3"] is not None else "")
            address_string4 = (
                result["CONTACTS_HEADER_HomeAddressLine4"] if result["CONTACTS_HEADER_HomeAddressLine4"] is not None else "")
            address_string5 = (
                result["CONTACTS_HEADER_HomeAddressLine5"] if result["CONTACTS_HEADER_HomeAddressLine5"] is not None else "")
            address = (
                address_string1
                + " "
                + address_string2
                + " "
                + address_string3
                + " "
                + address_string4
                + " "
                + address_string5
            ).strip()
        else:
            address = ''
        # Person
        if result["CONTACTS_HEADER_Personorcompany"] == "Individual":
            new_row = {
                "~id": "v" + str(tracker.vertex_id),
                "id": tracker.vertex_id,
                "~label": 'Person',
                "node_type:String": "Person",
                "name:String": result['CONTACTS_HEADER_FullName'],
                "bvd_id:String": result['CONTACTS_HEADER_BvdId'] if result['CONTACTS_HEADER_BvdId'] is not None else None,
                "address:String": address,
                "city:String": result['CONTACTS_HEADER_HomeCity'] if result['CONTACTS_HEADER_HomeCity'] is not None else "",
                "postcode:String": result['CONTACTS_HEADER_HomePostcode'] if result['CONTACTS_HEADER_HomePostcode'] is not None else "",
                "country_iso:String": "",
                "country:String": result['CONTACTS_HEADER_HomeCountry'] if result['CONTACTS_HEADER_HomeCountry'] is not None else "",
                "phone:String": result['CONTACTS_HEADER_HomePhone'] if result['CONTACTS_HEADER_HomePhone'] is not None else "",
                "email:String": result['CONTACTS_HEADER_HomeEmail'] if result['CONTACTS_HEADER_HomeEmail'] is not None else "",
                "entity_type:String": result['CONTACTS_HEADER_Personorcompany'] if result['CONTACTS_HEADER_Personorcompany'] is not None else "",
                "industry_classification:String": "",
                "industry_primary_label:String": "",
                "industry_secondary_label:String": "",
                "uci:String": result['CONTACTS_HEADER_IdDirector'] if result['CONTACTS_HEADER_IdDirector'] is not None else None,
                "gender:String": result['CONTACTS_HEADER_Gender'] if result['CONTACTS_HEADER_Gender'] is not None else "",
                "is_manager_type:String": "",
                "first_name:String": result['CONTACTS_HEADER_FirstName'] if result['CONTACTS_HEADER_FirstName'] is not None else "",
                "last_name:String": result['CONTACTS_HEADER_LastName'] if result['CONTACTS_HEADER_LastName'] is not None else "",
                "current_previous_membership_count:Int": result['CONTACTS_COUNTERS_Membership'] if result['CONTACTS_COUNTERS_Membership'] is not None else None,
            }
        # Organization
        else:
            new_row = {
                "~id": "v" + str(tracker.vertex_id),
                "id": tracker.vertex_id,
                "~label": 'Entity',
                "node_type:String": "Entity",
                "name:String": result['CONTACTS_HEADER_FullName'],
                "bvd_id:String": result['CONTACTS_HEADER_BvdId'] if result['CONTACTS_HEADER_BvdId'] is not None else None,
                "address:String": address,
                "city:String": result['CONTACTS_HEADER_HomeCity'] if result['CONTACTS_HEADER_HomeCity'] is not None else "",
                "postcode:String": result['CONTACTS_HEADER_HomePostcode'] if result['CONTACTS_HEADER_HomePostcode'] is not None else "",
                "country_iso:String": "",
                "country:String": result['CONTACTS_HEADER_HomeCountry'] if result['CONTACTS_HEADER_HomeCountry'] is not None else "",
                "phone:String": result['CONTACTS_HEADER_HomePhone'] if result['CONTACTS_HEADER_HomePhone'] is not None else "",
                "email:String": result['CONTACTS_HEADER_HomeEmail'] if result['CONTACTS_HEADER_HomeEmail'] is not None else "",
                "entity_type:String": result['CONTACTS_HEADER_Personorcompany'] if result['CONTACTS_HEADER_Personorcompany'] is not None else "",
                "industry_classification:String": "",
                "industry_primary_label:String": "",
                "industry_secondary_label:String": "",
                "uci:String": result['CONTACTS_HEADER_IdDirector'] if result['CONTACTS_HEADER_IdDirector'] is not None else None,
                "gender:String": result['CONTACTS_HEADER_Gender'] if result['CONTACTS_HEADER_Gender'] is not None else "",
                "is_manager_type:String": "",
                "first_name:String": result['CONTACTS_HEADER_FirstName'] if result['CONTACTS_HEADER_FirstName'] is not None else "",
                "last_name:String": result['CONTACTS_HEADER_LastName'] if result['CONTACTS_HEADER_LastName'] is not None else "",
                "current_previous_membership_count:Int": result['CONTACTS_COUNTERS_Membership'] if result['CONTACTS_COUNTERS_Membership'] is not None else None,
            }
        temp = pd.DataFrame([new_row])
        node_df = pd.concat([node_df, temp], ignore_index=True)
        # main_vertex_id contains the director's vertex id
        main_vertex_id = "v" + str(tracker.vertex_id)
        if result["CONTACTS_HEADER_HomePhone"] is not None:
            extract_phone(result, prefix, "v" +
                          str(tracker.vertex_id), tracker)
        if result["CONTACTS_HEADER_HomeEmail"] is not None:
            extract_email(result, prefix, "v" +
                          str(tracker.vertex_id), tracker)
        if result["CONTACTS_HEADER_HomeAddressLine1"] is not None:
            extract_address(result, prefix, "v" +
                            str(tracker.vertex_id), tracker)

        # Extract membership - Can be array or object
        current_membership = result["MEMBERSHIP_DATA"]
        if type(current_membership) is list:
            i = 0
            for i in range(len(current_membership)):
                if current_membership[i]['CONTACTS_MEMBERSHIP_NameCompany'] is not None or current_membership[i]['CONTACTS_MEMBERSHIP_IdCompany'] is not None:
                    tracker.vertex_id = 1
                    add_edge("v" + str(tracker.vertex_id), main_vertex_id,
                             current_membership[i]["CONTACTS_MEMBERSHIP_Function"], tracker)
                    extract_entities(current_membership[i], "", tracker)

        else:
            if current_membership['CONTACTS_MEMBERSHIP_NameCompany'] is not None or current_membership['CONTACTS_MEMBERSHIP_IdCompany'] is not None:
                tracker.vertex_id = 1
                add_edge("v" + str(tracker.vertex_id), main_vertex_id,
                         current_membership[i]["CONTACTS_MEMBERSHIP_Function"], tracker)
                extract_entities(current_membership,
                                 "", tracker)
    else:
        # Membership
        new_row = {
            "~id": "v" + str(tracker.vertex_id),
            "id": tracker.vertex_id,
            "~label": 'Entity',
            "node_type:String": "Entity",
            "name:String": result['CONTACTS_MEMBERSHIP_NameCompany'] if result['CONTACTS_MEMBERSHIP_NameCompany'] is not None else None,
            "bvd_id:String": result['CONTACTS_MEMBERSHIP_IdCompany'] if result['CONTACTS_MEMBERSHIP_IdCompany'] is not None else None,
            "address:String": result['CONTACTS_MEMBERSHIP_FullAddressCompany'] if result['CONTACTS_MEMBERSHIP_FullAddressCompany'] is not None else None,
            "city:String": "",
            "postcode:String": "",
            "country_iso:String": "",
            "country:String": result['CONTACTS_MEMBERSHIP_CountryCompany'] if result['CONTACTS_MEMBERSHIP_CountryCompany'] is not None else None,
            "phone:String": result['CONTACTS_MEMBERSHIP_WorkPhone'] if result['CONTACTS_MEMBERSHIP_WorkPhone'] is not None else None,
            "email:String": result['CONTACTS_MEMBERSHIP_WorkEmail'] if result['CONTACTS_MEMBERSHIP_WorkEmail'] is not None else None,
            "entity_type:String": "",
            "industry_classification:String": "",
            "industry_primary_label:String": result['CONTACTS_MEMBERSHIP_USSicLabel'] if result['CONTACTS_MEMBERSHIP_USSicLabel'] is not None else None,
            "industry_secondary_label:String": "",
            "uci:String": "",
            "gender:String": "",
            "is_manager_type:String": "",
            "first_name:String": "",
            "last_name:String": "",
            "current_previous_membership_count:Int": None,
        }
        temp = pd.DataFrame([new_row])
        node_df = pd.concat([node_df, temp], ignore_index=True)
        if result["CONTACTS_MEMBERSHIP_FullAddressCompany"] is not None:
            extract_address(result, "membership", "v" +
                            str(tracker.vertex_id), tracker)


def process_dir(result, tracker):

    global node_df
    global edge_df

    i = 0
    director = result['Data']
    for i in range(len(director)):
        tracker.vertex_id = 1
        add_edge("v" + str(tracker.main_vertex_id),
                 "v" + str(tracker.vertex_id), "Director", tracker)
        extract_entities(director[i], "CONTACTS_HEADER_", tracker)

    node_df.replace({0: None, "": None, np.nan: None}, inplace=True)
    edge_df.replace({0: None, "": None, np.nan: None}, inplace=True)

    return (node_df, edge_df)
