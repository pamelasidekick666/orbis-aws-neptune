import pandas as pd
from datetime import datetime

edge_columns = ["~id", "id", "~label", "~from", "~to", "relationship:String"]
node_columns = ["~id", "id", "~label", "name:String",
                       "gender:String", "bvd_id:String", "uci:String", "is_manager_type:String", "entity_type:String", "first_name:String", "last_name:String", "address:String",
                       "postcode:String", "city:String", "country_iso:String", "phone:String", "email:String", "node_type:String", "industry_classification:String", "industry_primary_label:String", "industry_secondary_label:String"]
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
        "name:String": result[prefix + 'PHONE_NUMBER'] if type(result[prefix + 'PHONE_NUMBER']) is not list else result[prefix + 'PHONE_NUMBER'][0],
        "country_iso:String": "",
        "city:String": "",
        "postcode:String": "",
        "bvd_id:String": "",
        "address:String": "",
        "phone:String": result[prefix + 'PHONE_NUMBER'] if type(result[prefix + 'PHONE_NUMBER']) is not list else result[prefix + 'PHONE_NUMBER'][0],
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
        "name:String": result[prefix + 'EMAIL'],
        "country_iso:String": "",
        "city:String": "",
        "postcode:String": "",
        "bvd_id:String": "",
        "address:String": "",
        "phone:String": "",
        "email:String": result[prefix + 'EMAIL'],
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
    if prefix == "" or prefix == "SH_":
        address_string1 = (result[prefix + "ADDRESS_LINE1"]
                           if result[prefix + "ADDRESS_LINE1"] is not None else "")
        address_string2 = (result[prefix + "ADDRESS_LINE2"]
                           if result[prefix + "ADDRESS_LINE2"] is not None else "")
        address_string3 = (result[prefix + "ADDRESS_LINE3"]
                           if result[prefix + "ADDRESS_LINE3"] is not None else "")
        address_string4 = (result[prefix + "ADDRESS_LINE4"]
                           if result[prefix + "ADDRESS_LINE4"] is not None else "")
        address = (
            address_string1
            + " "
            + address_string2
            + " "
            + address_string3
            + " "
            + address_string4
        ).strip()
    else:
        address = result[prefix + "ADDRESS"]
    new_row = {
        "~id": "v" + str(tracker.vertex_id),
        "id": tracker.vertex_id,
        "~label": 'Address',
        "node_type:String": "Address",
        "name:String": address,
        "country_iso:String": result[prefix + 'COUNTRY_ISO_CODE'] if result[prefix + 'COUNTRY_ISO_CODE'] is not None else "",
        "city:String": result[prefix + 'CITY'] if result[prefix + 'CITY'] is not None else "",
        "postcode:String": result[prefix + 'POSTCODE'] if result[prefix + 'POSTCODE'] is not None else "",
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
    # Main
    if prefix == "":
        if result['ADDRESS_LINE1'] is not None:
            address_string1 = (
                result["ADDRESS_LINE1"] if result["ADDRESS_LINE1"] is not None else "")
            address_string2 = (
                result["ADDRESS_LINE2"] if result["ADDRESS_LINE2"] is not None else "")
            address_string3 = (
                result["ADDRESS_LINE3"] if result["ADDRESS_LINE3"] is not None else "")
            address_string4 = (
                result["ADDRESS_LINE4"] if result["ADDRESS_LINE4"] is not None else "")
            address = (
                address_string1
                + " "
                + address_string2
                + " "
                + address_string3
                + " "
                + address_string4
            ).strip()
        else:
            address = ''
        new_row = {
            "~id": "v" + str(tracker.vertex_id),
            "id": tracker.vertex_id,
            "~label": 'Entity',
            "node_type:String": "Entity",
            "name:String": result['NAME'],
            "bvd_id:String": result['BVD_ID_NUMBER'],
            "address:String": address,
            "city:String": result['CITY'] if result['CITY'] is not None else "",
            "postcode:String": result['POSTCODE'] if result['POSTCODE'] is not None else "",
            "country_iso:String": result['COUNTRY_ISO_CODE'] if result['COUNTRY_ISO_CODE'] is not None else "",
            "phone:String": result['PHONE_NUMBER'][0] if type(result['PHONE_NUMBER']) is list else result['PHONE_NUMBER'],
            "email:String": result['EMAIL'] if result['EMAIL'] is not None else "",
            "entity_type:String": result['ENTITY_TYPE'] if result['ENTITY_TYPE'] is not None else "",
            "industry_classification:String": result['INDUSTRY_CLASSIFICATION'] if result['INDUSTRY_CLASSIFICATION'] is not None else "",
            "industry_primary_label:String": result['INDUSTRY_PRIMARY_LABEL'][0] if result['INDUSTRY_PRIMARY_LABEL'] is not None else "",
            "industry_secondary_label:String": result['INDUSTRY_SECONDARY_LABEL'][0] if result['INDUSTRY_SECONDARY_LABEL'] is not None else "",
            "uci:String": "",
            "gender:String": "",
            "is_manager_type:String": "",
            "first_name:String": "",
            "last_name:String": ""
        }
        temp = pd.DataFrame([new_row])
        node_df = pd.concat([node_df, temp], ignore_index=True)
        if result[prefix + "PHONE_NUMBER"] is not None:
            extract_phone(result, prefix, "v" +
                          str(tracker.vertex_id), tracker)
        if result[prefix + "EMAIL"] is not None:
            extract_email(result, prefix, "v" +
                          str(tracker.vertex_id), tracker)
        if result['ADDRESS_LINE1'] is not None:
            extract_address(result, prefix, "v" +
                            str(tracker.vertex_id), tracker)
    elif prefix == "BO_":
        # Person
        if result["BO_UCI"] is not None:
            new_row = {
                "~id": "v" + str(tracker.vertex_id),
                "id": tracker.vertex_id,
                "~label": "Person",
                "node_type:String": "Person",
                "uci:String": result["BO_UCI"],
                "name:String": result["BO_NAME"] if result["BO_NAME"] is not None else "",
                "first_name:String": result["BO_FIRST_NAME"] if result["BO_FIRST_NAME"] is not None else "",
                "last_name:String": result["BO_LAST_NAME"] if result["BO_LAST_NAME"] is not None else "",
                "gender:String": result["BO_GENDER"] if result["BO_GENDER"] is not None else "",
                "bvd_id:String": result["BO_BVD_ID_NUMBER"] if result["BO_BVD_ID_NUMBER"] is not None else "",
                "is_manager_type:String": result["BO_CURRENT_PREVIOUS_MANAGER"] if result["BO_CURRENT_PREVIOUS_MANAGER"] is not None else "",
                "entity_type:String": result["BO_ENTITY_TYPE"] if result["BO_ENTITY_TYPE"] is not None else "",
                "address:String": result["BO_ADDRESS"] if result["BO_ADDRESS"] is not None else "",
                "city:String": result["BO_CITY"] if result["BO_CITY"] is not None else "",
                "postcode:String": result["BO_POSTCODE"] if result["BO_POSTCODE"] is not None else "",
                "country_iso:String": result["BO_COUNTRY_ISO_CODE"] if result["BO_COUNTRY_ISO_CODE"] is not None else "",
                "phone:String": result["BO_PHONE_NUMBER"] if result["BO_PHONE_NUMBER"] is not None else "",
                "email:String": result["BO_EMAIL"] if result["BO_EMAIL"] is not None else "",
                "industry_classification:String": "",
                "industry_primary_label:String": "",
                "industry_secondary_label:String": ""
            }
        # Organization
        else:
            new_row = {
                "~id": "v" + str(tracker.vertex_id),
                "id": tracker.vertex_id,
                "~label": "Entity",
                "node_type:String": "Entity",
                "uci:String": "",
                "name:String": result["BO_NAME"] if result["BO_NAME"] is not None else "",
                "first_name:String": result["BO_FIRST_NAME"] if result["BO_FIRST_NAME"] is not None else "",
                "last_name:String": result["BO_LAST_NAME"] if result["BO_LAST_NAME"] is not None else "",
                "gender:String": result["BO_GENDER"] if result["BO_GENDER"] is not None else "",
                "bvd_id:String": result["BVD_ID_NUMBER"] if result["BVD_ID_NUMBER"] is not None else "",
                "is_manager_type:String": result["BO_CURRENT_PREVIOUS_MANAGER"] if result["BO_CURRENT_PREVIOUS_MANAGER"] is not None else "",
                "entity_type:String": result["BO_ENTITY_TYPE"] if result["BO_ENTITY_TYPE"] is not None else "",
                "address:String": result["BO_ADDRESS"] if result["BO_ADDRESS"] is not None else "",
                "city:String": result["BO_CITY"] if result["BO_CITY"] is not None else "",
                "postcode:String": result["BO_POSTCODE"] if result["BO_POSTCODE"] is not None else "",
                "country_iso:String": result["BO_COUNTRY_ISO_CODE"] if result["BO_COUNTRY_ISO_CODE"] is not None else "",
                "phone:String": result["BO_PHONE_NUMBER"] if result["BO_PHONE_NUMBER"] is not None else "",
                "email:String": result["BO_EMAIL"] if result["BO_EMAIL"] is not None else "",
                "industry_classification:String": "",
                "industry_primary_label:String": "",
                "industry_secondary_label:String": ""
            }
        temp = pd.DataFrame([new_row])
        node_df = pd.concat([node_df, temp], ignore_index=True)
        if result[prefix + "PHONE_NUMBER"] is not None:
            extract_phone(result, prefix, "v" +
                          str(tracker.vertex_id), tracker)
        if result[prefix + "EMAIL"] is not None:
            extract_email(result, prefix, "v" +
                          str(tracker.vertex_id), tracker)
        if result[prefix + "ADDRESS"] is not None:
            extract_address(result, prefix, "v" +
                            str(tracker.vertex_id), tracker)
    else:
        if result['SH_ADDRESS_LINE1'] is not None:
            address_string1 = (
                result["SH_ADDRESS_LINE1"] if result["SH_ADDRESS_LINE1"] is not None else "")
            address_string2 = (
                result["SH_ADDRESS_LINE2"] if result["SH_ADDRESS_LINE2"] is not None else "")
            address_string3 = (
                result["SH_ADDRESS_LINE3"] if result["SH_ADDRESS_LINE3"] is not None else "")
            address_string4 = (
                result["SH_ADDRESS_LINE4"] if result["SH_ADDRESS_LINE4"] is not None else "")
            address = (
                address_string1
                + " "
                + address_string2
                + " "
                + address_string3
                + " "
                + address_string4
            ).strip()
        else:
            address = ''
        # Person
        if result["SH_UCI"] is not None:
            new_row = {
                "~id": "v" + str(tracker.vertex_id),
                "id": tracker.vertex_id,
                "~label": "Person",
                "node_type:String": "Person",
                "uci:String": result["SH_UCI"],
                "name:String": result["SH_NAME"] if result["SH_NAME"] is not None else "",
                "first_name:String": result["SH_FIRST_NAME"] if result["SH_FIRST_NAME"] is not None else "",
                "last_name:String": result["SH_LAST_NAME"] if result["SH_LAST_NAME"] is not None else "",
                "gender:String": "",
                "bvd_id:String": result["SH_BVD_ID_NUMBER"] if result["SH_BVD_ID_NUMBER"] is not None else "",
                "is_manager_type:String": result["SH_CURRENT_PREVIOUS_MANAGER"] if result["SH_CURRENT_PREVIOUS_MANAGER"] is not None else "",
                "entity_type:String": result["SH_ENTITY_TYPE"] if result["SH_ENTITY_TYPE"] is not None else "",
                "address:String": address,
                "city:String": result["SH_CITY"] if result["SH_CITY"] is not None else "",
                "postcode:String": result["SH_POSTCODE"] if result["SH_POSTCODE"] is not None else "",
                "country_iso:String": result["SH_COUNTRY_ISO_CODE"] if result["SH_COUNTRY_ISO_CODE"] is not None else "",
                "phone:String": result["SH_PHONE_NUMBER"] if result["SH_PHONE_NUMBER"] is not None else "",
                "email:String": "",
                "industry_classification:String": "",
                "industry_primary_label:String": "",
                "industry_secondary_label:String": ""
            }
        # Organization
        else:
            new_row = {
                "~id": "v" + str(tracker.vertex_id),
                "id": tracker.vertex_id,
                "~label": "Entity",
                "node_type:String": "Entity",
                "uci:String": "",
                "name:String": result["SH_NAME"] if result["SH_NAME"] is not None else "",
                "first_name:String": "",
                "last_name:String": "",
                "gender:String": "",
                "bvd_id:String": result["SH_BVD_ID_NUMBER"] if result["SH_BVD_ID_NUMBER"] is not None else "",
                "is_manager_type:String": result["SH_CURRENT_PREVIOUS_MANAGER"] if result["SH_CURRENT_PREVIOUS_MANAGER"] is not None else "",
                "entity_type:String": result["SH_ENTITY_TYPE"] if result["SH_ENTITY_TYPE"] is not None else "",
                "address:String": address,
                "city:String": result["SH_CITY"] if result["SH_CITY"] is not None else "",
                "postcode:String": result["SH_POSTCODE"] if result["SH_POSTCODE"] is not None else "",
                "country_iso:String": result["SH_COUNTRY_ISO_CODE"] if result["SH_COUNTRY_ISO_CODE"] is not None else "",
                "phone:String": result["SH_PHONE_NUMBER"] if result["SH_PHONE_NUMBER"] is not None else "",
                "email:String": "",
                "industry_classification:String": "",
                "industry_primary_label:String": result["SH_NACE_CORE_LABEL"],
                "industry_secondary_label:String": ""
            }
        temp = pd.DataFrame([new_row])
        node_df = pd.concat([node_df, temp], ignore_index=True)
        if result[prefix + "PHONE_NUMBER"] is not None:
            extract_phone(result, prefix, "v" +
                          str(tracker.vertex_id), tracker)
        if result[prefix + "ADDRESS_LINE1"] is not None:
            extract_address(result, prefix, "v" +
                            str(tracker.vertex_id), tracker)


def process_shareholder_bo(result, tracker):
    global node_df
    global edge_df

    main = result['Data'][0]
    extract_entities(main, "", tracker)

    if type(main['BENFICIAL_OWNERS']) is list:
        bo = main['BENFICIAL_OWNERS']
        i = 0
        for i in range(len(bo)):
            tracker.vertex_id = 1
            add_edge("v" + str(tracker.main_vertex_id), "v" + str(tracker.vertex_id),
                     "Beneficial Owner", tracker)
            extract_entities(bo[i], "BO_", tracker)
    else:
        if main['BENFICIAL_OWNERS']['BO_UCI'] is not None:
            extract_entities(main['BENFICIAL_OWNERS'], "BO_", tracker)

    if type(main['SHAREHOLDERS']) is list and (("SH_BVD_ID_NUMBER" in main['SHAREHOLDERS'][0] and main['SHAREHOLDERS'][0]["SH_BVD_ID_NUMBER"] is not None) or ("SH_UCI" in main['SHAREHOLDERS'][0] and main['SHAREHOLDERS'][0]["SH_UCI"] is not None)):
        sh = main['SHAREHOLDERS']
        i = 0
        for i in range(len(sh)):
            tracker.vertex_id = 1
            add_edge("v" + str(tracker.main_vertex_id), "v" +
                     str(tracker.vertex_id), "Shareholder", tracker)
            extract_entities(sh[i], "SH_", tracker)

    if node_df.empty or edge_df.empty:
        print("No vertices or edges for shareholders and bo.")
        return None
    return (node_df, edge_df)
