import pandas as pd
from datetime import datetime


node_df = None
edge_df = None


def process_edges(list_of_ids):
    global edge_df
    replace_value = list_of_ids[0]
    search_values = list_of_ids[1:]
    for col in ["~from", "~to"]:
        for search_value in search_values:
            edge_df[col] = edge_df[col].replace(
                search_value, replace_value)


def combine_group_edges(group):
    combined_row = {}
    for col in group.columns:
        values = group[col].dropna().unique().tolist()
        if len(values) == 1:
            combined_row[col] = values[0]
        elif len(values) > 1:
            if col == "relationship:String":
                result_string = " | ".join(values)
                combined_row[col] = result_string
            else:
                combined_row[col] = values[0]
    return pd.DataFrame([combined_row])


def combine_group(group):
    combined_row = {}
    for col in group.columns:
        values = group[col].dropna().unique().tolist()
        if len(values) == 1:
            combined_row[col] = values[0]
        elif len(values) > 1:
            if col == "name:String" or col == "entity_type:String":
                result_string = " | ".join(values)
                combined_row[col] = result_string
            elif col == "~id":
                combined_row[col] = values
                process_edges(combined_row[col])
                combined_row[col] = combined_row[col][0]
            else:
                combined_row[col] = values[0]
    return pd.DataFrame([combined_row])


def dedupe(n_df, e_df, by):
    global node_df
    global edge_df

    node_df = n_df
    edge_df = e_df

    # Create a new DataFrame with rows where 'Group' column has NaN values
    na_rows_df = node_df[node_df[by].isna()]

    df_filtered = node_df.dropna(subset=[by])

    grouped_df = df_filtered.groupby(by)

    # Combine rows for each group
    combined_rows_df = grouped_df.filter(lambda x: len(x) > 1).groupby(
        by).apply(combine_group)

    # Include original rows with only one row in a group
    original_rows_df = grouped_df.filter(lambda x: len(x) == 1)

    # Concatenate the original rows and the combined rows
    node_df = pd.concat(
        [original_rows_df, combined_rows_df, na_rows_df], ignore_index=True)

    node_df = node_df.sort_values(by="id").reset_index(drop=True)

    node_df.replace({0: None, "": None}, inplace=True)

    edge_grouped_df = edge_df.groupby(["~from", "~to"])
    edge_combined_rows_df = edge_grouped_df.filter(lambda x: len(
        x) > 1).groupby(["~from", "~to"]).apply(combine_group_edges)
    edge_original_rows_df = edge_grouped_df.filter(lambda x: len(x) == 1)
    edge_df = pd.concat(
        [edge_original_rows_df, edge_combined_rows_df], ignore_index=True)
    edge_df = edge_df.sort_values(by="id").reset_index(drop=True)
    edge_df.replace({0: None, "": None}, inplace=True)

    return (node_df, edge_df)
