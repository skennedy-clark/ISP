import sqlite3
import pandas as pd
import os
import sys
import numpy as np
import time
import yaml

# Extract the path of the base.
BASE_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
print(BASE_DIR)


def config(file_path):
    with open(file_path, 'r') as f:
        conf = yaml.safe_load(f)
    return conf

def create_table(conn):
    """
    Create tables in a given database.
    :param conn: sqlite3 connection of a database.
    """
    # create a cursor for writing sql.
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS mapping (
        Data_source TEXT,
        Attribute_type TEXT,
        Original_value TEXT,
        Standard_value TEXT,
        PRIMARY KEY (Data_source, Attribute_type, Original_value)
        )
    ''')
    print("Created table: mapping")

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS context (
        Id INTEGER PRIMARY KEY,
        Data_source TEXT,
        Scenario_1 TEXT,
        Scenario_2 TEXT,
        State TEXT,
        Region TEXT,
        Technology TEXT
        )
    ''')
    print("Created table: context")

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS data (
        Id INTEGER,
        Variable TEXT,
        Year INTEGER,
        Value DOUBLE,
        FOREIGN KEY (Id) REFERENCES context(Id)
            ON DELETE CASCADE
            ON UPDATE CASCADE
        )
    ''')
    print("Created table: data")


def extract_data(file_name, data_source, scenario_1):
    """
    Extract raw data from spreadsheet to dataframe.
    :param file_name: the file name of raw data.
    :param data_source: the data_source attribute in the table.
    :param scenario_1:the scenario_1 attribute in the table.
    :return: extracted dataframe.
    """
    print("    Processing Rez Capacity Data")
    # Read data input requirement.
    rez_req = pd.read_csv(os.path.join(
        BASE_DIR, "input_csv", "data_req_rez.csv"
    ), index_col=0)

    # Read raw data, where "variable = capacity".
    # REZ capacity data
    raw_df = pd.read_excel(os.path.join(BASE_DIR, RAW_DATA_DIR, file_name),
                                sheet_name='REZ Generation Capacity',
                           skiprows=2)
    raw_df = raw_df.dropna(how='all')

    # Insert Data_source and Scenario_1 attributes into the data.
    raw_df.insert(0, 'Data_source', data_source)
    raw_df.insert(1, 'Scenario_1', scenario_1)

    raw_df.rename({'Region': 'State', 'REZ': 'Region', 'CDP': 'Scenario_2'},
                       axis=1, inplace=True)
    raw_df.drop(['REZ Name'], axis=1, inplace=True)
    raw_df.loc[raw_df['Technology'] == 'Solar', 'Technology'] = \
        'Utility-scale Solar'

    # Melt multiple year columns as a single attribute "year".
    raw_df_melted = pd.melt(raw_df,id_vars=['Data_source', 'Scenario_1',
                                            'Scenario_2', 'State', 'Region',
                                            'Technology'], var_name='Year',
                            value_name='Value')
    raw_df_melted.insert(raw_df_melted.shape[1] - 1, 'Variable', 'capacity')
    if data_source == "2024 Final ISP":
        raw_df_melted.loc[:, 'Year'] = raw_df_melted['Year'].apply(lambda x: x[:2] + x [-2:])
    year_scope = raw_df_melted['Year'].unique()

    # Remove data according to the requirement "rez_req".
    target_value = 'Y'
    mask = rez_req == target_value  # Find cells that equal to the target value 'Y'.
    cell_positions = mask.stack()  # convert DataFrame to MultiIndex Series
    keep_list = cell_positions[cell_positions].index.tolist()  # filter True
    keep_df = pd.DataFrame(keep_list, columns=['Technology', 'State'])

    # Check lower case and upper case.
    keep_df.loc[:, 'tech_lower'] = keep_df['Technology'].astype(str).str.lower()
    keep_df.loc[:, 'state_lower'] = keep_df['State'].astype(str).str.lower()
    raw_df_melted.loc[:, 'tech_lower'] = raw_df_melted['Technology'].astype(str).str.lower()
    raw_df_melted.loc[:, 'state_lower'] = raw_df_melted['State'].astype(str).str.lower()
    raw_df_melted = raw_df_melted.drop(['Technology', 'State'], axis=1)

    # Only keep data in "rez_req", and give nan if data doesn't exist.
    merged_df = keep_df.merge(raw_df_melted, on=['tech_lower', 'state_lower'], how='left', indicator=True)


    # If no data matches req, then create nan for every year in the scope.
    merged_df.loc[merged_df['_merge'] == 'left_only', [
        'Data_source', 'Scenario_1', 'Variable'
    ]] = [data_source, scenario_1, "capacity"]
    slice_df = merged_df[merged_df['_merge'] == 'left_only']
    new_rows = []
    for _, row in slice_df.iterrows():
        for year in year_scope:
            new_row = row.copy()
            new_row['Year'] = year
            new_rows.append(new_row)
    new_rows_df = pd.DataFrame(new_rows)
    merged_df = merged_df.drop(slice_df.index)
    merged_df = pd.concat([merged_df, new_rows_df], ignore_index=True)

    raw_df_melted = merged_df[
        ['Data_source', 'Scenario_1', 'Scenario_2', 'State', 'Region', 'Technology', 'Year', 'Variable', 'Value']
    ].copy()

    temp_raw_df = raw_df_melted

    print("    Processing Rez Generation Data")
    # --------------------------------------
    # Read "variable = generation" raw data
    raw_df = pd.read_excel(os.path.join(BASE_DIR, RAW_DATA_DIR, file_name),
                           sheet_name='REZ Generation',
                           skiprows=2)
    raw_df = raw_df.dropna(how='all')

    raw_df.insert(0, 'Data_source', data_source)
    raw_df.insert(1, 'Scenario_1', scenario_1)
    raw_df.rename({'Region': 'State', 'REZ': 'Region', 'CDP': 'Scenario_2'},
                  axis=1, inplace=True)
    raw_df.drop(['REZ Name'], axis=1, inplace=True)
    raw_df.loc[raw_df['Technology'] == 'Solar', 'Technology'] = \
        'Utility-scale Solar'
    raw_df_melted = pd.melt(raw_df, id_vars=['Data_source', 'Scenario_1',
                                             'Scenario_2', 'State', 'Region',
                                             'Technology'], var_name='Year',
                            value_name='Value')
    raw_df_melted.insert(raw_df_melted.shape[1] - 1, 'Variable', 'generation')
    if data_source == "2024 Final ISP":
        raw_df_melted.loc[:, 'Year'] = raw_df_melted['Year'].apply(lambda x: x[:2] + x[-2:])
    year_scope = raw_df_melted['Year'].unique()

    # Remove data according to the requirement "rez_req".
    target_value = 'Y'
    mask = rez_req == target_value  # Find cells that equal to the target value 'Y'.
    cell_positions = mask.stack()  # convert DataFrame to MultiIndex Series
    keep_list = cell_positions[cell_positions].index.tolist()  # filter True
    keep_df = pd.DataFrame(keep_list, columns=['Technology', 'State'])

    # Check lower case and upper case.
    keep_df.loc[:, 'tech_lower'] = keep_df['Technology'].astype(str).str.lower()
    keep_df.loc[:, 'state_lower'] = keep_df['State'].astype(str).str.lower()
    raw_df_melted.loc[:, 'tech_lower'] = raw_df_melted['Technology'].astype(str).str.lower()
    raw_df_melted.loc[:, 'state_lower'] = raw_df_melted['State'].astype(str).str.lower()
    raw_df_melted = raw_df_melted.drop(['Technology', 'State'], axis=1)

    # Only keep data in "rez_req", and give nan if data doesn't exist.
    merged_df = keep_df.merge(raw_df_melted, on=['tech_lower', 'state_lower'], how='left', indicator=True)

    # If no data matches req, then create nan for every year in the scope.
    merged_df.loc[merged_df['_merge'] == 'left_only', [
        'Data_source', 'Scenario_1', 'Variable'
    ]] = [data_source, scenario_1, "generation"]
    slice_df = merged_df[merged_df['_merge'] == 'left_only']
    new_rows = []
    for _, row in slice_df.iterrows():
        for year in year_scope:
            new_row = row.copy()
            new_row['Year'] = year
            new_rows.append(new_row)
    new_rows_df = pd.DataFrame(new_rows)
    merged_df = merged_df.drop(slice_df.index)
    merged_df = pd.concat([merged_df, new_rows_df], ignore_index=True)

    raw_df_melted = merged_df[
        ['Data_source', 'Scenario_1', 'Scenario_2', 'State', 'Region', 'Technology', 'Year', 'Variable', 'Value']
    ].copy()

    raw_df = pd.concat([temp_raw_df, raw_df_melted], ignore_index=True)
    temp_raw_df = raw_df
    no_summary_df = raw_df

    # print(no_summary_df[~no_summary_df['Scenario_2'].isnull()])

    print("    Processing Capacity Summary Data")
    # --------------------------------------
    # Read data input requirement.
    state_capacity_req = pd.read_csv(os.path.join(
        BASE_DIR, "input_csv", "data_req_state_capacity.csv"
    ), index_col=0)

    # add summary capacity data
    raw_df = pd.read_excel(os.path.join(BASE_DIR, RAW_DATA_DIR, file_name),
                           sheet_name='Capacity',
                           skiprows=2)
    raw_df = raw_df.dropna(how='all')
    raw_df.rename({'Region': 'State', 'CDP': 'Scenario_2'},
                  axis=1, inplace=True)
    if data_source == "2024 Final ISP":
        raw_df.rename({'Subregion': 'Region'}, axis=1, inplace=True)

    raw_df.insert(0, 'Data_source', data_source)
    raw_df.insert(1, 'Scenario_1', scenario_1)
    new_column = pd.Series([np.nan] * len(raw_df), dtype="object")
    if 'Region' not in raw_df.columns:
        raw_df.insert(4, "Region", new_column)

    raw_df_melted = pd.melt(raw_df, id_vars=['Data_source', 'Scenario_1',
                                             'Scenario_2', 'State', 'Region',
                                             'Technology'], var_name='Year',
                            value_name='Value')
    raw_df_melted.insert(raw_df_melted.shape[1] - 1, 'Variable', 'capacity')
    if data_source == "2024 Final ISP":
        raw_df_melted.loc[:, 'Year'] = raw_df_melted['Year'].apply(lambda x: x[:2] + x[-2:])
    year_scope = raw_df_melted['Year'].unique()

    # Exclude existing data from summary.
    region_sum = no_summary_df[no_summary_df['Variable'] == 'capacity'].groupby([
        'Data_source', 'Scenario_1', 'Scenario_2', 'State', 'Technology', 'Year'
    ])['Value'].sum().reset_index()
    region_sum.rename(columns={'Value': 'Region_Sum'}, inplace=True)
    state_total = raw_df_melted[[
        'Data_source', 'Scenario_1', 'Scenario_2', 'State', 'Technology', 'Year', 'Value'
    ]].reset_index(drop=True)
    state_total.rename(columns={'Value': 'State_Total'}, inplace=True)

    # Check lower case and upper case.
    region_sum.loc[:, 'tech_lower'] = region_sum['Technology'].astype(str).str.lower()
    region_sum.loc[:, 'state_lower'] = region_sum['State'].astype(str).str.lower()
    state_total.loc[:, 'tech_lower'] = state_total['Technology'].astype(str).str.lower()
    state_total.loc[:, 'state_lower'] = state_total['State'].astype(str).str.lower()
    region_sum = region_sum.drop(['Technology', 'State'], axis=1)

    merged_df = pd.merge(state_total, region_sum, on=[
        'Data_source', 'Scenario_1', 'Scenario_2', 'state_lower', 'tech_lower', 'Year'
    ], how='left')
    merged_df['Variable'] = 'capacity'
    merged_df['Region_Sum'] = merged_df['Region_Sum'].fillna(0)
    merged_df.loc[:, 'Unaccounted_Value'] = merged_df['State_Total'] - merged_df['Region_Sum']

    existing_techs = no_summary_df['Technology'].unique()
    existing_techs = [item.lower() for item in existing_techs]

    merged_df['Region_1'] = np.where(
        merged_df['tech_lower'].isin(existing_techs),
        merged_df['State'].str[0] + "0",
        np.nan
    )

    merged_df['Value'] = merged_df['Unaccounted_Value']
    merged_df['Region'] = merged_df['Region_1']
    final_df = merged_df[
        ['Data_source', 'Scenario_1', 'Scenario_2', 'State', 'Region', 'Technology', 'Year', 'Variable', 'Value']
    ].copy()

    # Remove data according to the requirement "state_capacity_req".
    target_value = 'Y'
    mask = state_capacity_req == target_value  # Find cells that equal to the target value 'Y'.
    cell_positions = mask.stack()  # convert DataFrame to MultiIndex Series
    keep_list = cell_positions[cell_positions].index.tolist()  # filter True
    keep_df = pd.DataFrame(keep_list, columns=['Technology', 'State'])

    # Check lower case and upper case.
    keep_df.loc[:, 'tech_lower'] = keep_df['Technology'].astype(str).str.lower()
    keep_df.loc[:, 'state_lower'] = keep_df['State'].astype(str).str.lower()
    final_df.loc[:, 'tech_lower'] = final_df['Technology'].astype(str).str.lower()
    final_df.loc[:, 'state_lower'] = final_df['State'].astype(str).str.lower()
    final_df = final_df.drop(['Technology', 'State'], axis=1)

    # Only keep data in "state_capacity_req", and give nan if data doesn't exist.
    merged_df = keep_df.merge(final_df, on=['tech_lower', 'state_lower'], how='left', indicator=True)

    # If no data matches req, then create nan for every year in the scope.
    merged_df.loc[merged_df['_merge'] == 'left_only', [
        'Data_source', 'Scenario_1', 'Variable'
    ]] = [data_source, scenario_1, "capacity"]
    slice_df = merged_df[merged_df['_merge'] == 'left_only']
    new_rows = []
    for _, row in slice_df.iterrows():
        for year in year_scope:
            new_row = row.copy()
            new_row['Year'] = year
            new_rows.append(new_row)
    new_rows_df = pd.DataFrame(new_rows)
    merged_df = merged_df.drop(slice_df.index)
    merged_df = pd.concat([merged_df, new_rows_df], ignore_index=True)

    final_df = merged_df[
        ['Data_source', 'Scenario_1', 'Scenario_2', 'State', 'Region', 'Technology', 'Year', 'Variable', 'Value']
    ].copy()

    # check_req is no longer working here
    ##################################################################################
    existing_techs = no_summary_df['Technology'].unique()
    final_df = pd.concat([final_df[final_df['Technology'].isin(existing_techs)],
                          raw_df_melted[~raw_df_melted['Technology'].isin(existing_techs)]], ignore_index=True)

    raw_df = pd.concat([temp_raw_df, final_df], ignore_index=True)
    temp_raw_df = raw_df

    print("    Processing Generation Summary Data")
    # --------------------------------------
    # Read data input requirement.
    state_generation_req = pd.read_csv(os.path.join(
        BASE_DIR, "input_csv", "data_req_state_generation.csv"
    ), index_col=0)

    # add summary generation data
    raw_df = pd.read_excel(os.path.join(BASE_DIR, RAW_DATA_DIR, file_name),
                           sheet_name='Generation',
                           skiprows=2)
    raw_df = raw_df.dropna(how='all')

    raw_df.rename({'Region': 'State', 'CDP': 'Scenario_2'},
                  axis=1, inplace=True)
    if data_source == "2024 Final ISP":
        raw_df.rename({'Subregion': 'Region'}, axis=1, inplace=True)

    raw_df.insert(0, 'Data_source', data_source)
    raw_df.insert(1, 'Scenario_1', scenario_1)

    # Specify the data type of Region.
    new_column = pd.Series([np.nan] * len(raw_df), dtype="object")
    if 'Region' not in raw_df.columns:
        raw_df.insert(4, "Region", new_column)

    raw_df_melted = pd.melt(raw_df, id_vars=['Data_source', 'Scenario_1',
                                             'Scenario_2', 'State', 'Region',
                                             'Technology'], var_name='Year',
                            value_name='Value')
    raw_df_melted.insert(raw_df_melted.shape[1] - 1, 'Variable', 'generation')
    if data_source == "2024 Final ISP":
        raw_df_melted.loc[:, 'Year'] = raw_df_melted['Year'].apply(lambda x: x[:2] + x[-2:])
    year_scope = raw_df_melted['Year'].unique()

    # Exclude existing data from summary.
    region_sum = no_summary_df[no_summary_df['Variable'] == 'generation'].groupby([
        'Data_source', 'Scenario_1', 'Scenario_2', 'State', 'Technology', 'Year'
    ])['Value'].sum().reset_index()
    region_sum.rename(columns={'Value': 'Region_Sum'}, inplace=True)
    state_total = raw_df_melted[[
        'Data_source', 'Scenario_1', 'Scenario_2', 'State', 'Technology', 'Year', 'Value'
    ]].reset_index(drop=True)
    state_total.rename(columns={'Value': 'State_Total'}, inplace=True)

    # Check lower case and upper case.
    region_sum.loc[:, 'tech_lower'] = region_sum['Technology'].astype(str).str.lower()
    region_sum.loc[:, 'state_lower'] = region_sum['State'].astype(str).str.lower()
    state_total.loc[:, 'tech_lower'] = state_total['Technology'].astype(str).str.lower()
    state_total.loc[:, 'state_lower'] = state_total['State'].astype(str).str.lower()
    region_sum = region_sum.drop(['Technology', 'State'], axis=1)

    merged_df = pd.merge(state_total, region_sum, on=[
        'Data_source', 'Scenario_1', 'Scenario_2', 'state_lower', 'tech_lower', 'Year'
    ], how='left')

    merged_df['Variable'] = 'generation'
    merged_df['Region_Sum'] = merged_df['Region_Sum'].fillna(0)
    merged_df.loc[:, 'Unaccounted_Value'] = merged_df['State_Total'] - merged_df['Region_Sum']

    existing_techs = no_summary_df['Technology'].unique()
    existing_techs = [item.lower() for item in existing_techs]

    merged_df['Region_1'] = np.where(
        merged_df['tech_lower'].isin(existing_techs),
        merged_df['State'].str[0] + "0",
        np.nan
    )

    merged_df['Value'] = merged_df['Unaccounted_Value']
    merged_df['Region'] = merged_df['Region_1']
    final_df = merged_df[
        ['Data_source', 'Scenario_1', 'Scenario_2', 'State', 'Region', 'Technology', 'Year', 'Variable', 'Value']
    ].copy()

    # Remove data according to the requirement "state_generation_req".
    target_value = 'Y'
    mask = state_generation_req == target_value  # Find cells that equal to the target value 'Y'.
    cell_positions = mask.stack()  # convert DataFrame to MultiIndex Series
    keep_list = cell_positions[cell_positions].index.tolist()  # filter True
    keep_df = pd.DataFrame(keep_list, columns=['Technology', 'State'])

    # Check lower case and upper case.
    keep_df.loc[:, 'tech_lower'] = keep_df['Technology'].astype(str).str.lower()
    keep_df.loc[:, 'state_lower'] = keep_df['State'].astype(str).str.lower()
    final_df.loc[:, 'tech_lower'] = final_df['Technology'].astype(str).str.lower()
    final_df.loc[:, 'state_lower'] = final_df['State'].astype(str).str.lower()
    final_df = final_df.drop(['Technology', 'State'], axis=1)

    # Only keep data in "state_generation_req", and give nan if data doesn't exist.
    merged_df = keep_df.merge(final_df, on=['tech_lower', 'state_lower'], how='left', indicator=True)

    # If no data matches req, then create nan for every year in the scope.
    merged_df.loc[merged_df['_merge'] == 'left_only', [
        'Data_source', 'Scenario_1', 'Variable'
    ]] = [data_source, scenario_1, "generation"]
    slice_df = merged_df[merged_df['_merge'] == 'left_only']
    new_rows = []
    for _, row in slice_df.iterrows():
        for year in year_scope:
            new_row = row.copy()
            new_row['Year'] = year
            new_rows.append(new_row)
    new_rows_df = pd.DataFrame(new_rows)
    merged_df = merged_df.drop(slice_df.index)
    merged_df = pd.concat([merged_df, new_rows_df], ignore_index=True)

    final_df = merged_df[
        ['Data_source', 'Scenario_1', 'Scenario_2', 'State', 'Region', 'Technology', 'Year', 'Variable', 'Value']
    ].copy()

    # check_req is no longer working here
    ##################################################################################
    existing_techs = no_summary_df['Technology'].unique()
    final_df = pd.concat([final_df[final_df['Technology'].isin(existing_techs)],
                          raw_df_melted[~raw_df_melted['Technology'].isin(existing_techs)]], ignore_index=True)

    raw_df = pd.concat([temp_raw_df, final_df], ignore_index=True)
    temp_raw_df = raw_df

    print("    Processing Storage Capacity Data")
    # --------------------------------------
    # Read Storage Capacity table from the spreadsheet
    raw_df = pd.read_excel(os.path.join(BASE_DIR, RAW_DATA_DIR, file_name),
                           sheet_name='Storage Capacity',
                           skiprows=2)

    raw_df = raw_df.dropna(how='all')
    raw_df.rename({'Region': 'State', 'CDP': 'Scenario_2',
                   'Storage category': 'Technology', 'storage category': 'Technology'},
                  axis=1, inplace=True)
    if data_source == "2024 Final ISP":
        raw_df.rename({'Subregion': 'Region'}, axis=1, inplace=True)


    raw_df.insert(0, 'Data_source', data_source)
    raw_df.insert(1, 'Scenario_1', scenario_1)

    # Specify the data type of Region.
    new_column = pd.Series([np.nan] * len(raw_df), dtype="object")
    if 'Region' not in raw_df.columns:
        raw_df.insert(4, "Region", new_column)

    raw_df_melted = pd.melt(raw_df, id_vars=['Data_source', 'Scenario_1',
                                             'Scenario_2', 'State', 'Region',
                                             'Technology'], var_name='Year',
                            value_name='Value')
    raw_df_melted.insert(raw_df_melted.shape[1] - 1, 'Variable', 'storage capacity')
    if data_source == "2024 Final ISP":
        raw_df_melted.loc[:, 'Year'] = raw_df_melted['Year'].apply(lambda x: x[:2] + x[-2:])
    year_scope = raw_df_melted['Year'].unique()

    # Read data input requirement.
    state_storage_req = pd.read_csv(os.path.join(
        BASE_DIR, "input_csv", "data_req_state_storage.csv"
    ), index_col=0)

    # Remove data according to the requirement "state_storage_req".
    target_value = 'Y'
    mask = state_storage_req == target_value  # Find cells that equal to the target value 'Y'.
    cell_positions = mask.stack()  # convert DataFrame to MultiIndex Series
    keep_list = cell_positions[cell_positions].index.tolist()  # filter True
    keep_df = pd.DataFrame(keep_list, columns=['Technology', 'State'])

    # Check lower case and upper case.
    keep_df.loc[:, 'tech_lower'] = keep_df['Technology'].astype(str).str.lower()
    keep_df.loc[:, 'state_lower'] = keep_df['State'].astype(str).str.lower()
    raw_df_melted.loc[:, 'tech_lower'] = raw_df_melted['Technology'].astype(str).str.lower()
    raw_df_melted.loc[:, 'state_lower'] = raw_df_melted['State'].astype(str).str.lower()
    raw_df_melted = raw_df_melted.drop(['Technology', 'State'], axis=1)

    # Only keep data in "state_storage_req", and give nan if data doesn't exist.
    merged_df = keep_df.merge(raw_df_melted, on=['tech_lower', 'state_lower'], how='left', indicator=True)

    # If no data matches req, then create nan for every year in the scope.
    merged_df.loc[merged_df['_merge'] == 'left_only', [
        'Data_source', 'Scenario_1', 'Variable'
    ]] = [data_source, scenario_1, "storage capacity"]
    slice_df = merged_df[merged_df['_merge'] == 'left_only']
    new_rows = []
    for _, row in slice_df.iterrows():
        for year in year_scope:
            new_row = row.copy()
            new_row['Year'] = year
            new_rows.append(new_row)
    new_rows_df = pd.DataFrame(new_rows)
    merged_df = merged_df.drop(slice_df.index)
    merged_df = pd.concat([merged_df, new_rows_df], ignore_index=True)

    raw_df_melted = merged_df[
        ['Data_source', 'Scenario_1', 'Scenario_2', 'State', 'Region', 'Technology', 'Year', 'Variable', 'Value']
    ].copy()

    raw_df = pd.concat([temp_raw_df, raw_df_melted], ignore_index=True)
    temp_raw_df = raw_df

    print("    Processing Storage Energy Data")
    # --------------------------------------
    # Read Storage Energy table from the spreadsheet
    raw_df = pd.read_excel(os.path.join(BASE_DIR, RAW_DATA_DIR, file_name),
                           sheet_name='Storage Energy',
                           skiprows=2)

    raw_df = raw_df.dropna(how='all')
    raw_df.rename({'Region': 'State', 'CDP': 'Scenario_2'},
                  axis=1, inplace=True)
    if data_source == "2024 Final ISP":
        raw_df.rename({'Subregion': 'Region'}, axis=1, inplace=True)


    raw_df.insert(0, 'Data_source', data_source)
    raw_df.insert(1, 'Scenario_1', scenario_1)

    # Specify the data type of Region.
    new_column = pd.Series([np.nan] * len(raw_df), dtype="object")
    if 'Region' not in raw_df.columns:
        raw_df.insert(4, "Region", new_column)

    raw_df_melted = pd.melt(raw_df, id_vars=['Data_source', 'Scenario_1',
                                             'Scenario_2', 'State', 'Region',
                                             'Technology'], var_name='Year',
                            value_name='Value')
    raw_df_melted.insert(raw_df_melted.shape[1] - 1, 'Variable', 'storage energy')
    if data_source == "2024 Final ISP":
        raw_df_melted.loc[:, 'Year'] = raw_df_melted['Year'].apply(lambda x: x[:2] + x[-2:])
    year_scope = raw_df_melted['Year'].unique()

    # Read data input requirement.
    state_storage_req = pd.read_csv(os.path.join(
        BASE_DIR, "input_csv", "data_req_state_storage.csv"
    ), index_col=0)

    # Remove data according to the requirement "state_storage_req".
    target_value = 'Y'
    mask = state_storage_req == target_value  # Find cells that equal to the target value 'Y'.
    cell_positions = mask.stack()  # convert DataFrame to MultiIndex Series
    keep_list = cell_positions[cell_positions].index.tolist()  # filter True
    keep_df = pd.DataFrame(keep_list, columns=['Technology', 'State'])

    # Check lower case and upper case.
    keep_df.loc[:, 'tech_lower'] = keep_df['Technology'].astype(str).str.lower()
    keep_df.loc[:, 'state_lower'] = keep_df['State'].astype(str).str.lower()
    raw_df_melted.loc[:, 'tech_lower'] = raw_df_melted['Technology'].astype(str).str.lower()
    raw_df_melted.loc[:, 'state_lower'] = raw_df_melted['State'].astype(str).str.lower()
    raw_df_melted = raw_df_melted.drop(['Technology', 'State'], axis=1)

    # Only keep data in "state_storage_req", and give nan if data doesn't exist.
    merged_df = keep_df.merge(raw_df_melted, on=['tech_lower', 'state_lower'], how='left', indicator=True)

    # If no data matches req, then create nan for every year in the scope.
    merged_df.loc[merged_df['_merge'] == 'left_only', [
        'Data_source', 'Scenario_1', 'Variable'
    ]] = [data_source, scenario_1, "storage energy"]
    slice_df = merged_df[merged_df['_merge'] == 'left_only']
    new_rows = []
    for _, row in slice_df.iterrows():
        for year in year_scope:
            new_row = row.copy()
            new_row['Year'] = year
            new_rows.append(new_row)
    new_rows_df = pd.DataFrame(new_rows)
    merged_df = merged_df.drop(slice_df.index)
    merged_df = pd.concat([merged_df, new_rows_df], ignore_index=True)

    raw_df_melted = merged_df[
        ['Data_source', 'Scenario_1', 'Scenario_2', 'State', 'Region', 'Technology', 'Year', 'Variable', 'Value']
    ].copy()
    raw_df = pd.concat([temp_raw_df, raw_df_melted], ignore_index=True)
    final_raw_df = raw_df

    return final_raw_df


def insert_data(conn, final_raw_df):
    print("    -------------------------------")
    print("    Inserting data into the database")
    print("    -------------------------------")
    # --------------------------------------
    # Insert data to corresponding tables in the database.

    # Extract context table in the database
    context_cols = ['Data_source', 'Scenario_1', 'Scenario_2', 'State',
                    'Region', 'Technology']
    context_df = final_raw_df[context_cols].drop_duplicates().reset_index(drop=True)
    cursor = conn.cursor()
    cursor.execute('SELECT MAX(Id) FROM context')
    max_id = cursor.fetchone()[0]
    if max_id is None:
        max_id = 0

    context_df['Id'] = context_df.index + max_id + 1
    context_df = context_df[['Id'] + context_cols]

    # Insert data into context table
    context_df.to_sql('context', conn, if_exists='append', index=False)

    print("    Table 'context' Inserted")
    # --------------------------------------
    # Extract data table in the database
    data_df = pd.merge(final_raw_df, context_df, on=context_cols, how='left')
    data_df = data_df[['Id', 'Variable', 'Year', 'Value']]
    # Insert records into the data table
    data_df.to_sql('data', conn, if_exists='append', index=False)

    print("    Table 'data' Inserted")
    # --------------------------------------
    # Insert data into mapping table
    attribute_type = ['Scenario_1', 'Scenario_2', 'State',
                    'Region', 'Technology']
    for item in attribute_type:
        mapping_df = context_df[['Data_source', item]].drop_duplicates().reset_index(drop=True)
        mapping_df['Attribute_type'] = item
        mapping_df.rename({item: 'Original_value'}, axis=1, inplace=True)
        mapping_df['Standard_value'] = mapping_df['Original_value']
        mapping_df = mapping_df[['Data_source', 'Attribute_type', 'Original_value', 'Standard_value']].dropna()
        mapping_df = mapping_df.dropna(subset=['Original_value'])
        for _, row in mapping_df.iterrows():
            conn.execute('''
                INSERT OR IGNORE INTO mapping (Data_source, Attribute_type, Original_value, Standard_value)
                VALUES (?, ?, ?, ?)
            ''', (row['Data_source'], row['Attribute_type'], row['Original_value'], row['Standard_value']))

        conn.commit()
    print("    Table 'mapping' Inserted")


def drop_table(conn):
    cursor = conn.cursor()

    # Drop all existing tables.
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table[0]}")
        print(f"Dropped table: {table[0]}")


def check_negative(df, conf):
    df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
    result = df[(~df['Technology'].isin(conf['exception']['negative']['ignore_tech'])) &
                (df['Value'] < float(conf['exception']['negative']['threshold']))]
    result.insert(0, 'Exception Type', 'negative')
    region_list = ['Q0', 'N0', 'T0', 'V0', 'S0']
    result.insert(1, 'Data Type', np.where(result['Region'].isin(region_list), 'synthetic', 'raw'))
    return result


def main():
    global RAW_DATA_DIR

    total_start_time = time.perf_counter()

    # The path of database.
    db_path = os.path.join(BASE_DIR, "ISP.db")

    # Create database connection
    conn = sqlite3.connect(db_path)

    """
    Enable here
    """
    drop_table(conn)
    create_table(conn)

    file_count = 1

    conf = config(os.path.join(BASE_DIR, "create_db_config.yml"))

    # --------------------------------------
    # Read 2024 Final
    RAW_DATA_DIR = "data/2024 Final"
    files = os.listdir(os.path.join(BASE_DIR, RAW_DATA_DIR))

    # Build database.
    for file in files:
        start_time = time.perf_counter()
        print("===================================")
        # Extract data source and scenario 1
        data_source = "2024 Final ISP"
        scenario_1 = file[11:-5]
        print("Processing File " + str(file_count) + ": " + data_source + " - " + scenario_1)
        print("-----------------------------------")

        # target = os.path.join("C:\\Users\\Dr. Season\\Desktop\\programming\\shaochen_db\\exception",
        #                                    "2024 Final")
        #
        # if not os.path.exists(target):
        #     os.makedirs(target)
        df = extract_data(file, data_source, scenario_1)

        # Check exception here
        # exception_df = check_negative(df, conf)
        # if not exception_df.empty:
        #     exception_df.to_csv(os.path.join(target, data_source + " - " + scenario_1 + ".csv"), index=False)

        insert_data(conn, df)

        file_count += 1

        end_time = time.perf_counter()
        execution_time = end_time - start_time
        minutes, seconds = divmod(execution_time, 60)
        print(f"Execution time: {int(minutes)} minutes and {seconds:.2f} seconds")
        execution_time = end_time - total_start_time
        minutes, seconds = divmod(execution_time, 60)
        print(f"Total Execution time: {int(minutes)} minutes and {seconds:.2f} seconds")

    # --------------------------------------
    # Read 2024 draft
    RAW_DATA_DIR = "data/2024 Draft"
    files = os.listdir(os.path.join(BASE_DIR, RAW_DATA_DIR))

    # Build database.
    for file in files:
        start_time = time.perf_counter()
        print("===================================")
        # Extract data source and scenario 1
        data_source = "2024 Draft ISP"
        scenario_1 = file[34:-5]
        print("Processing File " + str(file_count) + ": " + data_source + " - " + scenario_1)
        print("-----------------------------------")

        # target = os.path.join("C:\\Users\\Dr. Season\\Desktop\\programming\\shaochen_db\\exception",
        #                       "2024 Draft")
        #
        # if not os.path.exists(target):
        #     os.makedirs(target)
        df = extract_data(file, data_source, scenario_1)
        # exception_df = check_negative(df, conf)
        # if not exception_df.empty:
        #     exception_df.to_csv(os.path.join(target, data_source + " - " + scenario_1 + ".csv"), index=False)
        insert_data(conn, df)

        file_count += 1

        end_time = time.perf_counter()
        execution_time = end_time - start_time
        minutes, seconds = divmod(execution_time, 60)
        print(f"Execution time: {int(minutes)} minutes and {seconds:.2f} seconds")
        execution_time = end_time - total_start_time
        minutes, seconds = divmod(execution_time, 60)
        print(f"Total Execution time: {int(minutes)} minutes and {seconds:.2f} seconds")

    # --------------------------------------
    # Read 2022 Draft
    RAW_DATA_DIR = "data/2022 Draft"
    files = os.listdir(os.path.join(BASE_DIR, RAW_DATA_DIR))

    # Build database.
    for file in files:
        start_time = time.perf_counter()
        print("===================================")
        # Extract data source and scenario 1
        data_source = "2022 Draft ISP"
        scenario_1 = file[34:-5]
        print("Processing File " + str(file_count) + ": " + data_source + " - " + scenario_1)
        print("-----------------------------------")

        # target = os.path.join("C:\\Users\\Dr. Season\\Desktop\\programming\\shaochen_db\\exception",
        #                       "2022 Draft")
        #
        # if not os.path.exists(target):
        #     os.makedirs(target)
        df = extract_data(file, data_source, scenario_1)
        # exception_df = check_negative(df, conf)
        # if not exception_df.empty:
        #     exception_df.to_csv(os.path.join(target, data_source + " - " + scenario_1 + ".csv"), index=False)
        insert_data(conn, df)

        file_count += 1

        end_time = time.perf_counter()
        execution_time = end_time - start_time
        minutes, seconds = divmod(execution_time, 60)
        print(f"Execution time: {int(minutes)} minutes and {seconds:.2f} seconds")
        execution_time = end_time - total_start_time
        minutes, seconds = divmod(execution_time, 60)
        print(f"Total Execution time: {int(minutes)} minutes and {seconds:.2f} seconds")

    # --------------------------------------
    # Read 2022 Final
    RAW_DATA_DIR = "data/2022 Final"
    files = os.listdir(os.path.join(BASE_DIR, RAW_DATA_DIR))

    # Build database.
    for file in files:
        start_time = time.perf_counter()
        print("===================================")
        # Extract data source and scenario 1
        data_source = "2022 Final ISP"
        scenario_1 = file[34:-5]
        print("Processing File " + str(file_count) + ": " + data_source + " - " + scenario_1)
        print("-----------------------------------")

        # target = os.path.join("C:\\Users\\Dr. Season\\Desktop\\programming\\shaochen_db\\exception",
        #                       "2022 Final")
        #
        # if not os.path.exists(target):
        #     os.makedirs(target)
        df = extract_data(file, data_source, scenario_1)
        # exception_df = check_negative(df, conf)
        # if not exception_df.empty:
        #     exception_df.to_csv(os.path.join(target, data_source + " - " + scenario_1 + ".csv"), index=False)
        insert_data(conn, df)

        file_count += 1

        end_time = time.perf_counter()
        execution_time = end_time - start_time
        minutes, seconds = divmod(execution_time, 60)
        print(f"Execution time: {int(minutes)} minutes and {seconds:.2f} seconds")
        execution_time = end_time - total_start_time
        minutes, seconds = divmod(execution_time, 60)
        print(f"Total Execution time: {int(minutes)} minutes and {seconds:.2f} seconds")

    # Close the connection
    conn.commit()
    conn.close()

    total_end_time = time.perf_counter()
    total_execution_time = total_end_time - total_start_time
    minutes, seconds = divmod(total_execution_time, 60)
    print("===================================")
    print(f"Total Execution time: {int(minutes)} minutes and {seconds:.2f} seconds")

if __name__ == "__main__":
    main()
