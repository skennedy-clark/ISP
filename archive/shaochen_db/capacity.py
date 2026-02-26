import pandas as pd
import os
import yaml
import sys
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))

def config(filePath):
    with open(filePath, 'r') as f:
        data = yaml.safe_load(f)
    return data

def connectToDB(data):
    conn = sqlite3.connect(os.path.join(BASE_DIR, data['database_file']))
    return conn

def parse_config(data):
    region_map = pd.read_csv(os.path.join(BASE_DIR, data['region_map']))
    technology_map = pd.read_csv(os.path.join(BASE_DIR, data['technology_map']), index_col=[0], header=[0, 1])
    return {'region': region_map, 'technology': technology_map}

def populate_input_files(data, mappings, conn, input_year, output_year):
    # get is there any 1 in each line
    region_mask = mappings['region'].eq('1').any(axis=1)
    # if there is 1 in a line, get its Zone name
    selected_regions = mappings['region'][region_mask]['Zone']
    # get a column if there is a "1" in this column
    state_mask = mappings['region'].columns[mappings['region'].eq('1').any(axis=0)]
    selected_states = mappings['region'][state_mask].iloc[0, :]
    df = pd.read_sql_query(sql="SELECT State, Region, Technology, Value "
                               "FROM context "
                               "LEFT JOIN data ON context.Id = data.Id "
                               "WHERE (context.Region IN {} OR context.State IN {}) "
                               "AND context.Scenario_2 = '{}' AND context.Scenario_1 = '{}' "
                               "AND context.Data_source = '{}' AND data.Variable = '{}' "
                               "AND data.Year = '{}';".format(tuple(selected_regions), tuple(selected_states),
                                                              data['scenario_2'], data['scenario_1'], data['data_source'],
                                                              data['variable'], input_year), con=conn)
    print("Data Extracted from DB:")
    pd.set_option('display.max_rows', None)
    print(df)
    #make the technology names in the DB match with the names in the technology map
    for col in mappings['technology'].columns:
        # add one more column 'Resource'
        mappings['technology'].loc[mappings['technology'][mappings['technology'][col].eq(1)].index, 'Resource'] = col[1]
    # left join for "technology" in df == "Resource" in technology
    df = df.set_index('Technology').join(mappings['technology'].droplevel('Resource', axis=1)['Resource'], how='left').reset_index()
    #do the mapping of the region names - append the number of the region, if it is state level then just the state
    # add name together for region na and region notna
    df.loc[df['Region'].notna(), 'Resource'] = df[df['Region'].notna()].agg(lambda x: f"{x['State']}_{str(x['Resource']).lower()}_{str(x['Region'])[-1]}", axis=1)
    df.loc[df['Region'].isna(), 'Resource'] = df[df['Region'].isna()].agg(lambda x: f"{x['State']}_{str(x['Resource']).lower()}", axis=1)
    print('CSVs created: ')

    if data['type'] == 'new':
        input_folder =  os.path.join(BASE_DIR, 'example_files/genx_inputs/{}/{}/resources/'.format(data['type'], output_year))
        #need to drop the Resource column we made earlier
        for filename in mappings['technology'].drop('Resource', axis=1, level=0).columns.get_level_values(0).unique():
            input_df = pd.read_csv(os.path.join(input_folder, filename + '.csv'))
            #find the technologies in that file
            technologies = mappings['technology'][mappings['technology'][mappings['technology'][filename] == 1].any(axis=1)][filename].index.to_list()
            df_subset = df[df['Technology'].isin(technologies)]
            #create a mapping of Resource, Value (for the input_year)
            mapping_dict = dict(zip(df_subset['Resource'], df_subset['Value']))
            for field in data['fields']:
                if data['missing_values'] == "Pass":
                    input_df['temp' + field] = input_df['Resource'].map(mapping_dict)
                    with pd.option_context("future.no_silent_downcasting", True):
                        input_df['temp' + field] = input_df['temp' + field].fillna(input_df[field]).infer_objects(copy=False)
                    input_df[field] = input_df['temp' + field]
                    input_df = input_df.drop('temp' + field, axis=1)
                elif data['missing_values'] == "None":
                    input_df[field] = input_df['Resource'].map(mapping_dict)
                else:
                    raise Exception('Missing Resources in input')
            #this is just testing, I assume we'd want to replace the file
            input_df.to_csv(os.path.join(input_folder, filename + '1.csv'))
            print(os.path.join(input_folder, filename + '1.csv'))
    else:
        input_folder =  os.path.join(BASE_DIR, 'example_files/genx_inputs/{}/{}/'.format(data['type'], output_year))
        input_df = pd.read_csv(os.path.join(input_folder, 'Generators_data.csv'))
        #find the technologies in that file
        technologies = mappings['technology'].index
        df_subset = df[df['Technology'].isin(technologies)]
        #create a mapping of Resource, Value (for the input_year)
        mapping_dict = dict(zip(df_subset['Resource'], df_subset['Value']))
        for field in data['fields']:
            if data['missing_values'] == "Pass":
                input_df['temp' + field] = input_df['Resource'].map(mapping_dict)
                with pd.option_context("future.no_silent_downcasting", True):
                    input_df['temp' + field] = input_df['temp' + field].fillna(input_df[field]).infer_objects(copy=False)
                input_df[field] = input_df['temp' + field]
                input_df = input_df.drop('temp' + field, axis=1)
            elif data['missing_values'] == "None":
                input_df[field] = input_df['Resource'].map(mapping_dict)
            else:
                raise Exception('Missing Resources in input')
        input_df.to_csv(os.path.join(input_folder, 'Generators_data1.csv'))
        print(os.path.join(input_folder, 'Generators_data1.csv'))

def main():
    print('Initialising settings...')
    data = config(os.path.join(BASE_DIR, sys.argv[1]))
    print('Connecting to database...')
    conn = connectToDB(data)
    print('Reading in data...')
    mappings = parse_config(data)
    print('Generating files...')
    if data['years_mode'] == "config":
        for i in range(len(data['input_year'])):
            populate_input_files(data, mappings, conn, data['input_year'][i], data['output_year'][i])

if __name__ == "__main__":
    main()
