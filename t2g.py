import numpy as np
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import threading
from pathlib import Path
import uuid
import re
import hydra
from omegaconf import DictConfig, OmegaConf
from pathlib import Path

INVALID_ENTRY_LIST = ["0", None, "", 0, "not reported", "None", "none"]
output_dir = Path("./")

def config_parser_fn(config_name):
    """
    Takes the input yaml config file's name (& relative path). Returns all the extracted data
    :param config_name: file name (& relative path) for the YAML config file
    :returns:
        - db_server: string denoting database server (initial support only for mariadb)
        - db_name: name of the database you need to pull from
        - entity_node_sql_queries: list of sql queries used to define entity nodes
        - edge_entity_entity_sql_queries: list of sql queries to define edges of type entity nodes to entity nodes 
            & the names of edges
        - edge_entity_feature_values_sql_queries: list of sql queries to define edges of type entity node to feature 
            values & also the names of edges
    """
    input_cfg = None
    input_config_path = Path(config_name).absolute()

    config_name = input_config_path.name
    config_dir = input_config_path.parent

    with hydra.initialize_config_dir(config_dir=config_dir.__str__()):
        input_cfg = hydra.compose(config_name=config_name)

    # db_server used to distinguish between different databases
    db_server = None
    if "db_server" in input_cfg.keys():
        db_server = input_cfg["db_server"]
        # print(db_server)
    else:
        print("ERROR: db_server is not defined")
        exit(1)

    # db_name is the name of the database to pull the data from
    db_name = None
    if "db_server" in input_cfg.keys():
        db_name = input_cfg["db_name"]
        # print(db_name)
    else:
        print("ERROR: db_name is not defined")
        exit(1)
    
    # Getting all the entity nodes sql queries in a list
    entity_node_sql_queries = list()
    if "entity_node_queries" in input_cfg.keys():
        query_filepath = input_cfg["entity_node_queries"]
        file = open(query_filepath, 'r')
        entity_node_sql_queries = file.readlines()
        for i in range(len(entity_node_sql_queries)):
            # Removing the last '\n' character
            if (entity_node_sql_queries[i][-1] == '\n'):
                entity_node_sql_queries[i] = entity_node_sql_queries[i][:-1]
        # print(entity_node_sql_queries)
    else:
        print("ERROR: entity_node_queries is not defined")
        exit(1)

    # Getting all edge queries for edge type entity node to entity node
    edge_entity_entity_sql_queries = list()
    edge_entity_entity_rel_list = list()
    if "edges_entity_entity_queries" in input_cfg.keys():
        query_filepath = input_cfg["edges_entity_entity_queries"]
        file = open(query_filepath, 'r')
        # edge_entity_entity_sql_queries = file.readlines()
        read_lines = file.readlines()
        for i in range(len(read_lines)):
            # Removing the last '\n' character
            if (read_lines[i][-1] == '\n'):
                read_lines[i] = read_lines[i][:-1]
            
            # Adding the line to rel_list if even else its a query
            if (i % 2 == 0):
                edge_entity_entity_rel_list.append(read_lines[i])
            else:
                edge_entity_entity_sql_queries.append(read_lines[i])
        # print(edge_entity_entity_sql_queries)
    else:
        print("ERROR: edges_entity_entity_queries is not defined")
        exit(1)

    # Gettting all edge queries for edge type entity node to feature values
    edge_entity_feature_values_sql_queries = list()
    edge_entity_feature_values_rel_list = list()
    if "edges_entity_feature_values_queries" in input_cfg.keys():
        query_filepath = input_cfg["edges_entity_feature_values_queries"]
        file = open(query_filepath, 'r')
        read_lines = file.readlines()
        for i in range(len(read_lines)):
            # Removing the last '\n' character
            if (read_lines[i][-1] == '\n'):
                read_lines[i] = read_lines[i][:-1]
            
            # Adding the line to rel_list if even else its a query
            if (i % 2 == 0):
                edge_entity_feature_values_rel_list.append(read_lines[i])
            else:
                edge_entity_feature_values_sql_queries.append(read_lines[i])
        # print(edge_entity_feature_values_sql_queries)
    else:
        print("ERROR: edges_entity_feature_values_queries is not defined")
        exit(1)

    return db_server, db_name, entity_node_sql_queries, edge_entity_entity_sql_queries, edge_entity_entity_rel_list, edge_entity_feature_values_sql_queries, edge_entity_feature_values_rel_list

def connect_to_db(db_server, db_name):
    """
    Function takes db_server and db_name as the input. Tries to connect to the database and returns an object
    which can be used to execute queries.
    Assumption: default user: root, host: 127.0.0.1 and password:"". You will need to change code if otherwise
    :param db_server: The name of the backend database application used for accessing data
    :param db_name: The name of the database where the data resides
    :return cursor: Object that can be used to execute the database queries
    """
    if db_server == 'maria-db':
        try:
            cnx = mysql.connector.connect(user='root',
                                        password='mohil123',  # change password to your own
                                        host='127.0.0.1',
                                        database=db_name)
            cursor = cnx.cursor()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Incorrect user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Non-existing database")
            else:
                print(err)
    else:
        print('Other databases are currently not supported.')
    
    return cnx, cursor


def clean_token(token):
    token = str(token)
    token = token.strip().strip("\t.\'\" ")
    return token.lower()

def get_uuid(val):
    # return uuid.uuid5(uuid.NAMESPACE_DNS, str(val))
    return val

# TODO: Why do we lower case things before processing?
def entity_node_to_uuids(cursor, entity_queries_list):
    """
    Takes entity node queries as inputs, execute the queries, store the results in temp dataframes,
    then concatenate each entity node with its respective table name & column name, 
    convert each into uuid, and store the mapping in dictionary
    Assumption: Entries are case insentitive, i.e. BOB and bob are considered as duplicates
    """
    entity_mapping = pd.DataFrame()

    for i in range(len(entity_queries_list)):
        entity_query = entity_queries_list[i]

        # Executing Part
        cursor.execute(entity_query)
        result = pd.DataFrame(cursor)
        result.columns = cursor.column_names
        #print(f'result\n{result}')

        # extracting table and column names
        table_name = entity_query.split()[-1].rsplit(';')[0]  # table name of the query to execute
        col_name = str(result.columns[0]) # column name of the query

        result = result[~result.iloc[:, 0].isin(INVALID_ENTRY_LIST)] # cleaning invalid entries

        # concatenate each entity node with its respective table nam
        result[result.columns[0]] = table_name + '_' + col_name + '_' + result[result.columns[0]].map(str)
        # print(f'result\n{result}')

        result['uuid'] = ''
        result.columns = ['entity_node', 'uuid']

        result['entity_node'] = result['entity_node'].str.lower() # entries in lower case
        result = result.drop_duplicates() # removing duplicates

        # convert each entity node to uuid
        for index, row in result.iterrows():
            result.at[index, 'uuid'] = get_uuid(result.at[index, result.columns[0]])

        entity_mapping = pd.concat([entity_mapping, result])
    return entity_mapping.set_index('entity_node').to_dict()['uuid']

# Clean-up and Output
def post_processing(cursor, edge_entity_entity_queries_list, edge_entity_entity_rel_list, 
    edge_entity_feature_val_queries_list, edge_entity_feature_val_rel_list, entity_mapping):
    """
    Executes the given queries_list one by one, cleanses the data by removing duplicates,
    then replace the entity nodes with their respective UUIDs, and store the final result in a dataframe/.txt file
    """
    if (len(edge_entity_entity_queries_list) != len(edge_entity_entity_rel_list)):
        print("wrong list")
        exit(1)
    
    if (len(edge_entity_feature_val_queries_list) != len(edge_entity_feature_val_rel_list)):
        print("wrong list")
        exit(1)

    src_rel_dst = pd.DataFrame()

    # These are just for metrics
    num_uniq = []  # number of entities
    num_edge_type = []  # number of edges

    # edges from entity node to entity node processing
    for i in range(len(edge_entity_entity_queries_list)):
        query = edge_entity_entity_queries_list[i]
        cursor.execute(query)
        result = pd.DataFrame(cursor)
        result.columns = cursor.column_names

        # TODO: Table Name splitting needs to be more robust - right now we are assuming that position 1 and 2
        # will always be src and dst. Is that a correct assumption in our paradigm think?
        table_name_list = re.split(' ', query)  # table name of the query to execute
        table_name1 = table_name_list[1].split('.')[0] # src table
        table_name2 = table_name_list[2].split('.')[0] # dst/target table

        # Cleaning Part
        result = result.applymap(clean_token)  # strip tokens and lower case strings
        result = result[~result.iloc[:, 1].isin(INVALID_ENTRY_LIST)]  # clean invalid data
        result = result[~result.iloc[:, 0].isin(INVALID_ENTRY_LIST)]
        result = result.drop_duplicates()  # remove invalid row

        result.iloc[:, 0] = table_name1 + "_" + result.columns[0] + '_' + result.iloc[:, 0]   # src
        result.iloc[:, 1] = table_name2 + "_" + result.columns[1] + '_' + result.iloc[:, 1] # dst/target
        result.insert(1, "rel", edge_entity_entity_rel_list[i])  # rel
        result.columns = ["src", "rel", "dst"]
        num_uniq.append(len(result.iloc[:, 2].unique()))
        num_edge_type.append(result.shape[0])
        # print(f'result\n{result}')

        # convert entity nodes to respective UUIDs
        for index, row in result.iterrows():
            # gets the UUID for the specific entity node from the entity_mapping
            result.at[index, 'src'] = entity_mapping.get(result.at[index, 'src'])
            result.at[index, 'dst'] = entity_mapping.get(result.at[index, 'dst'])

        src_rel_dst = pd.concat([src_rel_dst, result])
    
    # edges from entity node to feature values processing
    # Note: feature values will not have table_name and col_name appended
    # TODO: Test Feature values part of post processing
    for i in range(len(edge_entity_feature_val_queries_list)):
        query = edge_entity_feature_val_queries_list[i]
        cursor.execute(query)
        result = pd.DataFrame(cursor)
        result.columns = cursor.column_names

        # TODO: Table Name splitting needs to be more robust - right now we are assuming that position 1 and 2
        # will always be src and dst. Is that a correct assumption in our paradigm think?
        table_name_list = re.split(' ', query)  # table name of the query to execute
        table_name1 = table_name_list[1].split('.')[0] # src table

        # Cleaning Part
        result = result.applymap(clean_token)  # strip tokens and lower case strings
        result = result[~result.iloc[:, 1].isin(INVALID_ENTRY_LIST)]  # clean invalid data
        result = result[~result.iloc[:, 0].isin(INVALID_ENTRY_LIST)]
        result = result.drop_duplicates()  # remove invalid row

        result.iloc[:, 0] = table_name1 + "_" + result.columns[0] + '_' + result.iloc[:, 0]   # src
        result.insert(1, "rel", edge_entity_feature_val_rel_list[i])  # rel
        result.columns = ["src", "rel", "dst"]
        num_uniq.append(len(result.iloc[:, 2].unique()))
        num_edge_type.append(result.shape[0])
        # print(f'result\n{result}')

        # convert entity nodes to respective UUIDs
        for index, row in result.iterrows():
            # gets the UUID for the specific entity node from the entity_mapping
            result.at[index, 'src'] = entity_mapping.get(result.at[index, 'src'])

        src_rel_dst = pd.concat([src_rel_dst, result])

    print(f'src_rel_dst\n{src_rel_dst}\n')
    src_rel_dst.to_csv(output_dir / Path("all_edges(t2g).txt"), sep='\t', header=False, index=False)  # write to txt
    return src_rel_dst  # returns a dataframe or whatever format Marius wants

def main():
    ret_data = config_parser_fn("conf/config.yaml")
    db_server = ret_data[0]
    db_name = ret_data[1]
    entity_queries_list = ret_data[2]
    edge_entity_entity_queries_list = ret_data[3]
    edge_entity_entity_rel_list = ret_data[4]
    edge_entity_feature_val_queries_list = ret_data[5]
    edge_entity_feature_val_rel_list = ret_data[6]

    # returning both cnx & cursor because cnx is main object deleting it leads to lose of cursor
    cnx, cursor = connect_to_db(db_server, db_name) 
    entity_mapping = entity_node_to_uuids(cursor, entity_queries_list)
    src_rel_dst = post_processing(cursor, edge_entity_entity_queries_list, edge_entity_entity_rel_list, 
        edge_entity_feature_val_queries_list, edge_entity_feature_val_rel_list, entity_mapping)  # this is the pd dataframe
    # convert_to_int() should be next, but we are relying on the Marius' preprocessing module

if __name__ == "__main__":
    main()