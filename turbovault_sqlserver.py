import os
from configparser import ConfigParser
from procs.sqlite3 import stage
from procs.sqlite3 import satellite
from procs.sqlite3 import hub
from procs.sqlite3 import link
from procs.sqlite3 import pit
from procs.sqlite3 import nh_satellite
from procs.sqlite3 import ma_satellite
from procs.sqlite3 import rt_satellite
from procs.sqlite3 import nh_link
from procs.sqlite3 import sources
from procs.sqlite3 import properties
from procs.sqlite3 import generate_erd

import pyodbc
from logging import Logger
import pandas as pd
import sqlite3
from gooey import Gooey
from gooey import GooeyParser
from datetime import datetime
import time


image_path = os.path.join(os.path.dirname(__file__),"images")

def connect_sqlserver(connection_string,metadata_schema):
    
    sqlserver_client = pyodbc.connect(connection_string)

    print("Connection successful!")

    sql_source_data = f"""SELECT * FROM {metadata_schema}.source_data"""
    df_source_data = pd.read_sql(sql=sql_source_data, con=sqlserver_client)

    sql_hub_entities = f"SELECT * FROM {metadata_schema}.standard_hub"
    df_hub_entities = pd.read_sql(sql=sql_hub_entities, con=sqlserver_client)

    sql_hub_satellites = f"SELECT * FROM {metadata_schema}.standard_satellite"
    df_hub_satellites = pd.read_sql(sql=sql_hub_satellites, con=sqlserver_client)

    sql_link_entities = f"SELECT * FROM {metadata_schema}.standard_link"
    df_link_entities = pd.read_sql(sql=sql_link_entities, con=sqlserver_client)
    
    sql_pit_entities = f"SELECT * FROM {metadata_schema}.pit"
    df_pit_entities = pd.read_sql(sql=sql_pit_entities, con=sqlserver_client) 
    
    sql_non_historized_satellite_entities = f"SELECT * FROM {metadata_schema}.non_historized_satellite"
    df_non_historized_satellite_entities = pd.read_sql(sql=sql_non_historized_satellite_entities, con=sqlserver_client)
    
    sql_non_historized_link_entities = f"SELECT * FROM {metadata_schema}.non_historized_link"
    df_non_historized_link_entities = pd.read_sql(sql=sql_non_historized_link_entities, con=sqlserver_client)
    
    sql_multiactive_satellite_entities = f"SELECT * FROM {metadata_schema}.multiactive_satellite"
    df_multiactive_satellite_entities = pd.read_sql(sql=sql_multiactive_satellite_entities, con=sqlserver_client)

    
    dfs = { "source_data": df_source_data, 
            "standard_hub": df_hub_entities,
            "standard_link": df_link_entities, 
            "standard_satellite": df_hub_satellites,
            "pit": df_pit_entities,
            "non_historized_satellite": df_non_historized_satellite_entities,
            "non_historized_link": df_non_historized_link_entities,
            "multiactive_satellite": df_multiactive_satellite_entities}


    db = sqlite3.connect(':memory:')
    
    for table, df in dfs.items():
        df.to_sql(table, db)

    sqlite_cursor = db.cursor()

    return sqlite_cursor

        
@Gooey(
    navigation='TABBED',
    program_name='DBT Automation',
    default_size=(800,800),
    advanced=True,
    image_dir=image_path)
def main():

    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),"config.ini"))

    hashdiff_naming = config.get('SQLServer','hashdiff_naming')
    model_path = config.get('SQLServer','model_path')
    server = config.get('SQLServer', 'server')
    database = config.get('SQLServer', 'database')
    trusted_connection = config.get('SQLServer', 'trusted_connection')
    username = config.get('SQLServer', 'username')
    password = config.get('SQLServer', 'password')
    driver = config.get('SQLServer', 'driver')
    metadata_schema = config.get('SQLServer','metadata_schema')

    if username != '' and password != '': 
        conn_str = (
            fr"Driver={driver};"
            fr"Server={server};"
            fr"Database={database};"
            fr"Trusted_Connection={trusted_connection};"
            fr"UID={username};"
            fr"PWD={password};"
        )
    else:
        conn_str = (
            fr"Driver={driver};"
            fr"Server={server};"
            fr"Database={database};"
            fr"Trusted_Connection={trusted_connection};"
        )        

    print(f"Connection String: {conn_str}")
    cursor = connect_sqlserver(connection_string=conn_str, metadata_schema=metadata_schema)

    cursor.execute("SELECT DISTINCT SOURCE_SYSTEM || '__' || SOURCE_OBJECT FROM source_data")
    results = cursor.fetchall()
    available_sources = []

    for row in results:
        available_sources.append(row[0])


    generated_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    parser = GooeyParser(description='Config')
    parser.add_argument("--Tasks",help="Select the entities which You want to generate",action="append",widget='Listbox',
                        choices=['Stage','Standard Hub','Standard Satellite','Standard Link','Non Historized Link','Pit','Non Historized Satellite','Multi Active Satellite','Record Tracking Satellite'],
                        default=['Stage','Standard Hub','Standard Satellite','Standard Link','Non Historized Link','Pit','Non Historized Satellite','Multi Active Satellite','Record Tracking Satellite'],nargs='*',gooey_options={'height': 300})
    parser.add_argument("--Sources",action="append",nargs="+", widget='Listbox', choices=available_sources, gooey_options={'height': 300},
                       help="Select the sources which You want to process", default=[])
    parser.add_argument("--SourceYML",default=False,action="store_true",  help="Do You want to generate the sources.yml file?") #Create external Table (Y/N)
    parser.add_argument("--Properties",default=False,action="store_true",  help="Do You want to generate the properties.yml files?") #Create external Table (Y/N)
    parser.add_argument("--DBDocs",help="Please make sure to have DBDocs installed and that You are logged in.",default=False,action="store_true") #Create ER-Diagram (Y/N)

    args = parser.parse_args()

    try:
        todo = args.Tasks[9]
    except IndexError:
        print("No tasks selected.")
        todo = ""     

    rdv_default_schema = config.get('SQLServer',"rdv_schema")
    stage_default_schema = config.get('SQLServer',"stage_schema")

    if args.SourceYML:
        sources.gen_sources(cursor,args.Sources[0],generated_timestamp, model_path)



    try:
        for source in args.Sources[0]:
            if args.Properties:
                properties.gen_properties(cursor,source,generated_timestamp,model_path)
            if 'Stage' in todo:
                stage.generate_stage(cursor,source, generated_timestamp, stage_default_schema, model_path, hashdiff_naming)
            
            if 'Standard Hub' in todo: 
                hub.generate_hub(cursor,source, generated_timestamp, rdv_default_schema, model_path)
        
            if 'Standard Link' in todo: 
                link.generate_link(cursor,source, generated_timestamp, rdv_default_schema, model_path)

            if 'Standard Satellite' in todo: 
                satellite.generate_satellite(cursor, source, generated_timestamp, rdv_default_schema, model_path, hashdiff_naming)
                
            if 'Pit' in todo:
                pit.generate_pit(cursor,source, generated_timestamp, model_path)
                
            if 'Non Historized Satellite' in todo: 
                nh_satellite.generate_nh_satellite(cursor, source, generated_timestamp, rdv_default_schema, model_path)
                
            if 'Multi Active Satellite' in todo: 
                ma_satellite.generate_ma_satellite(cursor, source, generated_timestamp, rdv_default_schema, model_path, hashdiff_naming)
            
            if 'Record Tracking Satellite' in todo: 
                rt_satellite.generate_rt_satellite(cursor, source, generated_timestamp, rdv_default_schema, model_path)

            if 'Non Historized Link' in todo:
                nh_link.generate_nh_link(cursor,source, generated_timestamp, rdv_default_schema, model_path)

    except IndexError as e:
        print("No source selected.")

    if args.DBDocs:
        generate_erd.generate_erd(cursor,args.Sources[0],generated_timestamp,model_path,hashdiff_naming)


if __name__ == "__main__":
    print("Starting Script.")
    start = time.time()
    main()
    end = time.time()
    print("Script ends.")
    print("Total Runtime: " + str(round(end - start, 2)) + "s")