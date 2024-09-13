import os
from configparser import ConfigParser, RawConfigParser
from procs.sqlite3 import sources
from procs.sqlite3 import generate_selected_entities
from procs.sqlite3 import generate_erd
from procs.sqlite3 import properties
from logging import Logger
import pandas as pd
import sqlite3
from gooey import Gooey
from gooey import GooeyParser
from datetime import datetime

import snowflake.connector
import time

image_path = os.path.join(os.path.dirname(__file__),"images")

def connect_snowflake():
    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),"config.ini"))

    database = config.get('Snowflake', 'database')
    warehouse = config.get('Snowflake', 'warehouse')
    role = config.get('Snowflake', 'role')
    schema = config.get('Snowflake', 'meta_schema')

    snowflake_credentials = RawConfigParser()
    snowflake_credentials.read(config.get('Snowflake', 'credential_path'))

    user = snowflake_credentials.get('main', 'SNOWFLAKE_USER_NAME')
    password = snowflake_credentials.get('main', 'SNOWFLAKE_PASSWORD')
    
    ctx = snowflake.connector.connect(
    user= user,
    password=password,
    account=config.get('Snowflake', 'account_identifier'),
    database=database,
    warehouse=warehouse,
    role=role,
    schema=schema
    )
    
    cursor = ctx.cursor()

    sql_source_data = "SELECT * FROM SOURCE_DATA"
    cursor.execute(sql_source_data)
    df_source_data = cursor.fetch_pandas_all()    
    cursor.close()

    cursor = ctx.cursor()
    sql_hub_entities = "SELECT * FROM standard_hub"
    cursor.execute(sql_hub_entities)
    df_hub_entities = cursor.fetch_pandas_all()    
    cursor.close()

    cursor = ctx.cursor()
    sql_standard_satellite = "SELECT * FROM standard_satellite"
    cursor.execute(sql_standard_satellite)
    df_standard_satellite = cursor.fetch_pandas_all()    
    cursor.close()

    cursor = ctx.cursor()
    sql_link_entities = "SELECT * FROM standard_link"
    cursor.execute(sql_link_entities)
    df_link_entities = cursor.fetch_pandas_all()    
    cursor.close()
    
    cursor = ctx.cursor()
    sql_pit_entities = "SELECT * FROM pit"
    cursor.execute(sql_pit_entities)
    df_pit = cursor.fetch_pandas_all()    
    cursor.close()

    cursor = ctx.cursor()
    sql_ref_table_entities = "SELECT * FROM ref_table"
    cursor.execute(sql_ref_table_entities)
    df_ref_table = cursor.fetch_pandas_all()    
    cursor.close()

    cursor = ctx.cursor()
    sql_ref_hub_entities = "SELECT * FROM ref_hub"
    cursor.execute(sql_ref_hub_entities)
    df_ref_hub = cursor.fetch_pandas_all()    
    cursor.close()

    cursor = ctx.cursor()
    sql_ref_sat_entities = "SELECT * FROM ref_sat"
    cursor.execute(sql_ref_sat_entities)
    df_ref_sat = cursor.fetch_pandas_all()    
    cursor.close()
    
    cursor = ctx.cursor()
    sql_non_historized_satellite = "SELECT * FROM non_historized_satellite"
    cursor.execute(sql_non_historized_satellite)
    df_non_historized_satellite = cursor.fetch_pandas_all()    
    cursor.close()

    cursor = ctx.cursor()
    sql_multiactive_satellite = "SELECT * FROM multiactive_satellite"
    cursor.execute(sql_multiactive_satellite)
    df_multiactive_satellite = cursor.fetch_pandas_all()    
    cursor.close()

    cursor = ctx.cursor()
    sql_non_historized_link = "SELECT * FROM non_historized_link"
    cursor.execute(sql_non_historized_link)
    df_non_historized_link = cursor.fetch_pandas_all()    
    cursor.close()

    cursor.close()
    ctx.close()
    
    dfs = { "source_data": df_source_data, 
            "standard_hub": df_hub_entities,
            "standard_link": df_link_entities, 
            "standard_satellite": df_standard_satellite,
            "pit": df_pit,
            "non_historized_satellite": df_non_historized_satellite,
            "multiactive_satellite": df_multiactive_satellite,
            "non_historized_link" : df_non_historized_link,
            "ref_table": df_ref_table,
            "ref_hub": df_ref_hub,
            "ref_sat": df_ref_sat}


    db = sqlite3.connect(':memory:')
    
    for table, df in dfs.items():
        df.to_sql(table, db)

    sqlite_cursor = db.cursor()

    return sqlite_cursor

@Gooey(
    navigation='TABBED',
    program_name='TurboVault4dbt',
    default_size=(800,800),
    advanced=True,
    image_dir=image_path
)

def main():
    
    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),"config.ini"))
    cursor = connect_snowflake()
    cursor.execute("SELECT DISTINCT SOURCE_SYSTEM || '_' || SOURCE_OBJECT FROM source_data")
    results = cursor.fetchall()
    source_list = []

    for row in results:
        source_list.append(row[0])


    parser = GooeyParser(description='Config')
    parser.add_argument("--Tasks",help="Select the entities which You want to generate",action="append",widget='Listbox',
                        choices=['Stage','Standard Hub','Standard Satellite','Standard Link','Non Historized Link','Pit','Non Historized Satellite','Multi Active Satellite','Record Tracking Satellite','Reference Table'],
                        default=['Stage','Standard Hub','Standard Satellite','Standard Link','Non Historized Link','Pit','Non Historized Satellite','Multi Active Satellite','Record Tracking Satellite','Reference Table'],nargs='*',gooey_options={'height': 300})
    parser.add_argument("--Sources",action="append",nargs="+", widget='Listbox', choices=source_list, gooey_options={'height': 300},
                       help="Select the sources which You want to process", default=[])
    parser.add_argument("--SourceYML",default=False,action="store_true",  help="Do You want to generate the sources.yml file?") #Create external Table (Y/N)
    parser.add_argument("--Properties",default=False,action="store_true",  help="Do You want to generate the properties.yml files?") #Create external Table (Y/N)
    parser.add_argument("--DBDocs",help="Please make sure to have DBDocs installed and that You are logged in.",default=False,action="store_true") #Create ER-Diagram (Y/N)

    args = parser.parse_args()

    try:
        todo = args.Tasks[-1]
    except IndexError:
        print("No tasks selected.")
        todo = ""     
    data_structure ={
        'console_outputs': True,
        'cursor': cursor,
        'source': None,
        'generated_timestamp': datetime.now().strftime("%Y%m%d%H%M%S"),
        'rdv_default_schema': config.get('Snowflake',"rdv_schema"),
        'model_path': config.get('Snowflake','model_path'),
        'hashdiff_naming': config.get('Snowflake','hashdiff_naming'),
        'stage_default_schema': config.get('Snowflake',"stage_schema"),  
        'source_list': args.Sources[0]  ,
        'generateSources': False,
        'source_name' : None, # "Source" field splits into this field
        'source_object' : None, # "Source" field splits into this field
        }   

    if args.SourceYML:
        sources.gen_sources(data_structure)
    try:
        for data_structure['source'] in data_structure['source_list']:
            data_structure['source'] = data_structure['source'].replace('_','_.._')
            seperatedNameAsList = data_structure['source'].split('_.._')
            data_structure['source_name']   = seperatedNameAsList[0]
            data_structure['source_object'] = ''.join(seperatedNameAsList[1:])
            generate_selected_entities.generate_selected_entities(todo, data_structure)
            try:
                if args.Properties:
                    properties.gen_properties(data_structure)                 
            except Exception as e:
                print(e)
                print("Failed to generate {0}.yml properties file.".format(data_structure['source']))
    except IndexError as e:
        print("No source selected.")

    if args.DBDocs:
        generate_erd.generate_erd(data_structure['cursor'],args.Sources[0],data_structure['generated_timestamp'],data_structure['model_path'],data_structure['hashdiff_naming'])

if __name__ == "__main__":
    print("Starting Script.")
    start = time.time()
    main()
    end = time.time()
    print("Script ends.")
    print("Total Runtime: " + str(round(end - start, 2)) + "s")