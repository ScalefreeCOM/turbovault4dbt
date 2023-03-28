import os
from configparser import ConfigParser
from procs.sqlite3 import stage
from procs.sqlite3 import satellite
from procs.sqlite3 import hub
from procs.sqlite3 import link
from procs.sqlite3 import pit
from procs.sqlite3 import nh_satellite
from procs.sqlite3 import ma_satellite
from procs.sqlite3 import nh_link
from procs.sqlite3 import sources
from procs.sqlite3 import tests
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

    snowflake_credentials = ConfigParser()
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

    sql_source_data = "SELECT * FROM source_data"
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
            "non_historized_link" : df_non_historized_link}


    db = sqlite3.connect(':memory:')
    
    for table, df in dfs.items():
        df.to_sql(table, db)

    sqlite_cursor = db.cursor()

    return sqlite_cursor

@Gooey(
    navigation='TABBED',
    program_name='TurboVault',
    default_size=(800,800),
    advanced=True,
    image_dir=image_path)
def main():
    
    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),"config.ini"))

    model_path = config.get('Snowflake','model_path')
    hashdiff_naming = config.get('Snowflake','hashdiff_naming')
    cursor = connect_snowflake()
    cursor.execute("SELECT DISTINCT SOURCE_SYSTEM || '_' || SOURCE_OBJECT FROM source_data")
    results = cursor.fetchall()
    available_sources = []

    
    for row in results:
        available_sources.append(row[0])

    generated_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    parser = GooeyParser(description='Config')
    parser.add_argument("--Tasks",help="Select the entities which You want to generate",action="append",widget='Listbox',choices=['Stage','Standard Hub','Standard Satellite','Standard Link','Non Historized Link','Pit','Non Historized Satellite','Multi Active Satellite'],default=['Stage','Standard Hub','Standard Satellite','Standard Link','Non Historized Link','Pit','Non Historized Satellite','Multi Active Satellite'],nargs='*',gooey_options={'height': 300})
    parser.add_argument("--Sources",action="append",nargs="+", widget='Listbox', choices=available_sources, gooey_options={'height': 300},
                       help="Select the sources which You want to process", default=[])
    parser.add_argument("--SourceYML",default=False,action="store_true",  help="Do You want to generate the sources.yml?") #Create external Table (Y/N)
    parser.add_argument("--Test",default=False,action="store_true",  help="Do You want to generate YML-Files for Standard DataVault tests?") #Create external Table (Y/N)

    args = parser.parse_args()

    try:
        todo = args.Tasks[8]
    except IndexError:
        print("Keine Entitäten ausgesucht.")
        todo = ""     

    rdv_default_schema = config.get('Excel',"rdv_schema")
    stage_default_schema = config.get('Excel',"stage_schema")

    if args.SourceYML:
        sources.gen_sources(cursor,generated_timestamp,model_path)



    try:
        for source in args.Sources[0]:
            if args.Test:
                tests.gen_tests(cursor,source,generated_timestamp,model_path)
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
            
            if 'Non Historized Link' in todo:
                nh_link.generate_nh_link(cursor,source, generated_timestamp, rdv_default_schema, model_path)

    except IndexError:
        print("Keine Quelle ausgesucht.")

if __name__ == "__main__":
    print("Starting Script.")
    start = time.time()
    main()
    end = time.time()
    print("Script ends.")
    print("Total Runtime: " + str(round(end - start, 2)) + "s")