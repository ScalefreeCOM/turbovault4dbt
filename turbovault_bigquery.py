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

from google.cloud import bigquery
from google.oauth2 import service_account
from logging import Logger
import pandas as pd
import sqlite3
from gooey import Gooey
from gooey import GooeyParser
from datetime import datetime
import time


image_path = os.path.join(os.path.dirname(__file__),"images")

def connect_bigquery(credential_path,metadata_dataset,project_id):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_path
    credentials = service_account.Credentials.from_service_account_file(credential_path)
    
    bigquery_client = bigquery.Client(project = project_id,credentials = credentials)

    sql_source_data = f"""SELECT * FROM `{metadata_dataset}.source_data`"""
    df_source_data = bigquery_client.query(sql_source_data).to_dataframe()

    sql_hub_entities = f"SELECT * FROM {metadata_dataset}.standard_hub"
    df_hub_entities = bigquery_client.query(sql_hub_entities).to_dataframe() 

    sql_hub_satellites = f"SELECT * FROM {metadata_dataset}.standard_satellite"
    df_hub_satellites = bigquery_client.query(sql_hub_satellites).to_dataframe() 

    sql_link_entities = f"SELECT * FROM {metadata_dataset}.standard_link"
    df_link_entities = bigquery_client.query(sql_link_entities).to_dataframe() 
    
    sql_pit_entities = f"SELECT * FROM {metadata_dataset}.pit"
    df_pit_entities = bigquery_client.query(sql_pit_entities).to_dataframe() 
    
    sql_non_historized_satellite_entities = f"SELECT * FROM {metadata_dataset}.non_historized_satellite"
    df_non_historized_satellite_entities = bigquery_client.query(sql_non_historized_satellite_entities).to_dataframe()
    
    sql_non_historized_link_entities = f"SELECT * FROM {metadata_dataset}.non_historized_link"
    df_non_historized_link_entities = bigquery_client.query(sql_non_historized_link_entities).to_dataframe()  
    
    sql_multiactiv_satellite_entities = f"SELECT * FROM {metadata_dataset}.multiactive_satellite"
    df_multiactiv_satellite_entities = bigquery_client.query(sql_multiactiv_satellite_entities).to_dataframe()

    
    dfs = { "source_data": df_source_data, 
            "standard_hub": df_hub_entities,
            "standard_link": df_link_entities, 
            "standard_satellite": df_hub_satellites,
            "pit": df_pit_entities,
            "non_historized_satellite": df_non_historized_satellite_entities,
            "non_historized_link": df_non_historized_link_entities,
            "multiactive_satellite": df_multiactiv_satellite_entities}


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

    hashdiff_naming = config.get('BigQuery','hashdiff_naming')
    model_path = config.get('BigQuery','model_path')
    project_id = config.get('BigQuery','project_id')
    credential_path = config.get('BigQuery', 'credential_path')
    metadata_dataset = config.get('BigQuery','metadata_dataset')
    cursor = connect_bigquery(credential_path,metadata_dataset,project_id)

    cursor.execute("SELECT DISTINCT SOURCE_SYSTEM || '_' || SOURCE_OBJECT FROM source_data")
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

    rdv_default_schema = config.get('BigQuery',"rdv_schema")
    stage_default_schema = config.get('BigQuery',"stage_schema")

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