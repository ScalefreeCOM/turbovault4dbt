import os
from configparser import ConfigParser
from procs.sqlite3 import stage
from procs.sqlite3 import satellite
from procs.sqlite3 import hub
from procs.sqlite3 import link
from google.cloud import bigquery
from logging import Logger
import pandas as pd
import sqlite3
from gooey import Gooey
from gooey import GooeyParser
from datetime import datetime
import time


image_path = os.path.join(os.path.dirname(__file__),"images")

def connect_bigquery(credential_path,metadata_dataset):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_path
    bigquery_client = bigquery.Client()

    sql_source_data = f"SELECT * FROM {metadata_dataset}.source_data"
    df_source_data = bigquery_client.query(sql_source_data).to_dataframe()

    sql_hub_entities = f"SELECT * FROM {metadata_dataset}.standard_hub"
    df_hub_entities = bigquery_client.query(sql_hub_entities).to_dataframe() 

    sql_hub_satellites = f"SELECT * FROM {metadata_dataset}.standard_satellite"
    df_hub_satellites = bigquery_client.query(sql_hub_satellites).to_dataframe() 

    sql_link_entities = f"SELECT * FROM {metadata_dataset}.standard_link"
    df_link_entities = bigquery_client.query(sql_link_entities).to_dataframe() 

    
    dfs = { "source_data": df_source_data, 
            "hub_entities": df_hub_entities,
            "link_entities": df_link_entities, 
            "hub_satellites": df_hub_satellites}


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
    credential_path = config.get('BigQuery', 'credential_path')
    metadata_dataset = config.get('BigQuery','metadata_dataset')
    cursor = connect_bigquery(credential_path,metadata_dataset)

    cursor.execute("SELECT DISTINCT SOURCE_SYSTEM || '_' || SOURCE_OBJECT FROM source_data")
    results = cursor.fetchall()
    available_sources = []

    for row in results:
        available_sources.append(row[0])


    generated_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    parser = GooeyParser(description='Config')
    parser.add_argument("--Tasks",help="Select the entities which You want to generate",action="append",widget='Listbox',choices=['Stage','Hub','Satellite','Link'],default=['Stage','Hub','Satellite','Link'],nargs='*',gooey_options={'height': 300})
    parser.add_argument("--Sources",action="append",nargs="+", widget='Listbox', choices=available_sources, gooey_options={'height': 300},
                       help="Select the sources which You want to process")
    args = parser.parse_args()
   
    try:
        todo = args.Tasks[4]
    except IndexError:
        print("Keine Entitäten ausgesucht.")
        todo = ""      

    rdv_default_schema = config.get('BigQuery',"rdv_schema")
    stage_default_schema = config.get('BigQuery',"stage_schema")

   
    for source in args.Sources[0]:
        if 'Stage' in todo:
            stage.generate_stage(cursor,source, generated_timestamp, stage_default_schema, model_path, hashdiff_naming)
        
        if 'Hub' in todo: 
            hub.generate_hub(cursor,source, generated_timestamp, rdv_default_schema, model_path)
    
        if 'Link' in todo: 
            link.generate_link(cursor,source, generated_timestamp, rdv_default_schema, model_path)

        if 'Satellite' in todo: 
            satellite.generate_satellite(cursor, source, generated_timestamp, rdv_default_schema, model_path, hashdiff_naming)

   

if __name__ == "__main__":
    print("Starting Script.")
    start = time.time()
    main()
    end = time.time()
    print("Script ends.")
    print("Total Runtime: " + str(round(end - start, 2)) + "s")