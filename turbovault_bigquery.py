import os
from configparser import ConfigParser
from procs.sqlite3 import sources
from procs.sqlite3 import generate_erd
from procs.sqlite3 import generate_selected_entities
from procs.sqlite3 import properties
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

    sql_ref_table_entities = f"SELECT * FROM {metadata_dataset}.ref_table"
    df_ref_table_entities = bigquery_client.query(sql_ref_table_entities).to_dataframe() 

    sql_ref_hub_entities = f"SELECT * FROM {metadata_dataset}.ref_hub"
    df_ref_hub_entities = bigquery_client.query(sql_ref_hub_entities).to_dataframe() 

    sql_ref_sat_entities = f"SELECT * FROM {metadata_dataset}.ref_sat"
    df_ref_sat_entities = bigquery_client.query(sql_ref_sat_entities).to_dataframe() 
    
    sql_multiactiv_satellite_entities = f"SELECT * FROM {metadata_dataset}.multiactive_satellite"
    df_multiactiv_satellite_entities = bigquery_client.query(sql_multiactiv_satellite_entities).to_dataframe()

    
    dfs = { "source_data": df_source_data, 
            "standard_hub": df_hub_entities,
            "standard_link": df_link_entities, 
            "standard_satellite": df_hub_satellites,
            "pit": df_pit_entities,
            "non_historized_satellite": df_non_historized_satellite_entities,
            "non_historized_link": df_non_historized_link_entities,
            "multiactive_satellite": df_multiactiv_satellite_entities,
            "ref_table": df_ref_table_entities,
            "ref_hub": df_ref_hub_entities,
            "ref_sat": df_ref_sat_entities}


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

    hashdiff_naming = config.get('BigQuery','hashdiff_naming')
    model_path = config.get('BigQuery','model_path')
    project_id = config.get('BigQuery','project_id')
    credential_path = config.get('BigQuery', 'credential_path')
    metadata_dataset = config.get('BigQuery','metadata_dataset')
    cursor = connect_bigquery(credential_path,metadata_dataset,project_id)

    cursor.execute("SELECT DISTINCT SOURCE_SYSTEM || '_' || SOURCE_OBJECT FROM source_data")
    results = cursor.fetchall()
    source_list = []

    for row in results:
        source_list.append(row[0])

    generated_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

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


    dataStructure ={
        'console_outputs' : True,
        'cursor': cursor,
        'source': None,
        'generated_timestamp': datetime.now().strftime("%Y%m%d%H%M%S"),
        'rdv_default_schema': config.get('BigQuery',"rdv_schema"),
        'model_path': config.get('BigQuery','model_path'),
        'hashdiff_naming': config.get('BigQuery','hashdiff_naming'),
        'stage_default_schema': config.get('BigQuery',"stage_schema"),  
        'source_list': args.Sources[0]  ,
        'generateSources': False,
        'source_name' : None, # "Source" field splits into this field
        'source_object' : None, # "Source" field splits into this field
        }   

    if args.SourceYML:
        sources.gen_sources(dataStructure)
    try:
        for dataStructure['source'] in dataStructure['source_list']:
            dataStructure['source'] = dataStructure['source'].replace('_','_.._')
            seperatedNameAsList = dataStructure['source'].split('_.._')
            dataStructure['source_name']   = seperatedNameAsList[0]
            dataStructure['source_object'] = ''.join(seperatedNameAsList[1:])
            generate_selected_entities.generate_selected_entities(todo, dataStructure)
            try:
                if args.Properties:
                    properties.gen_properties(dataStructure)                 
            except Exception as e:
                print(e)
                print("Failed to generate {0}.yml properties file.".format(dataStructure['source']))
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