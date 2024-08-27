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
from procs.sqlite3 import ref
import pandas as pd
import gspread as gs
from google.oauth2.service_account import Credentials
import sqlite3
import time
from gooey import Gooey
from gooey import GooeyParser
from datetime import datetime
import numpy as np

image_path = os.path.join(os.path.dirname(__file__),"images")


@Gooey(
    navigation='TABBED',
    program_name='DBT Automation',
    default_size=(800,800),
    advanced=True,
    image_dir=image_path)
def main():

    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),"config.ini"))

    model_path = config.get('Google Sheets','model_path')
    sheet_url = config.get('Google Sheets', 'sheet_url')
    hashdiff_naming = config.get('Google Sheets','hashdiff_naming')
    credential_path = config.get('Google Sheets', 'gcp_oauth_credentials')

    gc = gs.oauth(credentials_filename=credential_path)

    sh = gc.open_by_url(sheet_url)

    hub_entities_df = pd.DataFrame(sh.worksheet('standard_hub').get_all_records())
    link_entities_df = pd.DataFrame(sh.worksheet('standard_link').get_all_records())
    standard_satellite_df = pd.DataFrame(sh.worksheet('standard_satellite').get_all_records())
    mas_satellite_df = pd.DataFrame(sh.worksheet('multiactive_satellite').get_all_records())
    nh_link_df = pd.DataFrame(sh.worksheet('non_historized_link').get_all_records())
    non_historized_satellite_df = pd.DataFrame(sh.worksheet('non_historized_satellite').get_all_records())
    pit_df = pd.DataFrame(sh.worksheet('pit').get_all_records())
    ref_table_df = pd.DataFrame(sh.worksheet('ref_table').get_all_records())
    ref_hub_df = pd.DataFrame(sh.worksheet('ref_hub').get_all_records())
    ref_sat_df = pd.DataFrame(sh.worksheet('ref_sat').get_all_records())
    source_data_df = pd.DataFrame(sh.worksheet('source_data').get_all_records())

    db = sqlite3.connect(':memory:')
    

    hub_entities_df = hub_entities_df.replace(r'^\s*$', np.nan, regex=True)
    link_entities_df = link_entities_df.replace(r'^\s*$', np.nan, regex=True)
    standard_satellite_df = standard_satellite_df.replace(r'^\s*$', np.nan, regex=True)
    mas_satellite_df = mas_satellite_df.replace(r'^\s*$', np.nan, regex=True)
    nh_link_df = nh_link_df.replace(r'^\s*$', np.nan, regex=True)
    non_historized_satellite_df = non_historized_satellite_df.replace(r'^\s*$', np.nan, regex=True)
    pit_df = pit_df.replace(r'^\s*$', np.nan, regex=True)
    ref_table_df = ref_table_df.replace(r'^\s*$', np.nan, regex=True)
    ref_hub_df = ref_hub_df.replace(r'^\s*$', np.nan, regex=True)
    ref_sat_df = ref_sat_df.replace(r'^\s*$', np.nan, regex=True)
    source_data_df = source_data_df.replace(r'^\s*$', np.nan, regex=True)
    

    hub_entities_df.to_sql('standard_hub', db)
    link_entities_df.to_sql('standard_link', db)
    standard_satellite_df.to_sql('standard_satellite', db)
    mas_satellite_df.to_sql('multiactive_satellite',db)
    nh_link_df.to_sql('non_historized_link',db)
    non_historized_satellite_df.to_sql('non_historized_satellite',db)
    pit_df.to_sql('pit',db)
    ref_table_df.to_sql('ref_table',db)
    ref_hub_df.to_sql('ref_hub',db)
    ref_sat_df.to_sql('ref_sat',db)
    source_data_df.to_sql('source_data',db)


    
    cursor = db.cursor()
    cursor.execute("SELECT DISTINCT SOURCE_SYSTEM || '_' || SOURCE_OBJECT FROM source_data")
    results = cursor.fetchall()
    available_sources = []
    for row in results:
        available_sources.append(row[0])

    generated_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    parser = GooeyParser(description='Config')
    parser.add_argument("--Tasks",help="Select the entities which You want to generate",action="append",widget='Listbox',
                        choices=['Stage','Standard Hub','Standard Satellite','Standard Link','Non Historized Link','Pit','Non Historized Satellite','Multi Active Satellite','Record Tracking Satellite','Reference Table'],
                        default=['Stage','Standard Hub','Standard Satellite','Standard Link','Non Historized Link','Pit','Non Historized Satellite','Multi Active Satellite','Record Tracking Satellite','Reference Table'],nargs='*',gooey_options={'height': 300})
    parser.add_argument("--Sources",action="append",nargs="+", widget='Listbox', choices=available_sources, gooey_options={'height': 300},
                       help="Select the sources which You want to process", default=[])
    parser.add_argument("--SourceYML",default=False,action="store_true",  help="Do You want to generate the sources.yml file?") #Create external Table (Y/N)
    parser.add_argument("--Properties",default=False,action="store_true",  help="Do You want to generate the properties.yml files?") #Create external Table (Y/N)
    parser.add_argument("--DBDocs",help="Please make sure to have DBDocs installed and that You are logged in.",default=False,action="store_true") #Create ER-Diagram (Y/N)

    args = parser.parse_args()

    try:
        todo = args.Tasks
    except IndexError:
        print("No tasks selected.")
        todo = ""     

    rdv_default_schema = config.get('Google Sheets',"rdv_schema")
    stage_default_schema = config.get('Google Sheets',"stage_schema")
    source_database = config.get('Google Sheets', "source_database")

    if args.SourceYML:
        sources.gen_sources(cursor,args.Sources[0],generated_timestamp, model_path, source_database)

    try:
        for source in args.Sources[0]:
            source = source.replace('_','_.._')
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
                
            if 'Reference Table' in todo:
                ref.generate_ref(cursor,source, generated_timestamp, rdv_default_schema, model_path, hashdiff_naming)

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