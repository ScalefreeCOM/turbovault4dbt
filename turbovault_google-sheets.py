import os
from configparser import ConfigParser
from procs.sqlite3 import sources
from procs.sqlite3 import generate_selected_entities
from procs.sqlite3 import generate_erd
from procs.sqlite3 import properties
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
    program_name='TurboVault4dbt',
    default_size=(800,800),
    advanced=True,
    image_dir=image_path
)

def main():

    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),"config.ini"))

    model_path = config.get('Google Sheets','model_path')
    sheet_url = config.get('Google Sheets', 'sheet_url')
    hashdiff_naming = config.get('Google Sheets','hashdiff_naming')
    credential_path = config.get('Google Sheets', 'gcp_oauth_credentials')

    gc = gs.oauth(credentials_filename=credential_path)

    sh = gc.open_by_url(sheet_url)
    
    pd.set_option('future.no_silent_downcasting', True)

    hub_entities_df             = pd.DataFrame(sh.worksheet('standard_hub')            .get_all_records())
    link_entities_df            = pd.DataFrame(sh.worksheet('standard_link')           .get_all_records())
    standard_satellite_df       = pd.DataFrame(sh.worksheet('standard_satellite')      .get_all_records())
    mas_satellite_df            = pd.DataFrame(sh.worksheet('multiactive_satellite')   .get_all_records())
    nh_link_df                  = pd.DataFrame(sh.worksheet('non_historized_link')     .get_all_records())
    non_historized_satellite_df = pd.DataFrame(sh.worksheet('non_historized_satellite').get_all_records())
    pit_df                      = pd.DataFrame(sh.worksheet('pit')                     .get_all_records())
    ref_table_df                = pd.DataFrame(sh.worksheet('ref_table')               .get_all_records())
    ref_hub_df                  = pd.DataFrame(sh.worksheet('ref_hub')                 .get_all_records())
    ref_sat_df                  = pd.DataFrame(sh.worksheet('ref_sat')                 .get_all_records())
    source_data_df              = pd.DataFrame(sh.worksheet('source_data')             .get_all_records())
    
    db = sqlite3.connect(':memory:')
    
    hub_entities_df             = hub_entities_df.replace(r'^\s*$', np.nan, regex=True)
    link_entities_df            = link_entities_df.replace(r'^\s*$', np.nan, regex=True)
    standard_satellite_df       = standard_satellite_df.replace(r'^\s*$', np.nan, regex=True)
    mas_satellite_df            = mas_satellite_df.replace(r'^\s*$', np.nan, regex=True)
    nh_link_df                  = nh_link_df.replace(r'^\s*$', np.nan, regex=True)
    non_historized_satellite_df = non_historized_satellite_df.replace(r'^\s*$', np.nan, regex=True)
    pit_df                      = pit_df.replace(r'^\s*$', np.nan, regex=True)
    ref_table_df                = ref_table_df.replace(r'^\s*$', np.nan, regex=True)
    ref_hub_df                  = ref_hub_df.replace(r'^\s*$', np.nan, regex=True)
    ref_sat_df                  = ref_sat_df.replace(r'^\s*$', np.nan, regex=True)
    source_data_df              = source_data_df.replace(r'^\s*$', np.nan, regex=True)
    
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
    """class args:
        SourceYML = False
        Properties = False
        DBDocs = False
        Sources = [source_list]
        Tasks = [0,0,0,0,0,0,0,0,0,0,
            ['Stage','Standard Hub','Standard Satellite','Standard Link','Non Historized Link','Pit','Non Historized Satellite','Multi Active Satellite','Record Tracking Satellite','Reference Table']
            ]"""
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
        'rdv_default_schema': config.get('Google Sheets',"rdv_schema"),
        'model_path': config.get('Google Sheets','model_path'),
        'hashdiff_naming': config.get('Google Sheets','hashdiff_naming'),
        'stage_default_schema': config.get('Google Sheets',"stage_schema"), 
        'source_database': config.get('Google Sheets', "source_database"),
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