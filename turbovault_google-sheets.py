import os
from configparser import ConfigParser
from procs.sqlite3 import stage
from procs.sqlite3 import satellite
from procs.sqlite3 import hub
from procs.sqlite3 import link
import pandas as pd
import gspread as gs
import sqlite3
import time
from gooey import Gooey
from gooey import GooeyParser

from datetime import datetime

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
    
    hub_entities_df = pd.DataFrame(sh.worksheet('hub_entities').get_all_records())
    link_entities_df = pd.DataFrame(sh.worksheet('link_entities').get_all_records())
    hub_satellite_df = pd.DataFrame(sh.worksheet('hub_satellites').get_all_records())
    link_satellite_df = pd.DataFrame(sh.worksheet('link_satellites').get_all_records())
    source_data_df = pd.DataFrame(sh.worksheet('source_data').get_all_records())

    db = sqlite3.connect(':memory:')
    
    hub_entities_df.to_sql('hub_entities', db)
    link_entities_df.to_sql('link_entities', db)
    hub_satellite_df.to_sql('hub_satellites', db)
    link_satellite_df.to_sql('link_satellites', db)
    source_data_df.to_sql('source_data',db)
    
    cursor = db.cursor()
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

    rdv_default_schema = sheet_url = config.get('Google Sheets', 'rdv_schema')
    stage_default_schema = sheet_url = config.get('Google Sheets', 'stage_schema')

   
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