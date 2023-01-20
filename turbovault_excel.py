import os
from configparser import ConfigParser
from procs.sqlite3 import stage
from procs.sqlite3 import satellite
from procs.sqlite3 import hub
from procs.sqlite3 import link
from logging import Logger
import pandas as pd
import sqlite3
from gooey import Gooey
from gooey import GooeyParser
from datetime import datetime
import time


image_path = os.path.join(os.path.dirname(__file__),"images")
log = Logger('log')

@Gooey(
    navigation='TABBED',
    program_name='TurboVault',
    default_size=(800,800),
    advanced=True,
    image_dir=image_path)
def main():
    
    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),"config.ini"))
    hashdiff_naming = config.get('Excel','hashdiff_naming')
    model_path = config.get('Excel','model_path')
    excel_path = config.get('Excel','excel_path')
    
    db = sqlite3.connect(':memory:')
    dfs = pd.read_excel(excel_path, sheet_name=None)
    for table, df in dfs.items():
        df.to_sql(table, db)

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
                       help="Select the sources which You want to process", default=[])
    args = parser.parse_args()

    try:
        todo = args.Tasks[4]
    except IndexError:
        print("Keine Entitäten ausgesucht.")
        todo = ""     

    rdv_default_schema = config.get('Excel',"rdv_schema")
    stage_default_schema = config.get('Excel',"stage_schema")

   
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