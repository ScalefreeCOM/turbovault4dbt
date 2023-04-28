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
from procs.sqlite3 import tests

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
    parser.add_argument("--Tasks",help="Select the entities which You want to generate",action="append",widget='Listbox',
                        choices=['Stage','Standard Hub','Standard Satellite','Standard Link','Non Historized Link','Pit','Non Historized Satellite','Multi Active Satellite','Record Tracking Satellite'],
                        default=['Stage','Standard Hub','Standard Satellite','Standard Link','Non Historized Link','Pit','Non Historized Satellite','Multi Active Satellite','Record Tracking Satellite'],nargs='*',gooey_options={'height': 300})
    parser.add_argument("--Sources",action="append",nargs="+", widget='Listbox', choices=available_sources, gooey_options={'height': 300},
                       help="Select the sources which You want to process", default=[])
    parser.add_argument("--SourceYML",default=False,action="store_true",  help="Do You want to generate the sources.yml?") #Create external Table (Y/N)
    parser.add_argument("--Test",default=False,action="store_true",  help="Do You want to generate YML-Files for Standard DataVault tests?") #Create external Table (Y/N)

    args = parser.parse_args()

    try:
        todo = args.Tasks[9]
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
            
            if 'Record Tracking Satellite' in todo: 
                rt_satellite.generate_rt_satellite(cursor, source, generated_timestamp, rdv_default_schema, model_path)

            if 'Non Historized Link' in todo:
                nh_link.generate_nh_link(cursor,source, generated_timestamp, rdv_default_schema, model_path)

    except IndexError as e:
        print("Keine Quelle ausgesucht.")

if __name__ == "__main__":
    print("Starting Script.")
    start = time.time()
    main()
    end = time.time()
    print("Script ends.")
    print("Total Runtime: " + str(round(end - start, 2)) + "s")