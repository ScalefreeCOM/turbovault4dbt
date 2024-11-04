import os
from configparser import ConfigParser
from procs.sqlite3 import generate_selected_entities, sources, generate_erd
from logging import Logger
import sqlite3
from gooey import Gooey
from gooey import GooeyParser
from datetime import datetime
import time
from procs.sqlite3 import properties
image_path = os.path.join(os.path.dirname(__file__),"images")
log = Logger('log')

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
    hashdiff_naming = config.get('db','hashdiff_naming')
    model_path = config.get('db','model_path')
    db_path = config.get('db','db_path')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.abspath(os.path.join(script_dir, db_path)) # If a file path is relative, then resolve to an absolute path 
    
    db = sqlite3.connect(db_path)

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
        todo = args.Tasks[-1]
    except IndexError:
        print("No tasks selected.")
        todo = ""     

    data_structure = {
        'console_outputs': True,
        'cursor': cursor,
        'source': None,
        'generated_timestamp': datetime.now().strftime("%Y%m%d%H%M%S"),
        'rdv_default_schema': config.get('db',"rdv_schema"),
        'model_path': config.get('db','model_path'),
        'hashdiff_naming': config.get('db','hashdiff_naming'),
        'stage_default_schema': config.get('db',"stage_schema"),  
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
        print(e)
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