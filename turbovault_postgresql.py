import os
from configparser import ConfigParser, RawConfigParser
from procs.sqlite3 import generate_erd, generate_selected_entitites, properties, sources
from logging import Logger
import pandas as pd
import sqlite3
from gooey import Gooey
from gooey import GooeyParser
from datetime import datetime

import psycopg2
import time

image_path = os.path.join(os.path.dirname(__file__), "images")

def connect_postgresql():
    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), "config.ini"))

    database = config.get('PostgreSQL', 'database')
    user = config.get('PostgreSQL', 'user')
    password = config.get('PostgreSQL', 'password')
    host = config.get('PostgreSQL', 'host')
    port = config.get('PostgreSQL', 'port')
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port
    )
    
    cursor = conn.cursor()

    queries = {
        "source_data": "SELECT * FROM source_data",
        "standard_hub": "SELECT * FROM standard_hub",
        "standard_satellite": "SELECT * FROM standard_satellite",
        "standard_link": "SELECT * FROM standard_link",
        "pit": "SELECT * FROM pit",
        "ref_table": "SELECT * FROM ref_table",
        "ref_hub": "SELECT * FROM ref_hub",
        "ref_sat": "SELECT * FROM ref_sat",
        "non_historized_satellite": "SELECT * FROM non_historized_satellite",
        "multiactive_satellite": "SELECT * FROM multiactive_satellite",
        "non_historized_link": "SELECT * FROM non_historized_link"
    }

    dfs = {}
    for name, query in queries.items():
        cursor.execute(query)
        colnames = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        dfs[name] = pd.DataFrame(data, columns=colnames)
    
    cursor.close()
    conn.close()
    
    db = sqlite3.connect(':memory:')
    
    for table, df in dfs.items():
        df.to_sql(table, db)

    sqlite_cursor = db.cursor()

    return sqlite_cursor

@Gooey(
    navigation='TABBED',
    program_name='TurboVault4dbt',
    default_size=(800, 800),
    advanced=True,
    image_dir=image_path
)

def main():
    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), "config.ini"))
    cursor = connect_postgresql()
    cursor.execute("SELECT DISTINCT SOURCE_SYSTEM || '_' || SOURCE_OBJECT FROM source_data")
    results = cursor.fetchall()
    source_list = [row[0] for row in results]

    parser = GooeyParser(description='Config')
    parser.add_argument("--Tasks", help="Select the entities you want to generate", action="append", widget='Listbox',
                        choices=['Stage', 'Standard Hub', 'Standard Satellite', 'Standard Link', 'Non Historized Link', 'Pit', 'Non Historized Satellite', 'Multi Active Satellite', 'Record Tracking Satellite', 'Reference Table'],
                        default=['Stage', 'Standard Hub', 'Standard Satellite', 'Standard Link', 'Non Historized Link', 'Pit', 'Non Historized Satellite', 'Multi Active Satellite', 'Record Tracking Satellite', 'Reference Table'], nargs='*', gooey_options={'height': 300})
    parser.add_argument("--Sources", action="append", nargs="+", widget='Listbox', choices=source_list, gooey_options={'height': 300},
                       help="Select the sources which you want to process", default=[])
    parser.add_argument("--SourceYML", default=False, action="store_true", help="Do you want to generate the sources.yml file?")
    parser.add_argument("--Properties", default=False, action="store_true", help="Do you want to generate the properties.yml files?")
    parser.add_argument("--DBDocs", help="Please make sure to have DBDocs installed and that you are logged in.", default=False, action="store_true")

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
        'rdv_default_schema': config.get('PostgreSQL', "rdv_schema"),
        'model_path': config.get('PostgreSQL', 'model_path'),
        'hashdiff_naming': config.get('PostgreSQL', 'hashdiff_naming'),
        'stage_default_schema': config.get('PostgreSQL', "stage_schema"),
        'source_list': args.Sources[0],
        'generateSources': False,
        'source_name': None,
        'source_object': None,
    }

    if args.SourceYML:
        sources.gen_sources(data_structure)
    try:
        for data_structure['source'] in data_structure['source_list']:
            data_structure['source'] = data_structure['source'].replace('_', '_.._')
            seperatedNameAsList = data_structure['source'].split('_.._')
            data_structure['source_name'] = seperatedNameAsList[0]
            data_structure['source_object'] = ''.join(seperatedNameAsList[1:])
            generate_selected_entities.generate_selected_entities(todo, data_structure)
            if args.Properties:
                properties.gen_properties(data_structure)
    except IndexError as e:
        print("No source selected.")

    if args.DBDocs:
        generate_erd.generate_erd(data_structure['cursor'], args.Sources[0], data_structure['generated_timestamp'], data_structure['model_path'], data_structure['hashdiff_naming'])

if __name__ == "__main__":
    print("Starting Script.")
    start = time.time()
    main()
    end = time.time()
    print("Script ends.")
    print("Total Runtime: " + str(round(end - start, 2)) + "s")