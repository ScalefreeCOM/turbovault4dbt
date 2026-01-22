import os
from backend.procs.sqlite3 import generate_selected_entities, sources, generate_erd
from logging import Logger
import sqlite3
import pandas as pd
from datetime import datetime
import time
from backend.procs.sqlite3 import properties
from pathlib import Path
image_path = os.path.join(os.path.dirname(__file__),"images")
log = Logger('log')

class Database:
    def __init__(self, **kwargs):
        self.todo = []
        self.config = kwargs.get('turboVaultconfigs')

        # Use pathlib for cross-platform path resolution
        current_file = Path(__file__).resolve()
        
        # Robustly resolve db_path
        db_path_raw = self.config.get('db_path')
        self.db_path = str((current_file.parent / db_path_raw).resolve())

        # Handle model_path by removing relative markers and joining with root
        semodel_path_ra = self.config.get('model_path').replace('../', '').replace('\\', '/').strip('/')

        self.data_structure = {
            'print2FeedbackConsole': kwargs.get('print2FeedbackConsole'),
            'console_outputs': True,
            'cursor': None,
            'source': None,
            'generated_timestamp': None,
            'rdv_default_schema': self.config.get("rdv_schema"),
            'model_path': self.model_path,
            'hashdiff_naming': self.config.get('hashdiff_naming'),
            'stage_default_schema': self.config.get("stage_schema"),  
            'source_list': None  ,
            'generateSources': False,
            'source_name' : None, # "Source" field splits into this field
            'source_object' : None, # "Source" field splits into this field
        }  

    
    def setTODO(self, **kwargs):
        self.SourceYML = kwargs.pop('SourceYML')
        self.todo = kwargs.pop('Tasks')
        self.DBDocs = kwargs.pop('DBDocs')
        self.Properties = kwargs.pop('Properties')
        self.selectedSources = kwargs.pop('Sources')
        
    def __initializeInMemoryDatabase(self):
        db = sqlite3.connect(self.db_path)
        return db.cursor()
                    
    def read(self):
        self.data_structure['generated_timestamp'] = datetime.now().strftime("%Y%m%d%H%M%S")
        self.data_structure['cursor'] = self.__initializeInMemoryDatabase()
        self.data_structure['cursor'].execute("SELECT DISTINCT SOURCE_SYSTEM || '_*-*_' || SOURCE_OBJECT FROM source_data")
        results = self.data_structure['cursor'].fetchall()
        source_list = []
        for row in results:
            source_list.append(row[0])
        self.data_structure['source_list'] = source_list
        
    def run(self):
        self.read()
        if self.SourceYML:
            sources.gen_sources(self.data_structure)

        for self.data_structure['source'] in self.selectedSources:
            seperatedNameAsList = self.data_structure['source'].split('_*-*_')
            self.data_structure['source_name']   = seperatedNameAsList[0]
            self.data_structure['source_object'] = ''.join(seperatedNameAsList[1:])
            generate_selected_entities.generate_selected_entities(self.todo, self.data_structure)
            if self.Properties:
                try:
                    properties.gen_properties(self.data_structure)
                except:
                    self.data_structure['print2FeedbackConsole'](message= 'Failed to generate properties for {0} : {1}.'.format(self.data_structure['source_name'], self.data_structure['source_object']))
        self.data_structure['print2FeedbackConsole'](message= 'Process successfully executed and models are ready to be used in Datavault 4dbt.')


        if self.DBDocs:
            generate_erd.generate_erd(self.data_structure['cursor'], self.selectedSources, self.data_structure['generated_timestamp'],self.data_structure['model_path'],self.data_structure['hashdiff_naming'])
