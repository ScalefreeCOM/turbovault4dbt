import os
import pandas as pd
import gspread as gs
import sqlite3
import numpy as np
from datetime              import datetime
from backend.procs.sqlite3 import sources
from backend.procs.sqlite3 import generate_selected_entities
from backend.procs.sqlite3 import generate_erd
from backend.procs.sqlite3 import properties


class Googlesheets:
    def __init__(self, **kwargs):
        self.todo = []
        self.config = kwargs.get('turboVaultconfigs')
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
        root = '\\'.join(root.split('\\')[0:-1])  ## get one step back from the root folder
        self.model_path = self.config.get('model_path')
        self.model_path = os.path.join(root , self.model_path.replace('../', '').replace('/', '\\'))
        self.credential_path = self.config.get('gcp_oauth_credentials')
        self.sheet_url = self.config.get('sheet_url')
        self.data_structure ={
            'print2FeedbackConsole': kwargs.get('print2FeedbackConsole'),
            'console_outputs': True,
            'cursor': None,
            'source': None,
            'generated_timestamp': datetime.now().strftime("%Y%m%d%H%M%S"),
            'rdv_default_schema': self.config.get("rdv_schema"),
            'model_path': self.model_path,
            'hashdiff_naming': self.config.get('hashdiff_naming'),
            'stage_default_schema': self.config.get("stage_schema"),  
            'source_list': None  ,
            'generateSources': False,
            'source_name' : None, # "Source" field splits into this field
            'source_object' : None, # "Source" field splits into this field
            'source_database': self.config.get("source_database"),
            }  
        if not kwargs.get('print2FeedbackConsole'):
            self.data_structure['console_outputs'] = False
    
    def setTODO(self, **kwargs):
        self.SourceYML = kwargs.pop('SourceYML')
        self.todo = kwargs.pop('Tasks')
        self.DBDocs = kwargs.pop('DBDocs')
        self.Properties = kwargs.pop('Properties')
        self.selectedSources = kwargs.pop('Sources')
        
    def __initializeInMemoryDatabase(self):
        gc = gs.oauth(credentials_filename=self.credential_path)
        sh = gc.open_by_url(self.sheet_url)
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
        return db.cursor()
    
    def read(self):
        self.data_structure['cursor'] = self.__initializeInMemoryDatabase()
        self.data_structure['cursor'].execute("SELECT DISTINCT SOURCE_SYSTEM || '_' || SOURCE_OBJECT FROM source_data")
        results = self.data_structure['cursor'].fetchall()
        source_list = []
        for row in results:
            source_list.append(row[0])
        self.data_structure['source_list'] = source_list
        
        self.catchDatabase()
        
    def catchDatabase(self):
        if os.path.exists('dump.db'):
            os.remove('dump.db')
        self.data_structure['cursor'].execute("vacuum main into 'dump.db'")
        self.data_structure['cursor'].close()  
                   
    def reloadDatabase(self):
        db = sqlite3.connect('dump.db')
        dest = sqlite3.connect(':memory:')
        db.backup(dest)
        db.close()
        os.remove('dump.db')
        return dest.cursor()
                                     
    def run(self):
        self.data_structure['cursor'] = self.reloadDatabase()
        if self.SourceYML:
            sources.gen_sources(self.data_structure)
        try:
            for self.data_structure['source'] in self.selectedSources:
                self.data_structure['source'] = self.data_structure['source'].replace('_','_.._')
                seperatedNameAsList = self.data_structure['source'].split('_.._')
                self.data_structure['source_name']   = seperatedNameAsList[0]
                self.data_structure['source_object'] = ''.join(seperatedNameAsList[1:])
                generate_selected_entities.generate_selected_entities(self.todo, self.data_structure)
                if self.Properties:
                    properties.gen_properties(self.data_structure)
            self.data_structure['print2FeedbackConsole']( 'Process successfully executed and models are ready to be used in Datavault 4dbt.')
        except Exception as e:
            self.data_structure['print2FeedbackConsole']( 'No sources selected!')

        if self.DBDocs:
            generate_erd.generate_erd(self.data_structure['cursor'],self.selectedSources,self.data_structure['generated_timestamp'],self.data_structure['model_path'],self.data_structure['hashdiff_naming'])
        self.data_structure['cursor'].close()  