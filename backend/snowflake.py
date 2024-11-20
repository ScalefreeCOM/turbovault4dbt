import os
import sqlite3
import snowflake.connector
from datetime              import datetime
from configparser          import RawConfigParser
from backend.procs.sqlite3 import sources
from backend.procs.sqlite3 import generate_selected_entities
from backend.procs.sqlite3 import generate_erd
from backend.procs.sqlite3 import properties


class Snowflake:
    def __init__(self, **kwargs):
        self.todo = []
        self.config = kwargs.get('turboVaultconfigs')

        self.data_structure = {
            'print2FeedbackConsole': kwargs.get('print2FeedbackConsole'),
            'console_outputs': True,
            'cursor': None,
            'source': None,
            'generated_timestamp': None,
            'rdv_default_schema'  : None,
            'model_path'          : None,
            'hashdiff_naming'     : None,
            'stage_default_schema': None,  
            'source_list': None,
            'generateSources': False,
            'source_name' : None, # "Source" field splits into this field
            'source_object' : None, # "Source" field splits into this field
        } 
        if not kwargs.get('print2FeedbackConsole'):
            self.data_structure['console_outputs'] = False    
    def configParser(self)->bool:
        try:
            root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
            root = '\\'.join(root.split('\\')[0:-1])  ## get one step back from the root folder
            self.model_path = self.config.get('model_path')
            self.model_path = os.path.join(root , self.model_path.replace('../', '').replace('/', '\\'))
            self.snowflake_credentials = RawConfigParser()
            self.snowflake_credentials.read(self.config.get('credential_path'))
            self.user = self.snowflake_credentials.get('main', 'SNOWFLAKE_USER_NAME')
            self.password = self.snowflake_credentials.get('main', 'SNOWFLAKE_PASSWORD')
            self.account = self.config.get( 'account_identifier')
            self.database = self.config.get( 'database')
            self.warehouse = self.config.get( 'warehouse')
            self.role = self.config.get( 'role')
            self.schema = self.config.get( 'meta_schema')   
            self.data_structure['rdv_default_schema']=self.config.get("rdv_schema")
            self.data_structure['model_path']=self.model_path
            self.data_structure['hashdiff_naming']=self.config.get('hashdiff_naming')
            self.data_structure['stage_default_schema']=self.config.get("stage_schema")           
            return True
        except:
            return False
        
    def setTODO(self, **kwargs):
        self.SourceYML = kwargs.pop('SourceYML')
        self.todo = kwargs.pop('Tasks')
        self.DBDocs = kwargs.pop('DBDocs')
        self.Properties = kwargs.pop('Properties')
        self.selectedSources = kwargs.pop('Sources')
        
    def __initializeInMemoryDatabase(self):
  
        ctx = snowflake.connector.connect(
            user        = self.user,
            password    = self.password,
            account     = self.account,
            database    = self.database,
            warehouse   = self.warehouse,
            role        = self.role,
            schema      = self.schema
        )
        
        cursor = ctx.cursor()
        sql_source_data = "SELECT * FROM SOURCE_DATA"
        cursor.execute(sql_source_data)
        df_source_data = cursor.fetch_pandas_all()    
        cursor.close()

        cursor = ctx.cursor()
        sql_hub_entities = "SELECT * FROM standard_hub"
        cursor.execute(sql_hub_entities)
        df_hub_entities = cursor.fetch_pandas_all()    
        cursor.close()

        cursor = ctx.cursor()
        sql_standard_satellite = "SELECT * FROM standard_satellite"
        cursor.execute(sql_standard_satellite)
        df_standard_satellite = cursor.fetch_pandas_all()    
        cursor.close()

        cursor = ctx.cursor()
        sql_link_entities = "SELECT * FROM standard_link"
        cursor.execute(sql_link_entities)
        df_link_entities = cursor.fetch_pandas_all()    
        cursor.close()
        
        cursor = ctx.cursor()
        sql_pit_entities = "SELECT * FROM pit"
        cursor.execute(sql_pit_entities)
        df_pit = cursor.fetch_pandas_all()    
        cursor.close()

        cursor = ctx.cursor()
        sql_ref_table_entities = "SELECT * FROM ref_table"
        cursor.execute(sql_ref_table_entities)
        df_ref_table = cursor.fetch_pandas_all()    
        cursor.close()

        cursor = ctx.cursor()
        sql_ref_hub_entities = "SELECT * FROM ref_hub"
        cursor.execute(sql_ref_hub_entities)
        df_ref_hub = cursor.fetch_pandas_all()    
        cursor.close()

        cursor = ctx.cursor()
        sql_ref_sat_entities = "SELECT * FROM ref_sat"
        cursor.execute(sql_ref_sat_entities)
        df_ref_sat = cursor.fetch_pandas_all()    
        cursor.close()
        
        cursor = ctx.cursor()
        sql_non_historized_satellite = "SELECT * FROM non_historized_satellite"
        cursor.execute(sql_non_historized_satellite)
        df_non_historized_satellite = cursor.fetch_pandas_all()    
        cursor.close()

        cursor = ctx.cursor()
        sql_multiactive_satellite = "SELECT * FROM multiactive_satellite"
        cursor.execute(sql_multiactive_satellite)
        df_multiactive_satellite = cursor.fetch_pandas_all()    
        cursor.close()

        cursor = ctx.cursor()
        sql_non_historized_link = "SELECT * FROM non_historized_link"
        cursor.execute(sql_non_historized_link)
        df_non_historized_link = cursor.fetch_pandas_all()    
        cursor.close()

        cursor.close()
        ctx.close()
        
        db = sqlite3.connect(':memory:')
        dfs = {
            "source_data": df_source_data, 
            "standard_hub": df_hub_entities,
            "standard_link": df_link_entities, 
            "standard_satellite": df_standard_satellite,
            "pit": df_pit,
            "non_historized_satellite": df_non_historized_satellite,
            "multiactive_satellite": df_multiactive_satellite,
            "non_historized_link" : df_non_historized_link,
            "ref_table": df_ref_table,
            "ref_hub": df_ref_hub,
            "ref_sat": df_ref_sat
        }
        for table, df in dfs.items():
            df.to_sql(table, db)

        return db.cursor()

    def read(self):
        self.configParser()
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
        self.data_structure['generated_timestamp'] = datetime.now().strftime("%Y%m%d%H%M%S")
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
            generate_erd.generate_erd(self.data_structure['cursor'], self.data_structure['source_list'],self.data_structure['generated_timestamp'],self.data_structure['model_path'],self.data_structure['hashdiff_naming'])
        self.data_structure['cursor'].close()  