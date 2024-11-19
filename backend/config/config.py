## TODO
import os
from configparser import ConfigParser

class MetadataInputConfig:
    def __init__(self)->None:
        self.supportedPlatforms: list = ['Excel','Google Sheets','Snowflake','BigQuery','db']        
        self.configExpectedFields: dict ={
        'Snowflake':[
            'stage_schema',
            'rdv_schema',
            'hashdiff_naming',
            'model_path',
            'account_identifier',
            'database',
            'warehouse',
            'role',
            'meta_schema',
            'credential_path',
        ],
        'Google Sheets':[
            'stage_schema',
            'rdv_schema',
            'hashdiff_naming',
            'model_path',
            'sheet_url',
            'gcp_oauth_credentials',
            'source_database',
        ],
        'BigQuery':[
            'stage_schema',
            'rdv_schema',
            'metadata_dataset',
            'project_id',
            'hashdiff_naming',
            'model_path',
            'credential_path',    
        ],
        'Excel':[
            'stage_schema',
            'rdv_schema',
            'hashdiff_naming',
            'model_path',
            'excel_path',
        ],
        'db': [
            'stage_schema',
            'rdv_schema',
            'hashdiff_naming',
            'model_path',
            'db_path',           
        ]}
        self.data: dict={
        'validSourcePlatforms': [], 
        'invalidSourcePlatforms': [], 
        'config':None, 
        }
        self.read()
        try:
            self.validate()
        except:
            print('Failed to validate the source platforms')
               
    def read(self):
        config = ConfigParser()
        config.read(os.path.join(os.path.dirname(__file__),"config.ini"))
        self.data['config']= config

    def validate(self):
        for key in self.data['config'].sections():
            if key in self.configExpectedFields.keys():
                ValidSource: bool = True
                for field in self.configExpectedFields[key]:
                    if field in self.data['config'][key]:
                        pass
                    else:
                        ValidSource = False
                        print('Expected field '+ field + ' in config.ini for '+ key +'!')
                if ValidSource:
                    self.data['validSourcePlatforms'].append(key)
                else:
                    self.data['invalidSourcePlatforms'].append(key)                      
            else:
                self.data['invalidSourcePlatforms'].append(key)
                print('Invalid source platform: '+ key)
                
