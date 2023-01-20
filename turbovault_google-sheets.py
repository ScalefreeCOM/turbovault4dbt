import os
from configparser import ConfigParser
from procs.sqlite3 import stage
from procs.sqlite3 import satellite
from procs.sqlite3 import hub
#import aws
from procs.sqlite3 import link
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError



from botocore.exceptions import ClientError

from logging import Logger

import pandas as pd
import gspread as gs
import sqlite3

from gooey import Gooey
from gooey import GooeyParser

from datetime import datetime


log = Logger('log')

SAMPLE_SPREADSHEET_ID = '1R11pVSaaWku-kafmw-mDS4mzhVLVjXqwcrhQsmqB2pE'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


def sheets_init():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None

    credential_path = config.get('Google Sheets', 'gcp_oauth_credentials')
    
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(credential_path):
        creds = Credentials.from_authorized_user_file(credential_path, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credential_path, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(credential_path, 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID).execute()
        values = result.get('values', [])

        if not values:
            print('No data found.')
            return

        print('Name, Major:')
        for row in values:
            # Print columns A and E, which correspond to indices 0 and 4.
            print('%s, %s' % (row[0], row[4]))
    except HttpError as err:
        print(err)

    return values

        
@Gooey(
    navigation='TABBED',
    program_name='DBT Automation',
    default_size=(800,800),
    advanced=True)
def main():

    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),"config.ini"))
    env_mode = config.get('Google Sheets',"env_mode")

    data_storage = config.get('Google Sheets','data_storage')

    model_path = config.get('Google Sheets','model_path')
    sheet_url = config.get('Google Sheets', 'sheet_url')

    
    credential_path = config.get('Google Sheets', 'gcp_oauth_credentials')

    gc = gs.oauth(credentials_filename=credential_path)

    #gc = gs.service_account(gcp_service_account)
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


    #[@@SourceSystem, @@SourceObject, @@EntityType, @@Timestamp]

    #available_sources, source_dict = get_available_objects()

    generated_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    source_object_list = []
        
    #print(available_sources)
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
    #print(args.Sources)        

    rdv_default_schema = sheet_url = config.get('Google Sheets', 'rdv_schema')
    stage_default_schema = sheet_url = config.get('Google Sheets', 'stage_schema')

   
    for source in args.Sources[0]:
        print(source)
        if 'Stage' in todo:
            stage.main(cursor,source, generated_timestamp, stage_default_schema, model_path)
        
        if 'Hub' in todo: 
            hub.main(cursor,source, generated_timestamp, rdv_default_schema, model_path)
    
        if 'Link' in todo: 
            link.main(cursor,source, generated_timestamp, rdv_default_schema, model_path)

        if 'Satellite' in todo: 
            satellite.main(cursor, source, generated_timestamp, rdv_default_schema, model_path)

   

if __name__ == '__main__':
    main()