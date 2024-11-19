from backend.turbovault import TurboVault
class Events:
    def __init__(self, **kwargs) -> None:

        self.print2FeedbackConsole : function = kwargs.get('print2FeedbackConsole')
        self.TurboVault : object = TurboVault(
            turboVaultconfigs= kwargs.get('config'), 
            print2FeedbackConsole= self.print2FeedbackConsole,
            validSourcePlatforms= kwargs.get('validSourcePlatforms'),
            )

    
    def onDropdownSelection(self, selection: str) -> list:
        if selection == 'Excel':
            self.TurboVault.excel.read()
            newSources : list = self.TurboVault.excel.data_structure['source_list']
        elif selection == 'Google Sheets':
            self.TurboVault.googleSheets.read()
            newSources : list = self.TurboVault.googleSheets.data_structure['source_list']
        elif selection == 'Snowflake':
            self.TurboVault.snowflake.read()
            newSources : list = self.TurboVault.snowflake.data_structure['source_list']         
        elif selection == 'BigQuery':
            self.TurboVault.bigquery.read()
            newSources : list = self.TurboVault.bigquery.data_structure['source_list']
        elif selection == 'db':
            self.TurboVault.db.read()
            newSources : list = self.TurboVault.db.data_structure['source_list']
        return newSources
    
    def onPressStart(self, selections) -> None:    
        if selections['SourcePlatform'] == 'Excel':
            self.TurboVault.excel.setTODO(
                SourceYML = selections['SourceYML'], 
                Tasks = selections['Tasks'] , 
                Sources= selections['Sources'], 
                DBDocs= selections['DBDocs'], 
                Properties= selections['Properties'],
                ) 
            self.TurboVault.doRunForExcel()

        elif selections['SourcePlatform'] == 'Google Sheets':
            self.TurboVault.googleSheets.setTODO(
                SourceYML = selections['SourceYML'], 
                Tasks = selections['Tasks'] , 
                Sources= selections['Sources'], 
                DBDocs= selections['DBDocs'], 
                Properties= selections['Properties'],
                ) 
            self.TurboVault.doRunForGoogleSheets()

        elif selections['SourcePlatform'] == 'Snowflake':
            self.TurboVault.snowflake.setTODO(
                SourceYML = selections['SourceYML'], 
                Tasks = selections['Tasks'] , 
                Sources= selections['Sources'], 
                DBDocs= selections['DBDocs'], 
                Properties= selections['Properties'],
                ) 
            self.TurboVault.doRunForSnowflake()
        
        elif selections['SourcePlatform'] == 'BigQuery':
            self.TurboVault.bigquery.setTODO(
                SourceYML = selections['SourceYML'], 
                Tasks = selections['Tasks'] , 
                Sources= selections['Sources'], 
                DBDocs= selections['DBDocs'], 
                Properties= selections['Properties'],
                ) 
            self.TurboVault.doRunForBigQuery()
            
        elif selections['SourcePlatform'] == 'db':
            self.TurboVault.db.setTODO(
                SourceYML = selections['SourceYML'], 
                Tasks = selections['Tasks'] , 
                Sources= selections['Sources'], 
                DBDocs= selections['DBDocs'], 
                Properties= selections['Properties'],
                ) 
            self.TurboVault.doRunForDb()