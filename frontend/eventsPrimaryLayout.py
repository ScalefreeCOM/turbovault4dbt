from backend.turbovault import TurboVault
class EventsPrimaryLayout:
    def __init__(self, **kwargs) -> None:
        try:
            self.switchLayout= kwargs.get('switchLayout')
            self.print2FeedbackConsole : function = kwargs.get('print2FeedbackConsole')
        except:
            print('GUI is disabled')
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
        platformMap = {
            'Excel': (self.TurboVault.excel, self.TurboVault.doRunForExcel),
            'Google Sheets': (self.TurboVault.googleSheets, self.TurboVault.doRunForGoogleSheets),
            'Snowflake': (self.TurboVault.snowflake, self.TurboVault.doRunForSnowflake),
            'BigQuery': (self.TurboVault.bigquery, self.TurboVault.doRunForBigQuery),
            'db': (self.TurboVault.db, self.TurboVault.doRunForDb)
        }
        
        platform, runFunction = platformMap.get(selections['SourcePlatform'], (None, None))
        
        if platform and runFunction:
            platform.setTODO(
                SourceYML=selections['SourceYML'],
                Tasks=selections['Tasks'],
                Sources=selections['Sources'],
                DBDocs=selections['DBDocs'],
                Properties=selections['Properties']
            )
            runFunction()
            
    def onPressConfig(self)-> None:
        self.switchLayout('config')
    
    def onPressHelp(self)-> None:
        self.switchLayout('help')
    
        