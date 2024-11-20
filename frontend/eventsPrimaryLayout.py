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
            self.TurboVault.Excel.read()
            newSources : list = self.TurboVault.Excel.data_structure['source_list']
        elif selection == 'Googlesheets':
            self.TurboVault.Googlesheets.read()
            newSources : list = self.TurboVault.Googlesheets.data_structure['source_list']
        elif selection == 'Snowflake':
            self.TurboVault.Snowflake.read()
            newSources : list = self.TurboVault.Snowflake.data_structure['source_list']         
        elif selection == 'Bigquery':
            self.TurboVault.Bigquery.read()
            newSources : list = self.TurboVault.Bigquery.data_structure['source_list']
        elif selection == 'Db':
            self.TurboVault.Db.read()
            newSources : list = self.TurboVault.Db.data_structure['source_list']
        return newSources
    
    def onPressStart(self, selections) -> None:
        
        selectedSourcePlatform = selections['SourcePlatform']
        platform = getattr(self.TurboVault, selectedSourcePlatform, None)
        if platform:
            platform.setTODO(
                SourceYML=selections['SourceYML'],
                Tasks=selections['Tasks'],
                Sources=selections['Sources'],
                DBDocs=selections['DBDocs'],
                Properties=selections['Properties']
            )
        method = getattr(self.TurboVault, f"doRunFor{selectedSourcePlatform.capitalize()}", None)
        if method:
            method()            

    def onPressConfig(self)-> None:
        self.switchLayout('config')
    
    def onPressHelp(self)-> None:
        self.switchLayout('help')
    
        