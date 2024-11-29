from backend.excel import Excel
from backend.googleSheets import Googlesheets
from backend.snowflake import Snowflake
from backend.bigquery import Bigquery
from backend.db import Database
class TurboVault():
    def __init__(self, **kwargs):
        self.turboVaultconfigs = kwargs.get('turboVaultconfigs')
        validSourcePlatforms: list = kwargs.get('validSourcePlatforms')
        if isinstance(validSourcePlatforms, list):
            pass
        else:
            validSourcePlatforms = [validSourcePlatforms]
        self.print2FeedbackConsole : function = kwargs.get('print2FeedbackConsole')
        if self.print2FeedbackConsole == None:
           self.print2FeedbackConsole = print
            
        self.supportedPlatforms= ['Excel', 'Googlesheets','Snowflake','Bigquery','Database']
        for validSource in validSourcePlatforms:
            if validSource in self.supportedPlatforms:
                platformClass = globals().get(validSource.capitalize())
                if platformClass:
                    setattr(self, validSource, platformClass(
                        turboVaultconfigs = self.turboVaultconfigs[validSource], 
                        print2FeedbackConsole = self.print2FeedbackConsole,
                        ))  
                                  
    def doRunForExcel(self):
        self.Excel.run()
        
    def doRunForBigquery(self):
        self.Bigquery.run()

    def doRunForGooglesheets(self):
        self.Googlesheets.run()
    
    def doRunForSnowflake(self):
        self.Snowflake.run()

    def doRunForDatabase(self):
        self.Database.run()