from backend.excel import Excel
from backend.googleSheets import Googlesheets
from backend.snowflake import Snowflake
from backend.bigquery import Bigquery
from backend.db import Db
class TurboVault():
    def __init__(self, **kwargs):
        self.turboVaultconfigs = kwargs.get('turboVaultconfigs')
        validSourcePlatforms= kwargs.get('validSourcePlatforms')
        self.print2FeedbackConsole : function = kwargs.get('print2FeedbackConsole')
        self.supportedPlatforms= ['Excel', 'Googlesheets','Snowflake','Bigquery','Db']
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

    def doRunForDb(self):
        self.Db.run()