from backend.excel import Excel
from backend.googleSheets import GoogleSheets
from backend.snowflake import Snowflake
from backend.bigquery import BigQuery
from backend.Database import Database
class TurboVault():
    def __init__(self, **kwargs):
        self.turboVaultconfigs = kwargs.get('turboVaultconfigs')
        validSourcePlatforms= kwargs.get('validSourcePlatforms')
        self.print2FeedbackConsole : function = kwargs.get('print2FeedbackConsole')
        if 'Excel' in validSourcePlatforms:
            self.excel        = Excel(turboVaultconfigs = self.turboVaultconfigs['Excel'], print2FeedbackConsole = self.print2FeedbackConsole)
        if 'Google Sheets' in validSourcePlatforms:    
            self.googleSheets = GoogleSheets(turboVaultconfigs = self.turboVaultconfigs['Google Sheets'], print2FeedbackConsole = self.print2FeedbackConsole)
        if 'Snowflake' in validSourcePlatforms:
            self.snowflake    = Snowflake(turboVaultconfigs = self.turboVaultconfigs['Snowflake'], print2FeedbackConsole = self.print2FeedbackConsole)
        if 'BigQuery' in validSourcePlatforms:
            self.bigquery     = BigQuery(turboVaultconfigs = self.turboVaultconfigs['BigQuery'], print2FeedbackConsole = self.print2FeedbackConsole)
        if 'Database' in validSourcePlatforms:
            self.Database     = Database(turboVaultconfigs = self.turboVaultconfigs['Database'], print2FeedbackConsole = self.print2FeedbackConsole)

    def doRunForExcel(self):
        self.excel.run()
        
    def doRunForBigQuery(self):
        self.bigquery.run()

    def doRunForGoogleSheets(self):
        self.googleSheets.run()
    
    def doRunForSnowflake(self):
        self.snowflake.run()

    def doRunForDatabase(self):
        self.Database.run()