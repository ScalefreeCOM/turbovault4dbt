from PyQt5.QtWidgets import QMainWindow, QStackedWidget
from PyQt5.QtGui import (QIcon)
from frontend.primaryLayout import PrimaryLayout
from frontend.configLayout import ConfigLayout
from frontend.formLayout import FormLayout
from frontend.styles import styles
class MainApp(QMainWindow):
    def __init__(self, **kwargs):
        super().__init__()
        self.configData = kwargs.pop('configData')
        self.primaryStyle, _, _, _, _, _, _, _, _ = styles()
        self.initUI()
        
    def initUI(self):
        self.setWindowTitle("TurboVault4dbt")
        self.setWindowIcon(QIcon(r".\frontend\images\app_icon.png")) # Icon image should be replaced with SVG (or .ico)
        self.setGeometry(100, 100, 800, 1280)

        self.stackedWidget = QStackedWidget()
        
        # Instantiate layouts
        self.primaryLayout = PrimaryLayout(
            switchLayout= self.switchLayout,
            configData= self.configData,
            )
        self.configLayout = ConfigLayout(self.switchLayout)
        self.formLayout= FormLayout(
            switchLayout= self.switchLayout,
            )
        
        # Add layouts to stacked widget
        self.stackedWidget.addWidget(self.formLayout)
        self.stackedWidget.addWidget(self.primaryLayout)
        self.stackedWidget.addWidget(self.configLayout)
        
        self.setCentralWidget(self.stackedWidget)
        self.setStyleSheet(self.primaryStyle)
        # Show primary layout at startup
        self.switchLayout("primary")

    def switchLayout(self, layoutName):
        if layoutName == "primary":
            self.stackedWidget.setCurrentWidget(self.primaryLayout)
        elif layoutName == "config":
            self.stackedWidget.setCurrentWidget(self.configLayout)
        elif layoutName == "form":
            self.stackedWidget.setCurrentWidget(self.formLayout)