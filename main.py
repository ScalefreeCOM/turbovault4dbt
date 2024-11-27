# The source which is defined becoming pre-selected

import sys
from PyQt5.QtWidgets import QApplication
from backend.config.config import MetadataInputConfig
from frontend.mainApp import MainApp
def main():
    app = QApplication(sys.argv)
    config = MetadataInputConfig()
    window = MainApp(
        configData = config.data,
        writeConfig = config.write,
        )
    window.show()
    sys.exit(app.exec_())
if __name__ == '__main__':
    main()