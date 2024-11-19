import sys
from PyQt5.QtWidgets import QApplication
from backend.config.config import MetadataInputConfig
from frontend.pyqt5 import MainApp
def main():
    app = QApplication(sys.argv)
    window = MainApp(configData = MetadataInputConfig().data)
    window.show()
    sys.exit(app.exec_())
if __name__ == '__main__':
    main()