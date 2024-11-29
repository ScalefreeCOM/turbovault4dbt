from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QSpacerItem, QSizePolicy, QFrame
from frontend.PyQt5CustomClasses import QPushButton, QLineEdit
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QFileDialog, QScrollArea
)
from PyQt5.QtGui import QFont
from PyQt5.QtCore import Qt


class FormLayout(QWidget):
    def __init__(self, **kwargs):
        super().__init__()
        self.setGeometry(100, 100, 800, 1280)
        self.config = kwargs.get('config')
        self.configPath = kwargs.get('configPath')
        self.switchLayout = kwargs.get('switchLayout')
        self.customStyle = kwargs.get('customStyle')
        self.sourcePlatform = kwargs.get('sourcePlatform')
        self.writeConfig = kwargs.get('writeConfig')
        self.onLoad()
        self._initUi()

    def genForm(self, keys, values, descriptions):
        self.content = {}
        if len(keys) != len(descriptions):
            print('TODO: Add the print method here!')
        else:
            for ind in range(len(values)):
                self.content[keys[ind]] = {
                    'description': descriptions[ind],
                    'validity': False,
                    'value': values[ind],
                }
                if values[ind] != '':
                    self.content[keys[ind]]['validity'] = True

    def _initUi(self):
        mainLayout = QVBoxLayout()
        mainLayout.setContentsMargins(0, 10, 0, 0)
        mainLayout.addWidget(self.__genFields())
        mainLayout.addLayout(self.__genButtons())
        self.setLayout(mainLayout)

    def __genFields(self):
        scrollArea = QScrollArea(self)
        scrollArea.setWidgetResizable(False)
        scrollContent = QWidget()
        scrollContent.setContentsMargins(0, 30, 0, 0)
        scrollLayout = QVBoxLayout(scrollContent)
        self.inputFields: list =[]
        # Add text input keys with labels
        for key, value in self.content.items():
            inputField = QLineEdit(
                text=value['value'],
                placeholderText=value['description'],
                style=self.customStyle.QLineEdit,
            )
            fieldLayout = QVBoxLayout()
            label = QLabel(key)
            label.setStyleSheet(self.customStyle.secondaryStyle+ "height: 40px;")
            label.setContentsMargins(60, 30, 0, 0)
            fieldLayout.addWidget(label, alignment=Qt.AlignLeft)
            fieldLayout.addWidget(inputField)

            #fieldLayoutFrame = QFrame()
            #fieldLayoutFrame.setFrameStyle(QFrame.Box | QFrame.Sunken)
            #fieldLayoutFrame.setLineWidth(2)  # Adjust line width as needed
            #fieldLayoutFrame.setMidLineWidth(0) 
            #fieldLayoutFrame.setLayout(fieldLayout)

            #scrollLayout.addWidget(fieldLayoutFrame)
            scrollLayout.addLayout(fieldLayout)
            self.inputFields.append((inputField, label))


        scrollArea.setWidget(scrollContent)
        scrollContent.setFixedWidth(self.width()) 
        return scrollArea
    
    def __genButtons(self):
        # Bottom buttons
        bottomButtonLayout = QHBoxLayout()

        bottomButtonLayout.setContentsMargins(12, 6, 12, 6)
        okButton = QPushButton(text="OK")
        okButton.clicked.connect(self.onOK)
        cancelButton = QPushButton(text="Cancel")
        cancelButton.clicked.connect(self.onCancel)


        bottomButtonLayout.addWidget(okButton, 
                                     alignment=Qt.AlignRight,
                                     )
        bottomButtonLayout.addWidget(cancelButton, 
                                     #alignment=Qt.AlignRight,
                                     )
        return bottomButtonLayout

    def changeLayout(self) -> None:
        self.switchLayout('config')
    
    def syncDataWithMemory(self):
        for field, label in self.inputFields:
            self.content[label.text()]['value'] = field.text()

    def onLoad(self):

        keys = []
        values = []
        descriptions = []
        for key, value in self.config[self.sourcePlatform].items():
            keys.append(key)
            values.append(value)
            descriptions.append("Enter value for " + key)
        self.genForm(keys, values, descriptions)

    def onOK(self):
        self.syncDataWithMemory()
        for key, value in self.content.items():
            self.config.set(self.sourcePlatform, key, value['value'])

        self.writeConfig()
        self.changeLayout()

    def onCancel(self):
        self.changeLayout()



