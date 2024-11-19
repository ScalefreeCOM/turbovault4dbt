from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QSpacerItem, QSizePolicy
from frontend.PyQt5CustomClasses import QPushButton, QLineEdit
from PyQt5.QtGui import QFont
class FormLayout(QWidget):
    def __init__(self, **kwargs):
        super().__init__()
        self.switchLayout= kwargs.get('switchLayout')
        self.customStyle: object = kwargs.get('customStyle')
        self._initUi()

    def _initUi(self):
        mainLayout = QVBoxLayout()

        # Add text input fields with labels
        for i in range(1, 11):
            labelText = f"Field {i}:"
            inputField = QLineEdit(f"Enter text for Field {i}")
            
            fieldLayout = QHBoxLayout()
            label = QLabel(labelText)
            label.setFont(QFont("Rajdhani", 10))
            label.setFixedWidth(80)
            fieldLayout.addWidget(label)
            fieldLayout.addWidget(inputField)
            
            mainLayout.addLayout(fieldLayout)
        
        # Bottom buttons
        bottomButtonLayout = QHBoxLayout()
        bottomButtonLayout.addSpacerItem(QSpacerItem(40, 20, QSizePolicy.Expanding, QSizePolicy.Minimum))

        saveButton = QPushButton("Save")
        okButton = QPushButton("OK")
        okButton.clicked.connect(self.changeLayout)
        cancelButton = QPushButton("Cancel")

        bottomButtonLayout.addWidget(saveButton)
        bottomButtonLayout.addWidget(okButton)
        bottomButtonLayout.addWidget(cancelButton)
        mainLayout.addLayout(bottomButtonLayout)

        self.setLayout(mainLayout)
    
    def changeLayout(self) -> None:
        self.switchLayout('config')