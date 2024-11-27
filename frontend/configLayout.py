from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QScrollArea, QGridLayout
)
from PyQt5.QtGui import QIcon
from PyQt5.QtCore import QSize, Qt
from frontend.PyQt5CustomClasses import QPushButton

class ConfigLayout(QWidget):
    def __init__(self, **kwargs):
        super().__init__()
        self.customStyle: object = kwargs.get('customStyle')
        self._switchLayout : function = kwargs.get('switchLayout')
        self._initUi()

    def _initUi(self):
        # Main layout
        mainLayout = QVBoxLayout()
        
        # Back button
        backButton = QPushButton(text="←")
        backButton.setFixedSize(100, 30)
        backButton.clicked.connect(lambda: self._switchLayout("primary"))
        mainLayout.addWidget(backButton, alignment=Qt.AlignLeft)

        # Scrollable icon view
        iconScrollArea = QScrollArea()
        iconScrollArea.setWidgetResizable(True)
        
        iconContainer = QWidget()
        iconGridLayout = QGridLayout(iconContainer)
        iconGridLayout.setContentsMargins(0, 0, 0, 0)
        iconGridLayout.setVerticalSpacing(50)

        # Add large icons with labels
        icons = [
            ("Snowflake", r".\frontend\images\snowflake.svg"),
            ("Google\nsheets", r".\frontend\images\googleSheets.svg"),
            ("Bigquery", r".\frontend\images\bigquery.svg"),
            ("Excel", r".\frontend\images\excel.svg"),
            ("Database", r".\frontend\images\database.svg"),
        ]
        itemPerRow: int = 3
        for index, (label, iconPath) in enumerate(icons):
            iconButton = QPushButton(
                backgroundColor = None,
                style= False, 
                )
            iconButton.setIcon(QIcon(iconPath))
            iconButton.setIconSize(QSize(120, 120))
            iconButton.setFixedSize(120, 120)
            iconButton.setFlat(True)
            iconButton.clicked.connect(lambda _, lbl=label: self._onIconSelected(lbl))
            
            iconLabel = QLabel(label)
            iconLabel.setFixedWidth(120)

            iconLabel.setAlignment(Qt.AlignCenter)

            iconContainerWidget = QWidget()
            iconContainerLayout = QVBoxLayout(iconContainerWidget)
            iconContainerLayout.addWidget(iconButton)
            iconContainerLayout.setAlignment(Qt.AlignHCenter)
            iconContainerLayout.addWidget(iconLabel)

            
            iconGridLayout.addWidget(iconContainerWidget, index // itemPerRow, index % itemPerRow)
            iconGridLayout.setRowStretch(-(-len(icons)//itemPerRow), itemPerRow)           
                                  
        iconScrollArea.setWidget(iconContainer)
        mainLayout.addWidget(iconScrollArea)

        # Bottom buttons (OK and Cancel)
        bottomButtonLayout = QHBoxLayout()
        bottomButtonLayout.addStretch()  

        okButton = QPushButton(text="OK")
        okButton.clicked.connect(self._onOkClicked)
        bottomButtonLayout.addWidget(okButton)

        cancelButton = QPushButton(text="CANCEL")
        cancelButton.clicked.connect(self._onCancelClicked)
        bottomButtonLayout.addWidget(cancelButton)

        mainLayout.addLayout(bottomButtonLayout)
        self.setLayout(mainLayout)

    def _onIconSelected(self, label):
        self._switchLayout('form'+label)

    def _onOkClicked(self):
        print("OK clicked")

    def _onCancelClicked(self):
        print("Cancel clicked")