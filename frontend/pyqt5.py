import os
import subprocess
import ctypes
from datetime import datetime
from threading import Thread, Lock
from PyQt5.QtCore import Qt
from PyQt5.QtGui import (
    QColor, QIcon, QMovie, QPixmap, QStandardItem, QStandardItemModel
)
from PyQt5.QtSvg import QSvgWidget
from PyQt5.QtWidgets import (
    QCheckBox, QComboBox, QLabel, QListWidget, QListWidgetItem,
    QStackedLayout, QVBoxLayout, QHBoxLayout, QGridLayout, QScrollBar, QTextEdit, QWidget
)
from frontend.PyQt5CustomClasses import QPushButton
from frontend.events import Events

class MainApp(QWidget):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        ConfigData: dict = kwargs.pop('configData') # Config data gettin poped
        self.validSourcePlatforms  : list = ConfigData['validSourcePlatforms']
        self.invalidSourcePlatforms: list = ConfigData['invalidSourcePlatforms']
        self.config: object = ConfigData['config']
        self.events: object = Events(
            config = self.config, 
            print2FeedbackConsole= self.print2FeedbackConsole,
            validSourcePlatforms = self.validSourcePlatforms,
        )
        self.leftLabelPath: str = r".\frontend\images\turbovault4dbt_logo_rgb.svg"
        self.rightLabelPath: str = r".\frontend\images\scalefree_logo_rgb.svg"
        self.selections: dict = {
            'Tasks': [], 
            'Sources': [], 
            'SourceYML': False, 
            'DBDocs': False, 
            'Properties': False,
            'SourcePlatform': None,           
        }
        self.primaryStyle: str = """background-color: white; border: 0px solid black; color: #2d2382; font-family: Rajdhani; font-size: 30px; padding: 0px;"""
        self.secondaryStyle: str = """background-color: white; border: 0px solid black; color: #2d2382; font-family: Rajdhani; font-size: 20px; padding: 0px;"""
        self.textStyle: str = """background-color: white; border: 0px solid black; color: #555555; font-family: "Droid Serif"; font-size: 20px; """
        self.dropdownStyle: str = """
            QComboBox {
                background-color: white; 
                border: 1px solid #2d2382; 
                color: #2d2382; 
                font-family: "Droid Serif"; 
                font-size: 20px; 
                padding-left: 12px;
            }
            QComboBox:hover { border: 2px solid #2d2382; }
            QComboBox:on { border: 1px solid #2d2382; }
            QComboBox:disabled {
                background-color: rgba(100, 100, 100, 0.5); /* light grey overlay with 50% opacity */
                color: rgba(255, 255, 255, 0.5); /* dimmed text color */
            }
            QComboBox:disabled {
                background-color: rgba(100, 100, 100, 0.5); /* light grey overlay with 50% opacity */
                color: rgba(255, 255, 255, 0.5); /* dimmed text color */
            }
            QComboBox QAbstractItemView {
                background-color: white;
                color: #555555;
                border: 1px solid #2d2382;
                outline: none; 
                selection-background-color: #f0f8ff;
                selection-color: #555555;
                padding: 6px;
            }
            QComboBox QAbstractItemView:disabled:hover { border: 2px solid #2d2382; }
            QComboBox::drop-down {
                subcontrol-position: center right;
                background-color: transparent; 
                border: 0px solid #2d2382;
                outline: none;
                width: 40px;
                height: 40px;   
                margin: 6px;      
            }
            QComboBox::down-arrow {
                image: url('./frontend/images/down_arrow.svg');
                subcontrol-origin: padding;
                padding-top: 4px;
                subcontrol-position: center;  /* Center the plus/minus symbol */
                width: 40px;
                height: 40px;             
            }
            QComboBox::down-arrow:on {
                image: url('./frontend/images/up_arrow.svg');
                subcontrol-origin: padding;
                padding-bottom: 4px;
                subcontrol-position: center;  /* Center the plus/minus symbol */
                width: 40px;
                height: 40px;  
            }
        """
        self.buttonStyle: str = """
            QPushButton {
                background-color: #00aabe; 
                border: 0px solid black; 
                color: white; 
                font-family: "Rajdhani"; 
                font-size: 24px; 
                font-weight: 500;
            }
            QPushButton:hover { background-color: #008c9e; }
            QPushButton:pressed { background-color: #007b8a; }
            QToolTip {
                background-color: white;  /* Light petrol background */
                color: #00aabe;              /* Petrol text color */
                font-size: 20px; 
                border: 1px solid #00aabe;   /* Petrol border */
                padding: 2px;
            }
            QPushButton:disabled {
                background-color: #d3d3d3;  /* Grey background for disabled state */
                color: #a9a9a9;  /* Dark grey text for disabled state */
                font-family: "Rajdhani"; 
                font-size: 24px; 
                font-weight: 500;
            }
            QToolTip:disabled {
                background-color: white;  /* Light petrol background */
                color: #00aabe;              /* Petrol text color */
                font-size: 20px; 
                border: 1px solid #00aabe;   /* Petrol border */
                padding: 2px;
            }
            QPushButton:disabled {
                background-color: #d3d3d3;  /* Grey background for disabled state */
                color: #a9a9a9;  /* Dark grey text for disabled state */
                font-family: "Rajdhani"; 
                font-size: 24px; 
                font-weight: 500;
            }
            QToolTip:disabled {
                background-color: white;  /* Light petrol background */
                color: #00aabe;              /* Petrol text color */
                font-size: 20px; 
                border: 1px solid #00aabe;   /* Petrol border */
                padding: 2px;
            }
        """
        self.disabledButtonStyle: str = """
            QPushButton {
                background-color: #d3d3d3;  /* Grey background for disabled state */
                color: #a9a9a9;  /* Dark grey text for disabled state */
                font-family: "Rajdhani"; 
                font-size: 24px; 
                font-weight: 500;
            }
            QToolTip {
                background-color: white;  /* Light petrol background */
                color: #00aabe;              /* Petrol text color */
                font-size: 20px; 
                border: 1px solid #00aabe;   /* Petrol border */
                padding: 2px;
            }
        """
        self.listStyle: str = """
            QListWidget {
                border: 1px solid #2d2382;
                font-family: "Droid Serif";
                font-size: 18px;
                color: #111111;
                background-color: white;
                padding: 6px;
            }
            QListWidget:hover { border: 1px solid #2d2382; }
            QListWidget:disabled {
                background-color: rgba(100, 100, 100, 0.5); /* light grey overlay with 50% opacity */
                color: rgba(255, 255, 255, 0.5); /* dimmed text color */
            }
            QListWidget:disabled {
                background-color: rgba(100, 100, 100, 0.5); /* light grey overlay with 50% opacity */
                color: rgba(255, 255, 255, 0.5); /* dimmed text color */
            }
            QListWidget::item {
                background-color: white;
                color: #111111;
                padding: 6px;  
                border-bottom: 1px solid #e0e0e0;
                margin-bottom: 3px;  /* Added gap between items */
            }
            QListWidget::item:hover { background-color: #f0f8ff; }
            QListWidget::item:selected {
                background-color: #d6f0f3;
                color: #2d2382;
                border-bottom: 0px solid #e0e0e0;
            }
            QListWidget::item:selected:hover { background-color: #BBD2D5; }
            QListWidget::item:disabled {
                background-color: rgba(100, 100, 100, 0.5); /* light grey overlay with 50% opacity */
                color: rgba(255, 255, 255, 0.5); /* dimmed text color */
            }
            QListWidget::item:disabled {
                background-color: rgba(100, 100, 100, 0.5); /* light grey overlay with 50% opacity */
                color: rgba(255, 255, 255, 0.5); /* dimmed text color */
            }
        """
        self.scrollBarStyle: str = """
            QScrollBar:vertical {
                background: lightgrey;
                width: 20px;
                margin: 0;
                border: none;
            }
            QScrollBar::handle:vertical {
                background: grey;
                min-height: 20px;
                border-radius: 0px;
                border: none;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                background: none;
                height: 0px;
            }
            QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical { background: none; }
        """
        self.checkboxStyle: str = """
            QCheckBox::indicator {
                width: 28px;                   /* Size of the checkbox indicator */
                height: 28px;                  /* Size of the checkbox indicator */
                border: 2px solid lightgrey;     /* Border color when unchecked */
                border-radius: 5px;            /* Rounded corners */
                background-color: white;       /* Background color when unchecked */
            }
            QCheckBox::indicator:checked {
                background-color: #00aabe;      /* Background color when checked */
                image: url('./frontend/images/checkmark.svg'); /* Path to the checkmark SVG */
                background-position: center;    /* Center the checkmark */
                background-repeat: no-repeat;   /* No repetition of the checkmark */
                border: 2px solid #00aabe;     /* Border color when unchecked */
            }
            QCheckBox::indicator:checked:hover {
                background-color: #008c9e;      /* Darker shade on hover when checked */
            }
            QCheckBox::indicator:checked:pressed {
                background-color: #007b8a;      /* Even darker blue when pressed */
            }
            QCheckBox::indicator:unchecked:hover {
                background-color: #99dde4;
            }
            QCheckBox::indicator:unchecked:pressed {
                background-color: #66ccd7;
            }
        """

        self.setWindowTitle("TurboVault4dbt")
        self.setWindowIcon(QIcon(r".\frontend\images\app_icon.png")) # Icon image should be replaced with SVG (or .ico)
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("TurboVault4dbt")
        self.setGeometry(100, 100, 800, 1280)
        self.setupUI()
        
    def setupUI(self) -> None:
        mainLayout: QVBoxLayout = QVBoxLayout()
        mainLayout.setContentsMargins(0, 0, 0, 0)

        # Create a horizontal layout for images
        imageLayout = QHBoxLayout()
        imageLayout.setContentsMargins(12, 12, 12, 12)

        # Left image
        self.leftImageLabel = QLabel(self)
        svgWidget = QSvgWidget(self.leftLabelPath)
        svgWidget.setFixedSize(300, 86)
        svgWidget.mousePressEvent = self.redirectToGoogle
        imageLayout.addWidget(svgWidget, alignment=Qt.AlignLeft | Qt.AlignTop)

        # Right image
        self.rightImageLabel = QLabel(self)
        pixmap = QPixmap(self.rightLabelPath)
        self.rightImageLabel.setPixmap(pixmap.scaled(250, 100, Qt.KeepAspectRatio))
        self.rightImageLabel.mousePressEvent = self.redirectToGoogle
        imageLayout.addWidget(self.rightImageLabel, alignment=Qt.AlignRight | Qt.AlignTop)

        # Add the image layout to the main layout
        mainLayout.addLayout(imageLayout)
        
        mainLayout.addLayout(self.__createDropdownLayout())
        mainLayout.addLayout(self.__createListsWithButtonsLayout())
        mainLayout.addLayout(self.__createCheckboxLayout())
        mainLayout.addLayout(self.__createStartCancelButtons())
        mainLayout.addLayout(self.__createFeedbackConsoleLayout())
        
        # Connect the itemSelectionChanged signals after all components are created
        self.sourcesList.itemSelectionChanged.connect(self.updateStartButtonState)
        self.tasksList.itemSelectionChanged.connect(self.updateStartButtonState)

        # Initial state update
        self.updateStartButtonState()
        
        self.setLayout(mainLayout)

        # Apply the background color and border styles to the main widget
        self.setStyleSheet(self.primaryStyle)

    def redirectToGoogle(self, event):
        pass
        
    def __createDropdownLayout(self) -> QVBoxLayout:
        dropdownLayout: QVBoxLayout = QVBoxLayout()  
        dropdownLabel: QLabel = QLabel("METADATA INPUT")

        # Set the style for the dropdown label
        dropdownLabel.setStyleSheet(self.primaryStyle)

        self.sourcePlatformCombo: QComboBox = QComboBox()
        # dropdownItems: list = ["Select a platform"] + self.validSourcePlatforms
        
        # Set up the model to allow styling each item individually
        model = QStandardItemModel()
        self.sourcePlatformCombo.setModel(model)

        # Add default option
        model.appendRow(QStandardItem("Select a platform"))

        # Add valid platforms
        for platform in self.validSourcePlatforms:
            item = QStandardItem(platform)
            model.appendRow(item)

        # Add invalid platforms as unselectable with custom color
        for platform in self.invalidSourcePlatforms:
            item = QStandardItem(platform)
            item.setForeground(QColor("#eb5a50"))  # Custom red color
            item.setEnabled(False)
            model.appendRow(item)
            
        # self.sourcePlatformCombo.addItems(dropdownItems)
        self.sourcePlatformCombo.currentIndexChanged.connect(self.updateSources)
        
        # Set the style for the combo box and adjust its height
        self.sourcePlatformCombo.setStyleSheet(self.dropdownStyle)
        self.sourcePlatformCombo.setFixedHeight(56) 
        self.sourcePlatformCombo.view().window().setWindowFlags(Qt.Popup | Qt.NoDropShadowWindowHint)
        self.sourcePlatformCombo.view().setSpacing(3)

        # Add widgets to the layout with alignment
        dropdownLayout.addWidget(dropdownLabel, alignment=Qt.AlignLeft)
        dropdownLayout.addWidget(self.sourcePlatformCombo)
        dropdownLayout.setContentsMargins(12, 12, 12, 12)

        return dropdownLayout
    
    def __createListsWithButtonsLayout(self):
        # Main grid layout
        self.mainLayout: QGridLayout = QGridLayout()
        self.mainLayout.setContentsMargins(6, 6, 6, 6)

        # Sources Layout
        self.sourcesLayout: QVBoxLayout = QVBoxLayout()
        self.sourcesLayout.setContentsMargins(6, 12, 6, 12)

        # Create a horizontal layout for the sources label and select button
        sourcesLabelLayout: QHBoxLayout = QHBoxLayout()
        sourcesLabel: QLabel = QLabel("SOURCES")
        sourcesLabel.setStyleSheet(self.primaryStyle)

        self.selectAllSourcesBtn: QPushButton = QPushButton(text="SELECT ALL")
        self.selectAllSourcesBtn.setFixedSize(200, 56)
        self.selectAllSourcesBtn.setStyleSheet(self.buttonStyle)
        self.selectAllSourcesBtn.clicked.connect(self.toggleSelectSources)

        # Add label and button to the horizontal layout
        sourcesLabelLayout.addWidget(sourcesLabel)
        sourcesLabelLayout.addWidget(self.selectAllSourcesBtn)

        self.sourcesList: QListWidget = QListWidget()
        self.sourcesList.setSelectionMode(QListWidget.MultiSelection)
        self.sourcesList.setMinimumHeight(56)
        self.sourcesList.setStyleSheet(self.listStyle + self.scrollBarStyle)

        # Add the label layout and list widget to the sources layout
        self.sourcesLayout.addLayout(sourcesLabelLayout)
        self.sourcesLayout.addWidget(self.sourcesList)

        # Tasks Layout
        self.tasksLayout: QVBoxLayout = QVBoxLayout()  # Now instance variable
        self.tasksLayout.setContentsMargins(6, 12, 6, 12)

        # Create a horizontal layout for the tasks label and deselect button
        tasksLabelLayout: QHBoxLayout = QHBoxLayout()
        tasksLabel = QLabel("ENTITIES")
        tasksLabel.setStyleSheet(self.primaryStyle)

        self.deselectAllTasksBtn: QPushButton = QPushButton(text="DESELECT ALL")
        self.deselectAllTasksBtn.setFixedSize(200, 56)
        self.deselectAllTasksBtn.setStyleSheet(self.buttonStyle)
        self.deselectAllTasksBtn.clicked.connect(self.toggleDeselectTasks)

        # Add label and button to the horizontal layout
        tasksLabelLayout.addWidget(tasksLabel)
        tasksLabelLayout.addWidget(self.deselectAllTasksBtn)

        self.tasksList: QListWidget = QListWidget()
        self.tasksList.setSelectionMode(QListWidget.MultiSelection)
        self.tasksList.setMinimumHeight(56)
        self.tasksList.setStyleSheet(self.listStyle + self.scrollBarStyle)

        tasks = [
            'Stage', 'Standard Hub', 'Standard Satellite', 'Standard Link', 'Non-Historized Link',
            'Point-in-Time', 'Non-Historized Satellite', 'Multi-Active Satellite', 'Record Tracking Satellite'
        ]

        for task in tasks:
            taskItem: QListWidgetItem = QListWidgetItem(task)
            self.tasksList.addItem(taskItem)

        self.tasksList.selectAll()

        # Add the label layout and list widget to the tasks layout
        self.tasksLayout.addLayout(tasksLabelLayout)
        self.tasksLayout.addWidget(self.tasksList)

        # Add layouts to the grid (this will be re-arranged in resize event)
        self.mainLayout.addLayout(self.sourcesLayout, 0, 0)  # Initially in first row
        self.mainLayout.addLayout(self.tasksLayout, 1, 0)    # Initially in second row

        return self.mainLayout
    
    def resizeEvent(self, event):
        self.mainLayout.removeItem(self.tasksLayout)
        if self.width() > self.height():  # If width is greater than height, arrange horizontally
            self.mainLayout.addLayout(self.tasksLayout, 0, 1)    # Second column
        else:  # If width is less than or equal to height, arrange vertically
            self.mainLayout.addLayout(self.tasksLayout, 1, 0)    # Second row

        super().resizeEvent(event)  # Call the base resize event handler

    def __createCheckboxLayout(self) -> QVBoxLayout:
        checkboxLayout: QVBoxLayout = QVBoxLayout()

        # Helper method to create checkbox layout
        def createCheckboxLayout(label_text: str, check_box: QCheckBox) -> QHBoxLayout:
            hbox: QHBoxLayout = QHBoxLayout()
            check_box.setStyleSheet(self.checkboxStyle)
            hbox.addWidget(check_box)
            label: QLabel = QLabel(label_text)
            label.setStyleSheet(self.textStyle)
            hbox.addWidget(label, alignment=Qt.AlignLeft, stretch=1)
            hbox.setContentsMargins(6, 6, 6, 6)
            return hbox

        # Create and add toggle boxes to the layout
        self.checkSourceYml: QCheckBox = QCheckBox(checked=True)
        self.checkSourceYml.labelName = "sources.yml"
        checkboxLayout.addLayout(createCheckboxLayout("Generate a sources.yml file", self.checkSourceYml))

        self.checkPropertiesYml: QCheckBox = QCheckBox(checked=True)
        self.checkPropertiesYml.labelName = "properties.yml"
        checkboxLayout.addLayout(createCheckboxLayout("Generate properties.yml files", self.checkPropertiesYml))

        self.checkVisualizeDbdocs: QCheckBox = QCheckBox()
        self.checkVisualizeDbdocs.labelName = "visualize in DBDocs"
        checkboxLayout.addLayout(createCheckboxLayout("Visualize Data Vault entities in DBDocs", self.checkVisualizeDbdocs))

        # Layout spacing and margins
        checkboxLayout.setContentsMargins(12, 12, 12, 12)

        return checkboxLayout

    def __createFeedbackConsoleLayout(self) -> QVBoxLayout:
        feedbackLayout: QVBoxLayout = QVBoxLayout()

        # Feedback console with GIF background
        self.feedbackConsole = QTextEdit(self)
        self.feedbackConsole.setReadOnly(True)
        self.feedbackConsole.setStyleSheet(self.textStyle + "background: rgba(237, 237, 237, 255); border: 0px solid #2d2382; padding: 6px;")
        
        # Create a custom QScrollBar and assign it to the console
        customScrollBar = QScrollBar()
        customScrollBar.setStyleSheet(self.scrollBarStyle)
        self.feedbackConsole.setVerticalScrollBar(customScrollBar)

        # Load and set a GIF animation as the background ## Change transparency vaule of the feedback console to reveal the background image
        self.gifBackground = QLabel(self)
        gif: QMovie = QMovie(r".\frontend\images\Test.gif")
        self.gifBackground.setMovie(gif)
        gif.start()

        # Create a layout to stack the QLabel (GIF background) and QTextEdit
        gifTextBoxLayout = QStackedLayout()
        gifTextBoxLayout.setStackingMode(QStackedLayout.StackAll)
        gifTextBoxLayout.addWidget(self.gifBackground)
        gifTextBoxLayout.addWidget(self.feedbackConsole)
        self.feedbackConsole.raise_()  # Ensure the feedback console is on top

        feedbackLayout.addLayout(gifTextBoxLayout)
        feedbackLayout.setSpacing(12)
        feedbackLayout.setContentsMargins(0, 6, 0, 12)

        return feedbackLayout
    
    def __createStartCancelButtons(self):
        buttonLayout: QHBoxLayout = QHBoxLayout()
        buttonLayout.setContentsMargins(12, 6, 12, 6)
        
        self.startButton: QPushButton = QPushButton(text= "START")
        self.startButton.setFixedSize(200, 56)
        
        # Set the style for the Start button
        self.startButton.setStyleSheet(self.buttonStyle)
        self.startButton.clicked.connect(self.onStart)
        
        self.startButton.setEnabled(False)
        self.startButton.setToolTip("PLEASE SELECT AT LEAST ONE SOURCE AND ONE TASK.")
        
        self.cancelButton: QPushButton = QPushButton(text= "CANCEL")
        self.cancelButton.setFixedSize(200, 56)

        # Set the style for the Cancel button
        self.cancelButton.setStyleSheet(self.buttonStyle)
        self.cancelButton.clicked.connect(self.onCancel)

        # Trademark label 
        trademarkLabel: QLabel = QLabel("© " + str(datetime.now().year) + " Scalefree International GmbH")
        trademarkLabel.setStyleSheet(self.secondaryStyle + "padding: 6px;")
        
        buttonLayout.addWidget(trademarkLabel)
        buttonLayout.addWidget(self.startButton, alignment=Qt.AlignRight)
        buttonLayout.addWidget(self.cancelButton)
        buttonLayout.setSpacing(20)
        
        return buttonLayout

    def updateGifBackgroundSize(self, event):
        self.gifBackground.setBaseSize(self.feedbackConsole.size())

    def __updateSources(self, lock):
        lock.acquire()
        self.sourcesList.clear()
        self.selections['SourcePlatform']= self.sourcePlatformCombo.currentText()
        sources: list = self.events.onDropdownSelection(self.selections['SourcePlatform'])
        for source in sources:
            self.sourcesList.addItem(QListWidgetItem(source))
        self.enableWidgets(True)
        lock.release()    
        
    def updateSources(self, index: int) -> None:
        if self.sourcePlatformCombo.findText('Select a platform') == 0:
            self.sourcePlatformCombo.removeItem(0)
        
        else:
            self.enableWidgets(False)
            Thread(
                target=self.__updateSources,  
                args= (Lock(),),
                name=('Metadata selection'),
            ).start()

    def updateStartButtonState(self):
        selectedSources: list = [self.sourcesList.item(i).text() for i in range(self.sourcesList.count()) if self.sourcesList.item(i).isSelected()]
        selectedTasks: list = [self.tasksList.item(i).text() for i in range(self.tasksList.count()) if self.tasksList.item(i).isSelected()]
        
        # Check if the button should be enabled
        isEnabled = bool(selectedSources) and bool(selectedTasks)

        # Set the button enabled state
        self.startButton.setEnabled(isEnabled)

        # Adjust the style and tooltip based on the state of the button
        if isEnabled:
            self.startButton.setStyleSheet(self.buttonStyle)
            self.startButton.setToolTip("CLICK TO START THE PROCESS.") 
        else:
            self.startButton.setStyleSheet(self.disabledButtonStyle)
            self.startButton.setToolTip("PLEASE SELECT AT LEAST ONE SOURCE AND ONE TASK.") 
        
    def toggleSelectSources(self) -> None:
        if self.selectAllSourcesBtn.text() == "SELECT ALL":
            self.sourcesList.selectAll()
            self.selectAllSourcesBtn.setText("DESELECT ALL")
        else:
            self.sourcesList.clearSelection()
            self.selectAllSourcesBtn.setText("SELECT ALL")

    def toggleDeselectTasks(self) -> None:
        if self.deselectAllTasksBtn.text() == "DESELECT ALL":
            self.tasksList.clearSelection()
            self.deselectAllTasksBtn.setText("SELECT ALL")
        else:
            for index in range(self.tasksList.count()):
                item: QListWidgetItem = self.tasksList.item(index)
                item.setSelected(True)
            self.deselectAllTasksBtn.setText("DESELECT ALL")
            
    def __onStart(self):
        self.selections['Tasks']= [self.tasksList.item(i).text() for i in range(self.tasksList.count()) if self.tasksList.item(i).isSelected()]
        self.selections['Sources']= [self.sourcesList.item(i).text() for i in range(self.sourcesList.count()) if self.sourcesList.item(i).isSelected()]
        self.selections['SourceYML']= self.checkSourceYml.isChecked
        self.selections['DBDocs']=self.checkVisualizeDbdocs.isChecked()
        self.selections['Properties']=self.checkPropertiesYml.isChecked()
        self.events.onPressStart(self.selections)
        self.sourcePlatformCombo.setDisabled(False)
        self.selectAllSourcesBtn.setDisabled(False)
        self.sourcesList.setDisabled(False)
        self.deselectAllTasksBtn.setDisabled(False)
        self.tasksList.setDisabled(False)
        subprocess.Popen(f'explorer "{os.path.abspath("./models/")}"')
        self.enableWidgets(True)
        
    def onStart(self):
        self.enableWidgets(False)
        Thread(
            target= self.__onStart, 
            name= 'generating',
            ).start()
        
    def onCancel(self):
        self.feedbackConsole.append("Process canceled.")
    
    def print2FeedbackConsole(self, message=None) -> None:
        self.feedbackConsole.append(message)

    def enableWidgets(self, state) -> None:
        self.sourcePlatformCombo.setEnabled(state)
        self.selectAllSourcesBtn.setEnabled(state)
        self.sourcesList.setEnabled(state)
        self.deselectAllTasksBtn.setEnabled(state)
        self.tasksList.setEnabled(state)
        self.startButton.setEnabled(state)
        