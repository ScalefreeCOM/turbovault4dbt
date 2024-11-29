import os
import subprocess
import ctypes
from datetime import datetime
from threading import Thread, Lock
from PyQt5.QtCore import Qt, QSize
from PyQt5.QtGui import (
    QColor, QMovie, QPixmap, QStandardItem, QStandardItemModel, QIcon 
)
from PyQt5.QtSvg import QSvgWidget
from PyQt5.QtWidgets import (
    QCheckBox, QComboBox, QLabel, QListWidget, QListWidgetItem,
    QStackedLayout, QVBoxLayout, QHBoxLayout, QGridLayout, QScrollBar, QTextEdit, QWidget, QSpacerItem, QSizePolicy
)
from frontend.PyQt5CustomClasses  import QPushButton
from frontend.eventsPrimaryLayout import EventsPrimaryLayout

class PrimaryLayout(QWidget):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        ConfigData: dict = kwargs.pop('configData')
        self.customStyle: object = kwargs.get('customStyle')
        self.validSourcePlatforms  : list = ConfigData['validSourcePlatforms']
        self.invalidSourcePlatforms: list = ConfigData['invalidSourcePlatforms']
        self.config: object = ConfigData['config']
        self.events: object = EventsPrimaryLayout(
            config = self.config, 
            print2FeedbackConsole= self.print2FeedbackConsole,
            validSourcePlatforms = self.validSourcePlatforms,
            switchLayout= kwargs.get('switchLayout'),
        )
        self.leftLabelPath: str = r".\frontend\images\turbovault4dbt_logo_rgb.svg"
        self.rightLabelPath: str = r".\frontend\images\scalefree_logo_rgb.svg"
        self.configIconPath: str = r".\frontend\images\config.svg"
        self.helpIconPath: str = r".\frontend\images\help.svg"
        self.selections: dict = {
            'Tasks': [], 
            'Sources': [], 
            'SourceYML': False, 
            'DBDocs': False, 
            'Properties': False,
            'SourcePlatform': None,           
        }

        self.lock = Lock()  
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("TurboVault4dbt")
        self.setupUI()
        
    def setupUI(self) -> None:
        mainLayout: QVBoxLayout = QVBoxLayout()
        mainLayout.setContentsMargins(0, 0, 0, 0)

        mainLayout.addLayout(self.__createConfigHelpLogo())
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
        self.setStyleSheet(self.customStyle.primaryStyle)

    def redirectToGoogle(self, event):
        pass
    
    def __createConfigHelpLogo(self)-> QVBoxLayout:
            container : QVBoxLayout = QVBoxLayout() 
            configButton = QPushButton(
                backgroundColor = None,
                style= False, 
                )
            configButton.setIcon(QIcon(self.configIconPath))
            configButton.setIconSize(QSize(self.customStyle.configIconSize[0], self.customStyle.configIconSize[1]))
            configButton.setFixedSize(self.customStyle.configIconSize[0], self.customStyle.configIconSize[1])
            configButton.setFlat(True)
            configButton.clicked.connect(self.events.onPressConfig)
            container.addWidget(configButton, alignment=Qt.AlignLeft | Qt.AlignTop)
            
            helpButton = QPushButton(
                backgroundColor = None,
                style= False, 
                )
            helpButton.setIcon(QIcon(self.helpIconPath))
            helpButton.setIconSize(QSize(self.customStyle.configIconSize[0], self.customStyle.configIconSize[1]))
            helpButton.setFixedSize(self.customStyle.configIconSize[0], self.customStyle.configIconSize[1])
            helpButton.setFlat(True)
            helpButton.clicked.connect(self.events.onPressHelp)            
            container.addWidget(helpButton, alignment=Qt.AlignLeft | Qt.AlignTop)

            # Create a horizontal layout for
            imageLayout = QHBoxLayout()
            imageLayout.setContentsMargins(12, 12, 12, 12)

            self.leftImageLabel = QLabel(self)
            Logo = QSvgWidget(self.leftLabelPath)
            #Logo.setFixedSize(self.customStyle.logoSize[0], self.customStyle.logoSize[1])
            Logo.mousePressEvent = self.redirectToGoogle
            imageLayout.addLayout(container)
            imageLayout.addWidget(Logo, alignment=Qt.AlignCenter | Qt.AlignTop)


            emptyLabel = QLabel()
            emptyLabel.setFixedWidth(self.customStyle.configIconSize[0])
            spacerBox = QHBoxLayout()
            spacerBox.addWidget(emptyLabel, alignment=Qt.AlignRight | Qt.AlignTop)
            imageLayout.addLayout(spacerBox)


            return imageLayout
              
    def __createDropdownLayout(self) -> QVBoxLayout:
        dropdownLayout: QVBoxLayout = QVBoxLayout()  
        dropdownLabel: QLabel = QLabel("METADATA INPUT")

        # Set the style for the dropdown label
        dropdownLabel.setStyleSheet(self.customStyle.primaryStyle)

        self.sourcePlatformCombo: QComboBox = QComboBox()
        
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
            
        self.sourcePlatformCombo.currentIndexChanged.connect(self.updateSources)
        
        # Set the style for the combo box and adjust its height
        self.sourcePlatformCombo.setStyleSheet(self.customStyle.dropdownStyle)
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
        sourcesLabel.setStyleSheet(self.customStyle.primaryStyle)

        self.selectAllSourcesBtn: QPushButton = QPushButton(text="SELECT ALL")
        self.selectAllSourcesBtn.clicked.connect(self.toggleSelectSources)

        # Add label and button to the horizontal layout
        sourcesLabelLayout.addWidget(sourcesLabel)
        sourcesLabelLayout.addWidget(self.selectAllSourcesBtn)

        self.sourcesList: QListWidget = QListWidget()
        self.sourcesList.setSelectionMode(QListWidget.MultiSelection)
        self.sourcesList.setMinimumHeight(56)
        self.sourcesList.setStyleSheet(self.customStyle.listStyle + self.customStyle.scrollBarStyle)

        # Add the label layout and list widget to the sources layout
        self.sourcesLayout.addLayout(sourcesLabelLayout)
        self.sourcesLayout.addWidget(self.sourcesList)

        # Tasks Layout
        self.tasksLayout: QVBoxLayout = QVBoxLayout()  # Now instance variable
        self.tasksLayout.setContentsMargins(6, 12, 6, 12)

        # Create a horizontal layout for the tasks label and deselect button
        tasksLabelLayout: QHBoxLayout = QHBoxLayout()
        tasksLabel = QLabel("ENTITIES")
        tasksLabel.setStyleSheet(self.customStyle.primaryStyle)

        self.deselectAllTasksBtn: QPushButton = QPushButton(text="DESELECT ALL")
        self.deselectAllTasksBtn.clicked.connect(self.toggleDeselectTasks)

        # Add label and button to the horizontal layout
        tasksLabelLayout.addWidget(tasksLabel)
        tasksLabelLayout.addWidget(self.deselectAllTasksBtn)

        self.tasksList: QListWidget = QListWidget()
        self.tasksList.setSelectionMode(QListWidget.MultiSelection)
        self.tasksList.setMinimumHeight(56)
        self.tasksList.setStyleSheet(self.customStyle.listStyle + self.customStyle.scrollBarStyle)

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
            check_box.setStyleSheet(self.customStyle.checkboxStyle)
            hbox.addWidget(check_box)
            label: QLabel = QLabel(label_text)
            label.setStyleSheet(self.customStyle.textStyle)
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
        self.feedbackConsole.setStyleSheet(self.customStyle.textStyle + "background: rgba(237, 237, 237, 255); border: 0px solid #2d2382; padding: 6px;")
        
        # Create a custom QScrollBar and assign it to the console
        customScrollBar = QScrollBar()
        customScrollBar.setStyleSheet(self.customStyle.scrollBarStyle)
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
        
        # Set the style for the Start button
        self.startButton.setStyleSheet(self.customStyle.buttonStyle)
        self.startButton.clicked.connect(self.onStart)
        
        self.startButton.setEnabled(False)
        self.startButton.setToolTip("PLEASE SELECT AT LEAST ONE SOURCE AND ONE TASK.")
        
        self.cancelButton: QPushButton = QPushButton(text= "CANCEL")

        # Set the style for the Cancel button
        self.cancelButton.setStyleSheet(self.customStyle.buttonStyle)
        self.cancelButton.clicked.connect(self.events.onPressConfig) # TODO: handle

        # Trademark label 
        trademarkLabel: QLabel = QLabel("© " + str(datetime.now().year) + " Scalefree International GmbH")
        trademarkLabel.setStyleSheet(self.customStyle.secondaryStyle + "padding: 6px;")
        
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
            self.startButton.setStyleSheet(self.customStyle.buttonStyle)
            self.startButton.setToolTip("CLICK TO START THE PROCESS.") 
        else:
            self.startButton.setStyleSheet(self.customStyle.disabledButtonStyle)
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
        
    
    def print2FeedbackConsole(self, message=None) -> None:
        self.feedbackConsole.append(message)

    def enableWidgets(self, state) -> None:
        self.sourcePlatformCombo.setEnabled(state)
        self.selectAllSourcesBtn.setEnabled(state)
        self.sourcesList.setEnabled(state)
        self.deselectAllTasksBtn.setEnabled(state)
        self.tasksList.setEnabled(state)
        self.startButton.setEnabled(state)
        