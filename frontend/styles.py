def styles():
    primaryStyle: str = """background-color: white; border: 0px solid black; color: #2d2382; font-family: Rajdhani; font-size: 30px; padding: 0px;"""
    secondaryStyle: str = """background-color: white; border: 0px solid black; color: #2d2382; font-family: Rajdhani; font-size: 20px; padding: 0px;"""
    textStyle: str = """background-color: white; border: 0px solid black; color: #555555; font-family: "Droid Serif"; font-size: 20px; """
    dropdownStyle: str = """
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
    buttonStyle: str = """
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
    disabledButtonStyle: str = """
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
    listStyle: str = """
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
    scrollBarStyle: str = """
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
    checkboxStyle: str = """
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
    return primaryStyle, secondaryStyle, textStyle, dropdownStyle, buttonStyle, disabledButtonStyle, listStyle, scrollBarStyle, checkboxStyle