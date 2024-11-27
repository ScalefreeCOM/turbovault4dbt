from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QLabel,QPushButton, QLineEdit, QHBoxLayout
)
from PyQt5.QtCore import Qt, QPropertyAnimation, QRect, QEasingCurve, QTimer, QEasingCurve, pyqtProperty, QPoint
from PyQt5.QtGui import QColor, QPainter, QPen, QMovie, QFont, QLinearGradient, QRadialGradient, QPixmap
from frontend.styles import customStyle

class LoadingScreen(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        # Attach to the main window as a child widget and set a semi-transparent background
        self.setFixedSize(parent.size())
        self.move(0, 0)  # Position at top-left corner
        self.setStyleSheet("background-color: rgba(45, 35, 130, 128);")  # 50% transparency
        
        # Center layout for loading animation
        overlay_layout = QVBoxLayout(self)
        overlay_layout.setAlignment(Qt.AlignCenter)
        
        # Loading animation label
        self.loading_label = QLabel(self)
        self.loading_label.setAlignment(Qt.AlignCenter)

        # Set GIF as loading animation
        self.movie = QMovie("loading.gif")  # Ensure the GIF file is available
        self.loading_label.setMovie(self.movie)
        self.movie.start()

        # Add loading label to overlay layout
        overlay_layout.addWidget(self.loading_label)

        # Ensures overlay resizes when the parent window is resized
        parent.installEventFilter(self)

    def eventFilter(self, source, event):
        # Resize the overlay when the parent window resizes
        if event.type() == event.Resize and source is self.parent():
            self.setFixedSize(self.parent().size())
        return super().eventFilter(source, event)

    def start(self):
        self.movie.start()
        self.show()

    def stop(self):
        self.movie.stop()
        self.close()

class ToggleBox(QWidget):
    def __init__(self, checked=False, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.setFixedSize(80, 40)  # Set size of the toggle
        self.checked = checked  # Set the initial checked state from the argument
        
        # Initialize handle position based on the checked state
        self._handle_position = self.width() - self.height() + 2 if self.checked else 2
        
        self.handle_radius = self.height() // 2 - 2

        # Create animation for the toggle
        self.animation = QPropertyAnimation(self, b"handlePosition", self)
        self.animation.setDuration(200)
        self.animation.setEasingCurve(QEasingCurve.InOutQuad)  # Set easing curve for smooth acceleration/deceleration

    def mousePressEvent(self, event):
        self.checked = not self.checked  # Toggle the state
        self.start_animation()  # Start the animation when clicked

    def start_animation(self):
        start_pos = 2 if self.checked else self.width() - self.height() + 2
        end_pos = self.width() - self.height() + 2 if self.checked else 2

        self.animation.setStartValue(start_pos)
        self.animation.setEndValue(end_pos)
        self.animation.start()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        # Draw border
        border_color = QColor(239, 239, 239)  # Define the border color
        border_width = 2  # Set the border width
        painter.setPen(QPen(border_color, border_width))  # Set pen with color and width for the border

        # Background gradient transition
        gradient = QLinearGradient(0, 0, self.width(), 0)
        if self.checked:
            gradient.setColorAt(0, QColor(204, 238, 242))  # Light petrol
            gradient.setColorAt(1, QColor(0, 170, 190))  # Petrol
        else:
            gradient.setColorAt(0, QColor(127, 127, 127))  # Medium gray
            gradient.setColorAt(1, QColor(239, 239, 239))  # Very light gray

        # Draw inner rounded rectangle (toggle background)
        painter.setBrush(gradient)
        painter.setPen(QPen(QColor(45, 35, 130), 1))
        painter.drawRoundedRect(border_width, border_width, self.width() - 2 * border_width, self.height() - 2 * border_width, (self.height() // 2) - border_width, (self.height() // 2) - border_width)

        # Handle gradient
        handle_gradient = QRadialGradient(self._handle_position + self.handle_radius, self.handle_radius, self.handle_radius)
        handle_gradient.setColorAt(0, QColor(239, 239, 239))  # Light in the center
        handle_gradient.setColorAt(1, QColor(191, 191, 191))  # Darker at the edges

        painter.setBrush(handle_gradient)
        painter.drawEllipse(self._handle_position, 2, self.handle_radius * 2, self.handle_radius * 2)

    @pyqtProperty(int)
    def handlePosition(self):
        return self._handle_position

    @handlePosition.setter
    def handlePosition(self, pos):
        self._handle_position = pos
        self.update()  # Update the widget to reflect the new position
    
class QPushButton(QPushButton):
    def __init__(
        self, 
        borderColor: str = "black", 
        borderWidth: int = 0,  
        fontSize: int = 16,
        backgroundColor: str = "lightgray", 
        textColor: str = "black", 
        fontName: str = "Arial", 
        hoverBackgroundColor: str = "#d6f0f3", 
        hoverTextColor: str = "black", 
        hoverBorderColor: str = "black", 
        hoverBorderWidth: int = 1, 
        style: bool = True, 
        size: list = [200, 56],
        *args, 
        **kwargs
        ) -> None:
        super().__init__(*args, **kwargs)
        
        self.customStyle : object = customStyle
        # Set initial styles
        self.borderColor: str = borderColor
        self.borderWidth: int = borderWidth
        self.backgroundColor: str = backgroundColor
        self.textColor: str = textColor
        self.fontName: str = fontName
        # Add hover colors to instance variables
        self.hoverBackgroundColor: str = hoverBackgroundColor
        self.hoverTextColor: str = hoverTextColor
        self.hoverBorderColor: str = hoverBorderColor
        self.hoverBorderWidth: int = hoverBorderWidth
        self.fontSize: int = fontSize
        self.setFixedSize(size[0], size[1])
        if style:
            self.setStyleSheet(self.customStyle.buttonStyle)
        else:
            self.setStyleSheet(self._getStylesheet())
            self.setFont(QFont(self.fontName))
        # Initialize elevation animation
        self.elevationAnimation: QPropertyAnimation = QPropertyAnimation(self, b"geometry")
        self.elevationAnimation.setDuration(200)

        self.pressed.connect(self.animatePress)
        self.released.connect(self.animateRelease)

        
    def animatePress(self) -> None:
        # Set the starting position
        self.initialValue = self.geometry()
        self.targetValue  = QRect(self.x(), self.y() + 5, self.width()-5, self.height()-5)
        self.elevationAnimation.setStartValue(self.initialValue)
        
        # Move the button down by 5 pixels
        self.elevationAnimation.setEndValue(self.targetValue)
        self.elevationAnimation.start()

        # Move it back to the original position after a short delay
        QTimer.singleShot(200, self.animateRelease)

    def animateRelease(self) -> None:
        self.elevationAnimation.setStartValue(self.targetValue)
        self.elevationAnimation.setEndValue(self.initialValue)
        self.elevationAnimation.start()

    def _getStylesheet(self) -> str:
        return f"""
            QPushButton {{
                border: {self.borderWidth}px solid {self.borderColor};
                background-color: {self.backgroundColor};
                color: {self.textColor};
                padding: 10px;
                border-radius: 5px;
                font-size: {self.fontSize}px;
            }}
            QPushButton:hover {{
                background-color: {self.hoverBackgroundColor};
                color: {self.hoverTextColor};
                border: {self.hoverBorderWidth}px solid {self.hoverBorderColor};
            }}
        """
              
class QLineEdit(QLineEdit):
    def __init__(self, **kwargs):
        super().__init__()
        self.redX: str = r".\images\redX.svg"
        self.greenCheck: str = r".\images\greenCheck.svg"
        #self.Height: int = int(kwargs.get('height'))
        self.setPlaceholderText(kwargs.get('placeholderText'))
        self.setText(kwargs.get('text'))
        #self.setFixedHeight(self.Height)
        self._defaultBorderColor: QColor = QColor('lightgrey')
        self._focusBorderColor: QColor = QColor('#00aabe')
        self._currentBorderColor: QColor = self._defaultBorderColor
        self._borderWidth: int = 2
        self._animationProgress: float = 0.0
        self.textPadding: int = 80
        self.statusIcon = None
        self.animationType = 'expandCover'

        self.setStyleSheet(kwargs.get('style'))
        
        self._animationTimer: QTimer = QTimer()
        self._animationTimer.timeout.connect(self._updateAnimation)
        
        self.textToBorderDistance = 0
        self.animationSpeed = 5


    def resizeEvent(self, event):
        self.widgetHeight, self.widgetWidth = self.size().height(), self.size().width()
        super().resizeEvent(event)
        if self.statusIcon:
            self.statusIcon.move(5, (self.height() - 24) // 2)

    def focusInEvent(self, event):
        super().focusInEvent(event)
        self._animationProgress = 0.0
        self._animationTimer.start(self.animationSpeed)

    def focusOutEvent(self, event):
        super().focusOutEvent(event)
        self._currentBorderColor = self._defaultBorderColor
        self._animationProgress = 0.0
        self._animationTimer.stop()
        self.update()

    def _updateAnimation(self):
        self._animationProgress += 0.02
        if self._animationProgress >= 1.0:
            self._animationProgress = 1.0
            self._animationTimer.stop()
        self.update()

    def paintEvent(self, event):
        super().paintEvent(event)
        painter = QPainter(self)
        pen = QPen(self._defaultBorderColor, self._borderWidth)
        painter.setPen(pen)

        # Default border line (static)
        painter.drawLine(
            self.textPadding,
            self.height() + self.textToBorderDistance,
            self.width() - self.textPadding,
            self.height() + self.textToBorderDistance,
        )

        # Animation: Expand Cover effect
        if self._animationProgress > 0.0:
            pen.setColor(self._focusBorderColor)
            painter.setPen(pen)

            # Step 1: Draw bottom border
            animatedWidth = (self.width() - 2 * self.textPadding) * self._animationProgress
            painter.drawLine(
                self.textPadding,
                self.height() + self.textToBorderDistance,
                self.textPadding + animatedWidth,
                self.height() + self.textToBorderDistance,
            )

            # Step 2: Draw vertical borders
            if self._animationProgress > 0.5:
                verticalProgress = (self._animationProgress - 0.5) * 2  # Normalize to [0, 1]
                topY = self.height() + self.textToBorderDistance - (self.height() * verticalProgress)
                painter.drawLine(
                    self.textPadding,
                    self.height() + self.textToBorderDistance,
                    self.textPadding,
                    topY,
                )
                painter.drawLine(
                    self.width() - self.textPadding,
                    self.height() + self.textToBorderDistance,
                    self.width() - self.textPadding,
                    topY,
                )

            # Step 3: Draw top border
            if self._animationProgress == 1.0:
                painter.drawLine(
                    self.textPadding + animatedWidth,
                    self.height()- self.widgetHeight,
                    self.textPadding,
                    self.height()- self.widgetHeight,
                )
        painter.end()
