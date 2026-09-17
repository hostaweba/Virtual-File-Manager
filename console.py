import sys
from PySide6.QtCore import Qt, QObject, Signal
from PySide6.QtGui import QFont
from PySide6.QtWidgets import QDockWidget, QTextBrowser
from datetime import datetime

class StreamInterceptor(QObject):
    """Intercepts print() and OS-level errors and safely routes them to the UI thread."""
    text_written = Signal(str, str)

    def __init__(self, original_stream, default_level="INFO"):
        super().__init__()
        self.original_stream = original_stream
        self.default_level = default_level

    def write(self, text):
        # Keep writing to the actual OS console just in case
        self.original_stream.write(text)
        self.original_stream.flush()
        
        # Clean the text and prevent empty newline spam
        clean_text = text.strip()
        if clean_text:
            # Auto-detect warnings and errors from raw system output
            level = self.default_level
            upper_text = clean_text.upper()
            if "WARNING" in upper_text:
                level = "WARNING"
            elif "ERROR" in upper_text or "EXCEPTION" in upper_text or "TRACEBACK" in upper_text:
                level = "ERROR"
                
            self.text_written.emit(clean_text, level)

    def flush(self):
        self.original_stream.flush()


class VManConsole(QDockWidget):
    def __init__(self, title="Live System Console", parent=None):
        super().__init__(title, parent)
        self.setAllowedAreas(Qt.BottomDockWidgetArea)
        
        self.log_console = QTextBrowser()
        self.log_console.setReadOnly(True)
        self.log_console.setFont(QFont("Consolas", 10))
        
        # PERFORMANCE LOCK: Prevents UI freeze if thousands of logs spawn instantly. 
        # Keeps only the latest 2000 lines in memory.
        self.log_console.document().setMaximumBlockCount(2000)
        
        self.log_console.setStyleSheet("""
            QTextBrowser {
                background-color: #0d1117;
                color: #c9d1d9;
                border: 1px solid #30363d;
                padding: 5px;
            }
        """)
        self.setWidget(self.log_console)

        # Hook into standard output and errors
        self._hijack_system_streams()

    def _hijack_system_streams(self):
        """Redirects sys.stdout (prints) and sys.stderr (errors) into this widget."""
        self.stdout_interceptor = StreamInterceptor(sys.stdout, "INFO")
        self.stdout_interceptor.text_written.connect(self.log)
        sys.stdout = self.stdout_interceptor

        self.stderr_interceptor = StreamInterceptor(sys.stderr, "ERROR")
        self.stderr_interceptor.text_written.connect(self.log)
        sys.stderr = self.stderr_interceptor

    def log(self, message: str, level: str = "INFO"):
        """Formats and appends a colorized message to the console based on level."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # Professional dark-theme color palette
        colors = {
            "INFO": "#58a6ff",     # Blue
            "SUCCESS": "#3fb950",  # Green
            "WARNING": "#d29922",  # Yellow/Orange
            "ERROR": "#f85149",    # Red
            "SYSTEM": "#a371f7"    # Purple
        }
        
        color = colors.get(level.upper(), "#c9d1d9")
        level_badge = f"<span style='color:{color}; font-weight:bold;'>[{level.upper()}]</span>"
        time_badge = f"<span style='color:#8b949e;'>[{timestamp}]</span>"
        
        # We use a simple HTML append which is very fast when combined with setMaximumBlockCount
        formatted_msg = f"{time_badge} {level_badge} <span style='color:#c9d1d9;'>{message}</span>"
        self.log_console.append(formatted_msg)