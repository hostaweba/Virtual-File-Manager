import json
from pathlib import Path
from PySide6.QtCore import Qt, QTimer, QSettings, QPointF, QPoint
from PySide6.QtGui import QFont, QPainter, QColor, QPen, QBrush, QPolygonF, QPixmap
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                               QPushButton, QPlainTextEdit, QApplication, QMenu,
                               QScrollArea, QInputDialog, QMessageBox, QSizeGrip,
                               QLineEdit, QFileDialog, QStackedWidget, QDialog,
                               QListWidget, QColorDialog, QGraphicsDropShadowEffect)
from datetime import datetime

# =========================================================================
# Analog Clock Face Engine
# =========================================================================
class AnalogWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumSize(120, 120)

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        is_dark = True
        if self.parent() and hasattr(self.parent(), 'is_dark_mode'):
            is_dark = self.parent().is_dark_mode
        
        face_col = QColor(13, 17, 23, 220) if is_dark else QColor(255, 255, 255, 220)
        border_col = QColor("#30363d") if is_dark else QColor("#d0d7de")
        accent_col = QColor("#58a6ff") if is_dark else QColor("#0969da")
        text_col = QColor("#c9d1d9") if is_dark else QColor("#24292f")
        sec_col = QColor("#f85149")

        side = min(self.width(), self.height())
        painter.translate(self.width() / 2, self.height() / 2)
        painter.scale(side / 200.0, side / 200.0)

        painter.setPen(QPen(border_col, 3))
        painter.setBrush(QBrush(face_col))
        painter.drawEllipse(-95, -95, 190, 190)

        painter.setPen(QPen(text_col, 2))
        for i in range(12):
            painter.drawLine(80, 0, 90, 0)
            painter.rotate(30.0)

        time = datetime.now()

        painter.setPen(Qt.NoPen)
        painter.setBrush(QBrush(text_col))
        painter.save()
        painter.rotate(30.0 * ((time.hour + time.minute / 60.0)))
        painter.drawConvexPolygon(QPolygonF([QPointF(-5, 8), QPointF(5, 8), QPointF(2, -50), QPointF(-2, -50)]))
        painter.restore()

        painter.setBrush(QBrush(accent_col))
        painter.save()
        painter.rotate(6.0 * (time.minute + time.second / 60.0))
        painter.drawConvexPolygon(QPolygonF([QPointF(-3, 8), QPointF(3, 8), QPointF(1, -75), QPointF(-1, -75)]))
        painter.restore()

        painter.setBrush(QBrush(sec_col))
        painter.save()
        painter.rotate(6.0 * time.second)
        painter.drawConvexPolygon(QPolygonF([QPointF(-1, 8), QPointF(1, 8), QPointF(0, -85)]))
        painter.drawEllipse(-3, -3, 6, 6) 
        painter.restore()

# =========================================================================
# Floating Clock (Digital, Minimalist & Analog)
# =========================================================================
class FloatingClock(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent, Qt.Window | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint | Qt.Tool)
        self.setAttribute(Qt.WA_TranslucentBackground)
        
        # Load Memory
        self.settings = QSettings("vmanOS", "FloatingClock")
        self.is_24h = self.settings.value("is_24h", True, type=bool)
        self.clock_mode = self.settings.value("clock_mode", "Digital") 
        
        self.layout = QVBoxLayout(self)
        
        self.stack = QStackedWidget()
        self.lbl_time = QLabel()
        self.lbl_time.setAlignment(Qt.AlignCenter)
        self.analog_clock = AnalogWidget(self)
        self.stack.addWidget(self.lbl_time)
        self.stack.addWidget(self.analog_clock)
        self.layout.addWidget(self.stack)
        
        self._apply_mode_geometry()
        
        # Restore Exact Position
        saved_pos = self.settings.value("pos")
        if isinstance(saved_pos, QPoint): self.move(saved_pos)
        
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_time)
        self.timer.start(1000)
        self.update_time()
        
        self._drag_pos = None
        
        # Restore Visibility after main.py initializes
        QTimer.singleShot(100, self._restore_visibility)

    def _restore_visibility(self):
        if self.settings.value("is_visible", False, type=bool): self.show()

    def showEvent(self, event):
        self.settings.setValue("is_visible", True); super().showEvent(event)
    def hideEvent(self, event):
        self.settings.setValue("is_visible", False); super().hideEvent(event)

    def _apply_mode_geometry(self):
        self.stack.setCurrentIndex(1 if self.clock_mode == "Analog" else 0)
        if self.clock_mode == "Analog":
            self.layout.setContentsMargins(10, 10, 10, 10)
            self.resize(180, 180)
        elif self.clock_mode == "Minimalist":
            self.layout.setContentsMargins(0, 0, 0, 0)
            self.resize(60, 30) 
        else:
            self.layout.setContentsMargins(10, 10, 10, 10)
            self.resize(150, 60)

    def update_time(self):
        if self.clock_mode == "Analog":
            self.analog_clock.update()
        else:
            is_dark = True
            if self.parent() and hasattr(self.parent(), 'is_dark_mode'): 
                is_dark = self.parent().is_dark_mode
                
            txt_col = "#58a6ff" if is_dark else "#0969da"

            if self.clock_mode == "Minimalist":
                fmt = "%H:%M" if self.is_24h else "%I:%M %p"
                self.lbl_time.setText(datetime.now().strftime(fmt))
                self.lbl_time.setStyleSheet(f"background: transparent; color: {txt_col}; border: none; padding: 0px; font-size: 20px; font-weight: bold; font-family: 'Segoe UI';")
            else:
                fmt = "%H:%M:%S" if self.is_24h else "%I:%M:%S %p"
                self.lbl_time.setText(datetime.now().strftime(fmt))
                bg_col = "rgba(13, 17, 23, 0.85)" if is_dark else "rgba(255, 255, 255, 0.85)"
                self.lbl_time.setStyleSheet(f"background-color: {bg_col}; color: {txt_col}; border: 1px solid #30363d; border-radius: 8px; padding: 5px 15px; font-size: 24px; font-weight: bold; font-family: 'Segoe UI';")

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton: 
            self._drag_pos = event.globalPosition().toPoint() - self.frameGeometry().topLeft()
        elif event.button() == Qt.RightButton:
            menu = QMenu(self)
            menu.setStyleSheet("background-color: #161b22; color: white; border: 1px solid #30363d;")
            
            mode_menu = menu.addMenu("🕒 Clock Mode")
            act_dig = mode_menu.addAction("Digital (With Box & Seconds)")
            act_min = mode_menu.addAction("Minimalist (Floating Text Only)")
            act_ana = mode_menu.addAction("Analog")
            
            act_dig.setCheckable(True); act_dig.setChecked(self.clock_mode == "Digital")
            act_min.setCheckable(True); act_min.setChecked(self.clock_mode == "Minimalist")
            act_ana.setCheckable(True); act_ana.setChecked(self.clock_mode == "Analog")
            
            menu.addSeparator()
            if self.clock_mode != "Analog": act_fmt = menu.addAction("Toggle 12h/24h Format")
            else: act_fmt = None
                
            menu.addSeparator()
            act_close = menu.addAction("Close Clock")
            
            res = menu.exec(event.globalPosition().toPoint())
            
            if res in (act_dig, act_min, act_ana):
                if res == act_dig: self.clock_mode = "Digital"
                elif res == act_min: self.clock_mode = "Minimalist"
                elif res == act_ana: self.clock_mode = "Analog"
                self.settings.setValue("clock_mode", self.clock_mode)
                self._apply_mode_geometry()
                self.update_time()
            elif act_fmt and res == act_fmt: 
                self.is_24h = not self.is_24h
                self.settings.setValue("is_24h", self.is_24h)
                self.update_time()
            elif res == act_close: 
                self.hide()

    def mouseMoveEvent(self, event):
        if self._drag_pos is not None and event.buttons() == Qt.LeftButton:
            self.move(event.globalPosition().toPoint() - self._drag_pos)
            
    def mouseReleaseEvent(self, event): 
        self._drag_pos = None
        self.settings.setValue("pos", self.pos()) # Save position instantly

# =========================================================================
# Floating Notepad
# =========================================================================
class FloatingNotepad(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent, Qt.Window | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint | Qt.Tool)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setMinimumSize(250, 200)
        
        # Load Memory
        self.settings = QSettings("vmanOS", "FloatingNotepad")
        self.is_pinned = self.settings.value("is_pinned", True, type=bool)
        self.is_wrapped = self.settings.value("is_wrapped", True, type=bool)
        self.current_font_size = self.settings.value("font_size", 13, type=int)
        
        # Apply Window Pin State
        if not self.is_pinned: self.setWindowFlag(Qt.WindowStaysOnTopHint, False)
        
        # Restore Size & Position
        saved_geom = self.settings.value("geometry")
        if saved_geom: self.restoreGeometry(saved_geom)
        else: self.resize(320, 400)
        
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(5, 5, 5, 5)
        self.layout.setSpacing(0)
        
        # --- Draggable Header ---
        self.header = QWidget()
        self.header.setStyleSheet("background-color: #21262d; border-top-left-radius: 8px; border-top-right-radius: 8px; border: 1px solid #30363d; border-bottom: none;")
        self.header.setFixedHeight(34)
        h_lay = QHBoxLayout(self.header)
        h_lay.setContentsMargins(8, 0, 5, 0)
        h_lay.setSpacing(2)
        
        lbl = QLabel("📝 Quick Pad")
        lbl.setStyleSheet("color: #c9d1d9; font-size: 12px; font-weight: bold; border: none;")
        h_lay.addWidget(lbl)
        h_lay.addStretch()
        
        def create_btn(icon, tip, callback, color="#58a6ff"):
            btn = QPushButton(icon)
            btn.setToolTip(tip)
            btn.setFixedSize(24, 24)
            btn.setStyleSheet(f"QPushButton {{ background: transparent; color: {color}; border: none; font-size: 14px; font-weight: bold; }} QPushButton:hover {{ background: #30363d; border-radius: 4px; }}")
            btn.clicked.connect(callback)
            h_lay.addWidget(btn)
            return btn
            
        self.btn_pin = create_btn("📌", "Toggle Always-On-Top", self.toggle_pin, "#e3b341" if self.is_pinned else "#8b949e")
        create_btn("💾", "Save to File", self.save_to_file, "#3fb950")
        create_btn("A+", "Increase Font Size", lambda: self.zoom_font(1))
        create_btn("A-", "Decrease Font Size", lambda: self.zoom_font(-1))
        self.btn_wrap = create_btn("↩️", "Toggle Word Wrap", self.toggle_wrap, "#58a6ff" if self.is_wrapped else "#8b949e")
        create_btn("📋", "Copy All", self.copy_all)
        create_btn("🗑️", "Clear Pad", self.clear_text, "#f85149")
        create_btn("✕", "Close", self.hide, "#8b949e")
        
        # --- Text Area ---
        self.text_edit = QPlainTextEdit()
        self.text_edit.setPlaceholderText("Type or paste quick notes here...\n\n(Auto-saves instantly)")
        self.text_edit.setStyleSheet("QPlainTextEdit { background-color: rgba(13, 17, 23, 0.95); color: #c9d1d9; border: 1px solid #30363d; border-bottom: none; padding: 10px; font-family: 'Segoe UI'; }")
        
        self.apply_font()
        self.text_edit.setLineWrapMode(QPlainTextEdit.WidgetWidth if self.is_wrapped else QPlainTextEdit.NoWrap)
        
        saved_text = self.settings.value("saved_notes", "")
        if saved_text: self.text_edit.setPlainText(str(saved_text))
            
        self.text_edit.textChanged.connect(self.on_text_changed)
        
        # --- Footer with Size Grip ---
        self.footer = QWidget()
        self.footer.setStyleSheet("background-color: rgba(13, 17, 23, 0.95); border: 1px solid #30363d; border-top: none; border-bottom-left-radius: 8px; border-bottom-right-radius: 8px;")
        self.footer.setFixedHeight(24)
        f_lay = QHBoxLayout(self.footer)
        f_lay.setContentsMargins(10, 0, 0, 0)
        
        self.lbl_stats = QLabel("0 chars")
        self.lbl_stats.setStyleSheet("color: #8b949e; font-size: 10px; border: none; background: transparent;")
        
        size_grip = QSizeGrip(self)
        size_grip.setStyleSheet("background: transparent;")
        
        f_lay.addWidget(self.lbl_stats)
        f_lay.addStretch()
        f_lay.addWidget(size_grip)
        
        self.layout.addWidget(self.header)
        self.layout.addWidget(self.text_edit, stretch=1)
        self.layout.addWidget(self.footer)
        
        self._drag_pos = None
        self.update_stats()
        
        QTimer.singleShot(100, self._restore_visibility)

    def _restore_visibility(self):
        if self.settings.value("is_visible", False, type=bool): self.show()

    def showEvent(self, event):
        self.settings.setValue("is_visible", True); super().showEvent(event)
    def hideEvent(self, event):
        self.settings.setValue("is_visible", False); super().hideEvent(event)
    def resizeEvent(self, event):
        super().resizeEvent(event); self.settings.setValue("geometry", self.saveGeometry())

    def apply_font(self):
        font = self.text_edit.font()
        font.setPointSize(self.current_font_size)
        self.text_edit.setFont(font)

    def zoom_font(self, delta):
        self.current_font_size = max(8, min(36, self.current_font_size + delta))
        self.settings.setValue("font_size", self.current_font_size)
        self.apply_font()

    def toggle_wrap(self):
        self.is_wrapped = not self.is_wrapped
        self.settings.setValue("is_wrapped", self.is_wrapped)
        self.text_edit.setLineWrapMode(QPlainTextEdit.WidgetWidth if self.is_wrapped else QPlainTextEdit.NoWrap)
        self.btn_wrap.setStyleSheet(f"QPushButton {{ background: transparent; color: {'#58a6ff' if self.is_wrapped else '#8b949e'}; border: none; font-size: 14px; font-weight: bold; }} QPushButton:hover {{ background: #30363d; border-radius: 4px; }}")

    def toggle_pin(self):
        self.is_pinned = not self.is_pinned
        self.settings.setValue("is_pinned", self.is_pinned)
        self.setWindowFlag(Qt.WindowStaysOnTopHint, self.is_pinned)
        self.btn_pin.setStyleSheet(f"QPushButton {{ background: transparent; color: {'#e3b341' if self.is_pinned else '#8b949e'}; border: none; font-size: 14px; font-weight: bold; }} QPushButton:hover {{ background: #30363d; border-radius: 4px; }}")
        self.show()

    def save_to_file(self):
        path, _ = QFileDialog.getSaveFileName(self, "Save Notes", "", "Text Files (*.txt)")
        if path:
            try:
                with open(path, "w", encoding="utf-8") as f: f.write(self.text_edit.toPlainText())
            except Exception as e:
                QMessageBox.warning(self, "Error", f"Failed to save file:\n{e}")

    def on_text_changed(self):
        self.settings.setValue("saved_notes", self.text_edit.toPlainText())
        self.update_stats()
        
    def update_stats(self):
        text = self.text_edit.toPlainText()
        self.lbl_stats.setText(f"{len(text)} chars | {len(text.split())} words")

    def copy_all(self):
        text = self.text_edit.toPlainText()
        if text: QApplication.clipboard().setText(text)
        
    def clear_text(self): 
        self.text_edit.clear()

    # --- Window Dragging Logic ---
    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton and event.position().y() <= 35:
            self._drag_pos = event.globalPosition().toPoint() - self.frameGeometry().topLeft()

    def mouseMoveEvent(self, event):
        if self._drag_pos is not None and event.buttons() == Qt.LeftButton:
            self.move(event.globalPosition().toPoint() - self._drag_pos)
            
    def mouseReleaseEvent(self, event):
        self._drag_pos = None
        self.settings.setValue("geometry", self.saveGeometry()) # Save movement


# =========================================================================
# Floating Quick Text Palette
# =========================================================================
class QuickTextButton(QPushButton):
    def __init__(self, text, parent_palette):
        super().__init__(text)
        self.text_val = text
        self.parent_palette = parent_palette
        self.setToolTip("Left-click to Copy | Right-click to Delete")
        
        is_dark = True
        if self.parent_palette.parent() and hasattr(self.parent_palette.parent(), 'is_dark_mode'):
            is_dark = self.parent_palette.parent().is_dark_mode
            
        txt_c = "#c9d1d9" if is_dark else "#24292f"
        bg_hover = "#1f6feb" if is_dark else "#0969da"
        
        self.setStyleSheet(f"""
            QPushButton {{
                background-color: transparent;
                color: {txt_c};
                border: 1px solid #30363d;
                border-radius: 4px;
                padding: 8px 12px;
                text-align: left;
                font-size: 13px;
            }}
            QPushButton:hover {{
                background-color: {bg_hover};
                color: white;
            }}
        """)

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            QApplication.clipboard().setText(self.text_val)
            main_app = self.parent_palette.parent()
            if main_app and hasattr(main_app, 'status'):
                main_app.status.showMessage(f"Copied: {self.text_val}", 2000)
                
        elif event.button() == Qt.RightButton:
            menu = QMenu(self)
            menu.setStyleSheet("background-color: #161b22; color: white; border: 1px solid #30363d;")
            act_del = menu.addAction("🗑️ Delete Quick Text")
            if menu.exec(event.globalPosition().toPoint()) == act_del:
                self.parent_palette.remove_text(self.text_val)
        else:
            super().mousePressEvent(event)

class FloatingQuickText(QWidget):
    def __init__(self, json_path, parent=None):
        super().__init__(parent, Qt.Window | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint | Qt.Tool)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setMinimumSize(250, 300)
        
        # Load Memory
        self.settings = QSettings("vmanOS", "FloatingQuickText")
        self.is_pinned = self.settings.value("is_pinned", True, type=bool)
        
        if not self.is_pinned: self.setWindowFlag(Qt.WindowStaysOnTopHint, False)
        
        saved_geom = self.settings.value("geometry")
        if saved_geom: self.restoreGeometry(saved_geom)
        else: self.resize(360, 450)
        
        self.json_path = Path(json_path)
        
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(5, 5, 5, 5)
        self.layout.setSpacing(0)
        
        # --- Draggable Header ---
        self.header = QWidget()
        self.header.setStyleSheet("background-color: #21262d; border-top-left-radius: 8px; border-top-right-radius: 8px; border: 1px solid #30363d; border-bottom: none;")
        self.header.setFixedHeight(34)
        h_lay = QHBoxLayout(self.header)
        h_lay.setContentsMargins(10, 0, 5, 0)
        h_lay.setSpacing(5)
        
        lbl = QLabel("⚡ Quick Text")
        lbl.setStyleSheet("color: #e3b341; font-size: 12px; font-weight: bold; border: none;")
        h_lay.addWidget(lbl)
        h_lay.addStretch()
        
        self.btn_pin = QPushButton("📌")
        self.btn_pin.setToolTip("Toggle Always-On-Top")
        self.btn_pin.setFixedSize(24, 24)
        self.btn_pin.setStyleSheet(f"QPushButton {{ background: transparent; color: {'#e3b341' if self.is_pinned else '#8b949e'}; border: none; font-size: 14px; font-weight: bold; }} QPushButton:hover {{ background: #30363d; border-radius: 4px; }}")
        self.btn_pin.clicked.connect(self.toggle_pin)
        
        btn_add = QPushButton("➕")
        btn_add.setToolTip("Add New Quick Text")
        btn_add.setFixedSize(24, 24)
        btn_add.setStyleSheet("QPushButton { background: transparent; color: #3fb950; border: none; font-size: 14px; font-weight: bold;} QPushButton:hover { background: #30363d; border-radius: 4px; }")
        btn_add.clicked.connect(self.add_text)
        
        btn_close = QPushButton("✕")
        btn_close.setToolTip("Close")
        btn_close.setFixedSize(24, 24)
        btn_close.setStyleSheet("QPushButton { background: transparent; color: #8b949e; border: none; font-weight: bold; } QPushButton:hover { color: #f85149; background: #30363d; border-radius: 4px; }")
        btn_close.clicked.connect(self.hide)
        
        h_lay.addWidget(self.btn_pin)
        h_lay.addWidget(btn_add)
        h_lay.addWidget(btn_close)
        
        # --- Live Search Filter ---
        self.search_bar = QLineEdit()
        self.search_bar.setPlaceholderText("🔍 Filter texts...")
        self.search_bar.setStyleSheet("QLineEdit { background-color: rgba(13, 17, 23, 0.95); color: #c9d1d9; border: 1px solid #30363d; border-bottom: none; border-top: none; padding: 6px 10px; font-size: 12px; }")
        self.search_bar.textChanged.connect(self.filter_texts)
        
        # --- Scroll Area ---
        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.scroll.setStyleSheet("""
            QScrollArea { background-color: rgba(13, 17, 23, 0.95); border: 1px solid #30363d; border-bottom: none; border-top: none; } 
            QScrollBar:vertical { width: 6px; background: transparent; } 
            QScrollBar::handle:vertical { background: #30363d; border-radius: 3px; }
            QScrollBar::handle:vertical:hover { background: #58a6ff; }
        """)
        
        self.content_widget = QWidget()
        self.content_widget.setStyleSheet("background: transparent;")
        self.content_layout = QVBoxLayout(self.content_widget)
        self.content_layout.setContentsMargins(10, 10, 10, 10)
        self.content_layout.setSpacing(8)
        self.content_layout.setAlignment(Qt.AlignTop)
        
        self.scroll.setWidget(self.content_widget)
        
        # --- Footer with Size Grip ---
        self.footer = QWidget()
        self.footer.setStyleSheet("background-color: rgba(13, 17, 23, 0.95); border: 1px solid #30363d; border-bottom-left-radius: 8px; border-bottom-right-radius: 8px;")
        self.footer.setFixedHeight(24)
        f_lay = QHBoxLayout(self.footer)
        f_lay.setContentsMargins(10, 0, 0, 0)
        
        self.lbl_count = QLabel("0 Items")
        self.lbl_count.setStyleSheet("color: #8b949e; font-size: 10px; border: none; background: transparent;")
        
        size_grip = QSizeGrip(self)
        size_grip.setStyleSheet("background: transparent;")
        
        f_lay.addWidget(self.lbl_count)
        f_lay.addStretch()
        f_lay.addWidget(size_grip)
        
        self.layout.addWidget(self.header)
        self.layout.addWidget(self.search_bar)
        self.layout.addWidget(self.scroll, stretch=1)
        self.layout.addWidget(self.footer)
        self._drag_pos = None
        
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self.show_bg_menu)
        self.scroll.setContextMenuPolicy(Qt.CustomContextMenu)
        self.scroll.customContextMenuRequested.connect(self.show_bg_menu)
        self.content_widget.setContextMenuPolicy(Qt.CustomContextMenu)
        self.content_widget.customContextMenuRequested.connect(self.show_bg_menu)
        
        self.load_texts()
        QTimer.singleShot(100, self._restore_visibility)

    def _restore_visibility(self):
        if self.settings.value("is_visible", False, type=bool): self.show()

    def showEvent(self, event):
        self.settings.setValue("is_visible", True); super().showEvent(event)
    def hideEvent(self, event):
        self.settings.setValue("is_visible", False); super().hideEvent(event)
    def resizeEvent(self, event):
        super().resizeEvent(event); self.settings.setValue("geometry", self.saveGeometry())

    def toggle_pin(self):
        self.is_pinned = not self.is_pinned
        self.settings.setValue("is_pinned", self.is_pinned)
        self.setWindowFlag(Qt.WindowStaysOnTopHint, self.is_pinned)
        self.btn_pin.setStyleSheet(f"QPushButton {{ background: transparent; color: {'#e3b341' if self.is_pinned else '#8b949e'}; border: none; font-size: 14px; font-weight: bold; }} QPushButton:hover {{ background: #30363d; border-radius: 4px; }}")
        self.show()

    def load_texts(self):
        while self.content_layout.count():
            item = self.content_layout.takeAt(0)
            if item.widget(): item.widget().deleteLater()
                
        saved = []
        if self.json_path.exists():
            try:
                with open(self.json_path, "r", encoding="utf-8") as f: saved = json.load(f)
            except Exception: pass
                
        if not saved:
            empty_lbl = QLabel("No quick texts saved.\nRight-click anywhere to add one.")
            empty_lbl.setStyleSheet("color: #8b949e; font-style: italic; border: none;")
            empty_lbl.setAlignment(Qt.AlignCenter)
            self.content_layout.addWidget(empty_lbl)
        else:
            for t in saved:
                self.content_layout.addWidget(QuickTextButton(t, self))
                
        self.lbl_count.setText(f"{len(saved)} Items")
        self.filter_texts(self.search_bar.text())

    def filter_texts(self, text):
        term = text.lower()
        for i in range(self.content_layout.count()):
            widget = self.content_layout.itemAt(i).widget()
            if isinstance(widget, QuickTextButton):
                widget.setVisible(term in widget.text_val.lower())

    def add_text(self):
        text, ok = QInputDialog.getMultiLineText(self, "Add Quick Text", "Enter text to save:")
        if ok and text.strip():
            saved = []
            if self.json_path.exists():
                try:
                    with open(self.json_path, "r", encoding="utf-8") as f: saved = json.load(f)
                except Exception: pass
            
            if text.strip() not in saved: 
                saved.append(text.strip())
                
            with open(self.json_path, "w", encoding="utf-8") as f: 
                json.dump(saved, f)
                
            self.load_texts()

    def remove_text(self, text):
        if self.json_path.exists():
            try:
                with open(self.json_path, "r", encoding="utf-8") as f: saved = json.load(f)
                if text in saved: saved.remove(text)
                with open(self.json_path, "w", encoding="utf-8") as f: json.dump(saved, f)
                self.load_texts()
            except Exception: pass

    def clear_all(self):
        if QMessageBox.question(self, "Clear All", "Delete ALL saved Quick Texts? This cannot be undone.", QMessageBox.Yes|QMessageBox.No) == QMessageBox.Yes:
            if self.json_path.exists():
                with open(self.json_path, "w", encoding="utf-8") as f: json.dump([], f)
                self.load_texts()

    def show_bg_menu(self, pos):
        if self.sender(): global_pos = self.sender().mapToGlobal(pos)
        else: global_pos = self.mapToGlobal(pos)
            
        menu = QMenu(self)
        menu.setStyleSheet("background-color: #161b22; color: white; border: 1px solid #30363d;")
        
        act_add = menu.addAction("➕ Add New Quick Text")
        menu.addSeparator()
        act_clear = menu.addAction("🗑️ Clear All Texts")
        
        action = menu.exec(global_pos)
        if action == act_add: self.add_text()
        elif action == act_clear: self.clear_all()

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton and event.position().y() <= 35:
            self._drag_pos = event.globalPosition().toPoint() - self.frameGeometry().topLeft()

    def mouseMoveEvent(self, event):
        if self._drag_pos is not None and event.buttons() == Qt.LeftButton:
            self.move(event.globalPosition().toPoint() - self._drag_pos)
            
    def mouseReleaseEvent(self, event):
        self._drag_pos = None
        self.settings.setValue("geometry", self.saveGeometry()) # Save movement
        
        
# =========================================================================
# Floating Quotes (Minimalist & Customizable)
# =========================================================================
class QuoteManagerDialog(QDialog):
    def __init__(self, parent_widget):
        super().__init__(parent_widget)
        self.parent_widget = parent_widget
        self.setWindowTitle("Manage Quotes")
        self.resize(450, 350)
        self.setStyleSheet("background-color: #0d1117; color: #c9d1d9; border: 1px solid #30363d;")
        
        layout = QVBoxLayout(self)
        
        lbl = QLabel("Stored Quotes:")
        lbl.setStyleSheet("font-weight: bold; border: none;")
        layout.addWidget(lbl)
        
        self.list_widget = QListWidget()
        self.list_widget.setStyleSheet("QListWidget { background-color: #161b22; border-radius: 4px; padding: 5px; font-size: 13px; } QListWidget::item:selected { background-color: #1f6feb; color: white; }")
        self.list_widget.addItems(self.parent_widget.quotes_list)
        layout.addWidget(self.list_widget)
        
        btn_lay = QHBoxLayout()
        
        btn_add = QPushButton("➕ Add Quote")
        btn_add.setStyleSheet("QPushButton { background: #238636; color: white; font-weight: bold; border-radius: 4px; padding: 6px; } QPushButton:hover { background: #2ea043; }")
        btn_add.clicked.connect(self.add_quote)
        
        btn_del = QPushButton("🗑️ Delete")
        btn_del.setStyleSheet("QPushButton { background: transparent; color: #f85149; border: 1px solid #f85149; border-radius: 4px; padding: 6px; } QPushButton:hover { background: #f85149; color: white; }")
        btn_del.clicked.connect(self.del_quote)
        
        btn_act = QPushButton("✅ Set Active Quote")
        btn_act.setStyleSheet("QPushButton { background: #1f6feb; color: white; font-weight: bold; border-radius: 4px; padding: 6px; } QPushButton:hover { background: #388bfd; }")
        btn_act.clicked.connect(self.set_active)
        
        btn_lay.addWidget(btn_add)
        btn_lay.addWidget(btn_del)
        btn_lay.addStretch()
        btn_lay.addWidget(btn_act)
        
        layout.addLayout(btn_lay)
        
    def save_state(self):
        self.parent_widget.settings.setValue("quotes_list", json.dumps(self.parent_widget.quotes_list))

    def add_quote(self):
        text, ok = QInputDialog.getMultiLineText(self, "New Quote", "Type your quote:")
        if ok and text.strip():
            clean_text = text.strip()
            self.parent_widget.quotes_list.append(clean_text)
            self.list_widget.addItem(clean_text)
            self.save_state()
            
    def del_quote(self):
        row = self.list_widget.currentRow()
        if row >= 0:
            self.list_widget.takeItem(row)
            self.parent_widget.quotes_list.pop(row)
            self.save_state()
            
    def set_active(self):
        row = self.list_widget.currentRow()
        if row >= 0:
            self.parent_widget.active_quote = self.parent_widget.quotes_list[row]
            self.parent_widget.settings.setValue("active_quote", self.parent_widget.active_quote)
            self.parent_widget.refresh_ui()
            self.accept()


class FloatingQuotes(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent, Qt.Window | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint | Qt.Tool)
        self.setAttribute(Qt.WA_TranslucentBackground)
        
        self.settings = QSettings("vmanOS", "FloatingQuotes")
        
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(10)
        self.layout.setAlignment(Qt.AlignCenter)
        
        self.lbl_img = QLabel()
        self.lbl_img.setAlignment(Qt.AlignCenter)
        self.lbl_img.hide()
        
        self.lbl_quote = QLabel()
        self.lbl_quote.setAlignment(Qt.AlignCenter)
        self.lbl_quote.setWordWrap(True)
        
        # Drop Shadow for perfect readability on any background
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(8)
        shadow.setColor(QColor(0, 0, 0, 200))
        shadow.setOffset(1, 1)
        self.lbl_quote.setGraphicsEffect(shadow)
        
        self.layout.addWidget(self.lbl_img)
        self.layout.addWidget(self.lbl_quote)
        
        self._drag_pos = None
        
        # Load State
        self.quotes_list = json.loads(self.settings.value("quotes_list", '["Inspiration exists, but it has to find you working."]', type=str))
        self.active_quote = self.settings.value("active_quote", "Inspiration exists, but it has to find you working.", type=str)
        self.text_color = self.settings.value("text_color", "#e3b341", type=str)
        self.font_size = self.settings.value("font_size", 22, type=int)
        self.image_path = self.settings.value("image_path", "", type=str)
        
        # --- NEW: Dimension Controls ---
        self.text_width = self.settings.value("text_width", 300, type=int)
        self.image_width = self.settings.value("image_width", 300, type=int)
        
        self.refresh_ui()
        
        saved_geom = self.settings.value("geometry")
        if saved_geom: self.restoreGeometry(saved_geom)
        
        QTimer.singleShot(100, self._restore_visibility)
        
    def _restore_visibility(self):
        if self.settings.value("is_visible", False, type=bool): self.show()
        
    def showEvent(self, event):
        self.settings.setValue("is_visible", True); super().showEvent(event)
    def hideEvent(self, event):
        self.settings.setValue("is_visible", False); super().hideEvent(event)
    def resizeEvent(self, event):
        super().resizeEvent(event); self.settings.setValue("geometry", self.saveGeometry())

    def refresh_ui(self):
        self.lbl_quote.setText(self.active_quote)
        self.lbl_quote.setStyleSheet(f"color: {self.text_color}; font-size: {self.font_size}px; font-weight: bold; font-family: 'Georgia'; font-style: italic; background: transparent; border: none;")
        
        # Force the text layout width
        self.lbl_quote.setFixedWidth(self.text_width)
        
        if self.image_path and Path(self.image_path).exists():
            pix = QPixmap(self.image_path)
            if not pix.isNull():
                # Apply custom image scaling
                self.lbl_img.setPixmap(pix.scaledToWidth(self.image_width, Qt.SmoothTransformation))
                self.lbl_img.show()
        else:
            self.lbl_img.hide()
            
        self.adjustSize()

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton: 
            self._drag_pos = event.globalPosition().toPoint() - self.frameGeometry().topLeft()
        elif event.button() == Qt.RightButton:
            menu = QMenu(self)
            menu.setStyleSheet("background-color: #161b22; color: #c9d1d9; border: 1px solid #30363d; font-size: 13px;")
            
            act_manage = menu.addAction("📝 Manage Quotes...")
            act_color = menu.addAction("🎨 Pick Text Color...")
            act_txt_width = menu.addAction("📏 Set Text Wrap Width...")
            
            menu.addSeparator()
            act_img = menu.addAction("🖼️ Set Image...")
            act_img_width = menu.addAction("📐 Set Image Width...")
            act_clr_img = menu.addAction("🚫 Remove Image")
            
            menu.addSeparator()
            act_up = menu.addAction("A+ Increase Font Size")
            act_dn = menu.addAction("A- Decrease Font Size")
            
            menu.addSeparator()
            act_close = menu.addAction("✕ Close Quotes widget")
            
            res = menu.exec(event.globalPosition().toPoint())
            
            if res == act_manage:
                QuoteManagerDialog(self).exec()
            elif res == act_color:
                color = QColorDialog.getColor(QColor(self.text_color), self, "Select Quote Color")
                if color.isValid():
                    self.text_color = color.name()
                    self.settings.setValue("text_color", self.text_color)
                    self.refresh_ui()
            elif res == act_txt_width:
                w, ok = QInputDialog.getInt(self, "Text Width", "Enter max width for text (px):", self.text_width, 100, 2000)
                if ok:
                    self.text_width = w
                    self.settings.setValue("text_width", self.text_width)
                    self.refresh_ui()
            elif res == act_img:
                path, _ = QFileDialog.getOpenFileName(self, "Select Image", "", "Images (*.png *.jpg *.jpeg *.bmp *.webp)")
                if path:
                    self.image_path = path
                    self.settings.setValue("image_path", self.image_path)
                    self.refresh_ui()
            elif res == act_img_width:
                w, ok = QInputDialog.getInt(self, "Image Width", "Enter width for image (px):", self.image_width, 50, 2000)
                if ok:
                    self.image_width = w
                    self.settings.setValue("image_width", self.image_width)
                    self.refresh_ui()
            elif res == act_clr_img:
                self.image_path = ""
                self.settings.setValue("image_path", "")
                self.refresh_ui()
            elif res == act_up:
                self.font_size = min(72, self.font_size + 2)
                self.settings.setValue("font_size", self.font_size)
                self.refresh_ui()
            elif res == act_dn:
                self.font_size = max(10, self.font_size - 2)
                self.settings.setValue("font_size", self.font_size)
                self.refresh_ui()
            elif res == act_close:
                self.hide()

    def mouseMoveEvent(self, event):
        if self._drag_pos is not None and event.buttons() == Qt.LeftButton:
            self.move(event.globalPosition().toPoint() - self._drag_pos)
            
    def mouseReleaseEvent(self, event): 
        self._drag_pos = None
        self.settings.setValue("geometry", self.saveGeometry())
