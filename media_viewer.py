
# media_viewer.py
import os
# --- CRITICAL FIX FOR LINUX MULTIMEDIA CRASHES (libavutil / vaMapBuffer2) ---
os.environ["LIBVA_DRIVER_NAME"] = "none"
os.environ["VDPAU_DRIVER"] = "none"
os.environ["QT_MEDIA_BACKEND"] = "ffmpeg"
os.environ["FFMPEG_HWACCEL_DISABLE"] = "1"

import re
import math
import time
import random
import json
import csv
import string
from datetime import datetime
from PySide6.QtCore import Qt, QThread, Signal, QTimer, QUrl, QEvent, QPointF, QRectF, QRunnable, QThreadPool, QObject, QMargins
from PySide6.QtGui import (
    QPainter, QImage, QPixmap, QTransform, QBrush, QColor, QKeySequence, 
    QShortcut, QFont, QFontMetrics, QAction, QClipboard, QSyntaxHighlighter, QTextCharFormat,
    QPainterPath, QPen
)
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QWidget, QLabel, QPushButton,
    QStackedWidget, QPlainTextEdit, QComboBox, QSlider, QProgressBar,
    QGraphicsView, QGraphicsScene, QScrollArea, QMessageBox, QInputDialog, 
    QMenu, QFileDialog, QApplication, QGraphicsOpacityEffect, QTabWidget,
    QSplitter, QGroupBox, QLineEdit, QTableWidget, QTableWidgetItem, QHeaderView, QColorDialog,
    QListWidget, QFormLayout, QTextBrowser, QScroller, QAbstractItemView,
    QGraphicsDropShadowEffect  # <--- ADD THIS
)

# --- PROTECT SLOW DRIVES (2MB/s) BY LIMITING CONCURRENT I/O ---
QThreadPool.globalInstance().setMaxThreadCount(2)

try:
    from PySide6.QtMultimedia import QMediaPlayer, QAudioOutput, QMediaMetaData
    from PySide6.QtMultimediaWidgets import QGraphicsVideoItem
    HAS_MULTIMEDIA = True
except ImportError:
    HAS_MULTIMEDIA = False

try:
    from PySide6.QtPdf import QPdfDocument
    from PySide6.QtPdfWidgets import QPdfView
    HAS_PDF = True
    
    class AdvancedPdfViewer(QPdfView):
        """Clean PDF Viewer for Normal Mode"""
        zoomChanged = Signal(float)

        def __init__(self, parent=None):
            super().__init__(parent)
            self.setPageMode(QPdfView.PageMode.MultiPage)
            self.viewport().setCursor(Qt.OpenHandCursor)
            self._is_dragging = False
            self.setDocumentMargins(QMargins(0, 0, 0, 0))
            
        def mousePressEvent(self, event):
            if event.button() == Qt.LeftButton:
                self._is_dragging = True
                self._drag_start_pos = event.position()
                self._h_scroll_start = self.horizontalScrollBar().value()
                self._v_scroll_start = self.verticalScrollBar().value()
                self.viewport().setCursor(Qt.ClosedHandCursor)
            super().mousePressEvent(event)

        def mouseMoveEvent(self, event):
            if self._is_dragging:
                delta = event.position() - self._drag_start_pos
                self.horizontalScrollBar().setValue(int(self._h_scroll_start - delta.x()))
                self.verticalScrollBar().setValue(int(self._v_scroll_start - delta.y()))
                return
            super().mouseMoveEvent(event)

        def mouseReleaseEvent(self, event):
            if event.button() == Qt.LeftButton:
                self._is_dragging = False
                self.viewport().setCursor(Qt.OpenHandCursor)
            super().mouseReleaseEvent(event)
            
        def wheelEvent(self, event):
            if event.modifiers() == Qt.AltModifier or event.modifiers() == Qt.ControlModifier:
                delta = event.angleDelta().y() if event.angleDelta().y() != 0 else event.angleDelta().x() 
                
                old_factor = self.zoomFactor()
                if old_factor <= 0: old_factor = 1.0
                
                if delta > 0: new_factor = old_factor * 1.15
                elif delta < 0: new_factor = old_factor / 1.15
                else: return event.accept()
                
                pos = event.position()
                h_bar = self.horizontalScrollBar()
                v_bar = self.verticalScrollBar()
                
                # Snapshot absolute document coordinates under the cursor
                doc_x = (h_bar.value() + pos.x()) / old_factor
                doc_y = (v_bar.value() + pos.y()) / old_factor
                
                p = self
                while p and not hasattr(p, 'macro_scroll_paused'): p = p.parent()
                if p: p.macro_scroll_paused = True
                
                self.setZoomMode(QPdfView.ZoomMode.Custom)
                self.setZoomFactor(new_factor)
                
                # FIX: Force instant layout recalculation so scrollbars adjust before we set their value!
                QApplication.processEvents()
                
                h_bar.setValue(int(doc_x * new_factor - pos.x()))
                v_bar.setValue(int(doc_y * new_factor - pos.y()))
                if p: p.macro_scroll_paused = False
                self.zoomChanged.emit(new_factor) 
                
                event.accept()
            else:
                super().wheelEvent(event)


    class MagazineWrapper(QGraphicsView):
        """Unified Master Controller with Macro Recording & Spread Sync"""
        zoom_req = Signal(float)
        scroll_spread = Signal(int)
        viewChanged = Signal(float, float, float)
        
        def __init__(self, scene, parent=None):
            super().__init__(scene, parent)
            self.setDragMode(QGraphicsView.ScrollHandDrag)
            self.setRenderHints(QPainter.Antialiasing | QPainter.SmoothPixmapTransform)
            self.setStyleSheet("background: #050505; border: none;")
            self.setAlignment(Qt.AlignCenter)
            
        def wheelEvent(self, event):
            if event.modifiers() == Qt.AltModifier or event.modifiers() == Qt.ControlModifier:
                delta = event.angleDelta().y() if event.angleDelta().y() != 0 else event.angleDelta().x()
                if delta > 0: self.zoom_req.emit(1.15)
                elif delta < 0: self.zoom_req.emit(1/1.15)
                self._emit_change()
                event.accept()
            else:
                vbar = self.verticalScrollBar()
                old_val = vbar.value()
                super().wheelEvent(event)
                
                delta = event.angleDelta().y()
                if delta < 0 and old_val == vbar.maximum(): self.scroll_spread.emit(1)
                elif delta > 0 and old_val == vbar.minimum(): self.scroll_spread.emit(-1)

        def scrollContentsBy(self, dx, dy):
            super().scrollContentsBy(dx, dy)
            self._emit_change()

        def _emit_change(self):
            z = self.transform().m11()
            c = self.mapToScene(self.viewport().rect().center())
            self.viewChanged.emit(z, c.x(), c.y())
            
except ImportError:
    HAS_PDF = False


# ==============================================================================
# ULTRA-PREMIUM MINIMALIST CSS (TRANSPARENT & PROFESSIONAL)
# ==============================================================================
VIEWER_CSS = """
QDialog { background-color: #000000; }
QWidget { color: #e5e5e5; font-family: "Segoe UI", "-apple-system", sans-serif; }

/* Minimalist Glass Menu */
QMenu {
    background-color: rgba(15, 15, 15, 0.98); 
    border: 1px solid rgba(255, 255, 255, 0.1);
    border-radius: 8px; 
    padding: 6px 0px; 
}
QMenu::item { padding: 8px 35px 8px 25px; font-size: 13px; background: transparent; }
QMenu::item:selected { background-color: rgba(255, 255, 255, 0.1); color: #ffffff; border-left: 2px solid #3b82f6;}
QMenu::separator { height: 1px; background: rgba(255, 255, 255, 0.05); margin: 6px 12px; }

/* Invisible Scrollbars */
QGraphicsView, QScrollArea, QPlainTextEdit, QLineEdit, QComboBox, QListWidget, QTextBrowser { background: rgba(20,20,20,0.8); border: 1px solid rgba(255,255,255,0.1); color: white; padding: 5px; border-radius: 4px; }
QScrollBar:vertical { border: none; background: transparent; width: 6px; margin: 0px; }
QScrollBar:horizontal { border: none; background: transparent; height: 6px; margin: 0px; }
QScrollBar::handle { background: rgba(255, 255, 255, 0.2); border-radius: 3px; }
QScrollBar::handle:hover { background: rgba(255, 255, 255, 0.5); }
QScrollBar::add-line, QScrollBar::sub-line, QScrollBar::add-page, QScrollBar::sub-page { background: none; border: none; width: 0; height: 0; }

/* VMSL IDE Tabs & Elements */
QTabWidget::pane { border: 1px solid rgba(255,255,255,0.1); border-radius: 6px; background: #050505; }
QTabBar::tab { background: #111; color: #888; padding: 10px 25px; border: 1px solid rgba(255,255,255,0.05); border-top-left-radius: 6px; border-top-right-radius: 6px; font-weight: bold; }
QTabBar::tab:selected { background: #1a1a1a; color: #fff; border-bottom: 2px solid #3b82f6; }
QPushButton { background: rgba(59, 130, 246, 0.3); border: 1px solid rgba(59, 130, 246, 0.6); color: white; border-radius: 4px; padding: 6px 12px; }
QPushButton:hover { background: rgba(59, 130, 246, 0.8); }
QPushButton.action-btn { background: rgba(59, 130, 246, 0.8); color: white; border-radius: 4px; padding: 8px 15px; font-weight: bold; }
QPushButton.action-btn:hover { background: #3b82f6; }
QPushButton.danger-btn { background: rgba(248, 81, 73, 0.1); color: #f85149; border: 1px solid #f85149; border-radius: 4px; padding: 8px 15px; }
QPushButton.danger-btn:hover { background: #f85149; color: white; }

/* CSV Table Overrides */
QTableWidget { background: #080808; color: #e5e5e5; gridline-color: #333; border: none; }
QHeaderView::section { background: #151515; color: #3b82f6; padding: 5px; border: 1px solid #222; font-weight: bold; }
QTableWidget::item { padding: 5px; }
QTableWidget::item:selected { background-color: rgba(59, 130, 246, 0.3); }

/* Markdown specific */
QTextBrowser { background: #0d1117; color: #c9d1d9; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 15px; padding: 40px; border: none; }
"""

# ==============================================================================
# SYNTAX HIGHLIGHTERS
# ==============================================================================
class VMSLHighlighter(QSyntaxHighlighter):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.highlighting_rules = []
        fmt_comment = QTextCharFormat(); fmt_comment.setForeground(QColor("#6a9955")); fmt_comment.setFontItalic(True)
        fmt_var = QTextCharFormat(); fmt_var.setForeground(QColor("#dcdcaa")); fmt_var.setFontWeight(QFont.Bold)
        fmt_frame = QTextCharFormat(); fmt_frame.setForeground(QColor("#ce9178"))
        fmt_cmd = QTextCharFormat(); fmt_cmd.setForeground(QColor("#569cd6")); fmt_cmd.setFontWeight(QFont.Bold)
        fmt_sys = QTextCharFormat(); fmt_sys.setForeground(QColor("#c586c0")); fmt_sys.setFontWeight(QFont.Bold)
        fmt_val = QTextCharFormat(); fmt_val.setForeground(QColor("#b5cea8"))
        self.highlighting_rules.append((re.compile(r"(#|rem ).*"), fmt_comment))
        self.highlighting_rules.append((re.compile(r"\$[a-zA-Z0-9_]+"), fmt_var))
        self.highlighting_rules.append((re.compile(r"\b\d+-\d+\b|\b\d+(,\d+)*\b"), fmt_frame))
        self.highlighting_rules.append((re.compile(r"\b(pause|stop|wait)\b", re.IGNORECASE), fmt_sys))
        self.highlighting_rules.append((re.compile(r"\b(dur|loop|t|z|zx|zy|zs|zxs|zys|start|end|rate|rev|sx|sy|sxs|sys|vol|dragu|dragd|dragl|dragr|scrollu|scrolld|scrolll|scrollr)="), fmt_cmd))
        self.highlighting_rules.append((re.compile(r"=\s*([a-zA-Z0-9_\.,\-]+)"), fmt_val))
        self.highlighting_rules.append((re.compile(r":\d+x\d+:[a-zA-Z0-9_]+|:\d+x\d+|:\d+"), fmt_val))

    def highlightBlock(self, text):
        for pattern, format in self.highlighting_rules:
            for match in pattern.finditer(text):
                self.setFormat(match.start(), match.end() - match.start(), format)

class UniversalHighlighter(QSyntaxHighlighter):
    def __init__(self, document, ext, config):
        super().__init__(document)
        self.rules = []
        self.ext = ext
        
        kw_color = config.get("keyword", "#569cd6")
        str_color = config.get("string", "#ce9178")
        cmt_color = config.get("comment", "#6a9955")
        custom_color = config.get("custom_color", "#c586c0")
        custom_words_str = config.get("custom_words", "")
        
        fmt_kw = QTextCharFormat(); fmt_kw.setForeground(QColor(kw_color)); fmt_kw.setFontWeight(QFont.Bold)
        fmt_str = QTextCharFormat(); fmt_str.setForeground(QColor(str_color))
        fmt_cmt = QTextCharFormat(); fmt_cmt.setForeground(QColor(cmt_color)); fmt_cmt.setFontItalic(True)
        fmt_custom = QTextCharFormat(); fmt_custom.setForeground(QColor(custom_color)); fmt_custom.setFontWeight(QFont.Bold)
        
        kws = []
        comment_patterns = []
        
        if ext == '.py':
            kws = [r'\bdef\b', r'\bclass\b', r'\bimport\b', r'\bfrom\b', r'\bif\b', r'\belif\b', r'\belse\b', r'\bfor\b', r'\bwhile\b', r'\breturn\b', r'\bpass\b', r'\bbreak\b', r'\bcontinue\b', r'\band\b', r'\bor\b', r'\bnot\b', r'\bin\b', r'\bis\b', r'\bTrue\b', r'\bFalse\b', r'\bNone\b']
            comment_patterns = [r'#.*']
        elif ext == '.js':
            kws = [r'\bfunction\b', r'\bclass\b', r'\bimport\b', r'\bexport\b', r'\bif\b', r'\belse\b', r'\bfor\b', r'\bwhile\b', r'\breturn\b', r'\blet\b', r'\bconst\b', r'\bvar\b', r'\btrue\b', r'\bfalse\b', r'\bnull\b', r'\bundefined\b', r'\bnew\b', r'\bthis\b']
            comment_patterns = [r'//.*', r'/\*[\s\S]*?\*/']
        elif ext == '.css':
            kws = [r'@import', r'@media', r'@keyframes', r'!important']
            comment_patterns = [r'/\*[\s\S]*?\*/']
        elif ext == '.sh':
            kws = [r'\bif\b', r'\bfi\b', r'\bthen\b', r'\belif\b', r'\belse\b', r'\bfor\b', r'\bdo\b', r'\bdone\b', r'\buntil\b', r'\bwhile\b', r'\bbreak\b', r'\bcontinue\b', r'\bcase\b', r'\bfunction\b', r'\breturn\b', r'\bin\b']
            comment_patterns = [r'#.*']
        elif ext == '.bat':
            kws = [r'\becho\b', r'\bset\b', r'\bif\b', r'\bexist\b', r'\bgoto\b', r'\bcall\b', r'\bexit\b', r'\bfor\b', r'\bin\b', r'\bdo\b']
            comment_patterns = [r'\brem\b.*', r'::.*']
        elif ext == '.ps1':
            kws = [r'\bfunction\b', r'\bif\b', r'\belse\b', r'\belseif\b', r'\bfor\b', r'\bforeach\b', r'\bin\b', r'\bwhile\b', r'\buntil\b', r'\breturn\b']
            comment_patterns = [r'#.*']
            
        self.rules.append((re.compile(r'"[^"\\]*(\\.[^"\\]*)*"'), fmt_str))
        self.rules.append((re.compile(r"'[^'\\]*(\\.[^'\\]*)*'"), fmt_str))
        for cp in comment_patterns: self.rules.append((re.compile(cp), fmt_cmt))
        for kw in kws: self.rules.append((re.compile(kw), fmt_kw))
        if custom_words_str:
            c_words = [w.strip() for w in custom_words_str.split(",") if w.strip()]
            for w in c_words:
                self.rules.append((re.compile(rf'\b{re.escape(w)}\b'), fmt_custom))

    def highlightBlock(self, text):
        for pattern, format in self.rules:
            for match in pattern.finditer(text):
                self.setFormat(match.start(), match.end() - match.start(), format)

# ==============================================================================
# SETTINGS & DATA MANAGERS
# ==============================================================================
class PreDecidedDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Media Filters")
        self.resize(850, 600) # Large window for comfortable editing
        self.setStyleSheet(VIEWER_CSS)
        
        self.data = {}
        self.load_data()
        
        layout = QVBoxLayout(self)
        
        # --- SYNTAX CHEAT SHEET ---
        help_box = QTextBrowser()
        help_box.setStyleSheet("background: #0a0a0a; color: #dcdcaa; padding: 15px; border: 1px solid rgba(255,255,255,0.1); border-radius: 6px;")
        help_box.setFont(QFont("Consolas", 12))
        help_box.setPlainText(
            "========================================================================\n"
            "                             FILTER SYNTAX GUIDE                       \n"
            "========================================================================\n\n"
            "• Frames/Indices : 1-10, 15, 20\n"
            "• Media Groups   : images, videos, docs, audios, codes (Filters by type)\n"
            "• Built-in Sort  : sn (Natural serial sorting), shuffle\n"
            "• Transforms     : c90, cc90, hf, vf (Applies to preceding frames)\n"
            "• Modifiers      : repeat=X, loop=X (e.g., repeat=5)\n"
            "• Extensions     : jpg, png, mp4 (Only include these formats)\n"
            "• Exclusions     : -mp4, -gif    (Hide these extensions completely)\n"
            "• Max File Size  : max:5mb, max:200kb, max:1000b\n"
            "• Min File Size  : min:1mb, min:50kb, min:500b\n"
            "• Name Length    : minic=3, maxc=6 (Filename character count, ignores ext)\n"
            "• Include Text   : includeFilename=hello (Filename must contain 'hello')\n"
            "• Exclude Text   : excludeFilename=bad   (Filename must NOT contain 'bad')\n\n"
            "Example Queries:\n"
            "images, sn, includeFilename=vacation, max:5mb\n"
            "1-50, videos, -mkv, loop=3\n"
            "1-5:c90 6-10, images, sn, max:2mb"
            "1-50, jpg, png, -mp4, max:5mb, min:200kb, minic=3, includeFilename=hello, loop=3\n"
            "1-50, jpg, sn, repeat=3, max:5mb, minic=3, includeFilename=hello\n"
            "1-5:c90 6-10 12:hf cc90, jpg, sn, repeat=3, max:5mb"
        )
        layout.addWidget(help_box)
        
        row1 = QHBoxLayout()
        row1.addWidget(QLabel("📂 Category Name:"))
        self.cb_category = QComboBox()
        self.cb_category.setEditable(True)
        self.cb_category.addItems(list(self.data.keys()))
        self.cb_category.currentTextChanged.connect(self._on_cat_change)
        row1.addWidget(self.cb_category, 1)
        layout.addLayout(row1)
        
        row2 = QVBoxLayout()
        row2.addWidget(QLabel("🔢 Query (Filters):"))
        self.le_seq = QLineEdit()
        self.le_seq.setFont(QFont("Consolas", 12))
        self.le_seq.setPlaceholderText("Enter query filters here...")
        row2.addWidget(self.le_seq)
        layout.addLayout(row2)
        
        btn_lay = QHBoxLayout()
        self.btn_save = QPushButton("💾 Save")
        self.btn_del = QPushButton("🗑️ Delete")
        self.btn_del.setProperty("class", "danger-btn")
        self.btn_act = QPushButton("▶️ Activate Filter")
        self.btn_act.setProperty("class", "action-btn")
        self.btn_deact = QPushButton("🛑 Deactivate")
        
        self.btn_save.clicked.connect(self._save)
        self.btn_del.clicked.connect(self._del)
        self.btn_act.clicked.connect(self._activate)
        self.btn_deact.clicked.connect(self._deactivate)
        
        btn_lay.addWidget(self.btn_save)
        btn_lay.addWidget(self.btn_del)
        btn_lay.addStretch()
        btn_lay.addWidget(self.btn_deact)
        btn_lay.addWidget(self.btn_act)
        layout.addLayout(btn_lay)
        
        self.result_action = None
        self.result_seq = ""
        self._on_cat_change(self.cb_category.currentText())

    def load_data(self):
        if os.path.exists("pre_decided_lists.json"):
            try:
                with open("pre_decided_lists.json", "r") as f: self.data = json.load(f)
            except: self.data = {}
            
    def save_data(self):
        with open("pre_decided_lists.json", "w") as f: json.dump(self.data, f, indent=4)
        
    def _on_cat_change(self, text):
        if text in self.data: self.le_seq.setText(self.data[text])
        
    def _save(self):
        cat = self.cb_category.currentText().strip()
        seq = self.le_seq.text().strip()
        if cat and seq:
            self.data[cat] = seq
            self.save_data()
            if self.cb_category.findText(cat) == -1: self.cb_category.addItem(cat)
            QMessageBox.information(self, "Saved", f"Category '{cat}' successfully saved.")
            
    def _del(self):
        cat = self.cb_category.currentText().strip()
        if cat in self.data:
            del self.data[cat]
            self.save_data()
            idx = self.cb_category.findText(cat)
            if idx != -1: self.cb_category.removeItem(idx)
            self.le_seq.clear()
            
    def _activate(self):
        self.result_seq = self.le_seq.text().strip()
        self.result_action = "ACTIVATE"
        self.accept()
        
    def _deactivate(self):
        self.result_action = "DEACTIVATE"
        self.accept()

class AdvancedTextColorManagerDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Text Color & Syntax Manager")
        self.resize(900, 600)
        self.setStyleSheet(VIEWER_CSS)
        
        self.data = {"mappings": {}, "active": True}
        self.load_data()
        
        main_layout = QHBoxLayout(self)
        
        left_panel = QVBoxLayout()
        left_panel.addWidget(QLabel("📁 Extensions"))
        self.list_ext = QListWidget()
        for ext in self.data["mappings"].keys(): self.list_ext.addItem(ext)
        self.list_ext.currentTextChanged.connect(self._load_ext_props)
        left_panel.addWidget(self.list_ext)
        
        add_lay = QHBoxLayout()
        self.le_new_ext = QLineEdit()
        self.le_new_ext.setPlaceholderText("e.g. .log")
        btn_add_ext = QPushButton("Add")
        btn_add_ext.clicked.connect(self._add_ext)
        add_lay.addWidget(self.le_new_ext)
        add_lay.addWidget(btn_add_ext)
        left_panel.addLayout(add_lay)
        main_layout.addLayout(left_panel, 1)
        
        right_panel = QVBoxLayout()
        self.grp_props = QGroupBox("Syntax Color Properties")
        form_lay = QFormLayout(self.grp_props)
        
        self.btn_base = QPushButton(); self.btn_base.clicked.connect(lambda: self._pick_color("base", self.btn_base))
        self.btn_kw = QPushButton(); self.btn_kw.clicked.connect(lambda: self._pick_color("keyword", self.btn_kw))
        self.btn_str = QPushButton(); self.btn_str.clicked.connect(lambda: self._pick_color("string", self.btn_str))
        self.btn_cmt = QPushButton(); self.btn_cmt.clicked.connect(lambda: self._pick_color("comment", self.btn_cmt))
        
        form_lay.addRow("Standard Text Color:", self.btn_base)
        form_lay.addRow("Built-in Keywords:", self.btn_kw)
        form_lay.addRow("Strings (Quotes):", self.btn_str)
        form_lay.addRow("Comments (#, //):", self.btn_cmt)
        
        self.txt_custom_words = QPlainTextEdit()
        self.txt_custom_words.setPlaceholderText("Enter custom reserve words separated by commas (e.g. error, warning, critical)")
        self.txt_custom_words.setMaximumHeight(100)
        self.btn_custom_color = QPushButton(); self.btn_custom_color.clicked.connect(lambda: self._pick_color("custom_color", self.btn_custom_color))
        
        form_lay.addRow(QLabel("\nCustom Reserved Words:"))
        form_lay.addRow(self.txt_custom_words)
        form_lay.addRow("Custom Words Color:", self.btn_custom_color)
        right_panel.addWidget(self.grp_props)
        
        btn_lay = QHBoxLayout()
        self.btn_save = QPushButton("💾 Save Preferences")
        self.btn_save.setProperty("class", "action-btn")
        self.btn_save.clicked.connect(self._save_ext_props)
        
        self.btn_del = QPushButton("🗑️ Delete Selected")
        self.btn_del.setProperty("class", "danger-btn")
        self.btn_del.clicked.connect(self._del_ext)
        
        self.btn_act = QPushButton("▶️ Activate Custom Highlighting")
        self.btn_act.clicked.connect(self._activate)
        self.btn_deact = QPushButton("🛑 Deactivate")
        self.btn_deact.clicked.connect(self._deactivate)
        
        btn_lay.addWidget(self.btn_save)
        btn_lay.addWidget(self.btn_del)
        btn_lay.addStretch()
        btn_lay.addWidget(self.btn_deact)
        btn_lay.addWidget(self.btn_act)
        
        right_panel.addLayout(btn_lay)
        main_layout.addLayout(right_panel, 2)
        
        self.current_ext = None
        self.current_config = {}
        if self.list_ext.count() > 0:
            self.list_ext.setCurrentRow(0)

    def load_data(self):
        if os.path.exists("text_colors.json"):
            try:
                with open("text_colors.json", "r") as f:
                    self.data = json.load(f)
            except: pass
        if "mappings" not in self.data: self.data["mappings"] = {}
        if "active" not in self.data: self.data["active"] = True

    def save_data(self):
        with open("text_colors.json", "w") as f:
            json.dump(self.data, f, indent=4)

    def _add_ext(self):
        ext = self.le_new_ext.text().strip().lower()
        if ext and not ext.startswith('.'): ext = '.' + ext
        if ext and ext not in self.data["mappings"]:
            self.data["mappings"][ext] = {
                "base": "#e5e5e5", "keyword": "#569cd6", "string": "#ce9178", 
                "comment": "#6a9955", "custom_color": "#c586c0", "custom_words": ""
            }
            self.list_ext.addItem(ext)
            self.list_ext.setCurrentRow(self.list_ext.count()-1)
            self.le_new_ext.clear()

    def _load_ext_props(self, ext):
        if not ext: return
        self.current_ext = ext
        config = self.data["mappings"].get(ext, {})
        self.current_config = config.copy()
        
        self._apply_btn_color(self.btn_base, config.get("base", "#e5e5e5"))
        self._apply_btn_color(self.btn_kw, config.get("keyword", "#569cd6"))
        self._apply_btn_color(self.btn_str, config.get("string", "#ce9178"))
        self._apply_btn_color(self.btn_cmt, config.get("comment", "#6a9955"))
        self._apply_btn_color(self.btn_custom_color, config.get("custom_color", "#c586c0"))
        self.txt_custom_words.setPlainText(config.get("custom_words", ""))

    def _apply_btn_color(self, btn, hex_color):
        btn.setStyleSheet(f"background-color: {hex_color}; color: {'#000' if QColor(hex_color).lightness() > 128 else '#fff'}; font-weight: bold;")
        btn.setText(hex_color)

    def _pick_color(self, key, btn):
        if not self.current_ext: return
        color = QColorDialog.getColor(QColor(self.current_config.get(key, "#ffffff")), self)
        if color.isValid():
            self.current_config[key] = color.name()
            self._apply_btn_color(btn, color.name())

    def _save_ext_props(self):
        if self.current_ext:
            self.current_config["custom_words"] = self.txt_custom_words.toPlainText().strip()
            self.data["mappings"][self.current_ext] = self.current_config
            self.save_data()
            QMessageBox.information(self, "Saved", f"Preferences for {self.current_ext} saved.")

    def _del_ext(self):
        row = self.list_ext.currentRow()
        if row >= 0:
            ext = self.list_ext.item(row).text()
            if ext in self.data["mappings"]: del self.data["mappings"][ext]
            self.list_ext.takeItem(row)
            self.save_data()

    def _activate(self):
        self.data["active"] = True; self.save_data(); self.accept()
    def _deactivate(self):
        self.data["active"] = False; self.save_data(); self.accept()

# ==============================================================================
# MEDIA ENGINE COMPONENTS
# ==============================================================================

class ImageLoader(QThread):
    finished = Signal(str, object)
    def __init__(self, path: str, max_size=(1920, 1080), parent=None): 
        super().__init__(parent)
        self.path, self.max_size = path, max_size
    def run(self):
        try:
            img = QImage(self.path)
            if not img.isNull() and (img.width() > self.max_size[0] or img.height() > self.max_size[1]): 
                img = img.scaled(*self.max_size, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            self.finished.emit(self.path, img)
        except Exception: 
            self.finished.emit(self.path, QImage())


class WorkerSignals(QObject):
    finished = Signal(str, object, int)

class ImageWorker(QRunnable):
    def __init__(self, path, max_size, load_id):
        super().__init__()
        self.path = path; self.max_size = max_size; self.load_id = load_id
        self.signals = WorkerSignals()
        
    def run(self):
        try:
            img = QImage(self.path)
            if not img.isNull() and (img.width() > self.max_size[0] or img.height() > self.max_size[1]): 
                img = img.scaled(*self.max_size, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            self.signals.finished.emit(self.path, img, self.load_id)
        except Exception:
            self.signals.finished.emit(self.path, QImage(), self.load_id)

class LazyImageLabel(QLabel):
    """High-Performance Lazy Loader with Active Garbage Collection for Slow Drives"""
    def __init__(self, path):
        super().__init__()
        self.path = path; self.loaded = False; self.loading = False
        self.load_id = 0; self.orig_size = None; self.current_zoom = 1.0
        self.flip_h = False; self.flip_v = False; self.rot_angle = 0
        self.raw_image = None
        self.setAlignment(Qt.AlignCenter); self.setFont(QFont("Segoe UI", 11))
        self.setStyleSheet("color: rgba(255,255,255,0.4); background: #050505; border-radius: 6px;")
        self.setText("Waiting..."); self.setScaledContents(True); self.setMinimumHeight(600)
        
    def load_if_needed(self, view_width):
        if not self.loaded and not self.loading:
            self.loading = True; self.load_id += 1; self.setText("Decoding High-Res Media...")
            worker = ImageWorker(self.path, (view_width - 20, 4000), self.load_id)
            worker.signals.finished.connect(self._on_loaded)
            QThreadPool.globalInstance().start(worker)
            
    def unload(self):
        if self.loaded or self.loading:
            self.load_id += 1 # Invalidate pending callbacks
            self.loaded = False; self.loading = False; self.orig_size = None
            self.raw_image = None
            self.setPixmap(QPixmap()) # Force garbage collection
            self.setText("Waiting..."); self.setMinimumHeight(600)
            
    def _on_loaded(self, path, image, load_id):
        if load_id != self.load_id: return # Stale callback
        if image and not image.isNull() and image.width() > 0:
            self.raw_image = image
            self._render_pixmap()
        else: self.setText("Decode Error")
        self.loaded = True; self.loading = False
        
    def _render_pixmap(self):
        if not self.raw_image or self.raw_image.isNull(): return
        img = self.raw_image
        if getattr(self, 'flip_h', False) or getattr(self, 'flip_v', False):
            img = img.mirrored(getattr(self, 'flip_h', False), getattr(self, 'flip_v', False))
            
        if getattr(self, 'rot_angle', 0) != 0:
            trans = QTransform().rotate(self.rot_angle)
            pm = QPixmap.fromImage(img).transformed(trans, Qt.SmoothTransformation)
        else:
            pm = QPixmap.fromImage(img)
            
        self.setPixmap(pm)
        self.orig_size = pm.size()
        self.apply_zoom(self.current_zoom)
        
    def apply_zoom(self, factor):
        self.current_zoom = factor
        if self.orig_size:
            self.setFixedSize(int(max(10, self.orig_size.width() * factor)), int(max(10, self.orig_size.height() * factor)))
            
    def mousePressEvent(self, ev): ev.ignore()
    def mouseMoveEvent(self, ev): ev.ignore()
    def mouseReleaseEvent(self, ev): ev.ignore()

class AdvancedImageViewer(QGraphicsView):
    viewChanged = Signal(float, float, float) 
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.scene = QGraphicsScene(self)
        self.setScene(self.scene)
        self.setRenderHints(QPainter.Antialiasing | QPainter.SmoothPixmapTransform)
        self.setDragMode(QGraphicsView.ScrollHandDrag)
        self._pixmap_item = None
        self.zoom_factor = 1.15
        
        # --- NEW: Hide scrollbars and remove borders ---
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setStyleSheet("background: transparent; border: none;")
        
    def set_image(self, pixmap, ufit=True):
        self.scene.clear()
        self._pixmap_item = self.scene.addPixmap(pixmap)
        self.setSceneRect(self._pixmap_item.boundingRect())
        if ufit:
            self.fitInView(self.sceneRect(), Qt.KeepAspectRatio)
        
    def wheelEvent(self, event):
        if event.angleDelta().y() > 0: self.scale(self.zoom_factor, self.zoom_factor)
        else: self.scale(1 / self.zoom_factor, 1 / self.zoom_factor)
        self._emit_change()
        
    def scrollContentsBy(self, dx, dy):
        super().scrollContentsBy(dx, dy)
        self._emit_change()
        
    def _emit_change(self):
        z = self.transform().m11()
        c = self.mapToScene(self.viewport().rect().center())
        self.viewChanged.emit(z, c.x(), c.y())
        
    def keyPressEvent(self, event):
        if event.key() in (Qt.Key_Left, Qt.Key_Right, Qt.Key_Up, Qt.Key_Down, Qt.Key_Space): event.ignore()
        else: super().keyPressEvent(event)    

class AdvancedVideoViewer(QGraphicsView):
    viewChanged = Signal(float, float, float)
    
    def __init__(self, scene, parent=None):
        super().__init__(scene, parent)
        self.setRenderHints(QPainter.Antialiasing | QPainter.SmoothPixmapTransform)
        self.setDragMode(QGraphicsView.ScrollHandDrag)
        self.zoom_factor = 1.15
        
        # --- NEW: Hide scrollbars and remove borders for true full screen ---
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setStyleSheet("background: transparent; border: none;")
      
    def wheelEvent(self, event):
        if event.modifiers() == Qt.ControlModifier:
            if event.angleDelta().y() > 0: self.scale(self.zoom_factor, self.zoom_factor)
            else: self.scale(1 / self.zoom_factor, 1 / self.zoom_factor)
            self._emit_change()
        else: super().wheelEvent(event)
        
    def scrollContentsBy(self, dx, dy):
        super().scrollContentsBy(dx, dy)
        self._emit_change()
        
    def _emit_change(self):
        z = self.transform().m11()
        c = self.mapToScene(self.viewport().rect().center())
        self.viewChanged.emit(z, c.x(), c.y())
        
    def keyPressEvent(self, event):
        if event.key() in (Qt.Key_Left, Qt.Key_Right, Qt.Key_Up, Qt.Key_Down, Qt.Key_Space): event.ignore()
        else: super().keyPressEvent(event)

class RealAudioVisualizer(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.file_path = None
        self.file_obj = None
        self.file_size = 0
        self.vis_mode = "CIRCULAR_STRING" 
        
        self.energy_history = []
        self.num_bubbles = 30 
        self.bubbles = []
        self.num_string_points = 64
        self.string_bars = [0] * self.num_string_points
        self.target_string_bars = [0] * self.num_string_points
        
        self._init_bubbles()
        self.anim_timer = QTimer(self)
        self.anim_timer.timeout.connect(self.smooth_animation)
        self.playing = False

    def _init_bubbles(self):
        self.bubbles = []
        for i in range(self.num_bubbles):
            hue = random.randint(0, 359)
            color = QColor.fromHsv(hue, random.randint(120, 200), random.randint(230, 255), 150)
            self.bubbles.append({
                'x': random.uniform(100, 800), 'y': random.uniform(100, 600), 
                'vx': 0.0, 'vy': 0.0, 'angle': random.uniform(0, math.pi * 2),
                'turn_speed': random.uniform(-0.03, 0.03), 'r_base': random.uniform(20, 50), 
                'current_r': 20, 'color': color, 'target_amp': 0.0, 'amp': 0.0
            })

    def showEvent(self, event):
        self.anim_timer.start(16)
        super().showEvent(event)
    def hideEvent(self, event):
        self.anim_timer.stop()
        super().hideEvent(event)

    def load_file(self, path):
        if self.file_obj:
            self.file_obj.close(); self.file_obj = None
        self.file_path = path; self.energy_history = []
        self.string_bars = [0] * self.num_string_points; self.target_string_bars = [0] * self.num_string_points
        try:
            self.file_size = os.path.getsize(path); self.file_obj = open(path, 'rb')
        except: self.file_size = 0
            
        w, h = self.width() or 1280, self.height() or 720
        for b in self.bubbles:
            b['x'] = random.uniform(100, w - 100); b['y'] = random.uniform(100, h - 100)
            b['target_amp'] = 0.0; b['amp'] = 0.0

    def update_pos(self, pos_ms, dur_ms):
        if self.vis_mode == "OFF": return
        if not self.file_obj or dur_ms <= 0 or not self.playing: return
        ratio = max(0, min(1.0, pos_ms / dur_ms))
        offset = int(ratio * self.file_size)
        try:
            self.file_obj.seek(offset)
            data = self.file_obj.read(2048) 
            if len(data) > 1:
                str_chunk_size = max(1, len(data) // self.num_string_points)
                for i in range(min(self.num_string_points, len(data) // str_chunk_size)):
                    start = i * str_chunk_size
                    sub_chunk = data[start:start+str_chunk_size]
                    if len(sub_chunk) > 0: self.target_string_bars[i] = (sum(sub_chunk) / len(sub_chunk)) / 255.0

                diffs = [abs(data[j] - data[j-1]) for j in range(1, len(data))]
                global_energy = sum(diffs) / max(1, len(diffs))
                self.energy_history.append(global_energy)
                if len(self.energy_history) > 20: self.energy_history.pop(0)
                    
                avg_energy = sum(self.energy_history) / max(1, len(self.energy_history))
                is_beat = global_energy > (avg_energy * 1.05)
                
                chunk_size = max(1, len(data) // self.num_bubbles)
                for i in range(min(self.num_bubbles, len(data) // chunk_size)):
                    start = i * chunk_size
                    sub_chunk = data[start:start+chunk_size]
                    if len(sub_chunk) > 1:
                        d = [abs(sub_chunk[j] - sub_chunk[j-1]) for j in range(1, len(sub_chunk))]
                        base_amp = min(1.0, ((sum(d) / max(1, len(d))) / 60.0))
                        if is_beat:
                            self.bubbles[i]['target_amp'] = min(1.0, base_amp * 2.5)
                            self.bubbles[i]['angle'] += random.uniform(-0.6, 0.6)
                        else: self.bubbles[i]['target_amp'] = base_amp * 0.5
        except: pass

    def start_vis(self): self.playing = True
    def stop_vis(self):
        self.playing = False
        for b in self.bubbles: b['target_amp'] = 0.0
        self.target_string_bars = [0] * self.num_string_points

    def smooth_animation(self):
        if self.vis_mode == "OFF": return
        w = self.width() or 1280; h = self.height() or 720
        time_now = time.time()
        
        for i in range(self.num_string_points):
            if not self.playing: self.target_string_bars[i] = 0.0
            self.string_bars[i] += (self.target_string_bars[i] - self.string_bars[i]) * 0.25
        
        for b in self.bubbles:
            b['angle'] += b['turn_speed']; b['amp'] += (b['target_amp'] - b['amp']) * 0.2
            target_speed = 0.5 + (b['amp'] * 12.0)
            target_vx = math.cos(b['angle']) * target_speed; target_vy = math.sin(b['angle']) * target_speed
            b['vx'] += (target_vx - b['vx']) * 0.15; b['vy'] += (target_vy - b['vy']) * 0.15
            b['x'] += b['vx']; b['y'] += b['vy']
            b['current_r'] = b['r_base'] + (b['amp'] * 40); r = b['current_r']
            
            if b['x'] < r: b['x'] = r; b['vx'] *= -1; b['angle'] = math.pi - b['angle']
            elif b['x'] > w - r: b['x'] = w - r; b['vx'] *= -1; b['angle'] = math.pi - b['angle']
            if b['y'] < r: b['y'] = r; b['vy'] *= -1; b['angle'] = -b['angle']
            elif b['y'] > h - r: b['y'] = h - r; b['vy'] *= -1; b['angle'] = -b['angle']
            b['target_amp'] *= 0.85 

        for i in range(len(self.bubbles)):
            for j in range(i + 1, len(self.bubbles)):
                b1 = self.bubbles[i]; b2 = self.bubbles[j]
                dx = b2['x'] - b1['x']; dy = b2['y'] - b1['y']
                dist = math.hypot(dx, dy)
                min_dist = b1['current_r'] + b2['current_r']
                if dist < min_dist and dist > 0.001:
                    overlap = min_dist - dist; nx = dx / dist; ny = dy / dist
                    b1['x'] -= nx * overlap * 0.15; b1['y'] -= ny * overlap * 0.15
                    b2['x'] += nx * overlap * 0.15; b2['y'] += ny * overlap * 0.15
                    b1['angle'] -= 0.05; b2['angle'] += 0.05
        self.update()

    def paintEvent(self, event):
        if self.vis_mode == "OFF": return
        painter = QPainter(self); painter.setRenderHint(QPainter.Antialiasing)
        if self.vis_mode == "BUBBLES":
            for b in self.bubbles:
                px, py = b['x'], b['y']; rad = b['current_r']
                painter.setPen(Qt.NoPen); painter.setBrush(b['color']); painter.drawEllipse(QPointF(px, py), rad, rad)
                rim = QColor(b['color'].red(), b['color'].green(), b['color'].blue(), 200)
                painter.setPen(QPen(rim, 2)); painter.setBrush(Qt.NoBrush); painter.drawEllipse(QPointF(px, py), rad - 1.5, rad - 1.5)
        elif self.vis_mode == "CIRCULAR_STRING":
            w, h = self.width(), self.height(); cx, cy = w / 2, h / 2
            base_r = min(w, h) * 0.20
            colors = [QColor(255, 215, 0, 180), QColor(218, 165, 32, 140), QColor(184, 134, 11, 100)]
            time_shift = time.time() * 2.0
            for layer_idx, color in enumerate(colors):
                pts = []
                for i in range(self.num_string_points):
                    angle = (i / self.num_string_points) * 2 * math.pi
                    sym_i = i if i < 32 else 63 - i
                    amp = self.string_bars[sym_i]
                    wave = math.sin(angle * (3 + layer_idx) + time_shift) * 12
                    r = base_r + (amp * 160) + wave + (layer_idx * 15)
                    x = cx + r * math.cos(angle - time_shift * 0.2)
                    y = cy + r * math.sin(angle - time_shift * 0.2)
                    pts.append((x, y))
                midpoints = []
                for i in range(self.num_string_points):
                    p1 = pts[i]; p2 = pts[(i + 1) % self.num_string_points]
                    midpoints.append(((p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2))
                path = QPainterPath(); path.moveTo(midpoints[-1][0], midpoints[-1][1])
                for i in range(self.num_string_points): path.quadTo(pts[i][0], pts[i][1], midpoints[i][0], midpoints[i][1])
                painter.setPen(QPen(color, 4 - layer_idx)); painter.setBrush(QColor(color.red(), color.green(), color.blue(), 15))
                painter.drawPath(path)

    def closeEvent(self, ev):
        if self.file_obj: self.file_obj.close()
        super().closeEvent(ev)

class VerticalStripViewer(QScrollArea):
    zoomChanged = Signal(float)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWidgetResizable(True)
        self.align_flag = Qt.AlignHCenter; self.setAlignment(self.align_flag | Qt.AlignTop)
        self.container = QWidget(); self.layout = QVBoxLayout(self.container)
        self.layout.setContentsMargins(0, 0, 0, 0); self.layout.setSpacing(10)
        self.setWidget(self.container)
        
        self.labels = []; self.playlist = []; self._render_idx = 0; self.zoom_factor = 1.0
        self.flip_h = False; self.flip_v = False; self.rot_angle = 0
        self.viewport().setCursor(Qt.OpenHandCursor); self._is_dragging = False
        self.verticalScrollBar().valueChanged.connect(self.check_visibility)
        
    def set_alignment(self, align_flag):
        self.align_flag = align_flag; self.setAlignment(align_flag | Qt.AlignTop)
        for i in range(self.layout.count()):
            item = self.layout.itemAt(i)
            if item.widget(): self.layout.setAlignment(item.widget(), align_flag | Qt.AlignTop)
                
    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self._is_dragging = True; self._drag_start_pos = event.position()
            self._h_scroll_start = self.horizontalScrollBar().value(); self._v_scroll_start = self.verticalScrollBar().value()
            self.viewport().setCursor(Qt.ClosedHandCursor)
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if self._is_dragging:
            delta = event.position() - self._drag_start_pos
            self.horizontalScrollBar().setValue(int(self._h_scroll_start - delta.x()))
            self.verticalScrollBar().setValue(int(self._v_scroll_start - delta.y()))
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        if event.button() == Qt.LeftButton: self._is_dragging = False; self.viewport().setCursor(Qt.OpenHandCursor)
        super().mouseReleaseEvent(event)
        
    def wheelEvent(self, event):
        if event.modifiers() == Qt.AltModifier:
            delta = event.angleDelta().y() if event.angleDelta().y() != 0 else event.angleDelta().x() 
            if delta > 0: self.zoom_factor = min(5.0, self.zoom_factor * 1.15)
            elif delta < 0: self.zoom_factor = max(0.1, self.zoom_factor / 1.15)
            
            p = self
            while p and not hasattr(p, 'macro_scroll_paused'): p = p.parent()
            if p: p.macro_scroll_paused = True
            
            for lbl in self.labels: lbl.apply_zoom(self.zoom_factor)
            
            def release_blindfold():
                if p: p.macro_scroll_paused = False
            QTimer.singleShot(25, release_blindfold)
            
            self.zoomChanged.emit(self.zoom_factor)
            event.accept()
        else: super().wheelEvent(event)
        
    def load_playlist(self, playlist):
        while self.layout.count():
            item = self.layout.takeAt(0)
            if item.widget(): item.widget().deleteLater()
        self.labels.clear(); self.playlist = playlist; self._render_idx = 0; self.verticalScrollBar().setValue(0)
        self._render_next_batch(5)
        
    def apply_global_transforms(self, flip_h, flip_v, rot_angle):
        self.flip_h = flip_h
        self.flip_v = flip_v
        self.rot_angle = rot_angle
        for lbl in self.labels:
            lbl.flip_h = flip_h
            lbl.flip_v = flip_v
            lbl.rot_angle = rot_angle
            if hasattr(lbl, '_render_pixmap'):
                lbl._render_pixmap()    
        
    def _render_next_batch(self, count=10):
        end = min(self._render_idx + count, len(self.playlist))
        rad = "0px" if self.layout.spacing() == 0 else "6px"
        for i in range(self._render_idx, end):
            item = self.playlist[i]
            lbl = LazyImageLabel(item['path'])
            lbl.flip_h = getattr(self, 'flip_h', False)
            lbl.flip_v = getattr(self, 'flip_v', False)
            lbl.rot_angle = getattr(self, 'rot_angle', 0)
            lbl.current_zoom = self.zoom_factor; lbl.setStyleSheet(f"background: #050505; border-radius: {rad};")
            self.layout.addWidget(lbl, 0, self.align_flag | Qt.AlignTop)
            self.labels.append(lbl)
        self._render_idx = end; QTimer.singleShot(50, self.check_visibility)
        
    def check_visibility(self):
        vw = self.viewport().width()
        scroll_y = self.verticalScrollBar().value()
        vp_height = self.viewport().height()
        
        if scroll_y >= self.verticalScrollBar().maximum() - 2000 and self._render_idx < len(self.playlist):
            self._render_next_batch(10)
            
        # Active Garbage Collection
        for lbl in self.labels:
            lbl_y = lbl.pos().y()
            if lbl_y + lbl.height() >= scroll_y - 2500 and lbl_y <= scroll_y + vp_height + 2500:
                lbl.load_if_needed(vw / self.zoom_factor)
            else:
                lbl.unload()
                
    def scroll_to_index(self, index):
        if 0 <= index < len(self.labels):
            self.verticalScrollBar().setValue(self.labels[index].pos().y())

# ==============================================================================
# MAIN VIEWER CLASS (OVERLAY ARCHITECTURE)
# ==============================================================================
class SrtParser:
    @staticmethod
    def parse(file_path):
        subs = []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            blocks = re.split(r'\n\s*\n', content.strip())
            for block in blocks:
                lines = block.split('\n')
                if len(lines) >= 3:
                    times = lines[1].split(' --> ')
                    if len(times) == 2:
                        def parse_ms(t_str):
                            h, m, s_ms = t_str.strip().replace(',', '.').split(':')
                            s, ms = s_ms.split('.')
                            return int(h)*3600000 + int(m)*60000 + int(s)*1000 + int(ms)
                        subs.append({
                            'start': parse_ms(times[0]), 'end': parse_ms(times[1]),
                            'text': '\n'.join(lines[2:])
                        })
        except: pass
        return subs

class AdvancedLoopDialog(QDialog):
    def __init__(self, current_raw, parent=None):
        super().__init__(parent)
        self.setWindowTitle("VMSL Studio - Macro Scripting IDE")
        self.resize(1000, 700)
        self.setStyleSheet(VIEWER_CSS)
        
        layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        self.tab_code = QWidget()
        code_lay = QVBoxLayout(self.tab_code)
        
        self.txt_input = QPlainTextEdit()
        self.txt_input.setPlainText(current_raw)
        self.txt_input.setFont(QFont("Consolas", 14))
        # --- CRITICAL FIX: Explicitly styling and enabling scrollbars for the code editor ---
        self.txt_input.setStyleSheet("""
            QPlainTextEdit { background: #080808; color: #f8f8f2; border: none; padding: 15px; }
            QScrollBar:vertical { border: none; background: #111; width: 14px; margin: 0px; }
            QScrollBar::handle:vertical { background: #444; border-radius: 7px; min-height: 20px; }
            QScrollBar::handle:vertical:hover { background: #555; }
            QScrollBar:horizontal { border: none; background: #111; height: 14px; margin: 0px; }
            QScrollBar::handle:horizontal { background: #444; border-radius: 7px; min-width: 20px; }
            QScrollBar::handle:horizontal:hover { background: #555; }
            QScrollBar::add-line, QScrollBar::sub-line { border: none; background: none; }
        """)
        self.txt_input.setLineWrapMode(QPlainTextEdit.NoWrap)
        
        self.highlighter = VMSLHighlighter(self.txt_input.document())
        code_lay.addWidget(self.txt_input)
        self.tabs.addTab(self.tab_code, "🖥️ VMSL Code Editor")
        
        self.tab_help = QWidget()
        help_lay = QVBoxLayout(self.tab_help)
        help_txt = QPlainTextEdit()
        help_txt.setReadOnly(True)
        help_txt.setFont(QFont("Consolas", 12))
        help_txt.setStyleSheet("background: #0a0a0a; color: #dcdcaa; padding: 20px; border: none;")
        help_txt.setPlainText("""
================================================================================
                    VMAN MACRO SCRIPTING LANGUAGE (VMSL)
================================================================================

[1] MODES & VIEW COMMANDS
-------------------------
mode=pdf               : Switches to Normal PDF Mode
mode=magazine_cover    : Switches to 2-Page Cover Mode (1, 2-3, 4-5)
mode=magazine_double   : Switches to 2-Page Double Mode (1-2, 3-4)
mode=web               : Switches to Webpage Mode (Vertical scroll)
mode=norm              : Standard Image/Video Mode
spread=1 / spread=-1   : Flips to the next/prev PDF Magazine spread

[2] FAST COMPACT SYNTAX (Optional defaults)
-------------------------------------------
Format:  [frames][:durXloops][:transforms]
Example: 1-5,8:150x5:hf     (Frames 1 to 5 & 8, 150ms, 5 loops, H-Flip)
Example: 1-10               (Plays 1-10 using your default engine speed)
Examples: 1:c90      (Rotate 90)
          1:x5       (Play 5 times default speed)
          1:1000:hf  (1 second, horiz flip)
          1:3000x5:vf

[3] SYSTEM COMMANDS
-------------------
pause           : Pauses execution until you manually hit play again.
stop            : Ends the macro script entirely.
wait [ms]       : Waits for [ms] milliseconds (e.g. wait 2000).

[4] VARIABLES & COMMENTS
------------------------
# This is a comment
$intro = 1-10           # Define a variable
$intro dur=200 t=c90    # Execute variable

[5] PARAMETERS (Used after a target)
------------------------------------------------------
dur=ms      : Duration in ms (e.g., dur=200)
loop=x      : Times to repeat block (e.g., loop=5)
t=type      : Transforms: hf, vf, c90, c180, c270, cc90
z=factor    : Zoom level (e.g., z=1.5)
zx/zy       : Absolute Pan X/Y when zoomed
sx/sy       : Absolute Scroll X/Y for Web/PDF/Text
zs/zxs/zys  : Smoothly animate Zoom and Pan
sxs/sys     : Smoothly animate Scroll
dragU/D/L/R : Relative drag by unit (e.g., dragU=2cm, dragR=1in, dragD=50px)
scrollU/D/L/R: Relative scroll by unit (e.g., scrolld=5cm, scrollr=2in)
vol=lvl     : Set volume 0-100 (e.g., vol=50)
start/end   : Media seek times
rate=speed  : Playback speed
rev=1       : Reverse frame sequence
ufit=1/0    : Force universal fit-to-screen globally or explicitly (e.g., ufit=1)


[6] FULL EXAMPLES
-------------------
# Fast syntax with default speed fallback
1-2,6,2,8-6:150x5:c90
1-5:hf

# Slide, Zoom smoothly over 2 seconds
5 dur=2000 zs=2.5 zxs=400 zys=100
wait 1000

# Play a video from 10s to 15s at double speed, then pause
8 start=10.0 end=15.0 rate=2.0
pause

5 dur=2000 zs=2.5 zxs=400 zys=100
10 sxs=500 sys=1200 dur=3000   # Smooth scroll down a webpage/PDF
        """.strip())
        help_lay.addWidget(help_txt)
        self.tabs.addTab(self.tab_help, "📖 VMSL Documentation & Rules")
        layout.addWidget(self.tabs)
        
        btn_lay = QHBoxLayout()
        self.btn_load = QPushButton("📂 Load Script"); self.btn_save = QPushButton("💾 Save to File (Optional)")
        self.btn_clear = QPushButton("🗑️ Clear Editor"); self.btn_deactivate = QPushButton("🛑 Deactivate Engine")
        self.btn_deactivate.setProperty("class", "danger-btn"); self.btn_apply = QPushButton("▶️ Test / Play Script")
        self.btn_apply.setProperty("class", "action-btn")
        
        self.btn_load.clicked.connect(self.load_file); self.btn_save.clicked.connect(self.save_file)
        self.btn_clear.clicked.connect(self.txt_input.clear); self.btn_deactivate.clicked.connect(self.deactivate)
        self.btn_apply.clicked.connect(self.accept)
        
        btn_lay.addWidget(self.btn_load); btn_lay.addWidget(self.btn_save); btn_lay.addWidget(self.btn_clear)
        btn_lay.addStretch(); btn_lay.addWidget(self.btn_deactivate); btn_lay.addWidget(self.btn_apply)
        layout.addLayout(btn_lay)
        self.result_raw = current_raw
        
    def load_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Load VMSL Script", "", "VMSL Scripts (*.vmsl *.txt);;All Files (*.*)")
        if path:
            try:
                with open(path, 'r', encoding='utf-8') as f: self.txt_input.setPlainText(f.read().strip())
            except Exception as e: QMessageBox.warning(self, "Error", str(e))
    def save_file(self):
        path, _ = QFileDialog.getSaveFileName(self, "Save VMSL Script", "", "VMSL Scripts (*.vmsl)")
        if path:
            try:
                if not path.endswith('.vmsl'): path += '.vmsl'
                with open(path, 'w', encoding='utf-8') as f: f.write(self.txt_input.toPlainText().strip())
                QMessageBox.information(self, "Saved", "Script saved successfully.")
            except Exception as e: QMessageBox.warning(self, "Error", str(e))
    def deactivate(self): self.result_raw = ""; super().accept()
    def accept(self): self.result_raw = self.txt_input.toPlainText().strip(); super().accept()


class vmanViewer(QDialog):
    IDX_TXT = 0
    IDX_CSV = 1
    IDX_IMG = 2
    IDX_MEDIA = 3
    IDX_WEB = 4
    IDX_MD = 5
    IDX_PDF = 6
    

    def __init__(self, playlist, start_index, parent=None):
        super().__init__(parent)
        
        self._is_initialized = False
        self.master_playlist = playlist
        self.active_playlist = playlist
        self.current_index = start_index
        self.current_filter = "ALL"
        
        self.settings_file = "vman_settings.json"
        self.vis_mode = "CIRCULAR_STRING" 
        self.load_settings()
        
        self.text_color_data = {"mappings": {}, "active": True}
        self._load_text_colors()
        self.current_highlighter = None
        self.is_md_rendered = True
        
        self.setWindowTitle("VMan Media")
        self.setMinimumSize(800, 600)  # CRITICAL: Prevents the small window flash
        self.resize(1280, 850)
        self.setWindowFlags(self.windowFlags() | Qt.WindowMaximizeButtonHint | Qt.WindowMinimizeButtonHint)
        self.setStyleSheet(VIEWER_CSS)
        
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)

        self.stack = QStackedWidget()
        self.main_layout.addWidget(self.stack)

        self.txt_view = QPlainTextEdit()
        self.txt_view.setReadOnly(True)
        self.txt_view.setFont(QFont("Consolas", 13))
        
        self.csv_view = QTableWidget()
        self.csv_view.setEditTriggers(QTableWidget.NoEditTriggers)
        self.csv_view.horizontalHeader().setStretchLastSection(True)
        self.csv_view.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.csv_view.verticalHeader().setSectionResizeMode(QHeaderView.Interactive)
        
        # --- NEW: Enable Smooth Drag-to-Pan for CSV ---
        self.csv_view.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.csv_view.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        QScroller.grabGesture(self.csv_view.viewport(), QScroller.LeftMouseButtonGesture)
        
        self.img_view = AdvancedImageViewer()
        self.img_view.viewChanged.connect(self._on_view_changed)
        
        self.vert_view = VerticalStripViewer()
        self.vert_view.zoomChanged.connect(self._record_zoom_event)
        
        self.md_view = QTextBrowser()
        self.md_view.setOpenExternalLinks(True)
        
        if HAS_PDF:
            self.pdf_doc = QPdfDocument(self)
            
            # Container for Magazine View (2-Page Side-by-Side)
            self.pdf_container = QWidget()
            self.pdf_container.setStyleSheet("background: transparent;")
            self.pdf_lay = QHBoxLayout(self.pdf_container)
            self.pdf_lay.setContentsMargins(0, 0, 0, 0)
            self.pdf_lay.setSpacing(0)
            self.pdf_lay.setAlignment(Qt.AlignTop | Qt.AlignHCenter) # CRITICAL: Forces pages to align perfectly at the top edge
            
            self.pdf_view = AdvancedPdfViewer(self.pdf_container)
            self.pdf_view.setDocument(self.pdf_doc)
            self.pdf_view.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
            self.pdf_view.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
            self.pdf_view.setStyleSheet("background: transparent; border: none; padding: 0px; margin: 0px;") 
            
            # --- CRITICAL FIX: Direct Normal Mode Zoom to the new Tracker ---
            self.pdf_view.zoomChanged.connect(self._on_normal_zoom_changed)
            
            self.pdf_view_right = AdvancedPdfViewer(self.pdf_container)
            self.pdf_view_right.setDocument(self.pdf_doc)
            self.pdf_view_right.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
            self.pdf_view_right.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
            self.pdf_view_right.setStyleSheet("background: transparent; border: none; padding: 0px; margin: 0px;") # Strip hidden borders
            self.pdf_view_right.hide()
            
            self.pdf_lay.addWidget(self.pdf_view)
            self.pdf_lay.addWidget(self.pdf_view_right)
            
            # Wrapper acts as the single master controller
            self.pdf_scene = QGraphicsScene(self)
            self.pdf_wrapper = MagazineWrapper(self.pdf_scene, self)
            self.pdf_proxy = self.pdf_scene.addWidget(self.pdf_container)
            
            self.is_magazine_mode = False
            self.pdf_wrapper.zoom_req.connect(self._apply_pdf_zoom)
            self.pdf_wrapper.scroll_spread.connect(self._navigate_spread)
            
            # CRITICAL: Connect the Macro Recorder to the Magazine Wrapper!
            self.pdf_wrapper.viewChanged.connect(self._on_view_changed)
        else:
            self.pdf_wrapper = QLabel("QtPdf module not found. Cannot render PDF.")
            self.pdf_wrapper.setAlignment(Qt.AlignCenter)
            self.pdf_wrapper.setStyleSheet("color: #ef4444; font-size: 20px;")
                
        self.stack.addWidget(self.txt_view)    # 0
        self.stack.addWidget(self.csv_view)    # 1
        self.stack.addWidget(self.img_view)    # 2
        
        self.media_container = QWidget()
        m_lay = QVBoxLayout(self.media_container)
        m_lay.setContentsMargins(0,0,0,0)
        
        self.wave_vis = RealAudioVisualizer()
        self.wave_vis.vis_mode = self.vis_mode
        self.wave_vis.hide()
        m_lay.addWidget(self.wave_vis, 1) 
        
        if HAS_MULTIMEDIA:
            self.video_scene = QGraphicsScene()
            self.video_view = AdvancedVideoViewer(self.video_scene)
            self.video_view.viewChanged.connect(self._on_view_changed)
            
            self.video_item = QGraphicsVideoItem()
            self.video_scene.addItem(self.video_item)
            self.video_item.nativeSizeChanged.connect(
                lambda size: (self.video_item.setSize(size), self.video_view.fitInView(self.video_item.boundingRect(), Qt.KeepAspectRatio))
            )
            m_lay.insertWidget(0, self.video_view, 1)
            
            self.player = QMediaPlayer()
            self.audio = QAudioOutput()
            self.player.setAudioOutput(self.audio)
            self.player.setVideoOutput(self.video_item)
            self.loop_a = -1
            self.loop_b = -1
            
            self.player.positionChanged.connect(self._check_loop)
            self.player.positionChanged.connect(lambda pos: self.wave_vis.update_pos(pos, self.player.duration()))
            
            # FIX: Hook into media status to trigger auto-play
            self.player.mediaStatusChanged.connect(self._on_media_status_changed)
            
        self.stack.addWidget(self.media_container) # 3
        self.stack.addWidget(self.vert_view)       # 4
        self.stack.addWidget(self.md_view)         # 5
        self.stack.addWidget(self.pdf_wrapper)        # 6

        # ==========================================================
        # DYNAMIC OVERLAYS
        # ==========================================================

        if not hasattr(self, 'prog_mode'): self.prog_mode = "SHOW"
        self.slide_progress = QProgressBar(self)
        self.slide_progress.setFixedHeight(2)
        self.slide_progress.setTextVisible(False)
        self.slide_progress.setRange(0, 100)
        self.slide_progress.hide()

        self.header_widget = QWidget(self)
        self.header_widget.setStyleSheet("background: rgba(10, 10, 10, 0.7);")
        h_lay = QHBoxLayout(self.header_widget)
        h_lay.setContentsMargins(20, 0, 20, 0)
        
        self.lbl_title = QLabel()
        self.lbl_title.setFont(QFont("Segoe UI", 12))
        self.lbl_title.setStyleSheet("color: rgba(255,255,255,0.85); background: transparent;")
        h_lay.addWidget(self.lbl_title, 1)

        if not hasattr(self, 'count_mode'): self.count_mode = "SHOW"
        self.overlay_counter = QLabel(self)
        self.overlay_counter.setFont(QFont("Segoe UI", 10, QFont.Bold))
        self.overlay_counter.setAlignment(Qt.AlignCenter)
        self.overlay_counter.setStyleSheet("background: rgba(0, 0, 0, 0.7); color: #3b82f6; border: 1px solid rgba(255,255,255,0.1); border-radius: 4px; padding: 2px 10px;")

        if not hasattr(self, 'deck_mode'): self.deck_mode = "AUTO" 
        if not hasattr(self, 'show_quick_tools'): self.show_quick_tools = False
        
        self.control_panel = QWidget(self)
        self.control_panel.setStyleSheet("""
            QWidget { background: rgba(15, 15, 15, 0.85); border: 1px solid rgba(255,255,255,0.08); border-radius: 8px; }
            QPushButton { background: transparent; border: none; border-radius: 4px; padding: 5px; font-size: 16px; color: #ccc; }
            QPushButton:hover { background: rgba(255,255,255,0.1); color: #fff; }
        """)
        cp_lay = QHBoxLayout(self.control_panel)
        cp_lay.setContentsMargins(15, 5, 15, 5)
        cp_lay.setSpacing(10)
        
        self.btn_cp_play = QPushButton("⏯")
        self.btn_cp_play.setFixedSize(35, 35)
        self.btn_cp_play.clicked.connect(self._toggle_playback)
        
        # Define all buttons first
        self.btn_cp_flip = QPushButton("↔")
        self.btn_cp_rot = QPushButton("↻")
        self.btn_cp_ab = QPushButton("🅰-🅱")
        self.btn_cp_repeat = QPushButton("🔁") 
        self.btn_cp_repeat.setCheckable(True) 
        self.btn_cp_shuffle = QPushButton("🔀")
        self.btn_cp_shuffle.setCheckable(True) # FIX: Make button stateful
        
        # NEW: Dedicated Zoom buttons replacing Shuffle/Repeat for Images
        self.btn_cp_z_in = QPushButton("🔍+")
        self.btn_cp_z_out = QPushButton("🔍-")
        
        # Connect clicks
        self.btn_cp_flip.clicked.connect(self._flip_img_horiz)
        self.btn_cp_rot.clicked.connect(self._rot_img_cw)
        self.btn_cp_ab.clicked.connect(self._set_ab_loop)
        self.btn_cp_shuffle.toggled.connect(self._toggle_shuffle) 
        if HAS_MULTIMEDIA: self.btn_cp_repeat.toggled.connect(self._toggle_repeat)
        self.btn_cp_z_in.clicked.connect(lambda: self._apply_general_zoom(1.15))
        self.btn_cp_z_out.clicked.connect(lambda: self._apply_general_zoom(1/1.15))
        
        # --- NEW: Toast Notification OSD ---
        self.toast_osd = QLabel(self)
        self.toast_osd.setFixedSize(300, 40)
        self.toast_osd.setStyleSheet("background: rgba(59, 130, 246, 0.95); color: white; border-radius: 8px; font-weight: bold; font-size: 15px; border: 1px solid rgba(255,255,255,0.2);")
        self.toast_osd.setAlignment(Qt.AlignCenter)
        self.toast_osd.hide()
        self.toast_timer = QTimer(self)
        self.toast_timer.timeout.connect(self.toast_osd.hide)
        self.toast_timer.setSingleShot(True)
        
        # Set visibility
        self.btn_cp_flip.setVisible(self.show_quick_tools)
        self.btn_cp_rot.setVisible(self.show_quick_tools)
        self.btn_cp_ab.setVisible(self.show_quick_tools)
        self.btn_cp_repeat.setVisible(True) 
        self.btn_cp_shuffle.setVisible(True)
        
        
        self.cp_slider = QSlider(Qt.Horizontal)
        self.cp_slider.setStyleSheet("""
            QSlider::groove:horizontal { background: rgba(255,255,255,0.1); height: 4px; border-radius: 2px; } 
            QSlider::sub-page:horizontal { background: #3b82f6; border-radius: 2px; }
            QSlider::handle:horizontal { background: #fff; width: 14px; margin: -5px 0; border-radius: 7px; }
        """)
        self.lbl_time = QLabel("--:--")
        self.lbl_time.setFont(QFont("Consolas", 10))
        self.lbl_time.setStyleSheet("border: none; background: transparent; color: #aaa;")
        
        if HAS_MULTIMEDIA:
            self.player.positionChanged.connect(self._update_slider)
            
            self.player.durationChanged.connect(self.cp_slider.setMaximum)
            self.cp_slider.sliderMoved.connect(self.player.setPosition)
            self.cp_slider.sliderReleased.connect(self._on_seek_recorded)

        cp_lay.addWidget(self.btn_cp_play)
        cp_lay.addWidget(self.btn_cp_flip)
        cp_lay.addWidget(self.btn_cp_rot)
        cp_lay.addWidget(self.btn_cp_ab)
        cp_lay.addWidget(self.btn_cp_shuffle) # ADD SHUFFLE TO LAYOUT
        cp_lay.addWidget(self.btn_cp_z_in)
        cp_lay.addWidget(self.btn_cp_z_out)
        cp_lay.addWidget(self.btn_cp_repeat)
        cp_lay.addWidget(self.cp_slider, 1)
        cp_lay.addWidget(self.lbl_time)
        
        # --- PDF Dedicated Deck ---
        self.pdf_control_panel = QWidget(self)
        self.pdf_control_panel.setStyleSheet("""
            QWidget { background: rgba(15, 15, 15, 0.85); border: 1px solid rgba(255,255,255,0.08); border-radius: 8px; }
            QPushButton { background: transparent; border: none; border-radius: 4px; padding: 5px 15px; font-size: 14px; font-weight: bold; color: #ccc; }
            QPushButton:hover { background: rgba(255,255,255,0.1); color: #fff; }
            QPushButton:checked { color: #3b82f6; }
        """)
        pdf_cp_lay = QHBoxLayout(self.pdf_control_panel)
        
        self.btn_pdf_z_in = QPushButton("🔍+")
        self.btn_pdf_z_out = QPushButton("🔍-")
        self.btn_pdf_fit_w = QPushButton("↔ Fit Width")
        self.btn_pdf_fit_p = QPushButton("↕ Fit Page")
        
        # NEW PDF Buttons
        self.btn_pdf_flip = QPushButton("↔ Flip")
        self.btn_pdf_flip.setCheckable(True)
        
        self.btn_pdf_scroll = QPushButton("⏬ Scroll")
        self.btn_pdf_scroll.setCheckable(True)
        
        self.auto_scroll_speed = 1
        
        if HAS_PDF:
            self.btn_pdf_flip.toggled.connect(self._toggle_pdf_flip)
            # Unified zoom hooks
            self.btn_pdf_z_in.clicked.connect(lambda: self._apply_pdf_zoom(1.2))
            self.btn_pdf_z_out.clicked.connect(lambda: self._apply_pdf_zoom(1/1.2))
            self.btn_pdf_fit_w.clicked.connect(lambda: self._set_pdf_zoom_mode(QPdfView.ZoomMode.FitToWidth))
            self.btn_pdf_fit_p.clicked.connect(lambda: self._set_pdf_zoom_mode(QPdfView.ZoomMode.FitInView))
            
        self.btn_pdf_scroll.toggled.connect(self._toggle_auto_scroll)
            
        pdf_cp_lay.addWidget(self.btn_pdf_z_in)
        pdf_cp_lay.addWidget(self.btn_pdf_z_out)
        pdf_cp_lay.addWidget(self.btn_pdf_fit_w)
        pdf_cp_lay.addWidget(self.btn_pdf_fit_p)
        pdf_cp_lay.addWidget(self.btn_pdf_flip)
        pdf_cp_lay.addWidget(self.btn_pdf_scroll)
        self.pdf_control_panel.hide()
        
        self.auto_scroll_timer = QTimer(self)
        self.auto_scroll_timer.timeout.connect(self._do_auto_scroll)

        # --- Floating Nav Arrows (Overlay) ---
        if not hasattr(self, 'nav_mode'): self.nav_mode = "AUTO"
        nav_style = """
            QPushButton { background: transparent; color: rgba(255,255,255,0.2); border: none; font-size: 40px; font-weight: 200; }
            QPushButton:hover { background: rgba(255,255,255,0.08); color: #fff; border-radius: 15px; }
        """
        self.btn_float_prev = QPushButton("❮", self)
        self.btn_float_next = QPushButton("❯", self)
        for btn in [self.btn_float_prev, self.btn_float_next]:
            btn.setFixedSize(50, 180)
            btn.setStyleSheet(nav_style)
            btn.hide() 
        self.btn_float_prev.clicked.connect(self._prev_item)
        self.btn_float_next.clicked.connect(self._next_item)

        # --- On-Screen Volume OSD (Overlay) ---
        self.vol_osd = QWidget(self)
        self.vol_osd.setFixedSize(220, 40)
        self.vol_osd.setStyleSheet("background: rgba(20,20,20,0.95); border-radius: 8px; border: 1px solid rgba(255,255,255,0.15);")
        vol_lay = QHBoxLayout(self.vol_osd)
        vol_lay.setContentsMargins(15, 5, 15, 5)
        
        self.lbl_vol_icon = QLabel("🔊")
        self.lbl_vol_icon.setStyleSheet("color: #3b82f6; font-size: 16px; background: transparent; border: none;")
        self.bar_vol = QProgressBar()
        self.bar_vol.setFixedHeight(4)
        self.bar_vol.setTextVisible(False)
        self.bar_vol.setRange(0, 100)
        self.bar_vol.setStyleSheet("QProgressBar { background: rgba(255,255,255,0.1); border: none; border-radius: 2px; } QProgressBar::chunk { background: #3b82f6; border-radius: 2px; }")
        
        vol_lay.addWidget(self.lbl_vol_icon)
        vol_lay.addWidget(self.bar_vol)
        self.vol_osd.hide()
        
        # --- Brightness Overlay ---
        self.brightness_overlay = QWidget(self)
        self.brightness_overlay.setAttribute(Qt.WA_TransparentForMouseEvents)
        self.brightness_overlay.setStyleSheet("background-color: rgba(0, 0, 0, 0);")
        self.brightness_level = 50 # 50 is normal, <50 darker, >50 brighter
        
        # --- Subtitles ---
        self.subtitles = []
        if not hasattr(self, 'subtitle_color'): self.subtitle_color = "#FFFFFF"
        
        self.lbl_subtitle = QLabel(self)
        self.lbl_subtitle.setAttribute(Qt.WA_TransparentForMouseEvents)
        self.lbl_subtitle.setAlignment(Qt.AlignCenter | Qt.AlignBottom)
        self.lbl_subtitle.setFont(QFont("Arial", 24, QFont.Bold))
        self.lbl_subtitle.setWordWrap(True) # FIX: Ensure long subtitles wrap to next line
        
        # FIX: Make background transparent so it shows directly on screen
        self.lbl_subtitle.setStyleSheet(f"color: {self.subtitle_color}; background: transparent; padding: 0px;")
        
        # Add a text shadow so it's readable without a container
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(5)
        shadow.setColor(QColor(0, 0, 0, 255))
        shadow.setOffset(2, 2)
        self.lbl_subtitle.setGraphicsEffect(shadow)
        
        self.lbl_subtitle.hide()
        
        # --- Miniplayer State ---
        self.is_miniplayer = False
        self.normal_geometry = None
        
        self.vol_timer = QTimer(self)
        self.vol_timer.timeout.connect(self.vol_osd.hide)
        self.vol_timer.setSingleShot(True)

        # --- Audio Metadata Overlay ---
        self.lbl_metadata = QLabel(self)
        self.lbl_metadata.setFont(QFont("Segoe UI", 16, QFont.Bold))
        self.lbl_metadata.setStyleSheet("color: rgba(255,255,255,0.95); background: rgba(0, 0, 0, 0.5); border-radius: 8px; padding: 10px;")
        self.lbl_metadata.setAlignment(Qt.AlignCenter)
        self.lbl_metadata.hide()
        
        if HAS_MULTIMEDIA:
            self.player.metaDataChanged.connect(self._update_metadata)

        # --- Macro Recorder Engine ---
        self.macro_recording = False
        self.macro_paused = False
        self.macro_events = []
        self.macro_last_time = 0
        
        self.last_macro_view_state = None
        self.last_macro_scroll_state = None
        
        self.macro_debouncer = QTimer(self)
        self.macro_debouncer.setSingleShot(True)
        self.macro_debouncer.timeout.connect(self._commit_view_change)
        self.pending_view_state = None
        
        self.macro_scroll_debouncer = QTimer(self)
        self.macro_scroll_debouncer.setSingleShot(True)
        self.macro_scroll_debouncer.timeout.connect(self._commit_scroll_macro)
        self.pending_scroll_state = None
        
        # --- PDF Wrapper Connections ---
        if HAS_PDF:
            self.pdf_wrapper.zoom_req.connect(self._apply_pdf_zoom)
            self.pdf_wrapper.scroll_spread.connect(self._navigate_spread)
            self.pdf_wrapper.viewChanged.connect(self._on_view_changed) # NEW: Hook into Macro Recorder!

        # --- Macro Recorder Engine OSD (With Shortcut Hints) ---
        self.macro_osd = QWidget(self)
        self.macro_osd.setFixedSize(620, 50)
        self.macro_osd.setStyleSheet("background: rgba(20,20,20,0.95); border: 2px solid #ef4444; border-radius: 8px;")
        m_lay = QHBoxLayout(self.macro_osd)
        
        self.btn_m_rec = QPushButton("🔴 Rec")
        self.btn_m_rec.setStyleSheet("color: #ef4444; font-weight: bold;")
        self.btn_m_pause = QPushButton("⏸ Pause [P]")  # Shows [P]
        self.btn_m_stop = QPushButton("⏹ Stop / Kill [K]")  # Shows [K]
        
        self.lbl_macro_log = QLabel("Ready... (Shortcuts: P = Pause | K = Kill/Stop)")
        self.lbl_macro_log.setStyleSheet("color: #a8a8a8; font-family: Consolas; border: none; font-size: 11px;")
        
        m_lay.addWidget(self.btn_m_rec)
        m_lay.addWidget(self.btn_m_pause)
        m_lay.addWidget(self.btn_m_stop)
        m_lay.addWidget(self.lbl_macro_log, 1)
        
        self.btn_m_rec.clicked.connect(self._start_macro)
        self.btn_m_pause.clicked.connect(self._pause_macro)
        self.btn_m_stop.clicked.connect(self._stop_macro)
        self.macro_osd.hide()
        
        QShortcut(QKeySequence("P"), self, self._pause_macro)
        
        QShortcut(QKeySequence("K"), self, self._handle_k_key)

        def track_scroll(widget):
            widget.horizontalScrollBar().valueChanged.connect(lambda v, w=widget: self._on_scroll_changed(w))
            widget.verticalScrollBar().valueChanged.connect(lambda v, w=widget: self._on_scroll_changed(w))

        track_scroll(self.vert_view)
        track_scroll(self.txt_view)
        track_scroll(self.csv_view)
        if HAS_PDF: track_scroll(self.pdf_view)

        # --- Smooth Animation Engine (Macro Playback) ---
        self.smooth_anim_timer = QTimer(self)
        self.smooth_anim_timer.timeout.connect(self._smooth_anim_tick)
        self.smooth_target = {}

        # --- Effects & Timers ---
        self.ui_locked_hidden = False 
        
        self.cp_opacity = QGraphicsOpacityEffect(self.control_panel)
        self.control_panel.setGraphicsEffect(self.cp_opacity)
        self.pdf_opacity = QGraphicsOpacityEffect(self.pdf_control_panel)
        self.pdf_control_panel.setGraphicsEffect(self.pdf_opacity)
        self.head_opacity = QGraphicsOpacityEffect(self.header_widget)
        self.header_widget.setGraphicsEffect(self.head_opacity)
        self.count_opacity = QGraphicsOpacityEffect(self.overlay_counter)
        self.overlay_counter.setGraphicsEffect(self.count_opacity)
        
        self.ui_timer = QTimer(self)
        self.ui_timer.timeout.connect(self._fade_out_controls)
        self.setMouseTracking(True)
        self.stack.setMouseTracking(True)
        self.installEventFilter(self)

        # System Variables
        self.slide_tick_timer = QTimer(self)
        self.slide_tick_timer.timeout.connect(self._slide_tick)
        self.slide_elapsed = 0
        self.rot_angle = 0
        self.flip_h = False
        self.flip_v = False
        self.slideshow_speed = 3000
        self.is_slideshow_active = False
        self.is_webpage_mode = False
        self.webpage_align = Qt.AlignHCenter

        # Global Shortcuts
        QShortcut(QKeySequence("F"), self, self._flip_img_horiz)
        QShortcut(QKeySequence("Shift+F"), self, self._flip_img_vert)
        QShortcut(QKeySequence("R"), self, self._rot_img_cw)
        QShortcut(QKeySequence("Shift+R"), self, self._rot_img_ccw)
        
        QShortcut(QKeySequence("G"), self, self._jump_to_file) 
        QShortcut(QKeySequence("F11"), self, self._toggle_fullscreen)
        QShortcut(QKeySequence("H"), self, self._toggle_ui_visibility)
        
        QShortcut(QKeySequence(Qt.Key_Right), self, self._next_item)
        QShortcut(QKeySequence(Qt.Key_Left), self, self._prev_item)
        QShortcut(QKeySequence(Qt.SHIFT | Qt.Key_Right), self, self._seek_forward)
        QShortcut(QKeySequence(Qt.SHIFT | Qt.Key_Left), self, self._seek_backward)
        
        QShortcut(QKeySequence(Qt.Key_Space), self, self._toggle_playback)
        QShortcut(QKeySequence(Qt.Key_Up), self, self._handle_up_arrow)
        QShortcut(QKeySequence(Qt.Key_Down), self, self._handle_down_arrow)
        
        # Apply Saved Settings from Previous Session
        if hasattr(self, 'saved_vol') and hasattr(self, 'audio'): 
            self.audio.setVolume(self.saved_vol)
            
        self._set_prog_mode(getattr(self, 'prog_mode', "SHOW"))
        self._set_count_mode(getattr(self, 'count_mode', "SHOW"))
        self._set_nav_mode(getattr(self, 'nav_mode', "AUTO"))
        self._set_deck_mode(getattr(self, 'deck_mode', "AUTO"))
        
        # Apply all quick tools visibility
        sqt = getattr(self, 'show_quick_tools', False)
        self.btn_cp_flip.setVisible(sqt)
        self.btn_cp_rot.setVisible(sqt)
        self.btn_cp_ab.setVisible(sqt)
        
        # Apply Universal Fit properly
        self.universal_fit = getattr(self, 'universal_fit', True)
        
        self.resizeEvent(None) # Force a geometry refresh so the UI adapts
        
        parent_win = self.parent() if self.parent() else self
        self.sc_hide = QShortcut(QKeySequence("B"), parent_win)
        self.sc_hide.setContext(Qt.ApplicationShortcut)
        self.sc_hide.activated.connect(self._toggle_hide)
        
        # FIX: Missing global mute shortcut explicitly linked to system cleanup
        self.sc_mute = QShortcut(QKeySequence("M"), parent_win)
        self.sc_mute.setContext(Qt.ApplicationShortcut)
        self.sc_mute.activated.connect(self._toggle_mute)

        # Context Menus Connections
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self.show_context_menu)
        self.stack.setContextMenuPolicy(Qt.CustomContextMenu)
        self.stack.customContextMenuRequested.connect(self.show_context_menu)
        self.txt_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.txt_view.customContextMenuRequested.connect(self.show_context_menu)
        self.csv_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.csv_view.customContextMenuRequested.connect(self.show_context_menu)
        self.vert_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.vert_view.customContextMenuRequested.connect(self.show_context_menu)
        self.md_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.md_view.customContextMenuRequested.connect(self.show_context_menu)
        self.pdf_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.pdf_view.customContextMenuRequested.connect(self.show_context_menu)

        # --- NEW: Macro Scroll Tracking ---
        self.macro_scroll_debouncer = QTimer(self)
        self.macro_scroll_debouncer.setSingleShot(True)
        self.macro_scroll_debouncer.timeout.connect(self._commit_scroll_macro)
        self.pending_scroll_state = None
        
        def track_scroll(widget):
            widget.horizontalScrollBar().valueChanged.connect(lambda v, w=widget: self._on_scroll_changed(w))
            widget.verticalScrollBar().valueChanged.connect(lambda v, w=widget: self._on_scroll_changed(w))

        track_scroll(self.vert_view)
        track_scroll(self.txt_view)
        track_scroll(self.csv_view)
        if HAS_PDF: 
            track_scroll(self.pdf_view)
            track_scroll(self.pdf_wrapper) # CRITICAL FIX: Track Magazine scrolling!

        # Initialize
        self._apply_filter("ALL", reset_index=False)
        self._is_initialized = True

    def _set_subtitle_color(self, hex_color):
        if hex_color == "CUSTOM":
            color = QColorDialog.getColor(QColor(getattr(self, 'subtitle_color', "#FFFFFF")), self, "Select Subtitle Color")
            if color.isValid():
                hex_color = color.name()
            else:
                return
                
        self.subtitle_color = hex_color
        self.lbl_subtitle.setStyleSheet(f"color: {self.subtitle_color}; background: transparent; padding: 0px;")
        self.save_settings()
        
    def _apply_pdf_zoom(self, multiplier):
        if not HAS_PDF: return
        self.macro_scroll_paused = True # Blindfold

        if getattr(self, 'is_magazine_mode', False):
            self.pdf_wrapper.scale(multiplier, multiplier)
            QTimer.singleShot(15, lambda: setattr(self, 'macro_scroll_paused', False))
        else:
            old_z = self.pdf_view.zoomFactor()
            if old_z <= 0: old_z = 1.0
            new_zoom = old_z * multiplier
            
            cx = self.pdf_view.viewport().width() / 2.0
            cy = self.pdf_view.viewport().height() / 2.0
            
            doc_x = (self.pdf_view.horizontalScrollBar().value() + cx) / old_z
            doc_y = (self.pdf_view.verticalScrollBar().value() + cy) / old_z
            
            self.pdf_view.setZoomMode(QPdfView.ZoomMode.Custom)
            self.pdf_view.setZoomFactor(new_zoom)
            
            # FIX: Force instant layout update to stop it jumping large first
            QApplication.processEvents()
            
            self.pdf_view.horizontalScrollBar().setValue(int(doc_x * new_zoom - cx))
            self.pdf_view.verticalScrollBar().setValue(int(doc_y * new_zoom - cy))
            
            self.macro_scroll_paused = False
            self._on_normal_zoom_changed(new_zoom)

    def _set_pdf_zoom_mode(self, mode):
        if not HAS_PDF: return
        if getattr(self, 'is_magazine_mode', False):
            self._fit_magazine_view() 
        else:
            self.pdf_wrapper.resetTransform()
            if getattr(self, 'btn_pdf_flip', None) and self.btn_pdf_flip.isChecked():
                trans = QTransform()
                trans.translate(self.pdf_wrapper.width(), 0)
                trans.scale(-1, 1)
                self.pdf_wrapper.setTransform(trans)
                
            # FIX: Force exact math calculation instead of buggy Qt layout mode
            if mode == QPdfView.ZoomMode.FitInView:
                self._fit_normal_pdf()
            else:
                self.pdf_view.setZoomMode(mode)
            self._show_toast("📄 Fit Width" if mode == QPdfView.ZoomMode.FitToWidth else "📄 Fit Page")

    def _toggle_pdf_mag(self, checked):
        if not HAS_PDF: return
        self.is_magazine_mode = checked
        if checked:
            self.is_webpage_mode = False # <-- FIX: Clear conflicting webpage state
            self.pdf_view.setPageMode(QPdfView.PageMode.SinglePage)
            self.pdf_view_right.setPageMode(QPdfView.PageMode.SinglePage)
            self.pdf_view.setAttribute(Qt.WA_TransparentForMouseEvents, True)
            self.pdf_view_right.setAttribute(Qt.WA_TransparentForMouseEvents, True)
            self.pdf_view.verticalScrollBar().setDisabled(True)
            self.pdf_view.horizontalScrollBar().setDisabled(True)
            self.pdf_view_right.verticalScrollBar().setDisabled(True)
            self.pdf_view_right.horizontalScrollBar().setDisabled(True)
            self.pdf_wrapper.setDragMode(QGraphicsView.ScrollHandDrag)
            self._sync_magazine_pages(self.pdf_view.pageNavigator().currentPage())
            QTimer.singleShot(50, self._fit_magazine_view)
        else:
            self.pdf_view.setPageMode(QPdfView.PageMode.MultiPage)
            self.pdf_view_right.hide()
            self.pdf_view.setAttribute(Qt.WA_TransparentForMouseEvents, False)
            self.pdf_view_right.setAttribute(Qt.WA_TransparentForMouseEvents, False)
            self.pdf_view.verticalScrollBar().setDisabled(False)
            self.pdf_view.horizontalScrollBar().setDisabled(False)
            self.pdf_view_right.verticalScrollBar().setDisabled(False)
            self.pdf_view_right.horizontalScrollBar().setDisabled(False)
            self.pdf_wrapper.setDragMode(QGraphicsView.NoDrag)
            self.pdf_view.setMinimumSize(0,0)
            self.pdf_view.setMaximumSize(16777215, 16777215)
            self.pdf_container.setMinimumSize(0,0)
            self.pdf_container.setMaximumSize(16777215, 16777215)
            self.pdf_wrapper.resetTransform()
            
            self.resizeEvent(None)
            QApplication.processEvents()
            self._fit_normal_pdf()
            
        if self.macro_recording and not self.macro_paused:
            self._flush_macro_state()
            if checked:
                layout = getattr(self, 'magazine_layout', 'COVER').lower()
                self._record_macro_action(f"mode=magazine_{layout}")
            else:
                self._record_macro_action("mode=norm")
            self._record_macro_action(f"{self.current_index + 1} pdf_mag={1 if checked else 0} dur=0")
            QTimer.singleShot(100, lambda: self._snapshot_macro_state(self.IDX_PDF))
            
        self._show_toast("📖 Magazine Mode" if checked else "📄 Normal View")
            
        if self.macro_recording and not self.macro_paused:
            # FLUSH pending actions before wiping memory!
            self._commit_scroll_macro()
            self._commit_view_change()
            
            # Wipe memory to prevent massive scroll glitches when switching modes
            self.last_macro_scroll_state = None
            self.pending_scroll_state = None
            self.last_macro_view_state = None
            self.pending_view_state = None
            
            if checked:
                layout = getattr(self, 'magazine_layout', 'COVER').lower()
                self._record_macro_action(f"mode=magazine_{layout}")
            else:
                self._record_macro_action("mode=norm")
            self._record_macro_action(f"{self.current_index + 1} pdf_mag={1 if checked else 0} dur=0")
            
        self._show_toast("📖 Magazine Mode" if checked else "📄 Normal View")
    
    def _fit_normal_pdf(self):
        if not HAS_PDF or self.pdf_doc.pageCount() == 0: return
        nav = self.pdf_view.pageNavigator()
        sz = self.pdf_doc.pagePointSize(nav.currentPage())
        
        vw = self.pdf_view.viewport().width()
        vh = self.pdf_view.viewport().height()
        
        if sz.width() > 0 and sz.height() > 0 and vw > 0 and vh > 0:
            # FIX: Calculate exact zoom and add a small padding to prevent scrollbars
            factor = min((vw - 20) / sz.width(), (vh - 20) / sz.height())
            
            self.pdf_view.setZoomMode(QPdfView.ZoomMode.Custom)
            self.pdf_view.setZoomFactor(factor)
            
            QApplication.processEvents()
            self.pdf_view.verticalScrollBar().setValue(0)
            self.pdf_view.horizontalScrollBar().setValue(0)
    
    def _flush_macro_state(self):
        """Wipes and commits all pending tracking data safely"""
        self._commit_scroll_macro()
        self._commit_view_change()
        self.last_macro_scroll_state = None
        self.pending_scroll_state = None
        self.last_macro_view_state = None
        self.pending_view_state = None

    def _get_t_str(self):
        """Helper to get the current transform state formatted for macros."""
        t_parts = []
        if getattr(self, 'flip_h', False): t_parts.append('hf')
        if getattr(self, 'flip_v', False): t_parts.append('vf')
        
        is_video = self.stack.currentIndex() == getattr(self, 'IDX_MEDIA', 3)
        rot = getattr(self, 'rot_v_angle', 0) if is_video else getattr(self, 'rot_angle', 0)
        
        if rot == 90: t_parts.append('c90')
        elif rot == 180: t_parts.append('c180')
        elif rot == 270: t_parts.append('c270')
        
        t_str = "".join(t_parts)
        return f"t={t_str} " if t_str else ""

    def _snapshot_macro_state(self, idx):
        """Grabs the exact mathematical starting coordinates for the current mode"""
        if not self.macro_recording or self.macro_paused: return
        t_str = self._get_t_str()
        
        if getattr(self, 'is_magazine_mode', False) and HAS_PDF:
            # FIX: Use absolute value to prevent negative zoom scale log
            z = abs(self.pdf_wrapper.transform().m11())
            c = self.pdf_wrapper.mapToScene(self.pdf_wrapper.viewport().rect().center())
            self._record_macro_action(f"{self.current_index + 1} {t_str}zs={z:.2f} zxs={int(c.x())} zys={int(c.y())} ufit=0 dur=0")
            self.last_macro_view_state = (z, c.x(), c.y())
            
        elif getattr(self, 'is_webpage_mode', False):
            z = self.vert_view.zoom_factor
            sx = self.vert_view.horizontalScrollBar().value()
            sy = self.vert_view.verticalScrollBar().value()
            self._record_macro_action(f"{self.current_index + 1} {t_str}z={z:.2f} sxs={sx} sys={sy} ufit=0 dur=0")
            self.last_macro_scroll_state = (sx, sy)
            
        elif idx == self.IDX_PDF and HAS_PDF:
            z = self.pdf_view.zoomFactor()
            self._record_macro_action(f"{self.current_index + 1} {t_str}z={z:.2f} ufit=0 dur=0")
            self.last_macro_scroll_state = (self.pdf_view.horizontalScrollBar().value(), self.pdf_view.verticalScrollBar().value())
            
        elif idx == self.IDX_IMG:
            z = self.img_view.transform().m11()
            c = self.img_view.mapToScene(self.img_view.viewport().rect().center())
            self._record_macro_action(f"{self.current_index + 1} {t_str}zs={z:.2f} zxs={int(c.x())} zys={int(c.y())} ufit=0 dur=0")
            self.last_macro_view_state = (z, c.x(), c.y())
    
    def _set_magazine_layout(self, layout):
        self.magazine_layout = layout
        self.save_settings()
        if getattr(self, 'is_magazine_mode', False):
            if self.macro_recording and not self.macro_paused:
                self._record_macro_action(f"mode=magazine_{layout.lower()} dur=0")
            curr = self.pdf_view.pageNavigator().currentPage()
            self._sync_magazine_pages(curr)
    
    def _fit_magazine_view(self):
        if not getattr(self, 'is_magazine_mode', False) or not HAS_PDF: return
        self._update_magazine_layout()
        self.pdf_wrapper.resetTransform()
        self.pdf_wrapper.fitInView(self.pdf_wrapper.sceneRect(), Qt.KeepAspectRatio)

    def _update_magazine_layout(self):
        """Locks the two pages securely into a single high-res texture block without ANY clipping"""
        if not getattr(self, 'is_magazine_mode', False) or not HAS_PDF: return
        
        # Use a massive base height. This ensures crisp text on zoom and stops Qt's DPI math bugs.
        base_h = 2400 
        
        nav_l = self.pdf_view.pageNavigator()
        sz_l = self.pdf_doc.pagePointSize(nav_l.currentPage())
        
        # Calculate width strictly by the PDF's native aspect ratio to prevent distortion/cropping
        ratio_l = sz_l.width() / sz_l.height() if sz_l.height() > 0 else 0.75
        w_l = math.ceil(base_h * ratio_l)
        
        self.pdf_view.setFixedSize(w_l, base_h)
        self.pdf_view.setZoomMode(QPdfView.ZoomMode.FitInView) # Safely guarantees 0 clipping
        
        w_r = 0
        if self.pdf_view_right.isVisible():
            nav_r = self.pdf_view_right.pageNavigator()
            sz_r = self.pdf_doc.pagePointSize(nav_r.currentPage())
            ratio_r = sz_r.width() / sz_r.height() if sz_r.height() > 0 else 0.75
            w_r = math.ceil(base_h * ratio_r)
            
            self.pdf_view_right.setFixedSize(w_r, base_h)
            self.pdf_view_right.setZoomMode(QPdfView.ZoomMode.FitInView)
            
        total_w = w_l + w_r
        
        self.pdf_container.setFixedSize(total_w, base_h)
        # Adds vertical margin to act as the "gap" you see when scrolling between spreads
        self.pdf_wrapper.setSceneRect(0, -80, total_w, base_h + 160)

    def _sync_magazine_pages(self, page):
        if getattr(self, 'is_magazine_mode', False) and HAS_PDF:
            nav_left = self.pdf_view.pageNavigator()
            nav_right = self.pdf_view_right.pageNavigator()
            layout_mode = getattr(self, 'magazine_layout', 'COVER')
            
            if layout_mode == "COVER":
                # MODE 1: Cover Page logic (1, 2-3, 4-5)
                if page == 0:
                    self.pdf_view_right.hide() 
                    if nav_left.currentPage() != 0: nav_left.jump(0, QPointF(0,0), 1.0)
                else:
                    self.pdf_view_right.show()
                    self.pdf_view.show()
                    
                    left_page = page if page % 2 != 0 else page - 1
                    right_page = left_page + 1
                    
                    if nav_left.currentPage() != left_page: nav_left.jump(left_page, QPointF(0,0), 1.0)
                    if right_page < self.pdf_doc.pageCount():
                        if nav_right.currentPage() != right_page: nav_right.jump(right_page, QPointF(0,0), 1.0)
                    else:
                        self.pdf_view_right.hide()
            else:
                # MODE 2: Double Page logic (1-2, 3-4, 5-6)
                self.pdf_view_right.show()
                self.pdf_view.show()
                
                left_page = page if page % 2 == 0 else page - 1
                right_page = left_page + 1
                
                if nav_left.currentPage() != left_page: nav_left.jump(left_page, QPointF(0,0), 1.0)
                if right_page < self.pdf_doc.pageCount():
                    if nav_right.currentPage() != right_page: nav_right.jump(right_page, QPointF(0,0), 1.0)
                else:
                    self.pdf_view_right.hide()
                    
            QTimer.singleShot(10, self._update_magazine_layout)

    def _navigate_spread(self, direction):
        # CRITICAL FIX: Never execute or record spread flips in normal PDF mode
        if not HAS_PDF or not getattr(self, 'is_magazine_mode', False): return
        
        if not hasattr(self, '_last_spread_jump'): self._last_spread_jump = 0
        if time.time() - self._last_spread_jump < 0.25: return
        self._last_spread_jump = time.time()
        
        if self.macro_recording and not self.macro_paused:
            self._commit_scroll_macro()
            self._record_macro_action(f"spread={direction} dur=0")
            
        curr = self.pdf_view.pageNavigator().currentPage()
        layout_mode = getattr(self, 'magazine_layout', 'COVER')
        
        if direction == 1: 
            if layout_mode == "COVER":
                next_page = 1 if curr == 0 else curr + 2
            else:
                next_page = curr + 2
                
            if next_page < self.pdf_doc.pageCount():
                self._sync_magazine_pages(next_page)
                self.pdf_wrapper.verticalScrollBar().setValue(0)
                
        elif direction == -1:
            if layout_mode == "COVER":
                next_page = 0 if curr in [1, 2] else max(0, curr - 2)
            else:
                next_page = max(0, curr - 2)
                
            self._sync_magazine_pages(next_page)
            self.pdf_wrapper.verticalScrollBar().setValue(self.pdf_wrapper.verticalScrollBar().maximum())
            
        if self.macro_recording and not self.macro_paused:
            self.last_macro_scroll_state = (
                self.pdf_wrapper.horizontalScrollBar().value(),
                self.pdf_wrapper.verticalScrollBar().value()
            )
            self.pending_scroll_state = None
            
    def _set_img_scale(self, fit_to_screen):
        self.universal_fit = fit_to_screen
        self.save_settings()
        self._apply_img_transform()
 
    def _toggle_pdf_flip(self, checked):
        if HAS_PDF and hasattr(self, 'pdf_wrapper'):
            is_mag = getattr(self, 'is_magazine_mode', False)
            
            # Save state ONLY if in magazine mode
            curr_z = abs(self.pdf_wrapper.transform().m11()) if is_mag else 1.0
            if curr_z == 0: curr_z = 1.0
            curr_c = self.pdf_wrapper.mapToScene(self.pdf_wrapper.viewport().rect().center()) if is_mag else None
            
            self.pdf_wrapper.resetTransform()
            
            if checked:
                trans = QTransform()
                trans.translate(self.pdf_wrapper.width(), 0)
                trans.scale(-1, 1)
                self.pdf_wrapper.setTransform(trans)
                
            if is_mag:
                self.pdf_wrapper.scale(curr_z, curr_z)
                self.pdf_wrapper.centerOn(curr_c)
                
            if getattr(self, 'macro_recording', False) and not getattr(self, 'macro_paused', False):
                self._commit_scroll_macro()
                self._commit_view_change()
                self._record_macro_action(f"{self.current_index + 1} pdf_flip={1 if checked else 0} dur=0")
                if is_mag:
                    new_c = self.pdf_wrapper.mapToScene(self.pdf_wrapper.viewport().rect().center())
                    self.last_macro_view_state = (curr_z, new_c.x(), new_c.y())
                self.pending_view_state = None
                
            self._show_toast("↔ PDF Flipped" if checked else "↔ PDF Normal")
            
    def _set_scroll_speed(self, speed):
        self.auto_scroll_speed = speed
        self._show_toast(f"⏬ Auto-Scroll Speed: {speed}x")
        
    def _handle_k_key(self):
        if getattr(self, 'is_slideshow_active', False):
            # Kill the playing script instantly
            self._toggle_slide(force=False)
            self.adv_idx = 0
            self._show_toast("🛑 Script Deactivated")
        else:
            # Otherwise, just act as the macro recorder pause button
            self._pause_macro()
            
    def _record_web_zoom(self, z):
        if self.macro_recording and not self.macro_paused:
            self._record_macro_action(f"{self.current_index + 1} z={z:.2f} ufit=0 dur=0")
            
    def _set_playback_rate(self, rate):
        if HAS_MULTIMEDIA:
            self.player.setPlaybackRate(rate)
            if self.macro_recording and not self.macro_paused:
                self._record_macro_action(f"{self.current_index + 1} rate={rate} dur=0")
            self._show_toast(f"⏩ Speed: {rate}x")

    def _show_toast(self, message):
        self.toast_osd.setText(message)
        self.toast_osd.show()
        self.toast_timer.start(2000)

    def _toggle_repeat(self, checked):
        if HAS_MULTIMEDIA:
            self.player.setLoops(QMediaPlayer.Infinite if checked else 1)
            self.btn_cp_repeat.setStyleSheet("color: #3b82f6;" if checked else "color: #ccc;")
            self._show_toast("🔁 Repeat: ON" if checked else "🔁 Repeat: OFF")
            
            # FIX: Mutual exclusivity - turn off shuffle if repeat is turned on
            if checked and self.btn_cp_shuffle.isChecked():
                self.btn_cp_shuffle.setChecked(False)
                
    def _update_deck_buttons(self):
        idx = self.stack.currentIndex()
        is_media = (idx == self.IDX_MEDIA)
        is_img_or_web = (idx in [self.IDX_IMG, self.IDX_WEB])
        is_doc = (idx in [self.IDX_TXT, self.IDX_CSV, self.IDX_MD, self.IDX_PDF, self.IDX_WEB])
        sqt = getattr(self, 'show_quick_tools', False)
        
        self.btn_cp_flip.setVisible(sqt and is_img_or_web)
        self.btn_cp_rot.setVisible(sqt and is_img_or_web)
        self.btn_cp_ab.setVisible(sqt and (is_media or is_doc))
        self.btn_cp_repeat.setVisible(is_media)
        self.btn_cp_shuffle.setVisible(sqt and is_media)
        
        # Zoom now explicitly supports BOTH Images and Webpages!
        is_zoomable = (idx in [self.IDX_IMG, self.IDX_WEB])
        self.btn_cp_z_in.setVisible(is_zoomable)
        self.btn_cp_z_out.setVisible(is_zoomable)

    def _apply_general_zoom(self, multiplier):
        """Universal handler for the UI zoom buttons across Image and Webpage modes"""
        if self.stack.currentIndex() == self.IDX_IMG:
            self.img_view.scale(multiplier, multiplier)
            self.img_view._emit_change() # Emits to macro recorder
            
        elif self.stack.currentIndex() == self.IDX_WEB:
            old_z = self.vert_view.zoom_factor
            new_z = max(0.1, min(5.0, old_z * multiplier))
            
            # Anchor to the center of the UI window
            pos = QPointF(self.vert_view.viewport().width() / 2.0, self.vert_view.viewport().height() / 2.0)
            h_bar = self.vert_view.horizontalScrollBar()
            v_bar = self.vert_view.verticalScrollBar()
            
            doc_x = (h_bar.value() + pos.x()) / old_z
            doc_y = (v_bar.value() + pos.y()) / old_z
            
            self.macro_scroll_paused = True
            self.vert_view.zoom_factor = new_z
            for lbl in self.vert_view.labels:
                lbl.apply_zoom(new_z)
                
            def apply_ui_scroll():
                h_bar.setValue(int(doc_x * new_z - pos.x()))
                v_bar.setValue(int(doc_y * new_z - pos.y()))
                self.macro_scroll_paused = False
                self.vert_view.zoomChanged.emit(new_z) # Safely records macro after settling
                
            QTimer.singleShot(25, apply_ui_scroll)
        
        
    def _toggle_shuffle(self, checked):
        if not self.active_playlist: return
        
        current_item = self.active_playlist[self.current_index]
        
        if checked:
            # Save original order
            self.unshuffled_playlist = self.active_playlist.copy()
            
            # FIX: Better shuffle logic - put current item first, shuffle the rest!
            # This guarantees the "next" button plays a truly random song.
            rem_list = self.active_playlist.copy()
            rem_list.pop(self.current_index)
            random.shuffle(rem_list)
            
            self.active_playlist = [current_item] + rem_list
            self.current_index = 0
            
            self.btn_cp_shuffle.setStyleSheet("color: #3b82f6;")
            self._show_toast("🔀 Shuffle: ON")
            
            # FIX: Mutual exclusivity - turn off repeat if shuffle is turned on
            if getattr(self, 'btn_cp_repeat', None) and self.btn_cp_repeat.isChecked():
                self.btn_cp_repeat.setChecked(False)
        else:
            # Restore original order
            if hasattr(self, 'unshuffled_playlist'):
                self.active_playlist = self.unshuffled_playlist.copy()
                try:
                    self.current_index = self.active_playlist.index(current_item)
                except ValueError:
                    self.current_index = 0
            self.btn_cp_shuffle.setStyleSheet("color: #ccc;")
            self._show_toast("🔀 Shuffle: OFF")
            
        self._restore_title()
        if getattr(self, 'is_webpage_mode', False):
            self.vert_view.load_playlist(self.active_playlist)
            self.vert_view.scroll_to_index(self.current_index)

    def _on_media_status_changed(self, status):
        # FIX: Automatically play the next media file when current finishes!
        if status == QMediaPlayer.MediaStatus.EndOfMedia:
            if self.player.loops() == 1: # Only proceed if Repeat is OFF
                if self.current_index < len(self.active_playlist) - 1:
                    self._next_item()
                else:
                    self._show_toast("End of Playlist")
    
    def load_settings(self):
        if os.path.exists(self.settings_file):
            try:
                with open(self.settings_file, "r") as f:
                    d = json.load(f)
                    self.subtitle_color = d.get("subtitle_color", "#FFFFFF")
                    self.vis_mode = d.get("vis_mode", "CIRCULAR_STRING")
                    self.prog_mode = d.get("prog_mode", "SHOW")
                    self.count_mode = d.get("count_mode", "SHOW")
                    self.nav_mode = d.get("nav_mode", "AUTO")
                    self.deck_mode = d.get("deck_mode", "AUTO")
                    self.show_quick_tools = d.get("show_quick_tools", False)
                    self.universal_fit = d.get("universal_fit", True)
                    self.magazine_layout = d.get("magazine_layout", "COVER") # NEW: Load Layout
                    
                    # Store volume temporarily until the audio engine initializes
                    self.saved_vol = d.get("volume", 0.5) 
            except: pass

    def save_settings(self):
        # Prevent wiping settings during app startup
        if not getattr(self, '_is_initialized', False): return
        
        try:
            v = self.audio.volume() if hasattr(self, 'audio') and self.audio is not None else getattr(self, 'saved_vol', 0.5)
            with open(self.settings_file, "w") as f:
                json.dump({
                    "subtitle_color": getattr(self, 'subtitle_color', "#FFFFFF"),
                    "vis_mode": getattr(self, 'vis_mode', "CIRCULAR_STRING"), 
                    "prog_mode": getattr(self, 'prog_mode', 'SHOW'),
                    "count_mode": getattr(self, 'count_mode', 'SHOW'), 
                    "nav_mode": getattr(self, 'nav_mode', 'AUTO'),
                    "deck_mode": getattr(self, 'deck_mode', 'AUTO'), 
                    "show_quick_tools": getattr(self, 'show_quick_tools', False),
                    "universal_fit": getattr(self, 'universal_fit', True), 
                    "magazine_layout": getattr(self, 'magazine_layout', 'COVER'), # NEW: Save Layout
                    "volume": v
                }, f)
        except Exception as e:
            print(f"Failed to save settings: {e}")
        
    def _load_text_colors(self):
        if os.path.exists("text_colors.json"):
            try:
                with open("text_colors.json", "r") as f:
                    self.text_color_data = json.load(f)
            except: pass

    # --- Overlay Geometry Engine ---
    def resizeEvent(self, event):
        if event: super().resizeEvent(event)
        w, h = self.width(), self.height()
        
        # Auto-fit media when window dynamically resizes
        if getattr(self, 'universal_fit', True):
            if self.stack.currentIndex() == self.IDX_IMG and hasattr(self, 'img_view'):
                self.img_view.fitInView(self.img_view.sceneRect(), Qt.KeepAspectRatio)
            elif HAS_MULTIMEDIA and self.stack.currentIndex() == self.IDX_MEDIA and hasattr(self, 'video_item'):
                self.video_view.fitInView(self.video_item.boundingRect(), Qt.KeepAspectRatio)
        
        if self.stack.currentIndex() == self.IDX_PDF and HAS_PDF:
            if getattr(self, 'is_magazine_mode', False):
                QTimer.singleShot(10, self._update_magazine_layout)
            else:
                self.pdf_wrapper.resetTransform()
                self.pdf_wrapper.setSceneRect(0, 0, w, h)
                self.pdf_container.setFixedSize(w, h)
                if getattr(self, 'btn_pdf_flip', None) and self.btn_pdf_flip.isChecked():
                    trans = QTransform()
                    trans.translate(w, 0)
                    trans.scale(-1, 1)
                    self.pdf_wrapper.setTransform(trans)
                
                if self.pdf_view.zoomMode() == QPdfView.ZoomMode.FitInView:
                    self.pdf_view.setZoomMode(QPdfView.ZoomMode.FitInView)
                
        self.slide_progress.setGeometry(0, 0, w, 2)
        self.header_widget.setGeometry(0, 2, w, 40)
        self.overlay_counter.setGeometry(w - 110, 10, 100, 26)
        
        cw = int(w * 0.75) if self.stack.currentIndex() == self.IDX_MEDIA else (450 if self.show_quick_tools else 300)
        self.control_panel.setGeometry((w - cw) // 2, h - 50 - 25, cw, 50)
        
        pw = 550
        self.pdf_control_panel.setGeometry((w - pw) // 2, h - 50 - 25, pw, 50)
        
        self.btn_float_prev.setGeometry(20, (h - 180) // 2, 50, 180)
        self.btn_float_next.setGeometry(w - 70, (h - 180) // 2, 50, 180)
        
        self.vol_osd.setGeometry((w - 220) // 2, h - 140, 220, 40)
        self.lbl_metadata.setGeometry(50, h - 250, w - 100, 80)
        self.macro_osd.setGeometry((w - 550) // 2, 60, 550, 50)
        
        self.brightness_overlay.setGeometry(0, 0, w, h)
        # Expand height to 200px so multi-line text doesn't get cut off
        self.lbl_subtitle.setGeometry(40, h - 250, w - 80, 200)
        
        self._restore_title()
        if hasattr(self, 'toast_osd'):
            self.toast_osd.setGeometry((w - 300) // 2, h - 120, 300, 40)
        

    # --- Auto-Hide UI Logic ---
    def eventFilter(self, obj, event):
        if event.type() == QEvent.MouseMove: self._show_controls()
        return super().eventFilter(obj, event)

    def _show_controls(self):
        if self.ui_locked_hidden: return 
        
        self.cp_opacity.setOpacity(1.0)
        self.pdf_opacity.setOpacity(1.0)
        self.head_opacity.setOpacity(1.0)
        self.count_opacity.setOpacity(1.0)
        
        if self.deck_mode in ["AUTO", "SHOW"]:
            if self.stack.currentIndex() == self.IDX_PDF:
                self.pdf_control_panel.show()
                self.control_panel.hide()
            else:
                self.control_panel.show()
                self.pdf_control_panel.hide()
                
        self.header_widget.show()
        
        if self.count_mode in ["AUTO", "SHOW"]: self.overlay_counter.show()
        
        if self.nav_mode in ["AUTO", "SHOW"]:
            self.btn_float_prev.show()
            self.btn_float_next.show()
            
        self.ui_timer.start(2500)

    def _fade_out_controls(self):
        if self.ui_locked_hidden: return
        
        if getattr(self, 'deck_mode', 'AUTO') in ["AUTO", "HIDE"]: 
            self.control_panel.hide()
            self.pdf_control_panel.hide()
            
        if self.isFullScreen(): self.header_widget.hide()
        
        if getattr(self, 'count_mode', 'AUTO') in ["AUTO", "HIDE"]: 
            self.overlay_counter.hide()
            
        if getattr(self, 'nav_mode', 'AUTO') in ["AUTO", "HIDE"]:
            self.btn_float_prev.hide()
            self.btn_float_next.hide()

    def _toggle_ui_visibility(self):
        self.ui_locked_hidden = not self.ui_locked_hidden
        if self.ui_locked_hidden:
            self.header_widget.hide(); self.control_panel.hide(); self.pdf_control_panel.hide()
            self.btn_float_prev.hide(); self.btn_float_next.hide()
            if self.count_mode == "AUTO": self.overlay_counter.hide()
            self.ui_timer.stop()
        else:
            self._show_controls()

    def _restore_title(self):
        if not self.active_playlist: 
            self.lbl_title.setText("No Media")
            self.overlay_counter.setText("")
            return
        item = self.active_playlist[self.current_index]
        if self.current_filter == "PRE-DECIDED":
            self.overlay_counter.setText(f"List: {self.current_index + 1}/{len(self.active_playlist)}")
        else:
            self.overlay_counter.setText(f"{self.current_index + 1}/{len(self.active_playlist)}")
            
        fm = QFontMetrics(self.lbl_title.font())
        elided = fm.elidedText(item['name'], Qt.ElideMiddle, max(150, self.header_widget.width() - 100))
        self.lbl_title.setText(elided)

    def _update_metadata(self):
        try:
            if self.stack.currentIndex() == self.IDX_MEDIA and self.active_playlist[self.current_index]['ext'].lower() in ['.mp3', '.wav', '.ogg', '.flac']:
                md = self.player.metaData()
                title = md.stringValue(QMediaMetaData.Title) if md.stringValue(QMediaMetaData.Title) else "Unknown Title"
                artist = md.stringValue(QMediaMetaData.ContributingArtist) if md.stringValue(QMediaMetaData.ContributingArtist) else "Unknown Artist"
                self.lbl_metadata.setText(f"🎵 {title}\n👤 {artist}")
                self.lbl_metadata.show()
            else: self.lbl_metadata.hide()
        except: pass

    def _toggle_auto_scroll(self, checked):
        if checked: self.auto_scroll_timer.start(16)
        else: self.auto_scroll_timer.stop()
        
    def _do_auto_scroll(self):
        speed = getattr(self, 'auto_scroll_speed', 1)
        if self.stack.currentIndex() == self.IDX_PDF and HAS_PDF:
            bar = self.pdf_view.verticalScrollBar()
            old_val = bar.value()
            bar.setValue(bar.value() + speed)
            
            # Sync right view's scrollbar if in magazine mode
            if getattr(self, 'is_magazine_mode', False):
                bar_right = self.pdf_view_right.verticalScrollBar()
                bar_right.setValue(bar.value())
                
            if bar.value() >= bar.maximum() and old_val == bar.maximum():
                if getattr(self, 'is_magazine_mode', False):
                    nav = self.pdf_view.pageNavigator()
                    # Turn to the next 2 pages if we hit bottom
                    if nav.currentPage() + 2 < self.pdf_doc.pageCount():
                        nav.jump(nav.currentPage() + 2, QPointF(0,0), self.pdf_view.zoomFactor())
                        bar.setValue(0)
                        self.pdf_view_right.verticalScrollBar().setValue(0)
                    else:
                        self.btn_pdf_scroll.setChecked(False)
                else:
                    self.btn_pdf_scroll.setChecked(False)
                    
        elif self.stack.currentIndex() == self.IDX_WEB:
            bar = self.vert_view.verticalScrollBar()
            bar.setValue(bar.value() + speed)
            if bar.value() >= bar.maximum():
                self.btn_pdf_scroll.setChecked(False)

    # --- Feature: Macro Recorder Hooks ---
    def _on_view_changed(self, z, zx, zy):
        """Strictly handles Images and Magazine Mode dragging/zooming"""
        if not getattr(self, 'macro_recording', False) or getattr(self, 'macro_paused', False): return
        if self.stack.currentIndex() == self.IDX_PDF and not getattr(self, 'is_magazine_mode', False): return
            
        # FIX: Always record absolute zoom to prevent negative scale flipping on playback
        self.pending_view_state = (abs(z), zx, zy)
        self.macro_debouncer.start(150)
    
    def _on_normal_zoom_changed(self, z_factor):
        if not getattr(self, 'macro_recording', False) or getattr(self, 'macro_paused', False): return
        if getattr(self, 'is_magazine_mode', False): return
        self._commit_scroll_macro()
        t_str = self._get_t_str()
        self._record_macro_action(f"{self.current_index + 1} {t_str}z={z_factor:.2f} ufit=0 dur=0")
        if hasattr(self, 'pdf_view'):
            self.last_macro_scroll_state = (self.pdf_view.horizontalScrollBar().value(), self.pdf_view.verticalScrollBar().value())
            self.pending_scroll_state = None

    def _record_zoom_event(self, z):
        if self.macro_recording and not self.macro_paused:
            self._commit_scroll_macro() 
            w = self.stack.currentWidget()
            t_str = self._get_t_str()
            if hasattr(w, 'horizontalScrollBar') and self.stack.currentIndex() == self.IDX_WEB:
                sx = w.horizontalScrollBar().value()
                sy = w.verticalScrollBar().value()
                self._record_macro_action(f"{self.current_index + 1} {t_str}z={z:.2f} sxs={sx} sys={sy} ufit=0 dur=0")
                self.last_macro_scroll_state = (sx, sy)
                self.pending_scroll_state = None
            else:
                self._record_macro_action(f"{self.current_index + 1} {t_str}z={z:.2f} ufit=0 dur=0")
        
    def _on_scroll_changed(self, widget):
        if not getattr(self, 'macro_recording', False) or getattr(self, 'macro_paused', False): return
        if getattr(self, 'macro_scroll_paused', False): return
        
        is_mag = getattr(self, 'is_magazine_mode', False)
        if is_mag and widget == getattr(self, 'pdf_view', None): return
        if widget == getattr(self, 'pdf_wrapper', None): return 
            
        self.pending_scroll_state = (widget.horizontalScrollBar().value(), widget.verticalScrollBar().value())
        self.macro_scroll_debouncer.start(150)

    def _commit_scroll_macro(self):
        if not self.pending_scroll_state: return
        sx, sy = self.pending_scroll_state
        t_str = self._get_t_str()
        
        if not self.last_macro_scroll_state:
            self.last_macro_scroll_state = (sx, sy)
            self._record_macro_action(f"{self.current_index + 1} {t_str}sxs={int(sx)} sys={int(sy)} ufit=0 dur=0")
            self.pending_scroll_state = None
            return

        last_sx, last_sy = self.last_macro_scroll_state
        dx = sx - last_sx
        dy = sy - last_sy
        
        parts = []
        if dx > 0: parts.append(f"scrollr={int(dx)}px")
        elif dx < 0: parts.append(f"scrolll={int(-dx)}px")
        if dy > 0: parts.append(f"scrolld={int(dy)}px")
        elif dy < 0: parts.append(f"scrollu={int(-dy)}px")
        
        if parts:
            self._record_macro_action(f"{self.current_index + 1} {t_str}{' '.join(parts)} ufit=0 dur=150")
            
        self.last_macro_scroll_state = (sx, sy)
        self.pending_scroll_state = None

    def _show_file_properties(self):
        if not self.active_playlist: return
        item = self.active_playlist[self.current_index]
        path = item['path']
        try:
            stat = os.stat(path)
            size_mb = stat.st_size / (1024 * 1024)
            ctime = datetime.fromtimestamp(stat.st_ctime).strftime('%Y-%m-%d %H:%M:%S')
            ext = item.get('ext', '').lower()
            
            info = f"<b>File Name:</b> {os.path.basename(path)}<br><br>"
            info += f"<b>Location:</b> {path}<br>"
            info += f"<b>Size:</b> {size_mb:.2f} MB ({stat.st_size:,} bytes)<br>"
            info += f"<b>Format:</b> {ext.upper()}<br>"
            info += f"<b>Date Created:</b> {ctime}<br>"
            
            if ext in ['.png', '.jpg', '.jpeg', '.bmp', '.webp']:
                img = QImage(path)
                info += f"<b>Resolution:</b> {img.width()} x {img.height()} px<br>"
                
            QMessageBox.information(self, "File Properties", info)
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Could not read properties: {e}")    
        
    def _on_seek_recorded(self):
        if not self.macro_recording or self.macro_paused: return
        pos = self.player.position() / 1000.0
        self._record_macro_action(f"{self.current_index + 1} start={pos:.2f} dur=0")

    def _commit_view_change(self):
        if not self.pending_view_state: return
        z, zx, zy = self.pending_view_state
        t_str = self._get_t_str()
        
        if not self.last_macro_view_state:
            self.last_macro_view_state = (z, zx, zy)
            # CRITICAL: Append ufit=0 so the engine stops trying to auto-fit!
            self._record_macro_action(f"{self.current_index + 1} {t_str}zs={z:.2f} zxs={int(zx)} zys={int(zy)} ufit=0 dur=0")
            self.pending_view_state = None
            return

        last_z, last_zx, last_zy = self.last_macro_view_state
        
        # If zoom changed significantly, record Absolute values
        if abs(z - last_z) > 0.01:
            action = f"{self.current_index + 1} {t_str}zs={z:.2f} zxs={int(zx)} zys={int(zy)} ufit=0 dur=150"
        else:
            # CRITICAL FIX: Convert Scene Coordinates to TRUE Physical Viewport Pixels
            dx_px = (zx - last_zx) * last_z
            dy_px = (zy - last_zy) * last_z
            
            parts = []
            if dx_px > 0: parts.append(f"dragr={int(dx_px)}px")
            elif dx_px < 0: parts.append(f"dragl={int(-dx_px)}px")
            if dy_px > 0: parts.append(f"dragd={int(dy_px)}px")
            elif dy_px < 0: parts.append(f"dragu={int(-dy_px)}px")
            
            if not parts: 
                self.pending_view_state = None
                return
            action = f"{self.current_index + 1} {t_str}{' '.join(parts)} ufit=0 dur=150"

        self._record_macro_action(action)
        self.last_macro_view_state = (z, zx, zy)
        self.pending_view_state = None

    def _record_macro_action(self, action_str):
        if self.macro_recording and not self.macro_paused:
            now = time.time()
            dt = int((now - self.macro_last_time) * 1000)
            if dt > 50: self.macro_events.append(f"wait {dt}")
            self.macro_events.append(action_str)
            self.macro_last_time = now
            self.lbl_macro_log.setText(f"Logged: {action_str}") # Live UI Feedback

    def _record_transform_macro(self):
        if not getattr(self, 'macro_recording', False) or getattr(self, 'macro_paused', False): return
        t_parts = []
        if getattr(self, 'flip_h', False): t_parts.append('hf')
        if getattr(self, 'flip_v', False): t_parts.append('vf')
        
        # FIX: Ensure we use the correct rotation variable for Webpage Mode
        is_video = self.stack.currentIndex() == self.IDX_MEDIA
        rot = getattr(self, 'rot_v_angle', 0) if is_video else getattr(self, 'rot_angle', 0)
        
        if rot == 90: t_parts.append('c90')
        elif rot == 180: t_parts.append('c180')
        elif rot == 270: t_parts.append('c270')
        
        t_str = "".join(t_parts)
        if t_str: self._record_macro_action(f"{self.current_index + 1} t={t_str} dur=0")
        else: self._record_macro_action(f"{self.current_index + 1} t=none dur=0")
        
    def _start_macro(self):
        self.macro_recording = True
        self.macro_paused = False
        self.macro_events = [f"# Recorded Macro - {datetime.now().strftime('%H:%M:%S')}"]
        
        idx = self.stack.currentIndex()
        if idx == self.IDX_WEB and getattr(self, 'is_webpage_mode', False):
            self.macro_events.append("mode=web")
        elif idx == self.IDX_PDF and getattr(self, 'is_magazine_mode', False):
            layout = getattr(self, 'magazine_layout', 'COVER').lower()
            self.macro_events.append(f"mode=magazine_{layout}")
        elif idx == self.IDX_PDF: 
            self.macro_events.append("mode=pdf")
        else: 
            self.macro_events.append("mode=norm")
            
        self.macro_last_time = time.time()
        self._flush_macro_state()
        
        # FIX: Delay snapshot by 200ms so PDF engines finish "Fit In View" calculations natively!
        QTimer.singleShot(200, lambda: self._snapshot_macro_state(idx)) 
        
        # Visually lock in the Recording UI State
        self.btn_m_rec.setText("🔴 Recording...")
        self.btn_m_rec.setStyleSheet("color: #22c55e; font-weight: bold;") 
        self.btn_m_pause.setText("⏸ Pause [P]")
        self.lbl_macro_log.setText("Recording started... (P: Pause | K: Kill)")
        QApplication.processEvents() 
        
        if HAS_MULTIMEDIA and idx == self.IDX_MEDIA:
            pos = self.player.position() / 1000.0
            self._record_macro_action(f"{self.current_index + 1} start={pos:.2f} rate={self.player.playbackRate():.2f} dur=0")
            
    def _pause_macro(self):
        if not getattr(self, 'macro_recording', False): return
        self.macro_paused = not self.macro_paused
        if self.macro_paused: 
            # FIX: Show Resume state when paused
            self.btn_m_pause.setText("▶ Resume [P]")
            self.btn_m_rec.setText("⏸ Paused")
            self.btn_m_rec.setStyleSheet("color: #eab308; font-weight: bold;") # Yellow warning
            self.lbl_macro_log.setText("Recording PAUSED [P to Resume].")
        else:
            # Revert back to active recording state
            self.btn_m_pause.setText("⏸ Pause [P]")
            self.btn_m_rec.setText("🔴 Recording...")
            self.btn_m_rec.setStyleSheet("color: #22c55e; font-weight: bold;")
            self.macro_last_time = time.time()
            self.lbl_macro_log.setText("Recording resumed...")

    def _stop_macro(self):
        self.macro_recording = False
        self.macro_osd.hide()
        # FIX: Reset original UI styling
        self.btn_m_rec.setText("🔴 Rec")
        self.btn_m_rec.setStyleSheet("color: #ef4444; font-weight: bold;") 
        self.lbl_macro_log.setText("Ready...")
        
        if self.macro_events and self.macro_events[-1] != "stop":
            self.macro_events.append("stop")
        
        self.adv_loop_raw = "\n".join(self.macro_events)
        self._open_adv_loop()
        
    def _smooth_anim_tick(self):
        t = self.smooth_target
        if t.get('steps_left', 0) <= 0:
            self.smooth_anim_timer.stop()
            return
            
        t['steps_left'] -= 1
        total = t.get('total_steps', 1)
        
        # Physics: Cubic Ease-Out (Glides naturally to a stop)
        progress = 1.0 - (t['steps_left'] / float(total))
        ease = 1.0 - (1.0 - progress) ** 3  
        
        if 'view' in t:
            curr_z = t['start_z'] + (t['end_z'] - t['start_z']) * ease
            curr_zx = t['start_zx'] + (t['end_zx'] - t['start_zx']) * ease
            curr_zy = t['start_zy'] + (t['end_zy'] - t['start_zy']) * ease
            
            view = t['view']
            view.resetTransform()
            
            # FIX: Re-apply PDF Magazine horizontal flip base transform during smooth animation
            if view == getattr(self, 'pdf_wrapper', None) and getattr(self, 'btn_pdf_flip', None) and self.btn_pdf_flip.isChecked():
                trans = QTransform()
                trans.translate(view.width(), 0)
                trans.scale(-1, 1)
                view.setTransform(trans)
                
            view.scale(curr_z, curr_z)
            view.centerOn(curr_zx, curr_zy)
            
        if 'scroll_widget' in t:
            curr_sx = t['start_sx'] + (t['end_sx'] - t['start_sx']) * ease
            curr_sy = t['start_sy'] + (t['end_sy'] - t['start_sy']) * ease
            sw = t['scroll_widget']
            if t['end_sx'] >= 0: sw.horizontalScrollBar().setValue(int(curr_sx))
            if t['end_sy'] >= 0: sw.verticalScrollBar().setValue(int(curr_sy))
            
    def _jump_to_file(self):
        val, ok = QInputDialog.getInt(self, "Jump to File Number", f"Enter index (1-{len(self.active_playlist)}):", self.current_index + 1, 1, len(self.active_playlist), 1)
        if ok:
            self.current_index = val - 1
            self._record_macro_action(f"{self.current_index + 1} dur=0")
            self._load_current_item()
            self._show_controls()

    def _parse_advanced_sequence(self, seq_str, master_playlist):
        max_size = float('inf'); min_size = 0; min_chars = 0; max_chars = float('inf')
        includes_filename = []; excludes_filename = []
        
        category_exts = {
            'image': ['png', 'jpg', 'jpeg', 'bmp', 'gif', 'webp'], 'images': ['png', 'jpg', 'jpeg', 'bmp', 'gif', 'webp'],
            'video': ['mp4', 'avi', 'mkv', 'mov', 'webm'], 'videos': ['mp4', 'avi', 'mkv', 'mov', 'webm'],
            'audio': ['mp3', 'wav', 'ogg', 'flac'], 'audios': ['mp3', 'wav', 'ogg', 'flac'],
            'doc': ['txt', 'csv', 'json', 'xml', 'py', 'md', 'log', 'ini', 'sh', 'cpp', 'c', 'h', 'pdf'], 'docs': ['txt', 'csv', 'json', 'xml', 'py', 'md', 'log', 'ini', 'sh', 'cpp', 'c', 'h', 'pdf'],
            'code': ['json', 'xml', 'py', 'sh', 'cpp', 'c', 'h', 'js', 'css', 'html'], 'codes': ['json', 'xml', 'py', 'sh', 'cpp', 'c', 'h', 'js', 'css', 'html']
        }
        
        def parse_size(val):
            val = val.lower()
            if val.endswith('mb'): return float(val[:-2]) * 1024 * 1024
            if val.endswith('kb'): return float(val[:-2]) * 1024
            if val.endswith('b'): return float(val[:-1])
            return float(val)

        clean_tokens = []
        for chunk in seq_str.split(','):
            chunk = chunk.strip()
            if not chunk: continue
            if chunk.lower().startswith('includefilename') or chunk.lower().startswith('excludefilename'):
                clean_tokens.append(chunk.lower())
            else:
                clean_tokens.extend(chunk.lower().split())
        
        includes_ext = []; excludes_ext = []; command_tokens = []
        current_frames = []; frame_transforms = {}
        
        for t in clean_tokens:
            if t in category_exts: includes_ext.extend(category_exts[t])
            elif t.startswith("max:"):
                try: max_size = parse_size(t.split(":", 1)[1])
                except: pass
            elif t.startswith("min:"):
                try: min_size = parse_size(t.split(":", 1)[1])
                except: pass
            elif t.startswith("minic="):
                try: min_chars = int(t.split("=", 1)[1])
                except: pass
            elif t.startswith("maxc="):
                try: max_chars = int(t.split("=", 1)[1])
                except: pass
            elif t.startswith("includefilename="): includes_filename.append(t.split("=", 1)[1])
            elif t.startswith("excludefilename="): excludes_filename.append(t.split("=", 1)[1])
            elif t in ['sn', 'shuffle'] or t.startswith('repeat=') or t.startswith('loop='): command_tokens.append(t)
            elif t.startswith('-'): excludes_ext.append(t[1:])
            elif re.match(r'^(\d+-\d+|\d+)(:.*)?$', t):
                parts = t.split(':')
                frame_part = parts[0]; t_part = "".join(parts[1:])
                new_frames = []
                if '-' in frame_part:
                    s_f, e_f = map(int, frame_part.split('-'))
                    step = 1 if s_f <= e_f else -1
                    new_frames.extend(range(s_f - 1, e_f - 1 + step, step))
                else: new_frames.append(int(frame_part) - 1)
                
                current_frames = [f for f in new_frames if 0 <= f < len(master_playlist)]
                for f in current_frames: frame_transforms[f] = frame_transforms.get(f, "") + t_part
            elif t in ['c90', 'c180', 'c270', 'cc90', 'cc270', 'hf', 'vf']:
                targets = current_frames if current_frames else range(len(master_playlist))
                for f in targets: frame_transforms[f] = frame_transforms.get(f, "") + t
            else: includes_ext.append(t)
                
        has_explicit_frames = any(re.match(r'^(\d+-\d+|\d+)(:.*)?$', x) for x in clean_tokens)
        indices_to_check = list(frame_transforms.keys()) if has_explicit_frames else list(range(len(master_playlist)))
        
        final_items = []
        for i in indices_to_check:
            if i >= len(master_playlist): continue
            item = master_playlist[i].copy()
            path = item['path']; ext = item.get('ext', '').lower().replace('.', '') 
            
            if ext in excludes_ext: continue
            if includes_ext and ext not in includes_ext: continue
            
            try:
                f_size = os.path.getsize(path)
                if f_size < min_size or f_size > max_size: continue
            except: pass
                
            fname_only = os.path.splitext(os.path.basename(path))[0].lower()
            if len(fname_only) < min_chars or len(fname_only) > max_chars: continue
            if excludes_filename and any(ex in fname_only for ex in excludes_filename): continue
            if includes_filename and not any(inc in fname_only for inc in includes_filename): continue
            
            if i in frame_transforms: item['t'] = frame_transforms[i]
            final_items.append(item)
            
        def natural_keys(it):
            text = os.path.basename(it['path']).lower()
            chunks = re.split(r'(\d+)', text)
            key = []
            for chunk in chunks:
                if not chunk: continue
                if chunk.isdigit(): key.append((1, int(chunk)))
                else: key.append((0, chunk))
            return key

        if 'sn' in command_tokens: final_items.sort(key=natural_keys)
        if 'shuffle' in command_tokens: random.shuffle(final_items)
        for t in command_tokens:
            if t.startswith("repeat=") or t.startswith("loop="):
                try: 
                    count = int(t.split("=")[1])
                    final_items = [item.copy() for _ in range(count) for item in final_items]
                except: pass
                
        return final_items

    def _open_pre_decided(self):
        dlg = PreDecidedDialog(self)
        if dlg.exec():
            if dlg.result_action == "ACTIVATE":
                items = self._parse_advanced_sequence(dlg.result_seq, self.master_playlist)
                if not items:
                    QMessageBox.warning(self, "Error", "No media matches your filter sequence.")
                    return
                
                # FIX: Maintain Shuffle state for Pre-Decided lists
                self.unshuffled_playlist = items.copy()
                if getattr(self, 'btn_cp_shuffle', None) and self.btn_cp_shuffle.isChecked():
                    random.shuffle(items)
                    
                self.active_playlist = items
                self.current_filter = "PRE-DECIDED"
                self.current_index = 0
                self._load_current_item()
                self._show_controls()
            elif dlg.result_action == "DEACTIVATE":
                self._apply_filter("ALL")

    def _open_text_color_mgr(self):
        dlg = AdvancedTextColorManagerDialog(self)
        if dlg.exec():
            self._load_text_colors()
            if self.stack.currentIndex() == self.IDX_TXT:
                self._load_current_item()

    def _apply_filter(self, filter_type, reset_index=True):
        self.current_filter = filter_type.upper()
        exts = {
            'PHOTOS': ['.png', '.jpg', '.jpeg', '.bmp', '.gif', '.webp'],
            'VIDEOS': ['.mp4', '.avi', '.mkv', '.mov', '.webm'],
            'AUDIO': ['.mp3', '.wav', '.ogg', '.flac'],
            'DOCS': ['.txt', '.csv', '.json', '.xml', '.py', '.md', '.log', '.ini', '.sh', '.cpp', '.c', '.h', '.pdf']
        }
        if filter_type == "ALL": self.active_playlist = self.master_playlist.copy()
        else: self.active_playlist = [i for i in self.master_playlist if i.get('ext', '').lower() in exts.get(filter_type, [])]
        
        # FIX: Maintain Shuffle state when changing filters
        self.unshuffled_playlist = self.active_playlist.copy()
        if getattr(self, 'btn_cp_shuffle', None) and self.btn_cp_shuffle.isChecked():
            random.shuffle(self.active_playlist)
        
        if reset_index:
            self.current_index = 0
            
        self._load_current_item()
        self._show_controls()
        if getattr(self, 'is_webpage_mode', False):
            self.vert_view.load_playlist(self.active_playlist)

    def _set_vis_mode(self, mode):
        self.vis_mode = mode
        self.wave_vis.vis_mode = mode
        self.save_settings()
        
        # Instantly hide/show the widget based on the mode if audio is playing
        if self.stack.currentIndex() == self.IDX_MEDIA and getattr(self, 'active_playlist', None):
            item = self.active_playlist[self.current_index]
            if item.get('ext', '').lower() in ['.mp3', '.wav', '.ogg', '.flac']:
                if mode == "OFF":
                    self.wave_vis.hide()
                    self.wave_vis.stop_vis()
                else:
                    self.wave_vis.show()
                    if hasattr(self, 'player') and self.player.playbackState() == QMediaPlayer.PlaybackState.PlayingState:
                        self.wave_vis.start_vis()

    def show_context_menu(self, pos):
        menu = QMenu(self)
        
        f_menu = menu.addMenu("🗂️ Media Filter")
        for f in ["ALL", "PHOTOS", "VIDEOS", "AUDIO", "DOCS"]:
            a = f_menu.addAction(f"Show {f}")
            a.setCheckable(True); a.setChecked(self.current_filter == f)
            a.triggered.connect(lambda checked, ft=f: self._apply_filter(ft))
        
        f_menu.addSeparator()
        pd_action = f_menu.addAction("✨ Pre-Decided Lists...")
        pd_action.setCheckable(True); pd_action.setChecked(self.current_filter == "PRE-DECIDED")
        pd_action.triggered.connect(self._open_pre_decided)
            
        view_mode_menu = menu.addMenu("👁️ View Mode")
        a_norm = view_mode_menu.addAction("🖼️ Normal View")
        a_norm.setCheckable(True); a_norm.setChecked(not self.is_webpage_mode)
        a_norm.triggered.connect(lambda: self._set_webpage_mode(False))
        a_web = view_mode_menu.addAction("📄 Webpage View (Vertical)")
        a_web.setCheckable(True); a_web.setChecked(self.is_webpage_mode)
        a_web.triggered.connect(lambda: self._set_webpage_mode(True))
        
        if self.stack.currentIndex() == self.IDX_IMG:
            img_menu = menu.addMenu("🖼️ Image Scaling")
            a_fit = img_menu.addAction("Fit to Screen (Default)")
            a_fit.setCheckable(True)
            a_fit.setChecked(getattr(self, 'universal_fit', True))
            a_fit.triggered.connect(lambda: self._set_img_scale(True))
            
            a_orig = img_menu.addAction("Original Size")
            a_orig.setCheckable(True)
            a_orig.setChecked(not getattr(self, 'universal_fit', True))
            a_orig.triggered.connect(lambda: self._set_img_scale(False))
            menu.addSeparator()
        
        if self.is_webpage_mode:
            view_mode_menu.addSeparator()
            align_menu = view_mode_menu.addMenu("📏 Webpage Alignment")
            for name, flag in [("Center", Qt.AlignHCenter), ("Left", Qt.AlignLeft), ("Right", Qt.AlignRight)]:
                a = align_menu.addAction(name)
                a.setCheckable(True)
                a.setChecked(self.webpage_align == flag)
                a.triggered.connect(lambda checked, f=flag: self._set_webpage_align(f))
            
            view_mode_menu.addAction("↕️ Adjust Image Spacing...").triggered.connect(self._adjust_spacing)

        doc_menu = menu.addMenu("📄 Document View Settings")
        doc_menu.addAction("Text Color Manager...").triggered.connect(self._open_text_color_mgr)
        
        item = self.active_playlist[self.current_index] if self.active_playlist else None
        if item and item.get('ext', '').lower() == '.md':
            doc_menu.addSeparator()
            a_md = doc_menu.addAction("Render GitHub Markdown")
            a_md.setCheckable(True)
            a_md.setChecked(self.is_md_rendered)
            a_md.triggered.connect(self._toggle_md_rendering)

        menu.addSeparator()

        p_menu = menu.addMenu("▶️ Playback & Automation")
        p_menu.addAction("Set Brightness...").triggered.connect(self._update_brightness)
        p_menu.addAction("Toggle Slideshow").triggered.connect(self._toggle_slide)
        p_menu.addAction("Set Default Speed...").triggered.connect(self._set_default_speed)
        p_menu.addAction("Set A-B Loop").triggered.connect(self._set_ab_loop)
        p_menu.addSeparator()
        p_menu.addAction("🔴 Record Macro...").triggered.connect(self.macro_osd.show)
        p_menu.addAction("VMSL Macro Studio...").triggered.connect(self._open_adv_loop)
        if HAS_MULTIMEDIA and self.stack.currentIndex() == self.IDX_MEDIA:
            p_menu.addSeparator()
            for r in [0.5, 1.0, 1.5, 2.0]: p_menu.addAction(f"{r}x Speed").triggered.connect(lambda c, rate=r: self._set_playback_rate(rate))

        if self.stack.currentIndex() in [self.IDX_IMG, self.IDX_MEDIA]:
            t_menu = menu.addMenu("📐 Transform Media")
            t_menu.addAction("Rotate Clockwise (R)").triggered.connect(self._rot_img_cw)
            t_menu.addAction("Rotate Counter-Clockwise (Shift+R)").triggered.connect(self._rot_img_ccw)
            t_menu.addAction("Flip Horizontal (F)").triggered.connect(self._flip_img_horiz)
            t_menu.addAction("Flip Vertical (Shift+F)").triggered.connect(self._flip_img_vert)
            
        if self.stack.currentIndex() in [self.IDX_PDF, self.IDX_WEB]:
            scroll_speed_menu = menu.addMenu("⏬ Auto-Scroll Speed")
            for speed in [1, 2, 3, 5, 10]:
                a = scroll_speed_menu.addAction(f"{speed}x Speed")
                a.setCheckable(True)
                a.setChecked(getattr(self, 'auto_scroll_speed', 1) == speed)
                a.triggered.connect(lambda checked, s=speed: self._set_scroll_speed(s))

        if self.stack.currentIndex() == self.IDX_PDF and HAS_PDF:
            pdf_menu = menu.addMenu("📄 PDF Controls")
            pdf_menu.addAction("Zoom In").triggered.connect(lambda: (self.pdf_view.setZoomMode(QPdfView.ZoomMode.Custom), self.pdf_view.setZoomFactor(self.pdf_view.zoomFactor() * 1.2)))
            pdf_menu.addAction("Zoom Out").triggered.connect(lambda: (self.pdf_view.setZoomMode(QPdfView.ZoomMode.Custom), self.pdf_view.setZoomFactor(self.pdf_view.zoomFactor() / 1.2)))
            pdf_menu.addAction("Fit Width").triggered.connect(lambda: self.pdf_view.setZoomMode(QPdfView.ZoomMode.FitToWidth))
            pdf_menu.addAction("Fit Page").triggered.connect(lambda: self.pdf_view.setZoomMode(QPdfView.ZoomMode.FitInView))
            a_scroll = pdf_menu.addAction("Toggle Auto-Scroll")
            a_scroll.setCheckable(True)
            a_scroll.setChecked(self.btn_pdf_scroll.isChecked())
            a_scroll.triggered.connect(lambda checked: self.btn_pdf_scroll.setChecked(checked))
            
            pdf_menu.addSeparator()
            a_mag = pdf_menu.addAction("📖 Magazine View (2-Page Side-by-Side)")
            a_mag.setCheckable(True)
            a_mag.setChecked(getattr(self, 'is_magazine_mode', False))
            a_mag.triggered.connect(self._toggle_pdf_mag)
            
            # --- NEW: Magazine Layout Modes ---
            mag_layout_menu = pdf_menu.addMenu("📖 Magazine Layout Style")
            a_cover = mag_layout_menu.addAction("Cover Mode (1, 2-3, 4-5)")
            a_cover.setCheckable(True)
            a_cover.setChecked(getattr(self, 'magazine_layout', 'COVER') == 'COVER')
            a_cover.triggered.connect(lambda: self._set_magazine_layout('COVER'))
            
            a_double = mag_layout_menu.addAction("Double Mode (1-2, 3-4, 5-6)")
            a_double.setCheckable(True)
            a_double.setChecked(getattr(self, 'magazine_layout', 'COVER') == 'DOUBLE')
            a_double.triggered.connect(lambda: self._set_magazine_layout('DOUBLE'))
            
        if HAS_MULTIMEDIA:
            vol_menu = menu.addMenu("🔊 Audio & Volume")
            vol_menu.addAction("Volume Up (↑)").triggered.connect(self._vol_up)
            vol_menu.addAction("Volume Down (↓)").triggered.connect(self._vol_down)
            vol_menu.addAction("Toggle Mute (M)").triggered.connect(self._toggle_mute)
            vol_menu.addAction("Set Exact Volume...").triggered.connect(self._set_exact_volume)
            vol_menu.addSeparator()
            vol_menu.addAction("Load Subtitle (.srt)...").triggered.connect(self._load_subtitle_file)
            
            # --- NEW: Subtitle Color Menu ---
            sub_color_menu = vol_menu.addMenu("📝 Subtitle Color")
            for c_name, c_hex in [("White", "#FFFFFF"), ("Dark Yellow", "#B8860B"), ("Yellow", "#FFFF00"), ("Custom...", "CUSTOM")]:
                a = sub_color_menu.addAction(c_name)
                a.triggered.connect(lambda checked, h=c_hex: self._set_subtitle_color(h))
                
            vol_menu.addAction("Toggle Miniplayer Mode").triggered.connect(self._toggle_miniplayer)
            vol_menu.addSeparator()
            vis_menu = vol_menu.addMenu("🎸 Visualizer Style")
            for mode_name, mode_val in [("Bubbles (Zero-G)", "BUBBLES"), ("Circular String (Dark Yellow)", "CIRCULAR_STRING"), ("None (Off)", "OFF")]:
                a = vis_menu.addAction(mode_name)
                a.setCheckable(True)
                a.setChecked(self.vis_mode == mode_val)
                a.triggered.connect(lambda checked, m=mode_val: self._set_vis_mode(m))
                
        menu.addSeparator()

        v_menu = menu.addMenu("⚙️ UI Settings")
        qt_opt = v_menu.addAction("Show Quick Tools on Deck (Rotate, Flip, A-B)")
        qt_opt.setCheckable(True); qt_opt.setChecked(self.show_quick_tools)
        qt_opt.triggered.connect(self._toggle_quick_tools)
        v_menu.addSeparator()
        p_line = v_menu.addAction("Show Top Progress Line")
        p_line.setCheckable(True); p_line.setChecked(self.prog_mode == "SHOW")
        p_line.triggered.connect(lambda checked: self._set_prog_mode("SHOW" if checked else "HIDE"))
        c_menu = v_menu.addMenu("File Counter Mode")
        for mode in ["AUTO", "SHOW", "HIDE"]:
            a = c_menu.addAction(mode.title())
            a.setCheckable(True); a.setChecked(self.count_mode == mode)
            a.triggered.connect(lambda checked, m=mode: self._set_count_mode(m))
        nav_menu = v_menu.addMenu("Nav Arrows Mode")
        for mode in ["AUTO", "SHOW", "HIDE"]:
            a = nav_menu.addAction(mode.title())
            a.setCheckable(True); a.setChecked(self.nav_mode == mode)
            a.triggered.connect(lambda checked, m=mode: self._set_nav_mode(m))
        deck_menu = v_menu.addMenu("Control Deck Mode")
        for mode in ["AUTO", "SHOW", "HIDE"]:
            a = deck_menu.addAction(mode.title())
            a.setCheckable(True); a.setChecked(self.deck_mode == mode)
            a.triggered.connect(lambda checked, m=mode: self._set_deck_mode(m))

        # 9. System Tools
        sys_menu = menu.addMenu("📋 System")
        sys_menu.addAction("ℹ️ File Properties...").triggered.connect(self._show_file_properties)
        sys_menu.addAction("🔢 Jump to File Number (G)...").triggered.connect(self._jump_to_file)
        sys_menu.addAction("📋 Copy Physical Path").triggered.connect(lambda: QApplication.clipboard().setText(self.active_playlist[self.current_index]['path']) if self.active_playlist else None)

        global_pos = self.sender().mapToGlobal(pos) if hasattr(self.sender(), 'mapToGlobal') else self.mapToGlobal(pos)
        menu.exec(global_pos)
        
    def _toggle_md_rendering(self, checked):
        self.is_md_rendered = checked
        self._load_current_item()

    def _set_webpage_align(self, align_flag):
        self.webpage_align = align_flag
        if self.is_webpage_mode:
            self.vert_view.set_alignment(align_flag)

    def _toggle_quick_tools(self, checked):
        self.show_quick_tools = checked
        self._update_deck_buttons()
        self.resizeEvent(None) 
        self.save_settings()

    def _set_prog_mode(self, mode):
        self.prog_mode = mode
        if mode == "HIDE": self.slide_progress.hide()
        elif self.is_slideshow_active: self.slide_progress.show()
        self.save_settings() 

    def _set_count_mode(self, mode):
        self.count_mode = mode
        if mode == "HIDE": self.overlay_counter.hide()
        elif mode == "SHOW": self.overlay_counter.show()
        else: self._show_controls()
        self.save_settings()

    def _set_nav_mode(self, mode):
        self.nav_mode = mode
        if mode == "HIDE": self.btn_float_prev.hide(); self.btn_float_next.hide()
        elif mode == "SHOW": self.btn_float_prev.show(); self.btn_float_next.show()
        else: self._show_controls()
        self.save_settings() 
        
    def _set_deck_mode(self, mode):
        self.deck_mode = mode
        if mode == "HIDE": self.control_panel.hide(); self.pdf_control_panel.hide()
        elif mode == "SHOW": 
            if self.stack.currentIndex() == self.IDX_PDF: self.pdf_control_panel.show()
            else: self.control_panel.show()
        else: self._show_controls()
        self.save_settings() 


    def _set_webpage_mode(self, enabled):
        if getattr(self, 'macro_recording', False) and not getattr(self, 'macro_paused', False):
            self._flush_macro_state()
            self._record_macro_action("mode=web" if enabled else "mode=norm")
            
        self.is_webpage_mode = enabled
        if enabled:
            self.is_magazine_mode = False # <-- FIX: Clear conflicting magazine state
            self.stack.setCurrentIndex(self.IDX_WEB)
            self.vert_view.set_alignment(self.webpage_align)
            self.vert_view.load_playlist(self.active_playlist)
            self.vert_view.scroll_to_index(self.current_index)
            if getattr(self, 'macro_recording', False): self._snapshot_macro_state(self.IDX_WEB)
        else: self._load_current_item()

    def _adjust_spacing(self):
        current = self.vert_view.layout.spacing()
        val, ok = QInputDialog.getInt(self, "Image Spacing", "Pixels between images (0-200):", current, 0, 200, 5)
        if ok:
            self.vert_view.layout.setSpacing(val)
            rad = "0px" if val == 0 else "4px"
            for lbl in self.vert_view.labels:
                lbl.setStyleSheet(f"background: #000; border-radius: {rad};")

    def _set_default_speed(self):
        val, ok = QInputDialog.getInt(self, "Default Speed", "Enter default interval in ms (e.g. 3000):", self.slideshow_speed, 100, 60000, 500)
        if ok: self.slideshow_speed = val

    def _set_exact_volume(self):
        if not HAS_MULTIMEDIA or not hasattr(self, 'audio'): return
        current = int(self.audio.volume() * 100)
        val, ok = QInputDialog.getInt(self, "Set Volume", "Volume % (0-100):", current, 0, 100, 5)
        if ok:
            self.audio.setVolume(val / 100.0)
            self._trigger_vol_osd()
            self.save_settings()

    def _parse_frames(self, f_str, max_len):
        frames = []
        for f_part in f_str.split(','):
            if '-' in f_part:
                s_f, e_f = map(int, f_part.split('-'))
                step = 1 if s_f <= e_f else -1
                frames.extend(range(s_f - 1, e_f - 1 + step, step))
            else: frames.append(int(f_part) - 1)
        return [f for f in frames if 0 <= f < max_len]

    def _open_adv_loop(self):
        dlg = AdvancedLoopDialog(getattr(self, 'adv_loop_raw', ""), self)
        if dlg.exec():
            text = dlg.result_raw
            self.adv_loop_raw = text
            if not text:
                self.adv_sequence = []
                return QMessageBox.information(self, "Deactivated", "Engine deactivated.")
            
            try:
                variables = {}
                seq = []
                global_ufit = True
                
                for line in text.splitlines():
                    line = line.strip()
                    if not line or line.startswith("rem") or line.startswith("#"): continue
                    if " rem " in line: line = line.split(" rem ")[0].strip()
                    if " # " in line: line = line.split(" # ")[0].strip()
                    
                    # Split line into words to securely process commands with trailing metadata
                    tokens = line.split()
                    if not tokens: continue
                    first_token = tokens[0].lower()

                    if first_token.startswith('ufit='):
                        global_ufit = first_token.split('=')[1] in ['1', 'true']
                        continue
                    
                    # Mode identification parser (Safely ignores trailing dur=0)
                    if first_token in ['mode=web', 'mode=norm', 'mode=magazine_cover', 'mode=magazine_double', 'mode=magazine', 'mode=pdf']:
                        seq.append({'type': 'cmd', 'cmd': first_token})
                        continue

                    if first_token.startswith('spread='):
                        val = int(first_token.split('=')[1])
                        seq.append({'type': 'cmd', 'cmd': 'spread', 'val': val})
                        continue
                    
                    if first_token in ['pause', 'stop', 'play']:
                        seq.append({'type': 'cmd', 'cmd': first_token})
                        continue
                        
                    if first_token == 'wait':
                        dur = int(tokens[1])
                        seq.append({'type': 'cmd', 'cmd': 'wait', 'dur': dur})
                        continue

                    if line.startswith("$") and "=" in line:
                        var_name, val = line.split("=", 1)
                        variables[var_name.strip()] = self._parse_frames(val.strip(), len(self.active_playlist))
                        continue

                    parts = line.split()
                    target = parts[0]
                    
                    if target.startswith("$"):
                        frames = variables.get(target, [])
                    elif ":" in target: 
                        t_parts = target.split(':')
                        frames = self._parse_frames(t_parts[0], len(self.active_playlist))
                        parts = parts[1:]
                        
                        for p in t_parts[1:]:
                            p = p.lower()
                            if 'x' in p and any(c.isdigit() for c in p):
                                if p.startswith('x'): parts.append(f"loop={p[1:]}")
                                elif p.endswith('x'): parts.append(f"dur={p[:-1]}")
                                else: parts.append(f"dur={p.split('x')[0]}"); parts.append(f"loop={p.split('x')[1]}")
                            elif p.isdigit(): parts.append(f"dur={p}")
                            else: parts.append(f"t={p}")
                    else:
                        frames = self._parse_frames(target, len(self.active_playlist))

                    dur = self.slideshow_speed; loops = 1; t_str = ""; z = None; zx = None; zy = None; start = -1.0; end = -1.0; rate = 1.0; rev = False
                    sx = -1; sy = -1; sxs = -1; sys = -1; vol = -1; ufit = global_ufit
                    dx = 0.0; dy = 0.0 
                    zs = None; zxs = None; zys = None  # CRITICAL: Initialize smooth zoom variables to None

                    play_val = False; pause_val = False
                    for p_item in parts[1:] if ":" not in target else parts:
                        if p_item.lower() == 'play': play_val = True; continue
                        if p_item.lower() == 'pause': pause_val = True; continue
                        if not "=" in p_item: continue
                        k, v = p_item.lower().split("=", 1)
                        if k == "dur": dur = int(v)
                        elif k == "loop": loops = int(v)
                        elif k == "t": t_str += v  # Critical fix: Appends transforms
                        elif k == "z": z = float(v)
                        elif k == "zx": zx = int(v)
                        elif k == "zy": zy = int(v)
                        elif k == "zs": zs = float(v)
                        elif k == "zxs": zxs = int(v)
                        elif k == "zys": zys = int(v)
                        elif k == "sx": sx = int(v)
                        elif k == "sy": sy = int(v)
                        elif k == "sxs": sxs = int(v)
                        elif k == "sys": sys = int(v)
                        elif k == "vol": vol = int(v)
                        elif k == "ufit": ufit = v in ["1", "true"]
                        elif k in ["dragu", "dragd", "dragl", "dragr", "scrollu", "scrolld", "scrolll", "scrollr"]:
                            multiplier = 1.0
                            if v.endswith("cm"): multiplier = 37.8; v = v[:-2]
                            elif v.endswith("in"): multiplier = 96.0; v = v[:-2]
                            elif v.endswith("px"): v = v[:-2]
                            try: px_val = float(v) * multiplier
                            except: px_val = 0.0
                            
                            if k in ["dragu", "scrollu"]: dy -= px_val
                            elif k in ["dragd", "scrolld"]: dy += px_val
                            elif k in ["dragl", "scrolll"]: dx -= px_val
                            elif k in ["dragr", "scrollr"]: dx += px_val
                        elif k == "start": start = float(v)
                        elif k == "end": end = float(v)
                        elif k == "rate": rate = float(v)
                        elif k == "rev": rev = v in ["1", "true"]
                        elif k == "pdf_flip": frame_data['pdf_flip'] = int(v)
                        elif k == "pdf_mag": frame_data['pdf_mag'] = int(v)

                    for _ in range(loops):
                        iter_frames = reversed(frames) if rev else frames
                        for f in iter_frames:
                            frame_data = {'type': 'frame', 'frame': f, 'dur': dur, 't': t_str, 'z': z, 'zx': zx, 'zy': zy, 'start': start, 'end': end, 'rate': rate, 'dx': dx, 'dy': dy, 'ufit': ufit}
                            if zs is not None: frame_data['zs'] = zs
                            if zxs is not None: frame_data['zxs'] = zxs
                            if zys is not None: frame_data['zys'] = zys
                            if zx is not None: frame_data['zx'] = zx
                            if zy is not None: frame_data['zy'] = zy
                            if sx != -1: frame_data['sx'] = sx
                            if sy != -1: frame_data['sy'] = sy
                            if sxs != -1: frame_data['sxs'] = sxs
                            if sys != -1: frame_data['sys'] = sys
                            if vol != -1: frame_data['vol'] = vol
                            if play_val: frame_data['play'] = True
                            if pause_val: frame_data['pause'] = True
                            seq.append(frame_data)
                
                if seq:
                    self.adv_sequence = seq
                    self.adv_idx = 0
                    self.is_slideshow_active = True
                    self._execute_vmsl_instruction()
                    self._toggle_slide(force=True)
                else: QMessageBox.warning(self, "Empty", "No valid instructions parsed.")
            except Exception as e: QMessageBox.warning(self, "Compiler Error", f"Failed to compile VMSL script:\n{e}")

    def _execute_vmsl_instruction(self):
        ins = self.adv_sequence[self.adv_idx]
        self.adv_current_instruction = ins
        
        # 1. System & Mode Commands
        if ins['type'] == 'cmd':
            cmd = ins['cmd']
            if cmd == 'pause':
                self._toggle_slide(force=False)
                return
            elif cmd == 'stop':
                self._toggle_slide(force=False)
                self.adv_idx = 0
                return
            elif cmd == 'wait':
                return 
            elif cmd == 'mode=web':
                self._set_webpage_mode(True)
            elif cmd == 'mode=magazine' or cmd == 'mode=magazine_cover':
                self.magazine_layout = 'COVER'
                self._toggle_pdf_mag(True)
            elif cmd == 'mode=magazine_double':
                self.magazine_layout = 'DOUBLE'
                self._toggle_pdf_mag(True)
            elif cmd == 'mode=pdf':
                self._toggle_pdf_mag(False)
                if self.stack.currentIndex() != self.IDX_PDF:
                    self.stack.setCurrentIndex(self.IDX_PDF)
            elif cmd == 'mode=norm':
                if getattr(self, 'is_magazine_mode', False): self._toggle_pdf_mag(False)
                if getattr(self, 'is_webpage_mode', False): self._set_webpage_mode(False)
            elif cmd == 'spread':
                self._navigate_spread(ins.get('val', 1))

            self.adv_idx = (self.adv_idx + 1) % len(self.adv_sequence)
            QTimer.singleShot(10, self._execute_vmsl_instruction)
            return

        # 2. PDF Flip & Magazine Execution
        if 'pdf_flip' in ins and HAS_PDF:
            btn = getattr(self, 'btn_pdf_flip', None)
            if btn and btn.isChecked() != (ins['pdf_flip'] == 1):
                btn.setChecked(ins['pdf_flip'] == 1)
                
        if 'pdf_mag' in ins and HAS_PDF:
            if getattr(self, 'is_magazine_mode', False) != (ins['pdf_mag'] == 1):
                self._toggle_pdf_mag(ins['pdf_mag'] == 1)

        just_loaded = getattr(self, '_last_loaded_index', -1) != ins['frame']
        if just_loaded:
            self.current_index = ins['frame']
            self._load_current_item()
        
        # 3. Media Transforms (Flip / Rotate)
        t = ins.get('t', '').lower()
        needs_transform = False
        is_video = self.stack.currentIndex() == self.IDX_MEDIA
        
        # FIX: Explicitly evaluate target flip state so it can flip BACK
        target_flip_h = 'hf' in t
        target_flip_v = 'vf' in t
        
        if target_flip_h != self.flip_h:
            self.flip_h = target_flip_h
            needs_transform = True
            
        if target_flip_v != self.flip_v:
            self.flip_v = target_flip_v
            needs_transform = True
            
        current_rot = self.rot_v_angle if is_video else self.rot_angle
        new_rot = 0 if 'none' in t else current_rot
        
        if 'c90' in t: new_rot = 90
        elif 'c180' in t: new_rot = 180
        elif 'c270' in t or 'cc90' in t: new_rot = 270
        elif 'cc270' in t: new_rot = 90
        
        if new_rot != current_rot:
            if is_video: self.rot_v_angle = new_rot
            else: self.rot_angle = new_rot
            needs_transform = True
            
        if 'ufit' in ins: self.universal_fit = ins['ufit']
        
        # FIX: Actively apply the recorded transforms to Webpage Mode during Playback
        if needs_transform and not just_loaded:
            if self.stack.currentIndex() == self.IDX_IMG:
                self._apply_img_transform()
            elif self.stack.currentIndex() == self.IDX_WEB:
                self.vert_view.apply_global_transforms(getattr(self, 'flip_h', False), getattr(self, 'flip_v', False), getattr(self, 'rot_angle', 0))
            elif HAS_MULTIMEDIA and self.stack.currentIndex() == self.IDX_MEDIA: 
                self._apply_video_transform()
        
        # 4. Audio / Video Controls
        if ins.get('vol', -1) >= 0 and HAS_MULTIMEDIA and hasattr(self, 'audio'):
            self.audio.setVolume(ins['vol'] / 100.0)
            self._trigger_vol_osd()

        if ins.get('play', False) and HAS_MULTIMEDIA and hasattr(self, 'player'):
            self.player.play()
            if not self.video_view.isVisible(): self.wave_vis.start_vis()
        if ins.get('pause', False) and HAS_MULTIMEDIA and hasattr(self, 'player'):
            self.player.pause()
            self.wave_vis.stop_vis()

        if HAS_MULTIMEDIA and self.stack.currentIndex() == self.IDX_MEDIA:
            if ins['start'] >= 0: self.player.setPosition(int(ins['start'] * 1000))
            if ins['rate'] != 1.0: self.player.setPlaybackRate(ins['rate'])


        # 5. Routing Views for Strict Playback Separation
        view = None
        scroll_widget = None
        
        is_mag = getattr(self, 'is_magazine_mode', False) and HAS_PDF
        is_pdf = self.stack.currentIndex() == self.IDX_PDF and HAS_PDF and not is_mag
        is_web = self.stack.currentIndex() == self.IDX_WEB
        is_img = self.stack.currentIndex() == self.IDX_IMG
        is_media = self.stack.currentIndex() == self.IDX_MEDIA
        
        if is_mag: view = self.pdf_wrapper; scroll_widget = self.pdf_wrapper
        elif is_img: view = self.img_view
        # FIX: Route Video Viewer to the smooth animation engine for zoom/drag!
        elif is_media and hasattr(self, 'video_view'): view = self.video_view 
        elif is_web: scroll_widget = self.vert_view
        elif is_pdf: scroll_widget = self.pdf_view
        elif self.stack.currentIndex() == self.IDX_TXT: scroll_widget = self.txt_view
        elif self.stack.currentIndex() == self.IDX_CSV: scroll_widget = self.csv_view

        dx = ins.get('dx', 0.0)
        dy = ins.get('dy', 0.0)

        has_smooth = 'zs' in ins or 'zxs' in ins or 'zys' in ins or (view and (dx != 0 or dy != 0))
        has_scroll_anim = 'sxs' in ins or 'sys' in ins or (scroll_widget and (dx != 0 or dy != 0))
        
        z_target = ins.get('zs', ins.get('z', None))
        zx_target = ins.get('zxs', ins.get('zx', None))
        zy_target = ins.get('zys', ins.get('zy', None))
        sx_target = ins.get('sxs', ins.get('sx', -1))
        sy_target = ins.get('sys', ins.get('sy', -1))

        steps = max(1, int(ins.get('dur', self.slideshow_speed) / 30))
        self.smooth_target = {'steps_left': steps}

        # --- A) Magazine & Image Smooth Panning ---
        if view and has_smooth:
            curr_z = abs(view.transform().m11()) # FIX: Prevent negative physics interpolation
            scene_center = view.mapToScene(view.viewport().rect().center())
            final_z = z_target if z_target is not None else curr_z
            final_zx = zx_target if zx_target is not None else scene_center.x()
            final_zy = zy_target if zy_target is not None else scene_center.y()
            if dx != 0: final_zx += (dx / final_z)
            if dy != 0: final_zy += (dy / final_z)
            self.smooth_target.update({
                'start_z': curr_z, 'end_z': final_z, 'start_zx': scene_center.x(), 'end_zx': final_zx,
                'start_zy': scene_center.y(), 'end_zy': final_zy, 'total_steps': steps, 'view': view
            })
        elif view:
            if z_target is not None or zx_target is not None or zy_target is not None:
                curr_z = abs(view.transform().m11())
                scene_center = view.mapToScene(view.viewport().rect().center())
                
                view.resetTransform()
                # FIX: Re-apply PDF Magazine horizontal flip base transform before zooming
                if view == getattr(self, 'pdf_wrapper', None) and getattr(self, 'btn_pdf_flip', None) and self.btn_pdf_flip.isChecked():
                    trans = QTransform()
                    trans.translate(view.width(), 0)
                    trans.scale(-1, 1)
                    view.setTransform(trans)
                    
                final_z = z_target if z_target is not None else curr_z
                view.scale(final_z, final_z)
                view.centerOn(zx_target if zx_target is not None else scene_center.x(), zy_target if zy_target is not None else scene_center.y())
                
        # --- B) Normal PDF (Proportional Zoom) ---
        if is_pdf and z_target is not None:
            self.macro_scroll_paused = True 
            old_z = self.pdf_view.zoomFactor()
            self.pdf_view.setZoomMode(QPdfView.ZoomMode.Custom)
            self.pdf_view.setZoomFactor(z_target)
            
            # Perfect Proportional Math for PDF!
            if old_z > 0:
                ratio = z_target / old_z
                cx = self.pdf_view.viewport().width() / 2.0
                cy = self.pdf_view.viewport().height() / 2.0
                old_x = self.pdf_view.horizontalScrollBar().value() + cx
                old_y = self.pdf_view.verticalScrollBar().value() + cy
                
                def apply_playback_scroll_pdf():
                    self.pdf_view.horizontalScrollBar().setValue(int(old_x * ratio - cx))
                    self.pdf_view.verticalScrollBar().setValue(int(old_y * ratio - cy))
                    self.macro_scroll_paused = False
                QTimer.singleShot(15, apply_playback_scroll_pdf)
                
            # Clear explicit scroll targets so they don't fight the proportional math
            sx_target = -1
            sy_target = -1
            
        # --- C) Webpage Mode (Absolute Zoom) ---
        elif is_web and z_target is not None:
            self.macro_scroll_paused = True
            self.vert_view.zoom_factor = z_target
            for lbl in self.vert_view.labels:
                lbl.apply_zoom(z_target)
            QTimer.singleShot(30, lambda: setattr(self, 'macro_scroll_paused', False))

        # --- Execute Delayed Scroll Animations ---
        def apply_playback_scrolls():
            if scroll_widget and has_scroll_anim:
                curr_sx = scroll_widget.horizontalScrollBar().value()
                curr_sy = scroll_widget.verticalScrollBar().value()
                self.smooth_target.update({
                    'start_sx': curr_sx, 'end_sx': curr_sx + dx if sx_target == -1 else sx_target,
                    'start_sy': curr_sy, 'end_sy': curr_sy + dy if sy_target == -1 else sy_target,
                    'total_steps': steps,
                    'scroll_widget': scroll_widget
                })
            elif scroll_widget:
                if sx_target >= 0: scroll_widget.horizontalScrollBar().setValue(int(sx_target))
                if sy_target >= 0: scroll_widget.verticalScrollBar().setValue(int(sy_target))

            if has_smooth or has_scroll_anim:
                self.smooth_anim_timer.start(30)
            else:
                self.smooth_anim_timer.stop()

        # If a zoom happened on Webpage, wait 30ms for Qt to re-render boundaries before scrolling
        if z_target is not None and is_web:
            QTimer.singleShot(30, apply_playback_scrolls)
        else:
            apply_playback_scrolls()
            
    def _load_current_item(self):
        if not self.active_playlist:
            self.stack.setCurrentIndex(self.IDX_TXT)
            self.txt_view.setPlainText("Empty List.")
            self.cp_slider.hide(); self.lbl_time.hide()
            self._restore_title()
            # CRITICAL FIX: Reset the Image view's underlying transform so the next image doesn't open zoomed in!
            self.img_view.resetTransform()
            return
            
        
        self._last_loaded_index = self.current_index
        item = self.active_playlist[self.current_index]
        self._restore_title()
        
        self.last_macro_view_state = None
        self.last_macro_scroll_state = None
        
        if hasattr(self, 'player'): self.player.stop()
        self.wave_vis.hide(); self.wave_vis.stop_vis()
        self.lbl_metadata.hide()
        self.lbl_subtitle.setText("")
        self.lbl_subtitle.hide()
        self.subtitles = []  # Clear previous video's subtitles
        self.flip_h = False; self.flip_v = False; self.rot_angle = 0; self.rot_v_angle = 0
        self.cp_slider.hide(); self.lbl_time.hide()
        self.btn_pdf_scroll.setChecked(False)
        # --- NEW: Apply Playlist Transforms Automatically ---
        pt = item.get('t', '').lower()
        self.flip_h = 'hf' in pt
        self.flip_v = 'vf' in pt
        self.rot_angle = 90 if 'c90' in pt else 180 if 'c180' in pt else 270 if 'c270' in pt or 'cc90' in pt else 0
        
        if HAS_MULTIMEDIA: 
            self._apply_video_transform(); self.player.setPlaybackRate(1.0)
        if hasattr(self, 'video_view'): self.video_view.hide()
        
        ext = item.get('ext', '').lower()
        
        if self.is_webpage_mode and ext in ['.png', '.jpg', '.jpeg', '.bmp', '.gif', '.webp']:
            self.stack.setCurrentIndex(self.IDX_WEB); self.vert_view.scroll_to_index(self.current_index)
            self._show_controls()
            return
            
        if ext in ['.png', '.jpg', '.jpeg', '.bmp', '.gif', '.webp']: 
            self.stack.setCurrentIndex(self.IDX_IMG)
            self.orig_pm = QPixmap(item['path'])
            self._apply_img_transform()
            
        elif ext == '.pdf':
            self.stack.setCurrentIndex(self.IDX_PDF)
            if HAS_PDF:
                self.pdf_doc.load(item['path'])
                
                # FIX: Clean out any lingering magazine sizes before setting zoom
                self.pdf_view.setMinimumSize(0,0)
                self.pdf_view.setMaximumSize(16777215, 16777215)
                self.pdf_container.setMinimumSize(0,0)
                self.pdf_container.setMaximumSize(16777215, 16777215)
                self.pdf_wrapper.resetTransform()
                
                # Apply True Custom Fit Page Math
                QTimer.singleShot(50, self._fit_normal_pdf)
                
                # Default to Normal Page Size smoothly
                self.pdf_view.setZoomMode(QPdfView.ZoomMode.FitInView)
            
        elif ext == '.md' and self.is_md_rendered:
            self.stack.setCurrentIndex(self.IDX_MD)
            try:
                with open(item['path'], 'r', encoding='utf-8', errors='replace') as f:
                    self.md_view.setMarkdown(f.read())
            except Exception as e:
                self.md_view.setMarkdown(f"**Error rendering Markdown:**\n{e}")
                
        elif ext == '.csv':
            self.stack.setCurrentIndex(self.IDX_CSV)
            self.csv_view.setSortingEnabled(False) 
            self.csv_view.clear()
            self.csv_view.setRowCount(0)
            self.csv_view.setColumnCount(0)
            try:
                with open(item['path'], 'r', encoding='utf-8', errors='replace') as f:
                    data = list(csv.reader(f))
                    if data:
                        self.csv_view.setRowCount(len(data))
                        self.csv_view.setColumnCount(max(len(r) for r in data))
                        
                        # Populate table with smart numeric conversion for correct sorting
                        for row_idx, row in enumerate(data):
                            for col_idx, val in enumerate(row):
                                table_item = QTableWidgetItem()
                                # Smartly convert to float if it's a number, otherwise keep as string
                                try:
                                    table_item.setData(Qt.EditRole, float(val))
                                except ValueError:
                                    table_item.setData(Qt.EditRole, val)
                                self.csv_view.setItem(row_idx, col_idx, table_item)
                        
                        # Auto-resize the columns to comfortably fit the data
                        self.csv_view.resizeColumnsToContents()
                        
                        # Enable interactive column click sorting
                        self.csv_view.setSortingEnabled(True)
            except Exception as e:
                self.stack.setCurrentIndex(self.IDX_TXT)
                self.txt_view.setPlainText(f"Error reading CSV:\n{e}")
                
        elif ext in ['.txt', '.json', '.xml', '.py', '.md', '.log', '.ini', '.sh', '.bat', '.ps1', '.css', '.js', '.cpp', '.c', '.h']:
            self.stack.setCurrentIndex(self.IDX_TXT)
            
            if hasattr(self, 'current_highlighter') and self.current_highlighter:
                self.current_highlighter.setDocument(None)
                self.current_highlighter = None
                
            config = {}
            if self.text_color_data.get("active", True) and ext in self.text_color_data.get("mappings", {}):
                config = self.text_color_data["mappings"][ext]
                
            default_base = config.get("base", "#00ff00" if ext == '.txt' else "#e5e5e5")
            self.txt_view.setStyleSheet(f"padding: 50px; background: #080808; color: {default_base}; border: none;")
            
            if ext in ['.py', '.js', '.css', '.bat', '.sh', '.ps1'] or config.get("custom_words"):
                self.current_highlighter = UniversalHighlighter(self.txt_view.document(), ext, config)
                
            try:
                with open(item['path'], 'r', encoding='utf-8', errors='replace') as f: 
                    self.txt_view.setPlainText(f.read())
            except Exception as e: 
                self.txt_view.setPlainText(str(e))
                
        elif ext in ['.mp3', '.wav', '.ogg', '.mp4', '.avi', '.mkv', '.mov'] and HAS_MULTIMEDIA: 
            self.stack.setCurrentIndex(self.IDX_MEDIA)
            self.cp_slider.show(); self.lbl_time.show()
            if ext in ['.mp3', '.wav', '.ogg', '.flac']: 
                self.wave_vis.load_file(item['path'])
                if self.vis_mode != "OFF":
                    self.wave_vis.show()
                    self.wave_vis.start_vis()
            else: self.video_view.show()
            self.player.setSource(QUrl.fromLocalFile(item['path'])); self.player.play()
        else:
            self.stack.setCurrentIndex(self.IDX_TXT); self.txt_view.setPlainText("Format unsupported natively.")
            
        self.resizeEvent(None)
        self._show_controls()
        self._update_deck_buttons()

    def _flip_img_horiz(self): 
        if self.stack.currentIndex() == self.IDX_IMG: 
            self.flip_h = not getattr(self, 'flip_h', False); self._apply_img_transform()
        elif self.stack.currentIndex() == self.IDX_WEB: 
            self.flip_h = not getattr(self, 'flip_h', False)
            self.vert_view.apply_global_transforms(self.flip_h, getattr(self, 'flip_v', False), getattr(self, 'rot_angle', 0))
        elif self.stack.currentIndex() == self.IDX_MEDIA and HAS_MULTIMEDIA: 
            self.flip_h = not getattr(self, 'flip_h', False); self._apply_video_transform()
        self._record_transform_macro()

    def _flip_img_vert(self):
        if self.stack.currentIndex() == self.IDX_IMG: 
            self.flip_v = not getattr(self, 'flip_v', False); self._apply_img_transform()
        elif self.stack.currentIndex() == self.IDX_WEB: 
            self.flip_v = not getattr(self, 'flip_v', False)
            self.vert_view.apply_global_transforms(getattr(self, 'flip_h', False), self.flip_v, getattr(self, 'rot_angle', 0))
        elif self.stack.currentIndex() == self.IDX_MEDIA and HAS_MULTIMEDIA: 
            self.flip_v = not getattr(self, 'flip_v', False); self._apply_video_transform()
        self._record_transform_macro()

    def _rot_img_cw(self): 
        if self.stack.currentIndex() == self.IDX_IMG: 
            self.rot_angle = (getattr(self, 'rot_angle', 0) + 90) % 360; self._apply_img_transform()
        elif self.stack.currentIndex() == self.IDX_WEB: 
            self.rot_angle = (getattr(self, 'rot_angle', 0) + 90) % 360
            self.vert_view.apply_global_transforms(getattr(self, 'flip_h', False), getattr(self, 'flip_v', False), self.rot_angle)
        elif self.stack.currentIndex() == self.IDX_MEDIA and HAS_MULTIMEDIA: 
            self.rot_v_angle = (getattr(self, 'rot_v_angle', 0) + 90) % 360; self._apply_video_transform()
        self._record_transform_macro()

    def _rot_img_ccw(self): 
        if self.stack.currentIndex() == self.IDX_IMG: 
            self.rot_angle = (getattr(self, 'rot_angle', 0) - 90) % 360; self._apply_img_transform()
        elif self.stack.currentIndex() == self.IDX_WEB: 
            self.rot_angle = (getattr(self, 'rot_angle', 0) - 90) % 360
            self.vert_view.apply_global_transforms(getattr(self, 'flip_h', False), getattr(self, 'flip_v', False), self.rot_angle)
        elif self.stack.currentIndex() == self.IDX_MEDIA and HAS_MULTIMEDIA: 
            self.rot_v_angle = (getattr(self, 'rot_v_angle', 0) - 90) % 360; self._apply_video_transform()
        self._record_transform_macro()
        
    def _apply_video_transform(self):
        if not HAS_MULTIMEDIA: return
        trans = QTransform(); c = self.video_item.boundingRect().center()
        trans.translate(c.x(), c.y()); trans.rotate(getattr(self, 'rot_v_angle', 0))
        sx = -1 if getattr(self, 'flip_h', False) else 1
        sy = -1 if getattr(self, 'flip_v', False) else 1
        trans.scale(sx, sy)
        trans.translate(-c.x(), -c.y())
        self.video_item.setTransform(trans); self.video_view.fitInView(self.video_item.boundingRect(), Qt.KeepAspectRatio)
        
    def _apply_img_transform(self):
        if hasattr(self, 'orig_pm'):
            img = self.orig_pm.toImage()
            h, v = getattr(self, 'flip_h', False), getattr(self, 'flip_v', False)
            if h or v: img = img.mirrored(h, v)
            trans = QTransform().rotate(getattr(self, 'rot_angle', 0))
            
            ufit = getattr(self, 'universal_fit', True)
            
            # CRITICAL: Preserve exact zoom/pan state if ufit is disabled
            curr_z, curr_c = None, None
            if not ufit and self.img_view.transform().m11() > 0:
                curr_z = self.img_view.transform().m11()
                curr_c = self.img_view.mapToScene(self.img_view.viewport().rect().center())

            self.img_view.set_image(QPixmap.fromImage(img).transformed(trans, Qt.SmoothTransformation), ufit)
            
            # Restore exact state
            if not ufit and curr_z is not None:
                self.img_view.resetTransform()
                self.img_view.scale(curr_z, curr_z)
                self.img_view.centerOn(curr_c)

    def _update_slider(self, pos):
        if not self.cp_slider.isSliderDown(): self.cp_slider.setValue(pos)
        m, s = divmod(pos // 1000, 60)
        self.lbl_time.setText(f"{m:02d}:{s:02d}")
        if self.subtitles:
            current_text = ""
            for sub in self.subtitles:
                if sub['start'] <= pos <= sub['end']:
                    current_text = sub['text']; break
            self.lbl_subtitle.setText(current_text)
        
    def _seek_forward(self):
        if HAS_MULTIMEDIA and self.stack.currentIndex() == self.IDX_MEDIA: 
            self.player.setPosition(self.player.position() + 5000)
            self._on_seek_recorded()
    def _seek_backward(self):
        if HAS_MULTIMEDIA and self.stack.currentIndex() == self.IDX_MEDIA: 
            self.player.setPosition(max(0, self.player.position() - 5000))
            self._on_seek_recorded()
        
    def _trigger_vol_osd(self):
        v = self.audio.volume()
        self.bar_vol.setValue(int(v * 100))
        self.lbl_vol_icon.setText("🔊" if v > 0 else "🔇")
        self.vol_osd.show()
        self.vol_timer.start(1500)
        
    def _handle_up_arrow(self):
        if self.stack.currentIndex() in [self.IDX_WEB, self.IDX_TXT, self.IDX_MD, self.IDX_CSV]:
            w = self.stack.currentWidget()
            if hasattr(w, 'verticalScrollBar'): w.verticalScrollBar().setValue(w.verticalScrollBar().value() - 50)
        elif self.stack.currentIndex() == self.IDX_PDF and HAS_PDF:
            if getattr(self, 'is_magazine_mode', False):
                self.pdf_wrapper.verticalScrollBar().setValue(self.pdf_wrapper.verticalScrollBar().value() - 50)
            else:
                self.pdf_view.verticalScrollBar().setValue(self.pdf_view.verticalScrollBar().value() - 50)
        else: self._vol_up()

    def _handle_down_arrow(self):
        if self.stack.currentIndex() in [self.IDX_WEB, self.IDX_TXT, self.IDX_MD, self.IDX_CSV]:
            w = self.stack.currentWidget()
            if hasattr(w, 'verticalScrollBar'): w.verticalScrollBar().setValue(w.verticalScrollBar().value() + 50)
        elif self.stack.currentIndex() == self.IDX_PDF and HAS_PDF:
            if getattr(self, 'is_magazine_mode', False):
                self.pdf_wrapper.verticalScrollBar().setValue(self.pdf_wrapper.verticalScrollBar().value() + 50)
            else:
                self.pdf_view.verticalScrollBar().setValue(self.pdf_view.verticalScrollBar().value() + 50)
        else: self._vol_down()


    def _load_subtitle_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Load Subtitle", "", "SRT Files (*.srt)")
        if path:
            self.subtitles = SrtParser.parse(path)
            self.lbl_subtitle.show()

    def _toggle_miniplayer(self):
        self.is_miniplayer = not self.is_miniplayer
        if self.is_miniplayer:
            self.normal_geometry = self.geometry()
            self.setWindowFlag(Qt.WindowStaysOnTopHint, True)
            self.setMinimumSize(576, 576) # 6x6 inches
            self.resize(576, 576)
            self.header_widget.hide()
        else:
            self.setWindowFlag(Qt.WindowStaysOnTopHint, False)
            self.setMinimumSize(600, 400) # Restore normal bounds
            if getattr(self, 'normal_geometry', None): self.setGeometry(self.normal_geometry)
            self.header_widget.show()
        self.show() # Critical to prevent window controls from disabling on Linux/Windows

    def _update_brightness(self):
        val, ok = QInputDialog.getInt(self, "Brightness", "Set Brightness (0-100, 50=Normal):", getattr(self, 'brightness_level', 50), 0, 100, 5)
        if ok:
            self.brightness_level = val
            if val < 50:
                alpha = int((50 - val) * 255 / 50)
                self.brightness_overlay.setStyleSheet(f"background-color: rgba(0, 0, 0, {alpha});")
            elif val > 50:
                # True Brightness Screen Blend
                alpha = int((val - 50) * 255 / 50)
                self.brightness_overlay.setStyleSheet(f"background-color: rgba(255, 255, 255, {alpha}); mix-blend-mode: screen;")
            else:
                self.brightness_overlay.setStyleSheet("background-color: rgba(0, 0, 0, 0);")

    def _vol_up(self):
        if HAS_MULTIMEDIA and hasattr(self, 'audio'): 
            self.audio.setVolume(min(1.0, self.audio.volume() + 0.05))
            self._trigger_vol_osd()
            self.save_settings()
            
    def _vol_down(self):
        if HAS_MULTIMEDIA and hasattr(self, 'audio'): 
            self.audio.setVolume(max(0.0, self.audio.volume() - 0.05))
            self._trigger_vol_osd()
            self.save_settings()
            
    def _toggle_mute(self):
        if HAS_MULTIMEDIA and hasattr(self, 'audio'): 
            self.audio.setMuted(not self.audio.isMuted())
            self._trigger_vol_osd()
        
    def _check_loop(self, pos):
        if getattr(self, 'loop_is_media', False) and getattr(self, 'loop_b', -1) > 0 and pos >= self.loop_b:
            self.player.setPosition(max(0, getattr(self, 'loop_a', 0)))
            
    def _set_ab_loop(self):
        is_media = (self.stack.currentIndex() == self.IDX_MEDIA and HAS_MULTIMEDIA)
        c = self.player.position() if is_media else self.current_index
        if getattr(self, 'loop_a', -1) == -1: 
            self.loop_a = c; self.loop_is_media = is_media; 
            QMessageBox.information(self, "Loop", "Point A Set")
        elif getattr(self, 'loop_b', -1) == -1: 
            self.loop_b = c; 
            QMessageBox.information(self, "Loop", "Point B Set. Looping Active.")
            
            # Feature: Record A-B loops to Macro!
            if self.macro_recording and not self.macro_paused and is_media:
                self._record_macro_action(f"{self.current_index + 1} start={self.loop_a/1000:.2f} end={self.loop_b/1000:.2f} dur=0")
        else: 
            self.loop_a, self.loop_b = -1, -1; 
            QMessageBox.information(self, "Loop", "Loop Cleared")
            
    def _toggle_playback(self):
        # Docs Mode (TXT, CSV, MD, PDF, WEB) and Images should all act as Slideshows
        if self.stack.currentIndex() in [self.IDX_IMG, self.IDX_WEB, self.IDX_TXT, self.IDX_CSV, self.IDX_MD, self.IDX_PDF]: 
            self._toggle_slide()
        elif hasattr(self, 'player'):
            # FIX: Use correct PySide6 PlaybackState enum
            if self.player.playbackState() == QMediaPlayer.PlaybackState.PlayingState: 
                self.player.pause()
                self.wave_vis.stop_vis()
                self._record_macro_action(f"{self.current_index + 1} pause dur=0")
            else: 
                self.player.play()
                if self.stack.currentIndex() == self.IDX_MEDIA and not self.video_view.isVisible(): self.wave_vis.start_vis()
                self._record_macro_action(f"{self.current_index + 1} play dur=0")
                
    def _toggle_slide(self, force=None):
        self.is_slideshow_active = force if force is not None else not self.is_slideshow_active
        if self.is_slideshow_active:
            self.slide_elapsed = 0; self.slide_progress.setValue(0); 
            if self.prog_mode == "SHOW": self.slide_progress.show()
            self.slide_tick_timer.start(30)
        else:
            self.slide_tick_timer.stop(); self.slide_progress.hide(); self.slide_progress.setValue(0)
            
    def _slide_tick(self):
        is_adv = hasattr(self, 'adv_sequence') and len(self.adv_sequence) > 0
        is_ab = getattr(self, 'loop_a', -1) != -1 and getattr(self, 'loop_b', -1) != -1 and not getattr(self, 'loop_is_media', False)
        
        t_ms = self.slideshow_speed
        if is_adv: 
            if self.adv_current_instruction['type'] == 'cmd' and self.adv_current_instruction['cmd'] == 'wait':
                t_ms = self.adv_current_instruction.get('dur', 0)
            else:
                t_ms = self.adv_current_instruction.get('dur', self.slideshow_speed)
        elif is_ab: t_ms = getattr(self, 'ab_gif_speed_ms', 150)
        
        if is_adv and self.adv_current_instruction['type'] == 'frame':
            if HAS_MULTIMEDIA and self.stack.currentIndex() == self.IDX_MEDIA:
                target_end = self.adv_current_instruction.get('end', -1)
                if target_end > 0 and self.player.position() >= (target_end * 1000):
                    self.slide_elapsed = t_ms
        
        # CRITICAL FIX: Only auto-scroll if we are NOT running an macro script
        if self.is_webpage_mode and not is_adv:
            b = self.vert_view.verticalScrollBar(); b.setValue(b.value() + 2) 
            if b.value() >= b.maximum(): self._toggle_slide()
        elif self.stack.currentIndex() == self.IDX_PDF and HAS_PDF and self.btn_pdf_scroll.isChecked() and not is_adv:
            pass
        else:
            self.slide_elapsed += 30
            pct = min(100, int((self.slide_elapsed / t_ms) * 100)) if t_ms > 0 else 100
            if self.prog_mode == "SHOW": self.slide_progress.setValue(pct)
            
            if self.slide_elapsed >= t_ms:
                self.slide_elapsed = 0
                if is_adv:
                    self.adv_idx = (self.adv_idx + 1) % len(self.adv_sequence)
                    self._execute_vmsl_instruction()
                else:
                    end = self.loop_b if getattr(self, 'loop_b', -1) != -1 and not getattr(self, 'loop_is_media', False) else len(self.active_playlist) - 1
                    if self.current_index < end: self._next_item()
                    else:
                        if getattr(self, 'loop_a', -1) != -1 and not getattr(self, 'loop_is_media', False): 
                            self.current_index = self.loop_a; self._load_current_item()
                        else: self._toggle_slide()

    def _next_item(self):
        if not self.active_playlist: return
        if self.current_index < len(self.active_playlist) - 1: 
            self.current_index += 1
            self._record_macro_action(f"{self.current_index + 1} dur=0")
            self._load_current_item()
            
    def _prev_item(self):
        if not self.active_playlist: return
        if self.current_index > 0: 
            self.current_index -= 1
            self._record_macro_action(f"{self.current_index + 1} dur=0")
            self._load_current_item()

    def _toggle_fullscreen(self):
        self.showNormal() if self.isFullScreen() else self.showFullScreen()
        self._fade_out_controls()
        
        # FIX: Force the layout to immediately auto-fit the media to the new screen size
        QTimer.singleShot(50, lambda: self.resizeEvent(None))

    def _toggle_hide(self):
        if self.isVisible(): self.hide()
        else: self.show(); self.raise_(); self.activateWindow()

    def closeEvent(self, ev):
        self.save_settings()
        if hasattr(self, 'slide_tick_timer'): self.slide_tick_timer.stop()
        if hasattr(self, 'wave_vis'): self.wave_vis.stop_vis()
        if HAS_MULTIMEDIA and hasattr(self, 'player') and self.player is not None:
            self.player.stop(); self.player.setSource(QUrl()); self.player.setVideoOutput(None); self.player.setAudioOutput(None); self.player.deleteLater(); self.player = None
        if HAS_MULTIMEDIA and hasattr(self, 'audio') and self.audio is not None:
            self.audio.deleteLater(); self.audio = None
        if hasattr(self, 'sc_hide'): self.sc_hide.setEnabled(False); self.sc_hide.deleteLater(); del self.sc_hide
        if hasattr(self, 'sc_mute'): self.sc_mute.setEnabled(False); self.sc_mute.deleteLater(); del self.sc_mute
        parent_win = self.parent()
        if parent_win and hasattr(parent_win, 'active_viewers') and self in parent_win.active_viewers:
            parent_win.active_viewers.remove(self)
        super().closeEvent(ev)
