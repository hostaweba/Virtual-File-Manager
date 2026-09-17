# search.py
import os
import sys
import sqlite3
import subprocess
import calendar
import datetime
import math
import hashlib
import csv
import re
import time
from collections import defaultdict
from pathlib import Path

# Silence harmless Qt Font & PNG profile warnings
os.environ["QT_LOGGING_RULES"] = "qt.qpa.fonts.warning=false;qt.gui.imageio.warning=false"

from PySide6.QtCore import Qt, QDate, QTime, QDateTime, QThread, Signal, QSize, QFileInfo, QSettings
from PySide6.QtGui import QAction, QFont, QIcon, QColor, QBrush, QTextCursor, QCursor, QShortcut, QKeySequence
from PySide6.QtWidgets import (
    QMainWindow, QVBoxLayout, QHBoxLayout, QGridLayout, 
    QLabel, QPushButton, QWidget, QLineEdit, QComboBox, 
    QCheckBox, QDoubleSpinBox, QSpinBox, QDateEdit, QTimeEdit, 
    QTableWidget, QTableWidgetItem, QHeaderView, QMessageBox, 
    QMenu, QDateTimeEdit, QApplication, QProgressBar, 
    QProgressDialog, QStyle, QFrame, QFormLayout, QDialog, 
    QFileIconProvider, QSizePolicy, QScrollArea, QPlainTextEdit, 
    QTabWidget, QButtonGroup, QRadioButton, QFileDialog, 
    QColorDialog, QInputDialog, QGroupBox, QTextBrowser, QWidgetAction,
    QListWidget, QListWidgetItem , QStackedWidget  # <-- Added these two imports
)
from themes import THEMES

try:
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.figure import Figure
    from matplotlib.collections import PatchCollection
    import matplotlib.dates as mdates
    import matplotlib.patches as mpatches
    MATPLOTLIB_AVAILABLE = True
except Exception:
    MATPLOTLIB_AVAILABLE = False


CAT_MAP = {
    "Images": ['.arw', '.avif', '.bmp', '.cr2', '.gif', '.heic', '.heif', '.ico', '.jfif', '.jp2', '.jpe', '.jpeg', '.jpg', '.nef', '.pbm', '.pcx', '.pgm', '.png', '.pnm', '.ppm', '.psd', '.raw', '.svg', '.tga', '.tif', '.tiff', '.webp'],
    "Videos": ['.3gp', '.3gpp', '.avi', '.flv', '.m2ts', '.m4v', '.mkv', '.mng', '.mov', '.mp4', '.ogv', '.swf', '.vob', '.webm', '.wmv'],
    "Audio": ['.aac', '.aif', '.aifc', '.aiff', '.alac', '.amr', '.au', '.bnk', '.flac', '.m4a', '.mid', '.midi', '.mp3', '.ogg', '.opus', '.wav', '.wma'],
    "Design": ['.ai', '.cur', '.eps', '.exr', '.fla', '.fon', '.gpl'],
    "Documents": ['.adoc', '.chm', '.doc', '.docs', '.docx', '.dot', '.dotx', '.epub', '.fodt', '.md', '.mobi', '.odp', '.ods', '.odt', '.pdf', '.ppsx', '.ppt', '.pptx', '.rtf', '.tex', '.text', '.txt'],
    "Data": ['.accda', '.accdb', '.accde', '.accdu', '.arff', '.bib', '.csv', '.db', '.db-journal', '.db-shm', '.db-wal', '.dbf', '.dic', '.dict', '.fdb', '.frm', '.h5', '.ibd', '.mat', '.mdb', '.npz', '.parquet', '.pkl', '.sqlite', '.sqlite3', '.sqlitedb', '.tsv'],
    "Code": ['.a', '.asm', '.asp', '.aspx', '.awk', '.bash', '.bash_logout', '.bashrc', '.bat', '.c', '.cc', '.class', '.cmake', '.cmd', '.coffeescript', '.cpp', '.cs', '.css', '.cu', '.cxx', '.d', '.diff', '.el', '.erl', '.es', '.f', '.f90', '.fs', '.g4', '.go', '.groovy', '.h', '.hpp', '.htm', '.html', '.java', '.js', '.jsx', '.less', '.lua', '.php', '.pl', '.ps1', '.pug', '.py', '.rb', '.rs', '.sass', '.scss', '.sh', '.shx', '.sql', '.swift', '.ts', '.tsx', '.vbs', '.wasm'],
    "Configs": ['.apache', '.apache2', '.cfg', '.conf', '.dtd', '.env', '.fish', '.ini', '.json', '.manifest', '.plist', '.properties', '.toml', '.xml', '.yaml', '.yml'],
    "Python": ['.egg', '.egg-info'],
    "Apps": ['.apk', '.apks', '.app', '.appimage', '.asar', '.bin', '.com', '.command', '.cpl', '.deb', '.desktop', '.dmg', '.efi', '.exe', '.jar', '.msi', '.rpm', '.run', '.whl', '.xapk'],
    "System": ['.dat', '.dll', '.drv', '.dylib', '.inf', '.kext', '.pid', '.reg', '.so', '.sys', '.sysk'],
    "Archives": ['.7z', '.apkzstd', '.bz2', '.cab', '.gz', '.iso', '.lz4', '.lzma', '.rar', '.tar', '.tgz', '.xz', '.z', '.zip'],
    "Forensics": ['.crash', '.dmp', '.evtx', '.har', '.mem', '.pcap', '.pcapng', '.ulog'],
    "Security": ['.asc', '.cer', '.certs', '.crl', '.crt', '.der', '.gpg', '.key', '.p12', '.pem', '.pfx', '.pub', '.rsa'],
    "VMs": ['.ova', '.ovf', '.qcow2', '.vdi', '.vhd', '.vhdx', '.vmdk'],
    "3D": ['.blend', '.dae', '.dwg', '.dxf', '.fbx', '.obj', '.step', '.stl'],
    "GIS": ['.geojson', '.gpx', '.kml', '.kmz', '.shp'],
    "Fonts": ['.afm', '.eot', '.otf', '.pfb', '.ttf', '.woff', '.woff2'],
    "Chats": ['.crypt1', '.crypt12', '.crypt14', '.crypt15', '.vcf', '.wa'],
    "Temp": ['.~', '.backup', '.bak', '.cache', '.chk', '.clean', '.crdownload', '.download', '.dthumb', '.exo', '.log', '.nomedia', '.old', '.part', '.swatch', '.swp', '.temp', '.thumb', '.thumbnails', '.tmp', '.trashed']
}
EXT_TO_CAT = {}
for cat, exts in CAT_MAP.items():
    for ext in exts: EXT_TO_CAT[ext] = cat

# Provides instant UI mapping for the 20 categories to prevent KeyErrors
GLOBAL_CAT_COLORS = {
    "Images": "#a371f7", "Videos": "#f85149", "Audio": "#ff7b72", "Design": "#ff9ade",
    "Documents": "#d2a8ff", "Data": "#58a6ff", "Code": "#79c0ff", "Configs": "#7ee787",
    "Python": "#e3b341", "Apps": "#ff9429", "System": "#8b949e", "Archives": "#e3b341",
    "Forensics": "#ff7b72", "Security": "#f0883e", "VMs": "#8b949e", "3D": "#a371f7",
    "GIS": "#3fb950", "Fonts": "#c9d1d9", "Chats": "#2ea043", "Temp": "#484f58", "Others": "#8b949e"
}


class ExtFilterDialog(QDialog):
    def __init__(self, exts, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Filter Found Extensions")
        self.resize(550, 650)
        
        # --- Native Light/Dark Theme Enforcer ---
        is_dark = True
        if parent and hasattr(parent, 'main_app') and hasattr(parent.main_app, 'theme_combo'):
            is_dark = parent.main_app.theme_combo.currentText() == "Dark"
        else:
            is_dark = QApplication.palette().window().color().lightness() < 128
            
        bg_col = "#161b22" if is_dark else "#f6f8fa"
        lbl_col = "#c9d1d9" if is_dark else "#24292f"
        input_bg = "#0d1117" if is_dark else "#ffffff"
        brd_col = "#30363d" if is_dark else "#d0d7de"
        
        # FIX: Removed the global "QWidget" selector so checkbox ticks render properly!
        self.setStyleSheet(f"""
            QDialog {{ background-color: {bg_col}; color: {lbl_col}; }}
            QLabel, QCheckBox {{ color: {lbl_col}; }}
            QLineEdit, QComboBox {{ background-color: {input_bg}; color: {lbl_col}; border: 1px solid {brd_col}; border-radius: 4px; padding: 4px; }}
            QGroupBox {{ border: 1px solid {brd_col}; border-radius: 6px; margin-top: 15px; padding-top: 15px; color: {lbl_col}; font-weight: bold; }}
            QGroupBox::title {{ subcontrol-origin: margin; left: 10px; top: 0px; color: {lbl_col}; }}
            QScrollArea, #scrollContainer {{ background-color: {bg_col}; border: 1px solid {brd_col}; }}
        """)
        
        self.settings = QSettings("VirtualMan", "ExtPresets")
        layout = QVBoxLayout(self)
        
        preset_group = QGroupBox("💾 Filter Presets")
        p_lay = QHBoxLayout(preset_group)
        self.combo_presets = QComboBox()
        self.update_presets_combo()
        btn_load = QPushButton("Load"); btn_load.clicked.connect(self.load_preset)
        btn_save = QPushButton("Save"); btn_save.clicked.connect(self.save_preset)
        btn_del = QPushButton("Delete"); btn_del.clicked.connect(self.delete_preset)
        p_lay.addWidget(self.combo_presets, stretch=1); p_lay.addWidget(btn_load); p_lay.addWidget(btn_save); p_lay.addWidget(btn_del)
        layout.addWidget(preset_group)
        
        paste_group = QGroupBox("📋 Bulk Untick")
        pst_lay = QHBoxLayout(paste_group)
        self.txt_paste = QLineEdit(); self.txt_paste.setPlaceholderText("Paste exts to untick (e.g. .jpg, .dll)")
        btn_untick = QPushButton("Untick Pasted")
        btn_untick.clicked.connect(self.untick_pasted)
        pst_lay.addWidget(self.txt_paste, stretch=1); pst_lay.addWidget(btn_untick)
        layout.addWidget(paste_group)
        
        layout.addWidget(QLabel("<b>Uncheck extensions to hide them from the final results:</b>"))
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        container = QWidget()
        container.setObjectName("scrollContainer") # Targets the specific background cleanly
        grid = QGridLayout(container)
        self.checkboxes = {}
        
        row, col = 0, 0
        for ext in sorted(exts):
            disp = ext if ext else "[Folders / No Ext]"
            cb = QCheckBox(disp); cb.setChecked(True)
            self.checkboxes[ext] = cb
            grid.addWidget(cb, row, col)
            col += 1
            if col > 2: 
                col = 0
                row += 1
                
        scroll.setWidget(container)
        layout.addWidget(scroll, stretch=1)
        
        btn_box_top = QHBoxLayout()
        btn_sel_all = QPushButton("Select All"); btn_sel_all.clicked.connect(lambda: self.toggle_all(True))
        btn_desel_all = QPushButton("Deselect All"); btn_desel_all.clicked.connect(lambda: self.toggle_all(False))
        btn_copy = QPushButton("Copy Shown Exts")
        btn_copy.clicked.connect(lambda: QApplication.clipboard().setText(", ".join([e for e in exts if e])))
        btn_box_top.addWidget(btn_sel_all); btn_box_top.addWidget(btn_desel_all); btn_box_top.addWidget(btn_copy)
        layout.addLayout(btn_box_top)
        
        btn_ok = QPushButton("Apply Filters & Render")
        btn_ok.setStyleSheet("background-color: #2ea043; color: white; font-weight: bold; padding: 12px; border-radius: 6px;")
        
        # --- FIX: Enable Enter key to trigger Apply ---
        btn_ok.setAutoDefault(True)
        btn_ok.setDefault(True)
        QShortcut(QKeySequence("Return"), self).activated.connect(self.accept)
        QShortcut(QKeySequence("Enter"), self).activated.connect(self.accept)
        
        btn_ok.clicked.connect(self.accept)
        layout.addWidget(btn_ok)
        
    def update_presets_combo(self):
        self.combo_presets.clear()
        presets = self.settings.value("presets", {})
        if presets: self.combo_presets.addItems(list(presets.keys()))
        
    def save_preset(self):
        name, ok = QInputDialog.getText(self, "Save Preset", "Enter Preset Name:")
        if ok and name.strip():
            allowed = list(self.get_allowed())
            presets = self.settings.value("presets", {})
            if not isinstance(presets, dict): presets = {}
            presets[name.strip()] = allowed
            self.settings.setValue("presets", presets)
            self.update_presets_combo()
            self.combo_presets.setCurrentText(name.strip())
            
    def load_preset(self):
        name = self.combo_presets.currentText()
        if not name: return
        presets = self.settings.value("presets", {})
        if name in presets:
            allowed = set(presets[name])
            for ext, cb in self.checkboxes.items():
                cb.setChecked(ext in allowed)
                
    def delete_preset(self):
        name = self.combo_presets.currentText()
        if not name: return
        presets = self.settings.value("presets", {})
        if name in presets:
            del presets[name]
            self.settings.setValue("presets", presets)
            self.update_presets_combo()
                
    def untick_pasted(self):
        text = self.txt_paste.text()
        if not text: return
        exts_to_untick = [e.strip().lower() for e in text.split(',') if e.strip()]
        for e in exts_to_untick:
            if not e.startswith('.') and e != "[folders / no ext]": e = '.' + e
            if e in self.checkboxes:
                self.checkboxes[e].setChecked(False)

    def toggle_all(self, state):
        for cb in self.checkboxes.values(): cb.setChecked(state)
        
    def get_allowed(self):
        return set(ext for ext, cb in self.checkboxes.items() if cb.isChecked())

#from PySide6.QtWidgets import QTextBrowser, QWidgetAction

class ClickableMenuLabel(QLabel):
    clicked = Signal()
    def mouseReleaseEvent(self, ev):
        if ev.button() == Qt.LeftButton:
            self.clicked.emit()
        super().mouseReleaseEvent(ev)
        
    def enterEvent(self, ev):
        self.setStyleSheet(self.styleSheet() + "background-color: #30363d;")
        super().enterEvent(ev)
        
    def leaveEvent(self, ev):
        self.setStyleSheet(self.styleSheet().replace("background-color: #30363d;", ""))
        super().leaveEvent(ev)

class TemplateBuilderDialog(QDialog):
    def __init__(self, sample_path, parent=None):
        super().__init__(parent)
        self.setWindowTitle("🏗️ Exact Template Builder")
        self.resize(800, 250)
        
        # Match parent theme
        if parent and hasattr(parent, 'styleSheet'): self.setStyleSheet(parent.styleSheet())
        
        layout = QVBoxLayout(self)
        
        lbl_info = QLabel(
            "<b>1. Your Sample Path:</b><br>"
            "<span style='color:#8b949e;'>This is the exact full path of the file you clicked.</span>"
        )
        layout.addWidget(lbl_info)
        
        self.txt_sample = QLineEdit(sample_path)
        self.txt_sample.setReadOnly(True)
        self.txt_sample.setStyleSheet("background-color: #0d1117; color: #8b949e; padding: 6px; border: 1px solid #30363d;")
        layout.addWidget(self.txt_sample)
        
        lbl_inst = QLabel(
            "<br><b>2. Build Your Template:</b><br>"
            "• Replace the exact numbers you want to extract with <b>[YYYY]</b>, <b>[YY]</b>, <b>[MM]</b>, <b>[DD]</b>, <b>[HH]</b>, <b>[mm]</b>, <b>[ss]</b>.<br>"
            "• Replace any folder names or text that changes between files with <b>*</b><br>"
            "<span style='color:#e3b341;'>Example:</span> <code>*/Private/[YY]/*/[MM]/[DD]/Screenshot_[YYYY][MM][DD]_[HH][mm][ss].*</code>"
        )
        layout.addWidget(lbl_inst)
        
        self.txt_template = QLineEdit(sample_path)
        self.txt_template.setStyleSheet("background-color: #0d1117; color: #58a6ff; font-weight: bold; padding: 6px; border: 1px solid #58a6ff;")
        layout.addWidget(self.txt_template)
        
        layout.addSpacing(10)
        btn_apply = QPushButton("Apply Exact Template to Highlighted Files")
        btn_apply.setStyleSheet("background-color: #2ea043; color: white; font-weight: bold; padding: 10px; border-radius: 4px;")
        btn_apply.clicked.connect(self.accept)
        layout.addWidget(btn_apply)

    def get_template(self):
        return self.txt_template.text()

# --- Advanced Forensic Timestamp Corrector Engine ---
class TimestampCorrectorDialog(QDialog):
    def __init__(self, selected_items, db_path, parent=None):
        super().__init__(parent)
        self.setWindowTitle("🕰️ Forensic Programmable Timestamp Corrector")
        self.resize(1150, 650)
        self.setWindowFlags(self.windowFlags() | Qt.WindowMinimizeButtonHint | Qt.WindowMaximizeButtonHint | Qt.WindowCloseButtonHint)
        self.setWindowModality(Qt.NonModal) 
        if parent and hasattr(parent, 'styleSheet'): self.setStyleSheet(parent.styleSheet())
        self.db_path = db_path
        self.selected_items = selected_items
        self.main_app = parent
        self.items_to_fix = []
        
        layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)
        
        # ==========================================
        # TAB 1: SYNTAX, EXAMPLES & CONFIGURATION
        # ==========================================
        tab_config = QWidget()
        conf_main_lay = QHBoxLayout(tab_config) 
        
        guide_txt = (
            "<div style='line-height: 1.4; padding-right: 10px; font-size: 13px;'>"
            "<h3 style='color:#58a6ff; margin-top: 0; margin-bottom: 5px;'>📖 How To Use</h3>"
            "<b>1. Auto-Scan:</b> Write a pattern below and click 'Auto-Scan'. It tests the pattern against the Scope (Path, Name, or Both).<br>"
            "<b>2. Manual Mode:</b> Click 'Load Files' to skip scanning. Go to 'Review & Apply', highlight files, right-click, and force-extract dates from specific text.<br>"
            "<b>3. Apply:</b> <span style='color:#58a6ff;'>Blue</span> rows are identical. <span style='color:#3fb950;'>Green</span> rows have newly extracted dates. <span style='color:#f85149;'>Red</span> failed. Highlight rows, right-click, and Apply.<br><br>"
            
            "<h3 style='color:#e3b341; margin-top: 10px; margin-bottom: 5px;'>⚙️ Token Syntax</h3>"
            "• <b style='color:#58a6ff;'>YYYY, YY</b>: Year | <b style='color:#58a6ff;'>MMM</b>: Month Name (Jan) | <b style='color:#58a6ff;'>MM, M</b>: Month<br>"
            "• <b style='color:#58a6ff;'>DD, D</b>: Day | <b style='color:#58a6ff;'>HH, mm, ss</b>: Time<br>"
            "• <b style='color:#3fb950;'>**</b>: Greedy Skip (Jumps across multiple folders/slashes)<br>"
            "• <b style='color:#3fb950;'>*</b>: Local Skip (Skips text inside the current folder/file only)<br>"
            "• <b style='color:#3fb950;'>&lt;dir&gt;</b>: Skips exactly one folder level<br><br>"
            
            "<h3 style='color:#c9d1d9; margin-top: 10px; margin-bottom: 5px;'>💡 Advanced Examples</h3>"
            "<b>1. Standard YYYYMMDD anywhere:</b><br>"
            "<code>**/*YYYYMMDD_HHmmss*</code> ➔ <span style='color:#8b949e;'>/A/B/IMG_<b>20240101_153000</b>.jpg</span><br>"
            "<b>2. Year/Month in Path, Day in File:</b><br>"
            "<code>**/YY/&lt;dir&gt;/MM/DD/**</code> ➔ <span style='color:#8b949e;'>/docs/<b>23</b>/work/<b>12</b>/<b>05</b>.png</span><br>"
            "<b>3. Target Specific Level (Ignore subfolders):</b><br>"
            "<code>*/YYYY.MM.DD/*/**</code> ➔ <span style='color:#8b949e;'>/drive/<b>2024.01.05</b>/sub/file.txt</span><br>"
            "<b>4. Months as Words:</b><br>"
            "<code>**/YY/MMM/** D.*</code> ➔ <span style='color:#8b949e;'>/docs/<b>23</b>/<b>February</b>/my files <b>20</b>.py</span><br>"
            "</div>"
        )
        self.guide_browser = QTextBrowser()
        self.guide_browser.setHtml(guide_txt)
        self.guide_browser.setStyleSheet("""
            QTextBrowser { background-color: transparent; border: none; }
            QScrollBar:vertical { width: 6px; background: transparent; }
            QScrollBar::handle:vertical { background: #58a6ff; border-radius: 3px; }
        """)
        conf_main_lay.addWidget(self.guide_browser, stretch=1)
        
        right_panel = QWidget()
        right_lay = QVBoxLayout(right_panel)
        right_lay.setContentsMargins(10, 0, 0, 0)
        
        self.txt_patterns = QPlainTextEdit()
        # FIX: Advanced Defaults that automatically catch YYYYMMDD schemas and deep nested paths
        self.txt_patterns.setPlainText("**/*YYYYMMDD_HHmmss*\n**/*YYYY-MM-DD HH-mm-ss*\n**/YY/<dir>/MM/DD/**\n**/YY/<dir>/MM/<dir>/DD/**\n**/*YYYY.MM.DD*/**\n**/YY/MMM/** D.*\n*DMMM*YYYY*\n*?YYYYMMDD?*")
        self.txt_patterns.setStyleSheet("color: #e3b341; font-family: Consolas; font-weight: bold; font-size: 13px; background-color: #0d1117; padding: 5px; border: 1px solid #30363d; border-radius: 4px;")
        
        self.combo_modify_target = QComboBox()
        self.combo_modify_target.addItems(["Modified Date (Default)", "Created Date", "Both (Modified & Created)"])
        self.combo_modify_target.setStyleSheet("font-weight: bold; padding: 6px; font-size: 13px;")
        
        # --- NEW: Target Scope (Allows user to prioritize File vs Path) ---
        self.combo_scan_scope = QComboBox()
        self.combo_scan_scope.addItems(["Scan: Full Virtual Path + Filename", "Scan: Filename Only", "Scan: Virtual Path Only"])
        self.combo_scan_scope.setStyleSheet("font-weight: bold; padding: 6px; font-size: 13px; color: #58a6ff;")
        
        right_lay.addWidget(QLabel("<b>Custom Regex Patterns:</b><br>(Scans top-to-bottom)"))
        right_lay.addWidget(self.txt_patterns, stretch=1)
        right_lay.addWidget(QLabel("<b>Extraction Scope:</b>"))
        right_lay.addWidget(self.combo_scan_scope)
        right_lay.addWidget(QLabel("<b>Target OS Property:</b>"))
        right_lay.addWidget(self.combo_modify_target)
        right_lay.addSpacing(15)
        
        btn_load_manual = QPushButton("📋 Load Files (Manual / Context Edit Mode)")
        btn_load_manual.setStyleSheet("padding: 10px; font-weight: bold;")
        btn_load_manual.clicked.connect(self.load_manual_files)
        
        btn_scan = QPushButton("🔍 Auto-Scan & Suggest from Patterns")
        btn_scan.setStyleSheet("background-color: #1f6feb; color: white; font-weight: bold; padding: 10px;")
        btn_scan.clicked.connect(self.scan_files)
        
        right_lay.addWidget(btn_load_manual)
        right_lay.addWidget(btn_scan)
        
        conf_main_lay.addWidget(right_panel, stretch=1)
        self.tabs.addTab(tab_config, "⚙️ Configuration & Syntax")
        
        # ==========================================
        # TAB 2: RESULTS TABLE
        # ==========================================
        tab_table = QWidget()
        lay_table = QVBoxLayout(tab_table)
        
        self.progress = QProgressBar(); self.progress.setVisible(False)
        self.progress.setFixedHeight(24); self.progress.setAlignment(Qt.AlignCenter)
        self.progress.setStyleSheet("""
            QProgressBar { border: 1px solid #30363d; border-radius: 4px; background: #0d1117; color: #ffffff; font-weight: bold; text-align: center; }
            QProgressBar::chunk { background: #2ea043; border-radius: 3px; }
        """)
        lay_table.addWidget(self.progress)
        
        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(["Filename", "Original Date", "Select Correct Date", "Match Pattern", "Virtual Path Used"])
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setColumnWidth(0, 250); self.table.setColumnWidth(1, 140); self.table.setColumnWidth(2, 190)
        self.table.setColumnWidth(3, 200)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)
        self.table.setSortingEnabled(True)
        lay_table.addWidget(self.table)
        
        self.tabs.addTab(tab_table, "📋 Review & Apply")
        
        # ==========================================
        # TAB 3: HELP & OPERATIONS
        # ==========================================
        tab_help = QWidget()
        lay_help = QVBoxLayout(tab_help)
        help_browser = QTextBrowser()
        help_browser.setStyleSheet("background-color: transparent; border: none; font-size: 14px; color: #c9d1d9;")
        
        help_html = """
        <h2 style='color:#58a6ff;'>🚀 Fast Workflow Guide</h2>
        <p><b>1. Auto-Scan vs Manual:</b> Use Auto-Scan for common patterns (like standard YYYYMMDD). Use <b>Manual Mode</b> to load files directly into the grid and use the right-click Context Menu for absolute precision.</p>
        <p><b>2. 🏗️ Exact Template Builder:</b> Right-click any file -> <i>Mix & Match -> Build Exact Template</i>. Replace numbers with <b>[YYYY]</b>, <b>[MM]</b>, <b>[DD]</b>, <b>[HH]</b>, etc., and replace changing folder/file names with <b>*</b>. The engine will instantly parse all table files using that exact mapping.</p>
        
        <h2 style='color:#e3b341;'>🎨 Row Color Legend</h2>
        <ul>
            <li><b style='color:#46a043;'>Green (Success - Corrected):</b> The date was extracted successfully and is DIFFERENT from the original database date.</li>
            <li><b style='color:#e3b341;'>Light Orange (Success - Unchanged):</b> The date was extracted successfully, but it is EXACTLY THE SAME as the original database date.</li>
            <li><b style='color:#f85149;'>Red (Failed):</b> The extraction failed to find a valid date.</li>
        </ul>
        
        <h2 style='color:#58a6ff;'>🖱️ Table Operations & Context Menu</h2>
        <ul>
            <li><b>Highlighting:</b> Click and drag, or hold Shift/Ctrl to highlight multiple rows before right-clicking to apply bulk extractions.</li>
            <li><b>Targeted Extraction:</b> If only the Year is wrong, highlight the files -> right-click -> <i>Extract Specific Parts -> Extract Year -> From Folder Name</i>. It will update the Year but leave the Month, Day, and Time completely untouched!</li>
            <li><b>Custom Highlight:</b> Use the Visibility menu to apply a custom transparent color to your highlighted rows to easily see what you are working on.</li>
        </ul>
        """
        help_browser.setHtml(help_html)
        lay_help.addWidget(help_browser)
        self.tabs.addTab(tab_help, "💡 Help & Operations")
        
        
 
 
    def _parse_custom_syntax(self, raw_pat):
        p = raw_pat.strip()
        if not p: return None
        
        p = p.replace('\\', '/')
        safe_p = ""
        special_chars = r"()[]{}.^$|+" 
        for char in p:
            if char in special_chars: safe_p += "\\" + char
            else: safe_p += char
        p = safe_p
        
        # 3. Robust Translators
        p = p.replace(' ', r'\s+')     
        p = p.replace('**', r'.*')         # Cross-directory greedy skip
        p = p.replace('*', r'[^/]*')       # In-directory skip (forces exact path locking)
        p = p.replace('<dir>', r'[^/]+')   # Skip exactly one folder name
        p = p.replace('<ext>', r'\.[^./]+$') 
        p = p.replace('<file>', r'[^/]+') 
        p = p.replace('/', r'/+')   
        
        counters = defaultdict(int)
        def repl(t_type, reg):
            counters[t_type] += 1
            return f"(?P<{t_type}_{counters[t_type]}>{reg})"
            
        # FIX: Removed negative lookarounds so YYYYMMDD compiles directly
        p = re.sub(r'YYYY', lambda m: repl('Y4', r'\d{4}'), p)
        p = re.sub(r'YY', lambda m: repl('Y2', r'\d{2}'), p)
        p = re.sub(r'MMM', lambda m: repl('M3', r'jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?'), p, flags=re.IGNORECASE)
        p = re.sub(r'MM', lambda m: repl('M2', r'0[1-9]|1[0-2]|\d{2}'), p)
        p = re.sub(r'\bM\b', lambda m: repl('M1', r'[1-9]|1[0-2]|\d{1,2}'), p)
        p = re.sub(r'DD', lambda m: repl('D2', r'0[1-9]|[12]\d|3[01]|\d{2}'), p)
        p = re.sub(r'\bD\b', lambda m: repl('D1', r'[1-9]|[12]\d|3[01]|\d{1,2}'), p)
        p = re.sub(r'HH', lambda m: repl('H2', r'[0-1]\d|2[0-3]|\d{2}'), p)
        p = re.sub(r'mm', lambda m: repl('m2', r'[0-5]\d|\d{2}'), p)
        p = re.sub(r'ss', lambda m: repl('s2', r'[0-5]\d|\d{2}'), p)
        p = re.sub(r'\?(.*?)\?', lambda m: f"(?:{m.group(1)})?", p) 
        
        try: return re.compile(p, re.IGNORECASE)
        except Exception: return None
            
    def load_manual_files(self):
        self.tabs.setCurrentIndex(1)
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        self.items_to_fix = []
        
        self.progress.setVisible(True)
        self.progress.setMaximum(len(self.selected_items))
        self.progress.setFormat("Loading: %p% (%v/%m)")
        QApplication.processEvents()
        
        for i, item in enumerate(self.selected_items):
            self.add_item_to_table(i, item, None, "Manual Mode")
            if i % 20 == 0:
                self.progress.setValue(i); QApplication.processEvents()
                
        self.progress.setVisible(False)
        self.table.setSortingEnabled(True)

    def scan_files(self):
        self.tabs.setCurrentIndex(1)
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        self.items_to_fix = []
        patterns_text = self.txt_patterns.toPlainText().split('\n')
        
        regex_list = []
        for line in patterns_text:
            if not line.strip(): continue
            comp = self._parse_custom_syntax(line)
            if comp: regex_list.append((line.strip(), comp))
            
        if not regex_list: 
            self.table.setSortingEnabled(True)
            return QMessageBox.warning(self, "No Patterns", "Could not compile valid extraction patterns.")
            
        self.progress.setVisible(True)
        self.progress.setMaximum(len(self.selected_items))
        self.progress.setFormat("Scanning: %p% (%v/%m)")
        QApplication.processEvents()
        
        MONTH_MAP = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
        
        for i, item in enumerate(self.selected_items):
            match_found = False; best_dt = None; best_pat = ""
            
            try: orig_dt = datetime.datetime.strptime(str(item['mod']), "%Y-%m-%d %H:%M:%S")
            except: orig_dt = datetime.datetime.now()
            
            # Combine Virtual Path and Filename to seamlessly support OS independent path parsing
            v_path = item.get('p_path', '')
            filename = item['name']
            scope = self.combo_scan_scope.currentText()
            
            # --- FIX: Apply user extraction scope ---
            if "Filename Only" in scope:
                full_target = filename
            elif "Virtual Path Only" in scope:
                full_target = v_path.replace('\\', '/')
            else:
                full_target = f"{v_path}{filename}".replace('\\', '/')
            
            for pat_str, regex in regex_list:
                m = regex.search(full_target)
                if m:
                    gd = m.groupdict()
                    y_str = m_str = d_str = h_str = mm_str = s_str = m_name_str = None
                    
                    for k, v in gd.items():
                        if v is None: continue
                        # FIX: Added 'not x_str' to ensure stronger tokens (YYYY) aren't overwritten by weaker ones (YY)
                        if k.startswith('Y4_'): y_str = v
                        elif k.startswith('Y2_') and not y_str: y_str = '20' + v
                        elif k.startswith('M3_'): m_name_str = v
                        elif (k.startswith('M2_') or k.startswith('M1_')) and not m_name_str: m_str = v
                        elif (k.startswith('D2_') or k.startswith('D1_')) and not d_str: d_str = v
                        elif k.startswith('H2_') and not h_str: h_str = v
                        elif k.startswith('m2_') and not mm_str: mm_str = v
                        elif k.startswith('s2_') and not s_str: s_str = v
                        
                    y_val = int(y_str) if y_str else orig_dt.year
                    if m_name_str: m_val = MONTH_MAP.get(m_name_str[:3].lower(), 1)
                    else: m_val = int(m_str) if m_str else (orig_dt.month if not y_str else 1)
                    d_val = int(d_str) if d_str else (orig_dt.day if not y_str and not m_str else 1)
                    h_val = int(h_str) if h_str else orig_dt.hour
                    mm_val = int(mm_str) if mm_str else orig_dt.minute
                    s_val = int(s_str) if s_str else orig_dt.second
                    
                    try:
                        best_dt = datetime.datetime(y_val, m_val, d_val, h_val, mm_val, s_val)
                        best_pat = pat_str
                        match_found = True; break
                    except: pass
                if match_found: break
                        
            self.add_item_to_table(i, item, best_dt, best_pat)
            if i % 10 == 0:
                self.progress.setValue(i); QApplication.processEvents()
                
        self.progress.setVisible(False)
        self.table.setSortingEnabled(True)

    def _apply_row_color(self, r, cur_dt, is_fail=False):
        if is_fail:
            bg_color = QColor(248, 81, 73, 40) # Transparent Red (No Match / Failed)
        else:
            cur_ts = cur_dt.toSecsSinceEpoch()
            orig_str = self.table.item(r, 1).text()
            try: orig_dt = QDateTime.fromString(orig_str, "yyyy-MM-dd HH:mm:ss")
            except: orig_dt = QDateTime.currentDateTime()
            
            orig_ts = orig_dt.toSecsSinceEpoch()
            if cur_ts == orig_ts: 
                bg_color = QColor(227, 179, 65, 40) # Light Orange (Extracted Successfully, but Date is the Same)
            else: 
                bg_color = QColor(46, 160, 67, 40) # Green (Extracted Successfully AND Date is Different/Corrected)
                
        for c in [0, 1, 3, 4]: 
            if self.table.item(r, c):
                self.table.item(r, c).setBackground(bg_color)

    def add_item_to_table(self, row_idx, item, extracted_dt, pat_str):
        self.table.insertRow(row_idx)
        self.table.setItem(row_idx, 0, QTableWidgetItem(item['name']))
        
        t_prop = self.combo_modify_target.currentText()
        disp_date = item.get('creation_date', item['mod']) if "Created" in t_prop else item['mod']
        self.table.setItem(row_idx, 1, QTableWidgetItem(str(disp_date)))
        
        dt_edit = QDateTimeEdit()
        dt_edit.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        dt_edit.setCalendarPopup(True)
        dt_edit.setStyleSheet("background-color: #0d1117; color: #58a6ff; font-weight: bold; border: 1px solid #3fb950; padding: 2px;")
        
        is_fail = False
        if extracted_dt: 
            dt_edit.setDateTime(extracted_dt)
        else:
            is_fail = True
            try: dt_edit.setDateTime(QDateTime.fromString(str(disp_date), "yyyy-MM-dd HH:mm:ss"))
            except: dt_edit.setDateTime(QDateTime.currentDateTime())
            pat_str = "Unmatched / Failed" if pat_str == "" else pat_str
            
        self.table.setCellWidget(row_idx, 2, dt_edit)
        self.table.setItem(row_idx, 3, QTableWidgetItem(pat_str))
        
        v_path = item.get('p_path', '')
        full_v_path = f"{v_path}{item['name']}".replace('\\', '/')
        path_item = QTableWidgetItem(full_v_path)
        path_item.setData(Qt.UserRole, {'id': item['id'], 'old_date': str(item['mod']), 'tags': item.get('tags', ''), 'p_path': v_path, 'real_path': item.get('real_path', '')})
        self.table.setItem(row_idx, 4, path_item)
        
        self._apply_row_color(row_idx, dt_edit.dateTime(), is_fail)
        self.items_to_fix.append({'id': item['id'], 'real_path': item.get('real_path', ''), 'old_date': str(item['mod']), 'tags': item.get('tags', ''), 'v_path': v_path, 'dt_widget': dt_edit})

    def show_properties(self, row):
        item = self.items_to_fix[row]
        msg = f"<b>Filename:</b> {self.table.item(row, 0).text()}<br><br>" \
              f"<b>Virtual Scanned Path:</b> {self.table.item(row, 4).text()}<br><br>" \
              f"<b>Physical OS Path:</b> {item['real_path']}<br><br>" \
              f"<b>Original Database Date:</b> {item['old_date']}<br><br>" \
              f"<b>VMan Tags:</b> {item['tags']}"
        dlg = QMessageBox(self)
        dlg.setWindowTitle("File Properties")
        dlg.setText(msg)
        dlg.exec()


    def _add_glow_action(self, menu, label, source, f_type, callback):
        selected_rows = self.table.selectionModel().selectedRows()
        count = 0
        for idx in selected_rows:
            r = idx.row()
            name = self.table.item(r, 0).text()
            v_path = self.items_to_fix[r]['v_path'].strip('/')
            folder = v_path.split('/')[-1] if v_path else ""
            path = self.table.item(r, 4).text()
            
            target_text = name if source == "name" else (folder if source == "folder" else path)
            if self._has_fragment(target_text, f_type):
                count += 1
                
        detected = count > 0
        display_label = f"🌟 {label} ({count} files)" if detected else f"   {label}"
        
        act = QWidgetAction(menu)
        lbl = ClickableMenuLabel(display_label)
        if detected:
            lbl.setStyleSheet("color: #e3b341; font-weight: bold; font-size: 13px; background: transparent; padding: 4px 10px;")
        else:
            lbl.setStyleSheet("color: #c9d1d9; font-size: 13px; background: transparent; padding: 4px 10px;")
            
        act.setDefaultWidget(lbl)
        lbl.clicked.connect(callback)
        lbl.clicked.connect(menu.close)
        menu.addAction(act)

    def toggle_highlight_color(self):
        from PySide6.QtWidgets import QColorDialog
        from PySide6.QtGui import QColor
        color = QColorDialog.getColor(QColor(227, 179, 65, 100), self, "Select Highlight Color", QColorDialog.ShowAlphaChannel)
        if color.isValid():
            rgba = f"rgba({color.red()}, {color.green()}, {color.blue()}, {color.alpha()})"
            self.table.setStyleSheet(f"QTableWidget::item:selected {{ background-color: {rgba}; color: #ffffff; }}")
            
    def reset_highlight_color(self):
        self.table.setStyleSheet("")

    def show_context_menu(self, pos):
        row = self.table.rowAt(pos.y())
        if row < 0: return
        menu = QMenu(self)
        
        # --- 1. Apply Actions ---
        m_apply = menu.addMenu("🚀 Apply Modifications (Highlighted Rows)")
        m_apply.addAction("💾 Modify Database Only").triggered.connect(lambda: self.apply_fixes("db"))
        m_apply.addAction("📂 Modify Physical File Only").triggered.connect(lambda: self.apply_fixes("os"))
        m_apply.addAction("🚀 Modify Both (DB + Physical)").triggered.connect(lambda: self.apply_fixes("both"))
        menu.addSeparator()

        # --- 2. Visibility & Selection Tools ---
        m_vis = menu.addMenu("👁️ Visibility & Selection")
        m_vis.addAction("🎨 Set Custom Transparent Highlight...").triggered.connect(self.toggle_highlight_color)
        m_vis.addAction("🧹 Reset Highlight to Default").triggered.connect(self.reset_highlight_color)
        menu.addSeparator()

        # --- 3. Full Date Extraction ---
        m_full = menu.addMenu("📅 Extract Full Date")
        self._add_glow_action(m_full, "From Path + Filename", "full_path", "full", lambda: self.extract_part("full", "full_path"))
        self._add_glow_action(m_full, "From Filename", "name", "full", lambda: self.extract_part("full", "name"))
        self._add_glow_action(m_full, "From Folder Name", "folder", "full", lambda: self.extract_part("full", "folder"))
        self._add_glow_action(m_full, "From Virtual Path", "path", "full", lambda: self.extract_part("full", "path"))
        
        # --- 4. Mixed & Fragmented Extraction ---
        m_mix = menu.addMenu("🧩 Mix & Match Extraction")
        
        # ADD THIS LINE RIGHT HERE:
        m_mix.addAction("🏗️ Build Exact Template from this File...").triggered.connect(lambda: self.open_template_builder(row))
        
        m_mix.addSeparator()
        m_mix.addAction("Year from Path + Month/Day from File").triggered.connect(lambda: self.extract_mixed("Y_path_MD_file"))
        m_mix.addAction("Year/Month from Path + Day from File").triggered.connect(lambda: self.extract_mixed("YM_path_D_file"))
        m_mix.addAction("Date from File + Time from Path").triggered.connect(lambda: self.extract_mixed("D_file_T_path"))
        menu.addSeparator()
        
        # --- 5. Individual Parts Extraction ---
        m_parts = menu.addMenu("✂️ Extract Specific Parts")
        
        m_yr = m_parts.addMenu("📆 Extract Year")
        self._add_glow_action(m_yr, "From Path + Filename", "full_path", "year", lambda: self.extract_part("year", "full_path"))
        self._add_glow_action(m_yr, "From Filename", "name", "year", lambda: self.extract_part("year", "name"))
        self._add_glow_action(m_yr, "From Folder Name", "folder", "year", lambda: self.extract_part("year", "folder"))
        self._add_glow_action(m_yr, "From Virtual Path", "path", "year", lambda: self.extract_part("year", "path"))
        
        m_mo = m_parts.addMenu("🗓 Extract Month")
        self._add_glow_action(m_mo, "From Path + Filename", "full_path", "month", lambda: self.extract_part("month", "full_path"))
        self._add_glow_action(m_mo, "From Filename", "name", "month", lambda: self.extract_part("month", "name"))
        self._add_glow_action(m_mo, "From Folder Name", "folder", "month", lambda: self.extract_part("month", "folder"))
        self._add_glow_action(m_mo, "From Virtual Path", "path", "month", lambda: self.extract_part("month", "path"))
        
        m_dy = m_parts.addMenu("📆 Extract Day")
        self._add_glow_action(m_dy, "From Path + Filename", "full_path", "day", lambda: self.extract_part("day", "full_path"))
        self._add_glow_action(m_dy, "From Filename", "name", "day", lambda: self.extract_part("day", "name"))
        self._add_glow_action(m_dy, "From Folder Name", "folder", "day", lambda: self.extract_part("day", "folder"))
        self._add_glow_action(m_dy, "From Virtual Path", "path", "day", lambda: self.extract_part("day", "path"))
        
        m_tm = m_parts.addMenu("⏱ Extract Time")
        self._add_glow_action(m_tm, "From Path + Filename", "full_path", "time", lambda: self.extract_part("time", "full_path"))
        self._add_glow_action(m_tm, "From Filename", "name", "time", lambda: self.extract_part("time", "name"))
        self._add_glow_action(m_tm, "From Folder Name", "folder", "time", lambda: self.extract_part("time", "folder"))
        self._add_glow_action(m_tm, "From Virtual Path", "path", "time", lambda: self.extract_part("time", "path"))
        
        menu.addSeparator()
        menu.addAction("ℹ️ Properties").triggered.connect(lambda: self.show_properties(row))
        
        menu.exec(self.table.viewport().mapToGlobal(pos))
 
    def open_template_builder(self, row):
        v_path = self.items_to_fix[row]['v_path']
        filename = self.table.item(row, 0).text()
        full_path = f"{v_path}{filename}".replace('\\', '/')
        
        dlg = TemplateBuilderDialog(full_path, self)
        if dlg.exec() == QDialog.Accepted:
            template = dlg.get_template()
            
            # --- FIX: Ask to apply to ALL files if user only right-clicked one row ---
            selected_rows = self.table.selectionModel().selectedRows()
            if len(selected_rows) <= 1:
                ans = QMessageBox.question(self, "Apply Template", "Do you want to apply this exact template to ALL files in the table?\n\n(Click 'Yes' for ALL files, 'No' for just this specific file).", QMessageBox.Yes | QMessageBox.No)
                if ans == QMessageBox.Yes:
                    target_rows = [self.table.model().index(i, 0) for i in range(self.table.rowCount())]
                else:
                    target_rows = selected_rows if selected_rows else [self.table.model().index(row, 0)]
            else:
                target_rows = selected_rows
                
            self.apply_exact_template(template, target_rows)

    def apply_exact_template(self, template, selected_rows):
        # 1. Escape regex specials safely
        safe_p = ""
        for char in template:
            if char in r"().^$|+?\{}": safe_p += "\\" + char
            else: safe_p += char
            
        # 2. Convert user syntax to Regex
        safe_p = safe_p.replace('*', r'.*?')
        safe_p = safe_p.replace('[YYYY]', r'(?P<Y4>\d{4})')
        safe_p = safe_p.replace('[YY]', r'(?P<Y2>\d{2})')
        safe_p = safe_p.replace('[MM]', r'(?P<M2>0[1-9]|1[0-2]|\d{2})')
        safe_p = safe_p.replace('[DD]', r'(?P<D2>0[1-9]|[12]\d|3[01]|\d{2})')
        safe_p = safe_p.replace('[HH]', r'(?P<H2>[0-1]\d|2[0-3]|\d{2})')
        safe_p = safe_p.replace('[mm]', r'(?P<m2>[0-5]\d|\d{2})')
        safe_p = safe_p.replace('[ss]', r'(?P<s2>[0-5]\d|\d{2})')
        
        try:
            rx = re.compile(safe_p, re.IGNORECASE)
        except Exception as e:
            return QMessageBox.warning(self, "Template Error", f"Invalid template generated:\n{e}")
            
        count = 0
        for idx in selected_rows:
            r = idx.row()
            dt_widget = self.items_to_fix[r]['dt_widget']
            cur_dt = dt_widget.dateTime()
            
            v_path = self.items_to_fix[r]['v_path']
            filename = self.table.item(r, 0).text()
            full_path = f"{v_path}{filename}".replace('\\', '/')
            
            m = rx.search(full_path)
            if m:
                gd = m.groupdict()
                
                # Default to existing values so optional extraction works perfectly
                y_val = cur_dt.date().year()
                m_val = cur_dt.date().month()
                d_val = cur_dt.date().day()
                h_val = cur_dt.time().hour()
                mm_val = cur_dt.time().minute()
                s_val = cur_dt.time().second()
                
                if 'Y4' in gd and gd['Y4']: y_val = int(gd['Y4'])
                elif 'Y2' in gd and gd['Y2']: y_val = 2000 + int(gd['Y2'])
                if 'M2' in gd and gd['M2']: m_val = int(gd['M2'])
                if 'D2' in gd and gd['D2']: d_val = int(gd['D2'])
                if 'H2' in gd and gd['H2']: h_val = int(gd['H2'])
                if 'm2' in gd and gd['m2']: mm_val = int(gd['m2'])
                if 's2' in gd and gd['s2']: s_val = int(gd['s2'])
                
                cur_dt.setDate(QDate(y_val, m_val, d_val))
                cur_dt.setTime(QTime(h_val, mm_val, s_val))
                
                dt_widget.setDateTime(cur_dt)
                self.table.item(r, 3).setText("Exact Visual Template")
                self._apply_row_color(r, cur_dt, is_fail=False)
                count += 1
                
        if count == 0:
            QMessageBox.information(self, "No Match", "The template did not match any of the selected files.\nMake sure you used '*' for changing text.")
 
    def _get_text_for_source(self, r, source):
        v_path = self.items_to_fix[r]['v_path'].strip('/')
        folder_text = v_path.split('/')[-1] if v_path else ""
        name = self.table.item(r, 0).text()
        full_path = self.table.item(r, 4).text()
        
        if source == "name": return name
        elif source == "folder": return folder_text
        elif source == "path": return v_path
        elif source == "full_path": return full_path
        return ""

    def _has_fragment(self, text, f_type):
        if not text: return False
        # FIX: The exact same regex is now used for both the yellow glow and the extraction, preventing false positives
        if f_type == 'year': return bool(re.search(r'(?<!\d)(19[8-9]\d|20[0-3]\d)(?!\d)', text))
        if f_type == 'month': return bool(re.search(r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b', text, re.I))
        if f_type == 'time': return bool(re.search(r'(?<!\d)([0-1]\d|2[0-3])[-_.:]([0-5]\d)(?:[-_.:]([0-5]\d))?(?!\d)', text))
        if f_type == 'full': 
            if re.search(r'(?<!\d)(19[8-9]\d|20[0-3]\d)(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?!\d)', text): return True
            return bool(re.search(r'(19\d{2}|20\d{2}|\d{2})[-_./\s]+(0?[1-9]|1[0-2]|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*[-_./\s]+(0?[1-9]|[12]\d|3[01])', text, re.I))
        if f_type == 'day': return False 
        return False

    def extract_part(self, part, source):
        selected_rows = self.table.selectionModel().selectedRows()
        if not selected_rows: return
        
        rx_yr = re.compile(r'(?<!\d)(19[8-9]\d|20[0-3]\d)(?!\d)')
        rx_mo = re.compile(r'\b(0?[1-9]|1[0-2])\b|\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b', re.I)
        rx_dy = re.compile(r'(?<!\d)(0?[1-9]|[12]\d|3[01])(?!\d)')
        rx_tm = re.compile(r'(?<!\d)([0-1]\d|2[0-3])[-_.:]([0-5]\d)(?:[-_.:]([0-5]\d))?(?!\d)')
        rx_full = re.compile(r'(?<!\d)(?P<Y>19[8-9]\d|20[0-3]\d)(?P<M>0[1-9]|1[0-2])(?P<D>0[1-9]|[12]\d|3[01])(?!\d)|(?P<Y2>19\d{2}|20\d{2}|\d{2})[-_./\s]+(?P<M2>0?[1-9]|1[0-2]|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*[-_./\s]+(?P<D2>0?[1-9]|[12]\d|3[01])', re.I)
        MONTH_MAP = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
        
        count = 0
        for idx in selected_rows:
            r = idx.row()
            dt_widget = self.items_to_fix[r]['dt_widget']
            cur_dt = dt_widget.dateTime()
            
            text = self._get_text_for_source(r, source)
            row_matched = False
            
            if part == "year":
                m = rx_yr.search(text)
                if m: cur_dt.setDate(QDate(int(m.group(1)), cur_dt.date().month(), cur_dt.date().day())); row_matched = True
            elif part == "month":
                m = rx_mo.search(text)
                if m: 
                    val_str = m.group(1) or m.group(2)
                    m_val = MONTH_MAP.get(val_str[:3].lower()) if val_str.isalpha() else int(val_str)
                    cur_dt.setDate(QDate(cur_dt.date().year(), m_val, cur_dt.date().day())); row_matched = True
            elif part == "day":
                m = rx_dy.search(text)
                if m: cur_dt.setDate(QDate(cur_dt.date().year(), cur_dt.date().month(), int(m.group(1)))); row_matched = True
            elif part == "time":
                m = rx_tm.search(text)
                if m: cur_dt.setTime(QTime(int(m.group(1)), int(m.group(2)), int(m.group(3) or 0))); row_matched = True
            elif part == "full":
                m = rx_full.search(text)
                if m: 
                    y_str = m.group('Y') or m.group('Y2')
                    m_str = m.group('M') or m.group('M2')
                    d_str = m.group('D') or m.group('D2')
                    m_val = MONTH_MAP.get(m_str[:3].lower()) if m_str.isalpha() else int(m_str)
                    y_val = int(y_str) if len(y_str) == 4 else 2000 + int(y_str)
                    cur_dt.setDate(QDate(y_val, m_val, int(d_str))); row_matched = True
                
            if row_matched:
                count += 1
                dt_widget.setDateTime(cur_dt)
                self.table.item(r, 3).setText(f"Manual ({part} from {source})")
                self._apply_row_color(r, cur_dt, is_fail=False)
            
        if count == 0: QMessageBox.information(self, "No Match", f"Could not find valid {part} data in {source}.")

    def extract_mixed(self, mix_type):
        selected_rows = self.table.selectionModel().selectedRows()
        if not selected_rows: return
        
        rx_yr = re.compile(r'(?<!\d)(19\d{2}|20\d{2})(?!\d)')
        rx_mo = re.compile(r'(?<!\d)(0?[1-9]|1[0-2])(?!\d)|(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)', re.I)
        rx_dy = re.compile(r'(?<!\d)(0?[1-9]|[12]\d|3[01])(?!\d)')
        rx_tm = re.compile(r'(?<!\d)([0-1]?\d|2[0-3])[-_.:]([0-5]\d)(?:[-_.:]([0-5]\d))?(?!\d)')
        MONTH_MAP = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
        
        count = 0
        for idx in selected_rows:
            r = idx.row()
            dt_widget = self.items_to_fix[r]['dt_widget']
            cur_dt = dt_widget.dateTime()
            
            name = self.table.item(r, 0).text()
            path = self.table.item(r, 4).text()
            row_matched = False
            
            if mix_type == "Y_path_MD_file":
                m_y = rx_yr.search(path); m_m = rx_mo.search(name); m_d = rx_dy.search(name)
                if m_y and m_m and m_d:
                    val_str = m_m.group(1) or m_m.group(2)
                    m_val = MONTH_MAP.get(val_str[:3].lower()) if val_str.isalpha() else int(val_str)
                    cur_dt.setDate(QDate(int(m_y.group(1)), m_val, int(m_d.group(1))))
                    row_matched = True
            elif mix_type == "YM_path_D_file":
                m_y = rx_yr.search(path); m_m = rx_mo.search(path); m_d = rx_dy.search(name)
                if m_y and m_m and m_d:
                    val_str = m_m.group(1) or m_m.group(2)
                    m_val = MONTH_MAP.get(val_str[:3].lower()) if val_str.isalpha() else int(val_str)
                    cur_dt.setDate(QDate(int(m_y.group(1)), m_val, int(m_d.group(1))))
                    row_matched = True
            elif mix_type == "D_file_T_path":
                m_d = rx_dy.search(name); m_t = rx_tm.search(path)
                if m_d and m_t:
                    cur_dt.setDate(QDate(cur_dt.date().year(), cur_dt.date().month(), int(m_d.group(1))))
                    cur_dt.setTime(QTime(int(m_t.group(1)), int(m_t.group(2)), int(m_t.group(3) or 0)))
                    row_matched = True
                    
            if row_matched:
                count += 1
                dt_widget.setDateTime(cur_dt)
                self.table.item(r, 3).setText("Manual (Mixed Extraction)")
                self._apply_row_color(r, cur_dt, is_fail=False)
                
        if count == 0: QMessageBox.information(self, "No Match", "Could not satisfy the mixed pattern criteria.")

    def apply_fixes(self, mode):
        selected_rows = self.table.selectionModel().selectedRows()
        if not selected_rows: 
            return QMessageBox.warning(self, "No Selection", "Please highlight the rows you want to apply modifications to.")
        
        self.progress.setVisible(True)
        self.progress.setMaximum(len(selected_rows))
        self.progress.setFormat("Modifying: %p% (%v/%m)")
        QApplication.processEvents()
        
        success_count = 0; error_log = []
        target_prop = self.combo_modify_target.currentText()
        
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            
            for i, idx in enumerate(selected_rows):
                row = idx.row()
                dt_widget = self.table.cellWidget(row, 2)
                new_date = dt_widget.dateTime().toString("yyyy-MM-dd HH:mm:ss")
                meta = self.table.item(row, 4).data(Qt.UserRole)
                real_path = self.items_to_fix[row]['real_path']
                
                try:
                    dt_obj = datetime.datetime.strptime(new_date, "%Y-%m-%d %H:%M:%S")
                    ts = dt_obj.timestamp()
                    
                    if mode in ["os", "both"]:
                        if real_path and os.path.exists(real_path):
                            # --- NATIVE WINDOWS API BYPASS ---
                            if sys.platform == "win32":
                                import ctypes
                                from ctypes import wintypes
                                
                                safe_ts = max(0.0, min(ts, 32535215999.0)) 
                                wintime = int((safe_ts + 11644473600) * 10000000)
                                
                                class FILETIME(ctypes.Structure):
                                    _fields_ = [("dwLowDateTime", wintypes.DWORD), ("dwHighDateTime", wintypes.DWORD)]
                                filetime = FILETIME(wintime & 0xFFFFFFFF, wintime >> 32)
                                
                                # 256 = FILE_WRITE_ATTRIBUTES (Bypasses OS locks)
                                handle = ctypes.windll.kernel32.CreateFileW(real_path, 256, 0, None, 3, 0x02000000, None)
                                if handle != -1 and handle != 0xFFFFFFFF:
                                    c_time = ctypes.byref(filetime) if "Created Date" in target_prop or "Both" in target_prop else None
                                    m_time = ctypes.byref(filetime) if "Both" in target_prop else None
                                    # Passing None for a_time ignores the broken access time entirely
                                    res = ctypes.windll.kernel32.SetFileTime(handle, c_time, None, m_time)
                                    ctypes.windll.kernel32.CloseHandle(handle)
                                    if res == 0: raise Exception("Windows rejected timestamp.")
                                else: raise Exception("Could not lock file attributes.")
                            else:
                                # Mac/Linux Fallback
                                stat = os.stat(real_path)
                                safe_atime = max(0.0, stat.st_atime)
                                safe_mtime = max(0.0, ts if "Both" in target_prop else stat.st_mtime)
                                os.utime(real_path, (safe_atime, safe_mtime))
                                if sys.platform == "darwin" and ("Created Date" in target_prop or "Both" in target_prop):
                                    date_str = datetime.datetime.fromtimestamp(ts).strftime('%m/%d/%Y %H:%M:%S')
                                    subprocess.run(['SetFile', '-d', date_str, real_path])
                        else:
                            error_log.append(f"Physical file missing for: {meta['id']}")
                            if mode == "os": continue
                            
                    if mode in ["db", "both"]:
                        new_tag = f"ts_revert:{meta['old_date']}"
                        ft = f"{meta['tags']},{new_tag}" if meta['tags'] else new_tag
                        if "Both" in target_prop:
                            cur.execute("UPDATE virtual_fs SET modified=?, creation_date=?, custom_tags=? WHERE id=?", (new_date, new_date, ft, meta['id']))
                        elif "Created Date" in target_prop:
                            cur.execute("UPDATE virtual_fs SET creation_date=?, custom_tags=? WHERE id=?", (new_date, ft, meta['id']))
                        else:
                            cur.execute("UPDATE virtual_fs SET modified=?, custom_tags=? WHERE id=?", (new_date, ft, meta['id']))
                        
                    success_count += 1
                except Exception as ex:
                    error_log.append(f"Row {row+1} Error: {ex}")
                    
                if i % 10 == 0:
                    self.progress.setValue(i); QApplication.processEvents()
                    
            if mode in ["db", "both"]: conn.commit()
            conn.close()
            
            self.progress.setVisible(False)
            msg = f"Successfully modified {success_count} item(s)."
            if error_log:
                msg += f"\n\nEncountered {len(error_log)} errors. See console."
                for e in error_log: print(e)
            QMessageBox.information(self, "Operation Complete", msg)
            if self.main_app and hasattr(self.main_app, 'trigger_search'): 
                self.main_app.trigger_search()
        except Exception as e:
            self.progress.setVisible(False)
            QMessageBox.critical(self, "Fatal Error", f"Failed to apply modifications:\n{e}")
            
# ... (MapColorConfigDialog identical to previous) ...
class MapColorConfigDialog(QDialog):
    def __init__(self, current_results, parent=None):
        super().__init__(parent)
        self.setWindowTitle("⚙️ Customize Map Colors")
        self.resize(450, 700)
        
        # --- Native Light/Dark Theme Enforcer ---
        is_dark = True
        if parent and hasattr(parent, 'main_app') and hasattr(parent.main_app, 'theme_combo'):
            is_dark = parent.main_app.theme_combo.currentText() == "Dark"
        else:
            is_dark = QApplication.palette().window().color().lightness() < 128
            
        bg_col = "#161b22" if is_dark else "#f6f8fa"
        lbl_col = "#c9d1d9" if is_dark else "#24292f"
        brd_col = "#30363d" if is_dark else "#d0d7de"
        
        self.setStyleSheet(f"""
            QDialog {{ background-color: {bg_col}; color: {lbl_col}; }}
            QLabel {{ color: {lbl_col}; font-size: 13px; }}
            QScrollArea, #scrollContainer {{ background-color: {bg_col}; border: none; }}
            QPushButton {{ color: {lbl_col}; background-color: {bg_col}; border: 1px solid {brd_col}; border-radius: 4px; padding: 4px; }}
            QPushButton:hover {{ background-color: {'#30363d' if is_dark else '#e1e4e8'}; }}
        """)
        
        self.settings = QSettings("vmanOS", "HeatmapColors")
        self.custom_colors = self.settings.value("custom_colors", {})
        if not isinstance(self.custom_colors, dict): self.custom_colors = {}
        
        self.found_exts = set(); self.found_tags = set()
        for r in current_results:
            if r[3]: self.found_exts.add(r[3].lower().strip('.'))
            if r[8]:
                for t in r[8].split(','):
                    if t.strip(): self.found_tags.add(t.strip().lower())
                    
        for k in self.custom_colors.keys():
            if k.startswith("Extension_"): self.found_exts.add(k.replace("Extension_", ""))
            elif k.startswith("Tag_"): self.found_tags.add(k.replace("Tag_", ""))

        self.main_layout = QVBoxLayout(self)
        self.scroll = QScrollArea(); self.scroll.setWidgetResizable(True); self.scroll.setFrameShape(QScrollArea.NoFrame)
        self.container = QWidget()
        self.container.setObjectName("scrollContainer")
        self.container_layout = QVBoxLayout(self.container)
        self.container_layout.setAlignment(Qt.AlignTop)
        self.scroll.setWidget(self.container); self.main_layout.addWidget(self.scroll)
        
        btn_box = QHBoxLayout()
        btn_save = QPushButton("Save & Apply")
        btn_save.setStyleSheet("background-color: #2ea043; color: white; font-weight: bold; padding: 8px; border-radius: 4px; border: none;")
        btn_save.clicked.connect(self.save_and_close)
        btn_box.addStretch(); btn_box.addWidget(btn_save)
        self.main_layout.addLayout(btn_box)
        
        self.color_btns = {}
        self.refresh_ui()

    def refresh_ui(self):
        while self.container_layout.count():
            child = self.container_layout.takeAt(0)
            if child.widget(): child.widget().deleteLater()
            
        self.color_btns = {}
        self.container_layout.addWidget(QLabel("<b style='color:#58a6ff; font-size:14px;'>Custom Intensity Gradient</b>"))
        self.add_row("Low Intensity (Min):", "Gradient_Low", self.custom_colors.get("Gradient_Low", "#21262d"), False)
        self.add_row("High Intensity (Peak):", "Gradient_High", self.custom_colors.get("Gradient_High", "#f85149"), False)
        self.container_layout.addWidget(QLabel(" "))
        
        self.container_layout.addWidget(QLabel("<b style='color:#58a6ff; font-size:14px;'>Category Colors</b>"))
        cats = list(CAT_MAP.keys()) + ["Others"]
        for c in cats:
            key = f"Category_{c}"
            self.add_row(c, key, self.custom_colors.get(key, GLOBAL_CAT_COLORS.get(c, "#8b949e")), False)
            
        self.container_layout.addWidget(QLabel(" "))
        self.container_layout.addWidget(QLabel("<b style='color:#58a6ff; font-size:14px;'>Extension Colors</b>"))
        ext_keys = sorted([k for k in self.custom_colors.keys() if k.startswith("Extension_")])
        for k in ext_keys:
            self.add_row(k.replace("Extension_", "Ext: "), k, self.custom_colors[k], True, "Extension")
            
        btn_add_ext = QPushButton("+ Add Extension Color")
        btn_add_ext.clicked.connect(lambda: self.add_new_mapping("Extension", sorted(list(self.found_exts))))
        self.container_layout.addWidget(btn_add_ext)
        
        self.container_layout.addWidget(QLabel(" "))
        self.container_layout.addWidget(QLabel("<b style='color:#58a6ff; font-size:14px;'>Tag Colors</b>"))
        tag_keys = sorted([k for k in self.custom_colors.keys() if k.startswith("Tag_")])
        for k in tag_keys:
            self.add_row(k.replace("Tag_", "Tag: "), k, self.custom_colors[k], True, "Tag")
            
        btn_add_tag = QPushButton("+ Add Tag Color")
        btn_add_tag.clicked.connect(lambda: self.add_new_mapping("Tag", sorted(list(self.found_tags))))
        self.container_layout.addWidget(btn_add_tag)

    def add_row(self, display_name, key, color_hex, is_editable, prefix=""):
        row_w = QWidget(); lay = QHBoxLayout(row_w); lay.setContentsMargins(0, 2, 0, 2)
        lbl = QLabel(display_name); lbl.setMinimumWidth(130); lay.addWidget(lbl)
        
        btn_color = QPushButton(); btn_color.setCursor(Qt.PointingHandCursor)
        btn_color.setStyleSheet(f"background-color: {color_hex}; border: 1px solid #30363d; border-radius: 4px; min-width: 60px; min-height: 25px;")
        self.color_btns[key] = color_hex
        btn_color.clicked.connect(lambda checked=False, k=key, b=btn_color: self.pick_color(k, b))
        lay.addWidget(btn_color)
        
        if is_editable:
            raw_name = key.replace(f"{prefix}_", "")
            btn_edit = QPushButton("✏️"); btn_edit.setToolTip("Rename"); btn_edit.setFixedWidth(30)
            btn_edit.clicked.connect(lambda checked=False, k=key, p=prefix, old=raw_name: self.rename_mapping(k, p, old))
            lay.addWidget(btn_edit)
            btn_del = QPushButton("❌"); btn_del.setToolTip("Remove"); btn_del.setFixedWidth(30)
            btn_del.clicked.connect(lambda checked=False, k=key: self.delete_mapping(k))
            lay.addWidget(btn_del)
        self.container_layout.addWidget(row_w)

    def pick_color(self, key, btn):
        color = QColorDialog.getColor(QColor(self.color_btns[key]), self, "Select Color")
        if color.isValid():
            self.color_btns[key] = color.name(); self.custom_colors[key] = color.name()
            btn.setStyleSheet(f"background-color: {color.name()}; border: 1px solid #30363d; border-radius: 4px; min-width: 60px; min-height: 25px;")
            
    def add_new_mapping(self, prefix, items_list):
        name, ok = QInputDialog.getItem(self, f"Add {prefix}", f"Select or type a new {prefix}:", items_list, 0, True)
        if ok and name.strip():
            clean_name = name.strip().lower()
            key = f"{prefix}_{clean_name}"
            if key not in self.custom_colors:
                color = QColorDialog.getColor(Qt.white, self, f"Select Color for {clean_name}")
                if color.isValid():
                    self.custom_colors[key] = color.name()
                    self.refresh_ui()

    def rename_mapping(self, key, prefix, old_name):
        new_name, ok = QInputDialog.getText(self, f"Rename {prefix}", f"Rename '{old_name}' to:", QLineEdit.Normal, old_name)
        if ok and new_name.strip():
            clean_name = new_name.strip().lower()
            new_key = f"{prefix}_{clean_name}"
            if new_key != key:
                self.custom_colors[new_key] = self.custom_colors.get(key, "#ffffff")
                if key in self.custom_colors: del self.custom_colors[key]
                self.refresh_ui()

    def delete_mapping(self, key):
        ans = QMessageBox.question(self, "Remove", f"Are you sure you want to remove this color mapping?", QMessageBox.Yes | QMessageBox.No)
        if ans == QMessageBox.Yes:
            if key in self.custom_colors: del self.custom_colors[key]
            self.refresh_ui()

    def save_and_close(self):
        for k, v in self.color_btns.items(): self.custom_colors[k] = v
        self.settings.setValue("custom_colors", self.custom_colors)
        self.accept()


# --- Beautiful Massive Dataset Dialog ---
class RowLimitDialogSearch(QDialog):
    def __init__(self, total_rows, parent=None):
        super().__init__(parent)
        self.total_rows = total_rows
        self.setWindowTitle("⚠️ Massive Dataset Found")
        self.resize(500, 380)
        
        # --- Light/Dark Dynamic Adaptation ---
        is_dark = True
        if parent and hasattr(parent, 'main_app') and hasattr(parent.main_app, 'theme_combo'):
            is_dark = parent.main_app.theme_combo.currentText() == "Dark"
        else:
            is_dark = QApplication.palette().window().color().lightness() < 128
            
        bg_col = "#161b22" if is_dark else "#f6f8fa"
        lbl_col = "#c9d1d9" if is_dark else "#24292f"
        txt_col = "#8b949e" if is_dark else "#57606a" # FIX: Adapts the subtext to the theme
        
        self.setStyleSheet(f"""
            QDialog {{ background-color: {bg_col}; color: {lbl_col}; }}
            QLabel, QCheckBox, QRadioButton {{ color: {lbl_col}; }}
        """)
        
        layout = QVBoxLayout(self)
        
        # Applied the dynamic text color here
        lbl = QLabel(f"<div style='text-align:center;'><b style='font-size:16px; color:#58a6ff;'>Found {total_rows:,} matching records.</b><br><br><span style='color:{txt_col};'>Loading massive amounts of rows into the UI grid consumes RAM. Visual Maps and Charts will process 100% of the data regardless. How would you like to render the Table?</span></div>")
        lbl.setWordWrap(True)
        layout.addWidget(lbl)
        
        self.radio_group = QButtonGroup(self)
        self.rb_rec = QRadioButton(f"Recommended (First {min(1000, total_rows)} rows)")
        self.rb_rec.setChecked(True)
        self.rb_parts = QRadioButton("Break into Parts")
        self.rb_custom = QRadioButton("Custom Range")
        self.rb_all = QRadioButton(f"All Rows ({total_rows}) - ⚠️ May stutter")
        self.rb_all.setStyleSheet("color: #d29922;") # Adjusted for better contrast on both themes
        self.rb_none = QRadioButton("0 Rows (Maximum Speed - Maps & Charts Only)")
        
        for i, rb in enumerate([self.rb_rec, self.rb_parts, self.rb_custom, self.rb_all, self.rb_none]):
            self.radio_group.addButton(rb, i)
            layout.addWidget(rb)
            
        custom_grp = QWidget()
        custom_lay = QGridLayout(custom_grp)
        custom_lay.setContentsMargins(25, 0, 0, 0)
        
        lbl_p = QLabel("Divide into chunks of:"); lbl_p.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.spin_parts = QSpinBox(); self.spin_parts.setRange(2, 100); self.spin_parts.setValue(5)
        self.spin_parts.setFixedWidth(80)
        custom_lay.addWidget(lbl_p, 0, 0)
        custom_lay.addWidget(self.spin_parts, 0, 1)
        
        lbl_f = QLabel("From Row:"); lbl_f.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.spin_from = QSpinBox(); self.spin_from.setRange(1, total_rows); self.spin_from.setValue(1)
        self.spin_from.setFixedWidth(80)
        
        lbl_t = QLabel("To Row:"); lbl_t.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.spin_to = QSpinBox(); self.spin_to.setRange(1, total_rows); self.spin_to.setValue(min(5000, total_rows))
        self.spin_to.setFixedWidth(80)
        
        custom_lay.addWidget(lbl_f, 1, 0); custom_lay.addWidget(self.spin_from, 1, 1)
        custom_lay.addWidget(lbl_t, 1, 2); custom_lay.addWidget(self.spin_to, 1, 3)
        layout.addWidget(custom_grp)
        
        self.chk_lazy = QCheckBox("Enable Lazy Walk (Smoothly load more rows as you scroll)")
        self.chk_lazy.setChecked(True)
        self.chk_lazy.setStyleSheet("margin-top: 10px; color: #3fb950; font-weight: bold;")
        layout.addWidget(self.chk_lazy)
        
        btn_box = QHBoxLayout()
        btn_cancel = QPushButton("Abort Search")
        btn_cancel.setStyleSheet("background-color: #f85149; color: white; font-weight: bold; padding: 10px; border-radius: 4px;")
        btn_cancel.clicked.connect(self.reject)
        
        btn_ok = QPushButton("Render Data")
        btn_ok.setStyleSheet("background-color: #2ea043; color: white; font-weight: bold; padding: 10px; border-radius: 4px;")
        btn_ok.clicked.connect(self.accept)
        
        btn_box.addWidget(btn_cancel)
        btn_box.addWidget(btn_ok)
        layout.addLayout(btn_box)

    def get_values(self):
        idx = self.radio_group.checkedId()
        lazy = self.chk_lazy.isChecked()
        if idx == 0: return 0, min(1000, self.total_rows), lazy
        elif idx == 1: 
            chunk = max(1, self.total_rows // self.spin_parts.value())
            return 0, chunk, lazy
        elif idx == 2: return max(0, self.spin_from.value() - 1), self.spin_to.value(), lazy
        elif idx == 3: return 0, self.total_rows, lazy
        elif idx == 4: return 0, 0, False


class NumericTableItem(QTableWidgetItem):
    def __lt__(self, other):
        return self.data(Qt.UserRole) < other.data(Qt.UserRole)


class SearchWorker(QThread):
    results_ready = Signal(list)
    progress_update = Signal(int)
    finished_search = Signal(bool)
    telemetry_update = Signal(str)
    
    def __init__(self, params):
        super().__init__()
        self.p = params
        self.is_running = True
        
    def abort(self):
        self.is_running = False
        
    def run(self):
        all_results = []
        db_list = self.p.get('db_list') or [self.p['db']]
        valid_dbs = [p for p in db_list if p and os.path.exists(p)]
        
        if not valid_dbs:
            self.finished_search.emit(False)
            return

        for db_idx, db_path in enumerate(valid_dbs):
            if not self.is_running: break
            db_label = Path(db_path).stem
            self.telemetry_update.emit(f"[{db_idx+1}/{len(valid_dbs)}] Querying {db_label}...")

            try:
                with sqlite3.connect(db_path, timeout=30) as conn:
                    cur = conn.cursor()
                    # Check columns for EACH database individually
                    cur.execute("PRAGMA table_info(virtual_fs)")
                    cols = [row[1] for row in cur.fetchall()]

                    selects = ["id", "name", "parent_path", "is_folder", "size", "extension", "modified", "real_path", "custom_tags"]
                    selects.append("color_tag" if "color_tag" in cols else "'' as color_tag")
                    selects.append("category" if "category" in cols else "'Others' as category")
                    if "sha256" in cols: selects.append("sha256")
                    elif "hash" in cols: selects.append("hash as sha256")
                    else: selects.append("'' as sha256")
                    selects.append("creation_date" if "creation_date" in cols else "modified as creation_date")

                    query = f"SELECT {', '.join(selects)} FROM virtual_fs WHERE in_trash=0"
                    sql_params = []

                    if self.p['look_for'] == "Files Only": query += " AND is_folder=0"
                    elif self.p['look_for'] == "Folders Only": query += " AND is_folder=1"

                    if self.p['name']:
                        n_tgt = self.p['name'] if self.p['case_sensitive'] else self.p['name'].lower()
                        target_col = "name" if self.p['case_sensitive'] else "LOWER(name)"
                        if self.p['case_sensitive']:
                            if self.p['match'] == "Contains": query += f" AND {target_col} GLOB ?"; sql_params.append(f"*{n_tgt}*")
                            elif self.p['match'] == "Exact": query += f" AND {target_col} GLOB ?"; sql_params.append(n_tgt)
                            elif self.p['match'] == "Starts With": query += f" AND {target_col} GLOB ?"; sql_params.append(f"{n_tgt}*")
                            elif self.p['match'] == "Ends With": query += f" AND {target_col} GLOB ?"; sql_params.append(f"*{n_tgt}")
                        else:
                            if self.p['match'] == "Contains": query += f" AND {target_col} LIKE ?"; sql_params.append(f"%{n_tgt}%")
                            elif self.p['match'] == "Exact": query += f" AND {target_col} = ?"; sql_params.append(n_tgt)
                            elif self.p['match'] == "Starts With": query += f" AND {target_col} LIKE ?"; sql_params.append(f"{n_tgt}%")
                            elif self.p['match'] == "Ends With": query += f" AND {target_col} LIKE ?"; sql_params.append(f"%{n_tgt}")

                    if self.p['path']: query += " AND parent_path LIKE ?"; sql_params.append(f"%{self.p['path']}%")

                    if self.p.get('tags'):
                        for tg in self.p['tags']:
                            query += " AND custom_tags LIKE ?"
                            sql_params.append(f"%{tg}%")

                    target_cat = self.p.get('category', 'All')

                    if self.p.get('ex_names'):
                        for ex_n in self.p['ex_names']:
                            query += " AND LOWER(name) NOT LIKE ?"
                            sql_params.append(f"%{ex_n}%")

                    cur.execute(query, sql_params)

                    days_map = {"Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6}
                    day_start_idx = days_map.get(self.p['day_start'], 0)
                    day_end_idx = days_map.get(self.p['day_end'], 6)
                    use_day_range = self.p['use_day_range']
                    time_s_str = self.p['time_start']
                    time_e_str = self.p['time_end']
                    lvl_min = self.p['lvl_min']
                    lvl_max = self.p['lvl_max']
                    seen_patterns = set()

                    while True:
                        if not self.is_running: break
                        chunk = cur.fetchmany(10000)
                        if not chunk: break

                        for row_data in chunk:
                            db_id, name, p_path, is_fldr, size, ext, modified, real_path, tags, color_tag, category_val, sha256_val, creation_date = row_data[:13]
                            ext_val = str(ext).lower() if ext else ""
                            size_val = size or 0
                            cat_val = EXT_TO_CAT.get(ext_val, category_val)

                            if target_cat != "All" and cat_val != target_cat: continue

                            lvl = max(0, str(p_path).strip('/').count('/'))
                            if not (lvl_min <= lvl <= lvl_max): continue

                            # --- EXACT NAME EXCLUDING EXTENSION ---
                            base_name = str(name).rsplit('.', 1)[0].lower() if not is_fldr and '.' in str(name) else str(name).lower()
                            
                            # 1. Length Check
                            if not (self.p['len_min'] <= len(base_name) <= self.p['len_max']): continue
                            
                            # 2. Pattern Check
                            if self.p['name_pattern'] == "Only Numbers (e.g. 12345)":
                                if not base_name.isdigit(): continue
                            elif self.p['name_pattern'] == "Hash / Frequent Numbers (e.g. a3c9f...)":
                                # --- FIXED: Requires at least 16 hex chars only ---
                                if not (len(base_name) >= 16 and re.fullmatch(r'[a-f0-9]+', base_name)): continue
                            elif self.p['name_pattern'] == "Name with special symbols":
                                if not re.search(r'[^a-zA-Z0-9\s_\-]', base_name): continue
                            elif self.p['name_pattern'] == "Name with alphabets only":
                                if not re.fullmatch(r'[a-zA-Z\s]+', base_name): continue
                            # --------------------------------------

                            is_match = True
                            if self.p['exts'] and ext_val not in self.p['exts']: is_match = False
                            elif self.p['ex_exts'] and ext_val in self.p['ex_exts']: is_match = False
                            elif not is_fldr and not (self.p['sz_min'] <= size_val <= self.p['sz_max']): is_match = False

                            target_dt_str = str(creation_date) if self.p['date_type'] == "Created Date" else str(modified)

                            if self.p['use_date']:
                                if not target_dt_str or len(target_dt_str) < 10: is_match = False
                                else:
                                    try:
                                        y, m, d = map(int, target_dt_str[:10].split('-'))
                                        dt_obj = QDate(y, m, d)
                                        if dt_obj < self.p['date_start'] or dt_obj > self.p['date_end']: is_match = False
                                    except: is_match = False

                            if is_match and use_day_range and target_dt_str and len(target_dt_str) >= 10:
                                try:
                                    y, m, d = map(int, target_dt_str[:10].split('-'))
                                    d_idx = datetime.date(y, m, d).weekday()
                                    if day_start_idx <= day_end_idx:
                                        if not (day_start_idx <= d_idx <= day_end_idx): is_match = False
                                    else:
                                        if not (d_idx >= day_start_idx or d_idx <= day_end_idx): is_match = False
                                except: is_match = False

                            if is_match and self.p['use_time'] and target_dt_str and len(target_dt_str) >= 16:
                                try:
                                    t_part = target_dt_str[11:16]
                                    if time_s_str <= time_e_str:
                                        if not (time_s_str <= t_part <= time_e_str): is_match = False
                                    else:
                                        if not (t_part >= time_s_str or t_part <= time_e_str): is_match = False
                                except: pass

                            if is_match and self.p['unique_patterns'] and not is_fldr:
                                base_name = str(name).rsplit('.', 1)[0].lower() if '.' in str(name) else str(name).lower()
                                if len(base_name) >= 4:
                                    pref = base_name[:4] + "_" + ext_val
                                    suff = base_name[-4:] + "_" + ext_val
                                    if pref in seen_patterns or suff in seen_patterns: is_match = False
                                    else: seen_patterns.add(pref); seen_patterns.add(suff)
                                else:
                                    pat = base_name + "_" + ext_val
                                    if pat in seen_patterns: is_match = False
                                    else: seen_patterns.add(pat)

                            if is_match:
                                all_results.append((db_id, name, p_path, ext_val, size_val, target_dt_str, is_fldr, real_path, tags, color_tag, cat_val, sha256_val, db_label))
            except Exception as e:
                print(f"Error querying DB {db_path}: {e}")

        self.progress_update.emit(100)
        self.results_ready.emit(all_results)
        self.finished_search.emit(False)


class AdvancedSearchWindow(QMainWindow):
    def __init__(self, active_db, parent=None):
        super().__init__(parent)
        self.active_db = active_db
        self.main_app = parent
        self.active_db_list = [active_db] if isinstance(active_db, str) else list(active_db)
        self.is_searching = False
        self.is_rendering = False
        self._abort_render = False 
        
        self.settings = QSettings("VirtualMan", "AdvancedSearchUI")
        self.show_icons = self.settings.value("show_icons", True, type=bool)
        self.verbose_telemetry = self.settings.value("verbose_telemetry", False, type=bool) 
        self.fast_mode = self.settings.value("fast_mode", False, type=bool)
        
        self.map_color_mode = self.settings.value("map_color_mode", "Intensity (Count)")
        self.map_gradient = self.settings.value("map_gradient", "Excel (Green-Yellow-Red)")
        self.map_layout_mode = self.settings.value("map_layout_mode", "True Calendar Standard (3x4 Grids)")
        
        self.time_color_mode = self.settings.value("time_color_mode", "Intensity (Count)")
        self.time_layout_mode = self.settings.value("time_layout_mode", "Compact Matrix")
        self.time_show_lines = self.settings.value("time_show_lines", False, type=bool)
        
        self.map_sort_order = self.settings.value("map_sort_order", "Top to Bottom (Newest First)")
        self.map_tile_size = self.settings.value("map_tile_size", "Large (1.2x)") 
        self.map_custom_scale = float(self.settings.value("map_custom_scale", 1.2))
        self.map_top_margin = float(self.settings.value("map_top_margin", 78.0)) 
        
        self.map_highlight_color = self.settings.value("map_highlight_color", "#b8860b")
        self.map_highlight_size = float(self.settings.value("map_highlight_size", 2.0))
        self.map_highlight_field = self.settings.value("map_highlight_field", "Name or Path")
        self.map_highlight_type = self.settings.value("map_highlight_type", "Files & Folders")
        self.map_show_dates = self.settings.value("map_show_dates", True, type=bool)
        self.dup_bg_mode = self.settings.value("dup_bg_mode", "Muted Blue Intensity")
        self._show_highlight_dialog = False
        
        self.table_view_mode = "Files & Folders"
        
        self.current_results = []
        self.matrix_cache = defaultdict(lambda: defaultdict(list))
        self.time_matrix_cache = defaultdict(lambda: defaultdict(list))
        self.inherited_tags = {}
        self.duplicate_map = {}
        self.max_hits = 1
        
        self.VMAN_COLORS = {
            "Red": QColor("#5c2121"), "Orange": QColor("#663c14"),
            "Gold": QColor("#5c4c21"), "Green": QColor("#215c2b"), 
            "Cyan": QColor("#1b5e5e"), "Blue": QColor("#213c5c"), 
            "Purple": QColor("#43215c"), "Pink": QColor("#5c2144")
        }
        
        self.THUMBS_DIR = Path("vman_data/thumbnails")
        self.ICONS_DIR = Path("icons")
        self.icon_cache = {}
        self.icon_provider = QFileIconProvider()
        
        self.setWindowTitle("🔍 VMan Search Engine")
        self.resize(1250, 850)
        self.setAttribute(Qt.WA_DeleteOnClose, False)
        
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setSpacing(8)
        main_layout.setContentsMargins(10, 10, 10, 10)

        # --- TOP WRAPPER ---
        self.top_wrap = QWidget()
        top_wrap_lay = QVBoxLayout(self.top_wrap)
        top_wrap_lay.setContentsMargins(0, 0, 0, 0)
        top_wrap_lay.setSpacing(5)

        # --- TOP AREA (SYMBOLIC GLASS BUTTONS) ---
        top_bar = QHBoxLayout()
        self.txt_name = QLineEdit()
        self.txt_name.setPlaceholderText("Search file or folder name (Alt+S)...")
        self.txt_name.setFixedHeight(40)
        self.txt_name.setStyleSheet("font-size: 15px; padding: 0px 10px;")
        
        self.btn_scope = QPushButton("🎯")
        self.btn_type = QPushButton("📂")
        self.btn_metrics = QPushButton("⚙️")
        self.btn_reset = QPushButton("⟲")
        self.btn_multi_db = QPushButton("🗄️")
        self.btn_multi_db.setToolTip("Select Databases to Search Across")
        self.btn_multi_db.setFixedSize(40, 40)
        self.btn_multi_db.clicked.connect(self.select_search_databases)
        
        self.btn_scope.setToolTip("Target Scope & Tags")
        self.btn_type.setToolTip("File Types & Categories")
        self.btn_metrics.setToolTip("Attributes & Time")
        self.btn_reset.setToolTip("Reset All Filters")
        self.btn_reset.clicked.connect(self.reset_filters)
        
        for btn in [self.btn_scope, self.btn_type, self.btn_metrics, self.btn_reset]:
            btn.setFixedSize(40, 40)
            if btn != self.btn_reset:
                btn.setCheckable(True)
                btn.toggled.connect(self.update_filter_visibility)

        self.btn_search = QPushButton("⚡ SEARCH")
        self.btn_search.setFixedSize(110, 40)
        self.btn_search.clicked.connect(self.handle_button_action)

        top_bar.addWidget(self.txt_name, stretch=1)
        top_bar.addWidget(self.btn_scope)
        top_bar.addWidget(self.btn_type)
        top_bar.addWidget(self.btn_metrics)
        top_bar.addWidget(self.btn_multi_db)   # <-- Added here
        top_bar.addWidget(self.btn_reset)
        top_bar.addWidget(self.btn_search)
        top_wrap_lay.addLayout(top_bar)

        # --- DYNAMIC FILTER CARDS ---
        self.filters_container = QWidget()
        self.filters_container.setVisible(False) 
        filters_h_layout = QHBoxLayout(self.filters_container)
        filters_h_layout.setContentsMargins(0, 5, 0, 10)
        filters_h_layout.setSpacing(10)
        
        self.card_scope = QFrame(); self.card_scope.setObjectName("FilterCard"); self.card_scope.setVisible(False)
        self.card_scope.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        f1 = QFormLayout(self.card_scope); f1.setContentsMargins(15, 15, 15, 15); f1.setVerticalSpacing(10)
        self.combo_match = QComboBox(); self.combo_match.addItems(["Contains", "Exact", "Starts With", "Ends With"])
        self.combo_look_for = QComboBox(); self.combo_look_for.addItems(["Files & Folders", "Files Only", "Folders Only"])
        self.txt_path = QLineEdit(); self.txt_path.setPlaceholderText("e.g. /Documents/")
        self.txt_search_tags = QLineEdit(); self.txt_search_tags.setPlaceholderText("e.g. holiday, work")
        self.chk_unique = QCheckBox("Show Unique Name Patterns Only")
        
        # FIX: Added Case Sensitive Checkbox
        self.chk_case_sensitive = QCheckBox("Case Sensitive Match") 
        
        f1.addRow("Match Mode:", self.combo_match); f1.addRow("Data Type:", self.combo_look_for)
        f1.addRow("Virtual Path:", self.txt_path); f1.addRow("Search Tags:", self.txt_search_tags)
        
        # Add both checkboxes cleanly
        chk_lay = QHBoxLayout()
        chk_lay.addWidget(self.chk_unique)
        chk_lay.addWidget(self.chk_case_sensitive)
        f1.addRow("", chk_lay)
        
        filters_h_layout.addWidget(self.card_scope)

        self.card_type = QFrame(); self.card_type.setObjectName("FilterCard"); self.card_type.setVisible(False)
        self.card_type.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        f2 = QFormLayout(self.card_type); f2.setContentsMargins(15, 15, 15, 15); f2.setVerticalSpacing(10)
        self.combo_category = QComboBox()
        self.combo_category.addItems(["All"] + list(CAT_MAP.keys()) + ["Others"])
        self.txt_ext = QLineEdit(); self.txt_ext.setPlaceholderText(".jpg, .pdf")
        self.txt_exclude_ext = QLineEdit(); self.txt_exclude_ext.setPlaceholderText(".tmp, .bak")
        self.txt_exclude_name = QLineEdit(); self.txt_exclude_name.setPlaceholderText("e.g. backup, temp")
        
        lvl_lay = QHBoxLayout()
        self.spin_lvl_min = QSpinBox(); self.spin_lvl_min.setRange(0, 999)
        self.spin_lvl_max = QSpinBox(); self.spin_lvl_max.setRange(0, 999); self.spin_lvl_max.setValue(999)
        to_lbl_lvl = QLabel("to"); to_lbl_lvl.setAlignment(Qt.AlignCenter)
        lvl_lay.addWidget(self.spin_lvl_min); lvl_lay.addWidget(to_lbl_lvl); lvl_lay.addWidget(self.spin_lvl_max)
        
        # --- ADDED: String Length Filter ---
        len_lay = QHBoxLayout()
        self.spin_len_min = QSpinBox(); self.spin_len_min.setRange(0, 999); self.spin_len_min.setValue(0)
        self.spin_len_max = QSpinBox(); self.spin_len_max.setRange(0, 999); self.spin_len_max.setValue(999)
        to_lbl_len = QLabel("to"); to_lbl_len.setAlignment(Qt.AlignCenter)
        len_lay.addWidget(self.spin_len_min); len_lay.addWidget(to_lbl_len); len_lay.addWidget(self.spin_len_max)
        # -----------------------------------
        
        f2.addRow("Category:", self.combo_category); f2.addRow("Include Ext:", self.txt_ext)
        f2.addRow("Exclude Ext:", self.txt_exclude_ext); f2.addRow("Skip Names:", self.txt_exclude_name)
        f2.addRow("Folder Level:", lvl_lay)
        f2.addRow("Name Length:", len_lay) # <-- Added to form
        # Add right below the name length block you added earlier
        self.combo_name_pattern = QComboBox()
        self.combo_name_pattern.addItems([
            "Any Name Pattern", 
            "Only Numbers (e.g. 12345)", 
            "Hash / Frequent Numbers (e.g. a3c9f...)",
            "Name with special symbols",
            "Name with alphabets only"
        ])
        f2.addRow("Name Pattern:", self.combo_name_pattern)
        filters_h_layout.addWidget(self.card_type)

        self.card_metrics = QFrame(); self.card_metrics.setObjectName("FilterCard"); self.card_metrics.setVisible(False)
        self.card_metrics.setFixedWidth(400) 
        f3 = QFormLayout(self.card_metrics); f3.setContentsMargins(15, 15, 15, 15); f3.setVerticalSpacing(8)

        size_lay = QHBoxLayout(); size_lay.setContentsMargins(0,0,0,0)
        self.spin_size_min = QDoubleSpinBox(); self.spin_size_min.setRange(0, 999999); self.spin_size_min.setDecimals(1); self.spin_size_min.setButtonSymbols(QDoubleSpinBox.NoButtons)
        self.spin_size_max = QDoubleSpinBox(); self.spin_size_max.setRange(0, 999999); self.spin_size_max.setValue(999999); self.spin_size_max.setDecimals(1); self.spin_size_max.setButtonSymbols(QDoubleSpinBox.NoButtons)
        self.combo_sz_unit = QComboBox(); self.combo_sz_unit.addItems(["MB", "B", "KB", "GB"]); self.combo_sz_unit.setFixedWidth(50)
        to_lbl1 = QLabel("to"); to_lbl1.setAlignment(Qt.AlignCenter)
        size_lay.addWidget(self.spin_size_min); size_lay.addWidget(to_lbl1); size_lay.addWidget(self.spin_size_max); size_lay.addWidget(self.combo_sz_unit)

        self.combo_date_type = QComboBox(); self.combo_date_type.addItems(["Modified Date", "Created Date"])
        
        date_lay = QHBoxLayout(); date_lay.setContentsMargins(0,0,0,0)
        self.chk_use_date = QCheckBox("")
        self.date_start = QDateEdit(QDate.currentDate().addYears(-1)); self.date_start.setCalendarPopup(True); self.date_start.setEnabled(False)
        self.date_end = QDateEdit(QDate.currentDate()); self.date_end.setCalendarPopup(True); self.date_end.setEnabled(False)
        self.chk_use_date.toggled.connect(self.date_start.setEnabled); self.chk_use_date.toggled.connect(self.date_end.setEnabled)
        to_lbl2 = QLabel("to"); to_lbl2.setAlignment(Qt.AlignCenter)
        date_lay.addWidget(self.chk_use_date); date_lay.addWidget(self.date_start); date_lay.addWidget(to_lbl2); date_lay.addWidget(self.date_end)
        
        time_lay = QHBoxLayout(); time_lay.setContentsMargins(0,0,0,0)
        self.chk_use_time = QCheckBox("")
        self.time_start = QTimeEdit(QTime(0, 0)); self.time_start.setEnabled(False)
        self.time_end = QTimeEdit(QTime(23, 59)); self.time_end.setEnabled(False)
        self.chk_use_time.toggled.connect(self.time_start.setEnabled); self.chk_use_time.toggled.connect(self.time_end.setEnabled)
        to_lbl3 = QLabel("to"); to_lbl3.setAlignment(Qt.AlignCenter)
        time_lay.addWidget(self.chk_use_time); time_lay.addWidget(self.time_start); time_lay.addWidget(to_lbl3); time_lay.addWidget(self.time_end)
        
        day_lay = QHBoxLayout(); day_lay.setContentsMargins(0,0,0,0)
        self.chk_day_range = QCheckBox("")
        self.combo_day_start = QComboBox()
        self.combo_day_end = QComboBox()
        days_list = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        self.combo_day_start.addItems(days_list)
        self.combo_day_end.addItems(days_list)
        self.combo_day_end.setCurrentIndex(6)
        self.combo_day_start.setEnabled(False); self.combo_day_end.setEnabled(False)
        self.chk_day_range.toggled.connect(self.combo_day_start.setEnabled)
        self.chk_day_range.toggled.connect(self.combo_day_end.setEnabled)
        to_lbl4 = QLabel("to"); to_lbl4.setAlignment(Qt.AlignCenter)
        day_lay.addWidget(self.chk_day_range); day_lay.addWidget(self.combo_day_start); day_lay.addWidget(to_lbl4); day_lay.addWidget(self.combo_day_end)
        
        f3.addRow("File Size:", size_lay)
        f3.addRow("Date Type:", self.combo_date_type)
        f3.addRow("Date Range:", date_lay)
        f3.addRow("Time Range:", time_lay)
        f3.addRow("Day Range:", day_lay)
        filters_h_layout.addWidget(self.card_metrics)
        top_wrap_lay.addWidget(self.filters_container)

        main_layout.addWidget(self.top_wrap)

        for box in [self.txt_name, self.txt_path, self.txt_search_tags, self.txt_ext, self.txt_exclude_ext, self.txt_exclude_name]:
            box.returnPressed.connect(self.handle_button_action)

        self.progress = QProgressBar(); self.progress.setVisible(False)
        self.progress.setFixedHeight(18); self.progress.setTextVisible(True); self.progress.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(self.progress)
        
        self.lbl_status = QLabel("Ready."); self.lbl_status.setStyleSheet("color: #8b949e; font-style: italic; padding-left: 5px;")
        self.lbl_status.setContextMenuPolicy(Qt.CustomContextMenu)
        self.lbl_status.customContextMenuRequested.connect(self.show_top_context_menu)
        main_layout.addWidget(self.lbl_status)

        self.tabs = QTabWidget()
        
        # --- TAB 1: RESULTS TABLE ---
        tab1_container = QWidget(); tab1_layout = QVBoxLayout(tab1_container); tab1_layout.setContentsMargins(0, 0, 0, 0); tab1_layout.setSpacing(5)
        quick_lay = QHBoxLayout()
        self.local_filter_input = QLineEdit(); self.local_filter_input.setPlaceholderText("🔍 Quick Filter (Alt+F): Name or Path...")
        self.local_filter_input.textChanged.connect(self.apply_local_filter)
        self.local_tag_filter_input = QLineEdit(); self.local_tag_filter_input.setPlaceholderText("🏷️ Quick Filter (Alt+T): Tags...")
        self.local_tag_filter_input.textChanged.connect(self.apply_local_filter)
        quick_lay.addWidget(self.local_filter_input); quick_lay.addWidget(self.local_tag_filter_input)
        
        self.btn_search_view_mode = QPushButton("🖼️ Grid View")
        self.btn_search_view_mode.setFixedHeight(28)
        self.btn_search_view_mode.clicked.connect(self.toggle_search_view_mode)
        quick_lay.addWidget(self.btn_search_view_mode)
        tab1_layout.addLayout(quick_lay)
        
        self.table = QTableWidget(0, 10)
        self.table.setHorizontalHeaderLabels(["S.No.", "Name", "Database", "Virtual Path", "Type", "Size", "Date", "Labels", "SHA-256", "HiddenMeta"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.setColumnWidth(0, 50); self.table.setColumnWidth(1, 280); self.table.setColumnWidth(2, 120)
        self.table.setColumnWidth(3, 280); self.table.setColumnWidth(4, 100); self.table.setColumnWidth(5, 90)
        self.table.setColumnWidth(6, 140); self.table.setColumnWidth(7, 120); self.table.setColumnWidth(8, 200)
        self.table.setColumnHidden(9, True) # HiddenMeta
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSortingEnabled(True); self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers); self.table.setShowGrid(False)
        self.table.verticalHeader().setVisible(False); self.table.setIconSize(QSize(20, 20))
        self.table.itemDoubleClicked.connect(self.handle_double_click)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)
        self.table.horizontalHeader().setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.horizontalHeader().customContextMenuRequested.connect(self.show_header_menu)
        
        # Initialize Stack EXACTLY ONCE
        self.search_view_stack = QStackedWidget()
        self.search_view_stack.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.search_view_stack.addWidget(self.table)
        
        self.search_grid = QListWidget()
        self.search_grid.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.search_grid.setViewMode(QListWidget.IconMode)
        self.search_grid.setGridSize(QSize(140, 160))
        self.search_grid.setIconSize(QSize(80, 80))
        self.search_grid.setResizeMode(QListWidget.Adjust)
        self.search_grid.setUniformItemSizes(True)
        self.search_grid.setWordWrap(True)
        self.search_grid.setContextMenuPolicy(Qt.CustomContextMenu)
        self.search_grid.customContextMenuRequested.connect(self.show_grid_context_menu)
        self.search_grid.itemDoubleClicked.connect(lambda item: self.handle_double_click_meta(item.data(Qt.UserRole)))
        
        self.search_grid.setStyleSheet("""
            QListWidget { background: transparent; border: none; outline: none; color: #c9d1d9; font-weight: bold; }
            QListWidget::item { background: rgba(33, 38, 45, 0.6); border: 1px solid #30363d; border-radius: 10px; padding: 10px; margin: 6px; }
            QListWidget::item:hover { background: rgba(88, 166, 255, 0.15); border: 1px solid #58a6ff; }
            QListWidget::item:selected { background: rgba(88, 166, 255, 0.35); border: 2px solid #58a6ff; color: #ffffff; }
        """)
        
        self.search_view_stack.addWidget(self.search_grid)
        tab1_layout.addWidget(self.search_view_stack)

        self.tabs.addTab(tab1_container, "📋 Search Results")

        # --- TAB 2: VISUAL GRID MAP ---
        map_tab_container = QWidget(); map_tab_layout = QVBoxLayout(map_tab_container); map_tab_layout.setContentsMargins(0, 0, 0, 0); map_tab_layout.setSpacing(5)
        map_quick_lay = QHBoxLayout()
        self.map_highlight_input = QLineEdit(); self.map_highlight_input.setPlaceholderText("🔍 Map Highlight (Alt+M): Name/Path (Press Enter)...")
        self.map_highlight_input.returnPressed.connect(self.trigger_map_highlight)
        self.map_tag_highlight_input = QLineEdit(); self.map_tag_highlight_input.setPlaceholderText("🏷️ Map Highlight (Alt+N): Enter Tag to Border Tiles (Press Enter)...")
        self.map_tag_highlight_input.returnPressed.connect(self.trigger_map_highlight)
        map_quick_lay.addWidget(self.map_highlight_input); map_quick_lay.addWidget(self.map_tag_highlight_input)
        map_tab_layout.addLayout(map_quick_lay)

        self.map_scroll = QScrollArea()
        self.map_scroll.setWidgetResizable(True); self.map_scroll.setFrameShape(QScrollArea.NoFrame)
        self.map_scroll.setAlignment(Qt.AlignTop | Qt.AlignHCenter)
        self.map_scroll.setContextMenuPolicy(Qt.CustomContextMenu)
        self.map_scroll.customContextMenuRequested.connect(self.show_map_context_menu)
        
        map_widget = QWidget(); map_lay = QVBoxLayout(map_widget); map_lay.setContentsMargins(0, 0, 0, 0)
        if MATPLOTLIB_AVAILABLE:
            self.figure_map = Figure(dpi=100)
            self.canvas_map = FigureCanvas(self.figure_map)
            self.canvas_map.setContextMenuPolicy(Qt.CustomContextMenu)
            self.canvas_map.customContextMenuRequested.connect(self.show_map_context_menu)
            self.canvas_map.mpl_connect('button_press_event', self.on_map_click)
            self.canvas_map.wheelEvent = lambda event: self.map_scroll.wheelEvent(event)
            map_lay.addWidget(self.canvas_map, alignment=Qt.AlignTop | Qt.AlignHCenter)
        else:
            self.figure_map = None; map_lay.addWidget(QLabel("Matplotlib is required."))
            
        self.map_scroll.setWidget(map_widget); map_tab_layout.addWidget(self.map_scroll); self.tabs.addTab(map_tab_container, "🗺️ Visual Grid Map")
        
        # --- TAB 3: 24 HOUR TIME HEATMAP ---
        time_widget = QWidget(); time_lay = QVBoxLayout(time_widget); time_lay.setContentsMargins(10, 10, 10, 10)
        self.time_scroll = QScrollArea()
        self.time_scroll.setWidgetResizable(True); self.time_scroll.setFrameShape(QScrollArea.NoFrame)
        self.time_scroll.setAlignment(Qt.AlignTop | Qt.AlignHCenter)
        self.time_scroll.setContextMenuPolicy(Qt.CustomContextMenu)
        self.time_scroll.customContextMenuRequested.connect(self.show_time_context_menu)
        
        tmap_w = QWidget(); tmap_l = QVBoxLayout(tmap_w); tmap_l.setContentsMargins(0,0,0,0)
        if MATPLOTLIB_AVAILABLE:
            self.figure_time = Figure(dpi=100)
            self.canvas_time = FigureCanvas(self.figure_time)
            self.canvas_time.setContextMenuPolicy(Qt.CustomContextMenu)
            self.canvas_time.customContextMenuRequested.connect(self.show_time_context_menu)
            self.canvas_time.mpl_connect('button_press_event', self.on_time_click)
            self.canvas_time.wheelEvent = lambda event: self.time_scroll.wheelEvent(event)
            tmap_l.addWidget(self.canvas_time, alignment=Qt.AlignTop | Qt.AlignHCenter)
        else:
            self.figure_time = None; tmap_l.addWidget(QLabel("Matplotlib is required."))
            
        self.time_scroll.setWidget(tmap_w); time_lay.addWidget(self.time_scroll, stretch=1)
        self.tabs.addTab(time_widget, "🕒 Time Heatmap")

        # --- TAB 4: ADVANCED SEARCH ANALYTICS ---
        chart_widget = QWidget(); chart_lay = QVBoxLayout(chart_widget); chart_lay.setContentsMargins(10, 10, 10, 10)
        c_top = QHBoxLayout()
        self.combo_chart_metric = QComboBox()
        self.combo_chart_metric.addItems([
            "By Database (Count)", "By Database (Size MB)",   # <-- Add these two metrics
            "By Category (Count)", "By Category (Size MB)",
            "By Extension (Count)", "By Extension (Size MB)", 
            "By Tags (Count)", "By Tags (Size MB)", "By Tags (Size MB - Flat)",
            "By Virtual Path (Count)", "By Virtual Path (Size MB)", 
            "By Year (Count)", "By Year (Size MB)",
            "By Month (Count)", "By Month (Size MB)",
            "By Week (Count)", "By Week (Size MB)",
            "By Day (Count)", "By Day (Size MB)",
            "By Date (Count)", "By Date (Size MB)",
            "By 24-Hour Time (Count)", "By 24-Hour Time (Size MB)"
        ])
        
        self.combo_chart_sort = QComboBox()
        self.combo_chart_sort.addItems([
            "Sort: Value (High to Low)",
            "Sort: Value (Low to High)",
            "Sort: Name/Time (Descending)",
            "Sort: Name/Time (Ascending)"
        ])
        
        self.combo_chart_metric.currentTextChanged.connect(self.safe_render_analytics)
        self.combo_chart_sort.currentTextChanged.connect(self.safe_render_analytics)
        
        c_top.addWidget(QLabel("<b>Chart Metric:</b>")); c_top.addWidget(self.combo_chart_metric, stretch=1)
        c_top.addWidget(QLabel("<b>Order:</b>")); c_top.addWidget(self.combo_chart_sort, stretch=1)
        chart_lay.addLayout(c_top)
        
        self.chart_filter = None  # Add this initialization

        if MATPLOTLIB_AVAILABLE:
            self.figure_chart = Figure(dpi=100)
            self.canvas_chart = FigureCanvas(self.figure_chart)
            self.canvas_chart.setContextMenuPolicy(Qt.CustomContextMenu)
            self.canvas_chart.customContextMenuRequested.connect(self.show_chart_context_menu)
            
            # --- ADD THESE TWO LINES FOR TOOLTIPS & CLICKS ---
            self.canvas_chart.mpl_connect("motion_notify_event", self.on_chart_hover)
            self.canvas_chart.mpl_connect("button_press_event", self.on_chart_click)
            
            chart_lay.addWidget(self.canvas_chart, stretch=1)
        else:
            self.figure_chart = None
            
        self.tabs.addTab(chart_widget, "📊 Search Analytics")

        # --- TAB 5: SIMPLE DIRECTORY SCANNER ---
        telemetry_widget = QWidget(); tele_lay = QVBoxLayout(telemetry_widget); tele_lay.setContentsMargins(10, 10, 10, 10)
        self.txt_tele_log = QPlainTextEdit(); self.txt_tele_log.setReadOnly(True)
        self.txt_tele_log.setContextMenuPolicy(Qt.CustomContextMenu)
        self.txt_tele_log.customContextMenuRequested.connect(self.show_telemetry_context_menu)
        tele_lay.addWidget(QLabel("Live Directory Path Tracing:")); tele_lay.addWidget(self.txt_tele_log)
        self.tabs.addTab(telemetry_widget, "📡 Live Scanner")

        main_layout.addWidget(self.tabs, stretch=1)

        # --- SAFE SHORTCUTS ---
        QShortcut(QKeySequence("Alt+h"), self).activated.connect(self.safe_toggle_zen)
        QShortcut(QKeySequence("Ctrl+h"), self).activated.connect(self.toggle_zen_mode)
        QShortcut(QKeySequence("Alt+f"), self).activated.connect(lambda: self.safe_focus_filter("name"))
        QShortcut(QKeySequence("Ctrl+f"), self).activated.connect(lambda: self.force_focus_filter("name"))
        QShortcut(QKeySequence("Alt+t"), self).activated.connect(lambda: self.safe_focus_filter("tag"))
        QShortcut(QKeySequence("Ctrl+t"), self).activated.connect(lambda: self.force_focus_filter("tag"))
        QShortcut(QKeySequence("Alt+m"), self).activated.connect(lambda: self.safe_focus_filter("mname"))
        QShortcut(QKeySequence("Ctrl+m"), self).activated.connect(lambda: self.force_focus_filter("mname"))
        QShortcut(QKeySequence("Alt+n"), self).activated.connect(lambda: self.safe_focus_filter("mtag"))
        QShortcut(QKeySequence("Ctrl+n"), self).activated.connect(lambda: self.force_focus_filter("mtag"))
        QShortcut(QKeySequence("Alt+s"), self).activated.connect(self.safe_focus_search)
        QShortcut(QKeySequence("Ctrl+s"), self).activated.connect(self.txt_name.setFocus)
        QShortcut(QKeySequence("Alt+c"), self).activated.connect(self.safe_trigger_context)
        QShortcut(QKeySequence("Ctrl+c"), self).activated.connect(self.trigger_context_menu_shortcut)
        QShortcut(QKeySequence("Alt+e"), self).activated.connect(self.safe_focus_viewport)
        for i in range(1, 6):
            QShortcut(QKeySequence(f"Alt+{i}"), self).activated.connect(lambda idx=i-1: self.safe_switch_tab(idx))
            QShortcut(QKeySequence(f"Ctrl+{i}"), self).activated.connect(lambda idx=i-1: self.force_switch_tab(idx))

        self.apply_theme()
        
        saved_state = self.settings.value("table_state")
        if saved_state:
            self.table.horizontalHeader().restoreState(saved_state)
        else:
            self.table.setColumnHidden(0, True) 
            self.table.setColumnHidden(7, True) 
            self.table.setColumnHidden(8, True) 

    def toggle_search_view_mode(self):
        new_idx = 1 if self.search_view_stack.currentIndex() == 0 else 0
        self.search_view_stack.setCurrentIndex(new_idx)
        self.btn_search_view_mode.setText("📄 List View" if new_idx == 1 else "🖼️ Grid View")

    def set_search_grid_size(self, size_name):
        sizes = {
            "Small": (QSize(100, 115), QSize(55, 55)),
            "Medium": (QSize(140, 160), QSize(80, 80)),
            "Large": (QSize(190, 210), QSize(125, 125)),
            "Extra Large": (QSize(250, 275), QSize(180, 180))
        }
        grid_sz, icon_sz = sizes.get(size_name, sizes["Medium"])
        self.search_grid.setGridSize(grid_sz)
        self.search_grid.setIconSize(icon_sz)
        self.settings.setValue("search_grid_size", size_name)

    def generate_thumbnails_search_results(self):
        items = []
        for r in self.current_results:
            db_id = r[0]
            real_p = r[7]
            is_fldr = r[6]
            if not is_fldr and real_p and os.path.exists(real_p):
                items.append(("file", real_p, db_id))

        if not items:
            return QMessageBox.information(self, "Thumbnails", "No physical files found to thumbnail.")

        from main import ThumbnailGeneratorThread
        self.thumb_dlg = QProgressDialog(f"Extracting thumbnails for {len(items)} files...", "Cancel", 0, len(items), self)
        self.thumb_dlg.setWindowModality(Qt.WindowModal)
        self.thumb_dlg.show()

        self.thumb_worker = ThumbnailGeneratorThread(items, self.active_db, self)
        self.thumb_worker.progress.connect(self.thumb_dlg.setValue)
        self.thumb_worker.finished.connect(lambda: (self.thumb_dlg.close(), self.icon_cache.clear(), self.trigger_search()))
        self.thumb_dlg.canceled.connect(self.thumb_worker.cancel)
        self.thumb_worker.start()

    def show_grid_context_menu(self, pos):
        item = self.search_grid.itemAt(pos)
        if not item: return
        meta = item.data(Qt.UserRole)
        # Find matching row in table to route to standard context menu
        for r in range(self.table.rowCount()):
            row_meta = self.table.item(r, 9).data(Qt.UserRole)
            if row_meta and row_meta.get('id') == meta.get('id'):
                self.table.selectRow(r)
                rect = self.search_grid.visualItemRect(item)
                self.show_context_menu(self.table.viewport().mapFromGlobal(self.search_grid.mapToGlobal(rect.center())))
                break

    def handle_double_click_meta(self, meta):
        if meta and meta.get('real_path') and os.path.exists(meta['real_path']):
            self.open_in_os(meta['real_path'])
        else:
            QMessageBox.warning(self, "Missing", "Physical file not found.")

    def show_multi_properties(self, selected_rows):
        total_size = 0
        folders = 0
        files = 0
        
        for idx in selected_rows:
            meta = self.table.item(idx.row(), 9).data(Qt.UserRole)
            if not meta: continue
            
            if meta.get('is_fldr'):
                folders += 1
                # Resolve the correct database path
                db = meta.get('db')
                db_path = str(Path("vman_data/compiled_views") / f"{db}.db") if db and db != "vman_vfs" else self.active_db
                
                try:
                    with sqlite3.connect(db_path) as conn:
                        cur = conn.cursor()
                        v_path = self.table.item(idx.row(), 3).text() + self.table.item(idx.row(), 1).text() + "/"
                        cnt, sz = cur.execute("SELECT COUNT(id), SUM(size) FROM virtual_fs WHERE parent_path LIKE ? AND is_folder=0 AND in_trash=0", (f"{v_path}%",)).fetchone()
                        files += cnt or 0
                        total_size += sz or 0
                except Exception as e: print(e)
            else:
                files += 1
                total_size += meta.get('size', 0)
                
        QMessageBox.information(self, "Multi-Selection Properties", 
            f"<b>Selected Items:</b> {len(selected_rows)}<br><br>"
            f"<b>Total Folders:</b> {folders}<br>"
            f"<b>Total Files:</b> {files}<br>"
            f"<b>Combined Size:</b> {self.human_size(total_size)}"
        )

    def select_search_databases(self):
        views_dir = Path("vman_data/compiled_views")
        main_db = Path("vman_data/vman_vfs.db")
        all_dbs = []
        if main_db.exists(): all_dbs.append(str(main_db))
        if views_dir.exists():
            for f in sorted(list(views_dir.glob("*.db"))):
                if str(f.resolve()) != str(main_db.resolve()):
                    all_dbs.append(str(f))

        dlg = QDialog(self)
        dlg.setWindowTitle("Select Databases to Search")
        dlg.resize(450, 400)
        lay = QVBoxLayout(dlg)
        lay.addWidget(QLabel("<b>Tick databases to include in multi-search:</b>"))

        list_w = QListWidget()
        for p in all_dbs:
            it = QListWidgetItem(Path(p).stem)
            it.setFlags(it.flags() | Qt.ItemIsUserCheckable)
            it.setCheckState(Qt.Checked if p in self.active_db_list else Qt.Unchecked)
            it.setData(Qt.UserRole, p)
            list_w.addItem(it)
        lay.addWidget(list_w)

        btn_box = QHBoxLayout()
        btn_ok = QPushButton("Save Selection")
        btn_ok.setStyleSheet("background-color: #2ea043; color: white; font-weight: bold; padding: 6px;")
        btn_ok.clicked.connect(dlg.accept)
        btn_box.addStretch(); btn_box.addWidget(btn_ok)
        lay.addLayout(btn_box)

        if dlg.exec() == QDialog.Accepted:
            selected = []
            for i in range(list_w.count()):
                it = list_w.item(i)
                if it.checkState() == Qt.Checked:
                    selected.append(it.data(Qt.UserRole))
            if selected:
                self.active_db_list = selected
                self.lbl_status.setText(f"Multi-Search configured: {len(selected)} databases selected.")
    
    def check_lazy_load(self, value):
        if not getattr(self, '_lazy_enabled', False) or self.is_searching or self.is_rendering: return
        scrollbar = self.table.verticalScrollBar()
        
        if value >= scrollbar.maximum() - 5: 
            current_rows = self.table.rowCount()
            total = len(self.current_results)
            if current_rows < total:
                end_idx = min(current_rows + 500, total)
                chunk = self.current_results[current_rows:end_idx]
                
                self.table.setUpdatesEnabled(False)
                for i, row_data in enumerate(chunk):
                    if self._abort_render: break
                    row = current_rows + i
                    self.table.insertRow(row)
                    
                    # Unpack 13 items
                    db_id, name, p_path, ext, size, mod, is_fldr, real_path, tags, color_tag, cat_val, sha256_val, db_label = row_data[:13]
                    
                    # Col 0: S.No
                    sno_item = NumericTableItem(str(row + 1)); sno_item.setData(Qt.UserRole, row + 1)
                    self.table.setItem(row, 0, sno_item)
                    
                    # Col 1: Name
                    name_item = QTableWidgetItem(name)
                    name_item.setIcon(self.get_icon(is_fldr, name, ext, db_id))
                    if color_tag and color_tag in self.VMAN_COLORS: 
                        name_item.setBackground(QBrush(self.VMAN_COLORS[color_tag])) 
                        name_item.setForeground(QColor("#ffffff")) 
                    self.table.setItem(row, 1, name_item)
                    
                    # Col 2: Database Origin
                    db_item = QTableWidgetItem(db_label)
                    db_item.setForeground(QBrush(QColor("#58a6ff")))
                    self.table.setItem(row, 2, db_item)

                    # Col 3: Virtual Path
                    self.table.setItem(row, 3, QTableWidgetItem(p_path))
                    
                    # Col 4: Type
                    type_str = "Folder" if is_fldr else (ext[1:].upper() + " File" if ext.startswith(".") else (ext.upper() + " File" if ext else "File"))
                    self.table.setItem(row, 4, QTableWidgetItem(type_str))
                    
                    # Col 5: Size
                    sz_str = "--" if is_fldr else self.human_size(size)
                    sz_item = NumericTableItem(sz_str); sz_item.setData(Qt.UserRole, size if not is_fldr else -1)
                    self.table.setItem(row, 5, sz_item)
                    
                    # Col 6: Mod Date
                    self.table.setItem(row, 6, QTableWidgetItem(str(mod)))
                    
                    # Col 7: Labels
                    eff_tag = tags if tags else self.inherited_tags.get(db_id, "")
                    self.table.setItem(row, 7, QTableWidgetItem(str(eff_tag) if eff_tag else ""))
                    
                    # Col 8: SHA
                    sha_item = QTableWidgetItem(str(sha256_val) if sha256_val else "--")
                    sha_item.setForeground(QColor("#8b949e"))
                    self.table.setItem(row, 8, sha_item)
                    
                    # Col 9: Meta Data
                    meta_item = QTableWidgetItem("")
                    meta_item.setData(Qt.UserRole, {'id': db_id, 'is_fldr': is_fldr, 'real_path': real_path, 'tags': eff_tag, 'size': size, 'mod': mod, 'name': name, 'ext': ext, 'cat': cat_val, 'db': db_label})
                    self.table.setItem(row, 9, meta_item)
                    
                self.table.setUpdatesEnabled(True)

    def is_typing(self):
        fw = QApplication.focusWidget()
        return isinstance(fw, (QLineEdit, QPlainTextEdit, QComboBox, QDoubleSpinBox, QSpinBox, QDateEdit, QTimeEdit))

    def safe_toggle_zen(self):
        if not self.is_typing(): self.toggle_zen_mode()
            
    def safe_focus_search(self):
        if not self.is_typing(): self.txt_name.setFocus()

    def safe_focus_filter(self, ftype):
        if self.is_typing(): return
        self.force_focus_filter(ftype)

    def force_focus_filter(self, ftype):
        idx = self.tabs.currentIndex()
        if ftype == "name" and idx == 0: self.local_filter_input.setFocus()
        elif ftype == "tag" and idx == 0: self.local_tag_filter_input.setFocus()
        elif ftype == "mname" or (ftype == "name" and idx in [1, 2]): self.map_highlight_input.setFocus()
        elif ftype == "mtag" or (ftype == "tag" and idx in [1, 2]): self.map_tag_highlight_input.setFocus()

    def safe_trigger_context(self):
        if not self.is_typing(): self.trigger_context_menu_shortcut()

    def safe_switch_tab(self, idx):
        if not self.is_typing(): self.force_switch_tab(idx)
            
    def force_switch_tab(self, idx):
        self.tabs.setCurrentIndex(idx)
        self.safe_focus_viewport()
        
    def safe_focus_viewport(self):
        idx = self.tabs.currentIndex()
        if idx == 0: self.table.setFocus()
        elif idx == 1: self.map_scroll.setFocus()
        elif idx == 2: self.time_scroll.setFocus()
        elif idx == 3: self.tabs.widget(3).setFocus()
        elif idx == 4: self.txt_tele_log.setFocus()

    def trigger_context_menu_shortcut(self):
        idx = self.tabs.currentIndex()
        if idx == 0: 
            if self.table.selectedItems():
                rect = self.table.visualItemRect(self.table.selectedItems()[0])
                pos = self.table.viewport().mapToGlobal(rect.center())
                self.show_context_menu(self.table.viewport().mapFromGlobal(pos))
            else:
                pos = self.table.viewport().mapToGlobal(self.table.rect().center())
                self.show_context_menu(self.table.viewport().mapFromGlobal(pos))
        elif idx == 1: self.show_map_context_menu(QCursor.pos())
        elif idx == 2: self.show_time_context_menu(QCursor.pos())
        elif idx == 3: self.show_chart_context_menu(QCursor.pos())
        elif idx == 4: self.show_telemetry_context_menu(QCursor.pos())

    def toggle_ui_visibility(self):
        self.top_wrap.setVisible(not self.top_wrap.isVisible())

    def toggle_zen_mode(self):
        state = not self.top_wrap.isVisible()
        self.top_wrap.setVisible(state)
        self.lbl_status.setVisible(state)
        self.tabs.tabBar().setVisible(state)

    def trigger_map_highlight(self):
        self._show_highlight_dialog = True
        self.safe_render_map()
        self.safe_render_time_heatmap()
        self._show_highlight_dialog = False

    def reset_filters(self):
        self.chart_filter = None 
        self.txt_name.clear()
        self.txt_path.clear()
        self.txt_search_tags.clear()
        self.txt_ext.clear()
        self.txt_exclude_ext.clear()
        self.txt_exclude_name.clear()
        self.combo_category.setCurrentIndex(0)
        self.combo_match.setCurrentIndex(0)
        self.combo_look_for.setCurrentIndex(0)
        self.chk_use_date.setChecked(False)
        self.chk_use_time.setChecked(False)
        self.spin_size_min.setValue(0)
        self.spin_size_max.setValue(999999)
        self.chk_unique.setChecked(False)
        self.chk_day_range.setChecked(False)
        self.spin_lvl_min.setValue(0)
        self.spin_lvl_max.setValue(999)
        
        # --- NEW RESETS ---
        self.spin_len_min.setValue(0)
        self.spin_len_max.setValue(999)
        if hasattr(self, 'combo_name_pattern'):
            self.combo_name_pattern.setCurrentIndex(0)

    def apply_local_filter(self):
        name_path_text = self.local_filter_input.text().lower()
        tag_text = self.local_tag_filter_input.text().lower()
        self.table.setUpdatesEnabled(False) 
        
        for row in range(self.table.rowCount()):
            item_name = self.table.item(row, 1)
            item_path = self.table.item(row, 3) # <-- Changed from 2 to 3
            item_tags = self.table.item(row, 7) # <-- Changed from 6 to 7
            item_meta = self.table.item(row, 9) # <-- Changed from 8 to 9
            
            if not item_name or not item_path or not item_tags or not item_meta: continue
            
            m_data = item_meta.data(Qt.UserRole)
            is_fldr = m_data.get('is_fldr', False)
            
            if self.table_view_mode == "Files Only" and is_fldr:
                self.table.setRowHidden(row, True)
                continue
            if self.table_view_mode == "Folders Only" and not is_fldr:
                self.table.setRowHidden(row, True)
                continue
            
            match_np = (not name_path_text) or (name_path_text in item_name.text().lower() or name_path_text in item_path.text().lower())
            match_tag = (not tag_text) or (tag_text in item_tags.text().lower())
            
            # --- NEW: Check against Chart Selection Drill-down ---
            match_chart = True
            if getattr(self, 'chart_filter', None):
                metric, label = self.chart_filter
                is_size = "Size" in metric
                is_flat = "Flat" in metric
                
                if is_size and is_fldr and not is_flat:
                    match_chart = False
                else:
                    k = "Unknown"
                    if "Extension" in metric:
                        ext = m_data.get('ext', '')
                        k = ext.upper() if ext else "NONE"
                    elif "Category" in metric:
                        cat = m_data.get('cat', 'Others')
                        k = cat if cat else "Others"
                    elif "By Tags" in metric:
                        tags_str = m_data.get('tags', '')
                        if tags_str:
                            tags_list = [t.strip() for t in tags_str.split(',') if t.strip()]
                            if label in tags_list: k = label
                            else: k = "Mismatch"
                        else:
                            k = "Untagged"
                    elif "Virtual Path" in metric:
                        p = m_data.get('path', item_path.text())
                        k = p.split('/')[1] if len(p.split('/')) > 1 else "Root"
                    elif "Year" in metric:
                        mod_str = str(m_data.get('mod', ''))
                        k = mod_str[:4] if len(mod_str)>=4 else "Unknown"
                    elif "Month" in metric:
                        mod_str = str(m_data.get('mod', ''))
                        k = mod_str[:7] if len(mod_str)>=7 else "Unknown"
                    elif "Week" in metric:
                        mod_str = str(m_data.get('mod', ''))
                        if len(mod_str) >= 10:
                            try:
                                dto = datetime.datetime.strptime(mod_str[:10], "%Y-%m-%d")
                                y, w, _ = dto.isocalendar()
                                k = f"{y}-W{w:02d}"
                            except: pass
                    elif "Day" in metric:
                        mod_str = str(m_data.get('mod', ''))
                        if len(mod_str) >= 10:
                            try:
                                dto = datetime.datetime.strptime(mod_str[:10], "%Y-%m-%d")
                                k = dto.strftime("%A")
                            except: pass
                    elif "By Date" in metric:
                        mod_str = str(m_data.get('mod', ''))
                        k = mod_str[8:10] if len(mod_str)>=10 else "Unknown"
                    elif "24-Hour Time" in metric:
                        dt_str = str(m_data.get('mod', ''))
                        k = dt_str[11:13] + ":00" if len(dt_str)>=16 else "Unknown"
                        
                    match_chart = (k == label)
            
            self.table.setRowHidden(row, not (match_np and match_tag and match_chart))
            
        self.table.setUpdatesEnabled(True)

    def on_chart_hover(self, event):
        if not getattr(self, 'chart_labels', None) or not self.figure_chart: return
        if event.inaxes != self.figure_chart.axes[0] or event.xdata is None or event.ydata is None:
            if getattr(self, 'chart_annot', None) and self.chart_annot.get_visible():
                self.chart_annot.set_visible(False)
                self.canvas_chart.draw_idle()
            return

        idx = int(round(event.xdata))
        if 0 <= idx < len(self.chart_labels) and abs(event.xdata - idx) <= 0.45:
            lbl = self.chart_labels[idx]
            val = self.chart_values[idx]
            metric = self.combo_chart_metric.currentText()
            unit = "MB" if "Size" in metric else "Files"
            val_str = f"{val:,.2f} {unit}" if "Size" in metric else f"{int(val):,} {unit}"

            if getattr(self, 'chart_annot', None):
                self.chart_annot.xy = (idx, val)
                self.chart_annot.set_text(f"{lbl}\n{val_str}")
                self.chart_annot.set_visible(True)
                self.canvas_chart.draw_idle()
        else:
            if getattr(self, 'chart_annot', None) and self.chart_annot.get_visible():
                self.chart_annot.set_visible(False)
                self.canvas_chart.draw_idle()

    def on_chart_click(self, event):
        if not getattr(self, 'chart_labels', None) or not self.figure_chart: return
        if event.inaxes != self.figure_chart.axes[0] or event.xdata is None: return

        idx = int(round(event.xdata))
        if not (0 <= idx < len(self.chart_labels) and abs(event.xdata - idx) <= 0.45):
            return

        lbl = self.chart_labels[idx]
        metric = self.combo_chart_metric.currentText()
        is_size = "Size" in metric
        is_flat = "Flat" in metric

        filtered_results = []
        for r in self.current_results:
            db_id = r[0]; name = r[1]; p_path = r[2]; ext = r[3]; size = r[4]; mod = r[5]
            is_fldr = r[6]; real_path = r[7]; tags = r[8]; color_tag = r[9]; cat_val = r[10]
            sha256_val = r[11]
            db_origin = r[12] if len(r) > 12 else Path(self.active_db).stem

            if is_size and is_fldr and not is_flat: continue

            k = "Unknown"
            if "By Database" in metric:
                k = db_origin
            elif "Extension" in metric:
                k = ext.upper() if ext else "NONE"
            elif "Category" in metric:
                k = cat_val if cat_val else "Others"
            elif "By Tags" in metric:
                tags_str = tags if (is_flat or not is_size) else (tags if tags else self.inherited_tags.get(db_id, ""))
                if tags_str:
                    tags_list = [t.strip() for t in tags_str.split(',') if t.strip()]
                    if lbl in tags_list: k = lbl
                    else: k = "Mismatch"
                else:
                    k = "Untagged"
            elif "Virtual Path" in metric:
                k = p_path.split('/')[1] if len(p_path.split('/')) > 1 else "Root"
            elif "Year" in metric:
                mod_str = str(mod)
                k = mod_str[:4] if len(mod_str) >= 4 else "Unknown"
            elif "Month" in metric:
                mod_str = str(mod)
                k = mod_str[:7] if len(mod_str) >= 7 else "Unknown"
            elif "Week" in metric:
                mod_str = str(mod)
                if len(mod_str) >= 10:
                    try:
                        dto = datetime.datetime.strptime(mod_str[:10], "%Y-%m-%d")
                        y, w, _ = dto.isocalendar()
                        k = f"{y}-W{w:02d}"
                    except: pass
            elif "Day" in metric:
                mod_str = str(mod)
                if len(mod_str) >= 10:
                    try:
                        dto = datetime.datetime.strptime(mod_str[:10], "%Y-%m-%d")
                        k = dto.strftime("%A")
                    except: pass
            elif "By Date" in metric:
                mod_str = str(mod)
                k = mod_str[8:10] if len(mod_str) >= 10 else "Unknown"
            elif "24-Hour Time" in metric:
                dt_str = str(mod)
                k = dt_str[11:13] + ":00" if len(dt_str) >= 16 else "Unknown"

            if k == lbl:
                filtered_results.append(r)

        if filtered_results:
            self._abort_render = False
            self.populate_table(filtered_results, len(self.current_results))
            self.tabs.setCurrentIndex(0)
            self.lbl_status.setText(f"Viewing {len(filtered_results)} items for Chart Selection: {metric} ➔ {lbl}")
        else:
            self.lbl_status.setText(f"No items found for Chart Selection: {metric} ➔ {lbl}")

    def _build_matrix_cache(self, files):
        self.matrix_cache = defaultdict(lambda: defaultdict(list))
        self.time_matrix_cache = defaultdict(lambda: defaultdict(list))
        self.max_hits = 1
        
        self.inherited_tags = {}
        folder_tags = { f"{r[2]}{r[1]}/": r[8] for r in files if r[6] and r[8] }
        sorted_folders = sorted(folder_tags.keys(), key=len, reverse=True)
        
        for r in files:
            if not r[6] and not r[8]:
                p = r[2]
                for f_path in sorted_folders:
                    if p.startswith(f_path):
                        self.inherited_tags[r[0]] = folder_tags[f_path]
                        break
                        
        self.duplicate_map = {}
        hash_groups = defaultdict(list)
        for r in files:
            if r[11]: hash_groups[r[11]].append(r)
            
        for h, items in hash_groups.items():
            if len(items) > 1:
                items_sorted = sorted(items, key=lambda x: str(x[5]))
                self.duplicate_map[items_sorted[0][0]] = "Original"
                for item in items_sorted[1:]:
                    self.duplicate_map[item[0]] = "Duplicate"
        
        for r in files:
            mod_str = str(r[5])
            if len(mod_str) >= 10:
                ym = mod_str[:7]
                try: day = int(mod_str[8:10])
                except: continue
                
                eff_tag = r[8] if r[8] else self.inherited_tags.get(r[0], "")
                meta = {
                    'id': r[0], 'ext': r[3], 'cat': r[10] if r[10] else "Others", 
                    'tag_exact': r[8], 'tag_inherited': eff_tag, 
                    'size': r[4] or 0, 'name': r[1].lower(), 'path': r[2].lower(), 'is_fldr': r[6],
                    'dup_status': self.duplicate_map.get(r[0], "Unique"), 'mod_full': mod_str
                } 
                self.matrix_cache[ym][day].append(meta)
                if len(self.matrix_cache[ym][day]) > self.max_hits: 
                    self.max_hits = len(self.matrix_cache[ym][day])
                    
                if len(mod_str) >= 16:
                    try:
                        hh = int(mod_str[11:13]); mm = int(mod_str[14:16])
                        b = hh + (0.5 if mm >= 30 else 0.0)
                        self.time_matrix_cache[ym][b].append(meta)
                    except: pass

    # --- Context Menus ---
    def show_top_context_menu(self, pos):
        menu = QMenu(self)
        
        act_refresh = menu.addAction("🔄 Refresh Entire Search Engine")
        act_refresh.triggered.connect(self.trigger_search)
        
        # --- FAST MODE MOVED HERE ---
        act_fast = menu.addAction("⚡ Fast Mode (Disable Visual Maps)")
        act_fast.setCheckable(True)
        act_fast.setChecked(self.fast_mode)
        act_fast.toggled.connect(self.toggle_fast_mode)
        # ----------------------------
        
        menu.addSeparator()
        
        act_toggle = menu.addAction("👁️ Toggle Search Box & Buttons")
        act_toggle.triggered.connect(self.toggle_ui_visibility)
        
        menu.exec(QCursor.pos())

    def show_telemetry_context_menu(self, pos):
        menu = QMenu(self)
        act_toggle = menu.addAction("Enable Live Directory Tracing (Impacts Performance)")
        act_toggle.setCheckable(True)
        act_toggle.setChecked(self.verbose_telemetry)
        act_toggle.toggled.connect(self.toggle_verbose)
        menu.addSeparator()
        menu.addAction("Clear Tracker Log", self.txt_tele_log.clear)
        menu.exec(QCursor.pos())
        
    def toggle_verbose(self, checked):
        self.verbose_telemetry = checked
        self.settings.setValue("verbose_telemetry", checked)
        if not checked: self.txt_tele_log.appendPlainText("[SYSTEM] Directory tracing paused.")

    def show_chart_context_menu(self, pos):
        menu = QMenu(self)
        act_save = menu.addAction("💾 Save Chart as Image")
        act_save.triggered.connect(lambda: self.save_canvas_as_image(self.figure_chart, "Analytics_Chart"))
        menu.exec(QCursor.pos())

    def _build_color_menu(self, menu, mode_attr, callback):
        color_menu = menu.addMenu("🎨 Map Color Mode")
        act = color_menu.addAction("By Duplicates")
        act.setCheckable(True); act.setChecked(getattr(self, mode_attr) == "By Duplicates")
        act.triggered.connect(lambda checked=False: callback("By Duplicates"))
        
        color_menu.addSeparator()
        m_int = color_menu.addMenu("Intensity")
        for m in ["Intensity (Count)", "Intensity (Size MB)"]:
            act = m_int.addAction(m.replace("Intensity ", ""))
            act.setCheckable(True); act.setChecked(getattr(self, mode_attr) == m)
            act.triggered.connect(lambda checked=False, mode=m: callback(mode))
            
        m_cat = color_menu.addMenu("Category")
        for m in ["Category Simple (Count)", "Category Simple (Size MB)", "Category Gradient (Count)", "Category Gradient (Size MB)"]:
            act = m_cat.addAction(m.replace("Category ", ""))
            act.setCheckable(True); act.setChecked(getattr(self, mode_attr) == m)
            act.triggered.connect(lambda checked=False, mode=m: callback(mode))
            
        m_ext = color_menu.addMenu("Extension")
        for m in ["Extension Simple (Count)", "Extension Simple (Size MB)", "Extension Gradient (Count)", "Extension Gradient (Size MB)"]:
            act = m_ext.addAction(m.replace("Extension ", ""))
            act.setCheckable(True); act.setChecked(getattr(self, mode_attr) == m)
            act.triggered.connect(lambda checked=False, mode=m: callback(mode))
            
        m_tag = color_menu.addMenu("Tag")
        for m in ["Tag Simple (Count)", "Tag Simple (Size MB)", "Tag Simple (Size MB - Flat)", "Tag Gradient (Count)", "Tag Gradient (Size MB)", "Tag Gradient (Size MB - Flat)"]:
            act = m_tag.addAction(m.replace("Tag ", ""))
            act.setCheckable(True); act.setChecked(getattr(self, mode_attr) == m)
            act.triggered.connect(lambda checked=False, mode=m: callback(mode))

        color_menu.addSeparator()
        bg_menu = color_menu.addMenu("🖼️ Untagged / Unique Background...")
        for bg_mode in ["Muted Blue Intensity", "Gray Intensity", "Solid Dark Blue", "Solid Dark Gray", "Hidden (Empty)", "Custom Intensity..."]:
            bg_act = bg_menu.addAction(bg_mode)
            bg_act.setCheckable(True)
            bg_act.setChecked(getattr(self, 'dup_bg_mode', "Muted Blue Intensity") == bg_mode)
            bg_act.triggered.connect(lambda checked=False, m=bg_mode: self.change_dup_bg_mode(m))

    def change_dup_bg_mode(self, mode):
        if mode == "Custom Intensity...":
            QMessageBox.information(self, "Custom Background", "This mode uses the 'Low Intensity' and 'High Intensity' Base/Peak colors defined in 'Customize Map Colors'.")
            self.open_color_config()
        self.dup_bg_mode = mode
        self.settings.setValue("dup_bg_mode", mode)
        self.safe_render_map()
        self.safe_render_time_heatmap()

    def show_time_context_menu(self, pos):
        menu = QMenu(self)
        act_save = menu.addAction("💾 Save Time Map as Image")
        act_save.triggered.connect(lambda: self.save_canvas_as_image(self.figure_time, "Time_Map"))
        menu.addSeparator()
        
        layout_menu = menu.addMenu("🗓️ Time Layout Mode")
        for mode in ["Compact Matrix", "Segmented Years"]:
            act = layout_menu.addAction(mode)
            act.setCheckable(True)
            act.setChecked(self.time_layout_mode == mode)
            act.triggered.connect(lambda checked=False, m=mode: self.change_time_layout(m))
            
        act_lines = menu.addAction("Show 6-Hour Vertical Dividers")
        act_lines.setCheckable(True)
        act_lines.setChecked(self.time_show_lines)
        act_lines.triggered.connect(self.toggle_time_lines)
        
        hl_menu = menu.addMenu("🔦 Highlight Settings")
        fld_menu = hl_menu.addMenu("Match Field")
        for f in ["Name or Path", "Name Only", "Path Only"]:
            act = fld_menu.addAction(f)
            act.setCheckable(True)
            act.setChecked(self.map_highlight_field == f)
            act.triggered.connect(lambda checked=False, val=f: self.change_map_highlight_setting('field', val))
            
        typ_menu = hl_menu.addMenu("Target Type")
        for t in ["Files & Folders", "Files Only", "Folders Only"]:
            act = typ_menu.addAction(t)
            act.setCheckable(True)
            act.setChecked(self.map_highlight_type == t)
            act.triggered.connect(lambda checked=False, val=t: self.change_map_highlight_setting('type', val))
            
        hl_menu.addSeparator()
        act_hl_col = hl_menu.addAction("🎨 Border Color...")
        act_hl_col.triggered.connect(self.change_map_highlight_color)
        act_hl_size = hl_menu.addAction("📏 Border Size...")
        act_hl_size.triggered.connect(self.change_map_highlight_size)
        
        menu.addSeparator()
        self._build_color_menu(menu, 'time_color_mode', self.change_time_mode)
        
        menu.addSeparator()
        grad_menu = menu.addMenu("🌈 Gradient Color (Intensity Mode)")
        for grad in ["Fire", "Green", "Blue", "Yellow", "Excel (Green-Yellow-Red)", "Custom..."]:
            act = grad_menu.addAction(grad)
            act.setCheckable(True)
            act.setChecked(self.map_gradient == grad)
            act.triggered.connect(lambda checked=False, g=grad: self.change_map_gradient(g))
        menu.exec(QCursor.pos())

    def show_map_context_menu(self, pos):
        menu = QMenu(self)
        
        act_colors = menu.addAction("⚙️ Customize Category/Extension/Tag Colors")
        act_colors.triggered.connect(self.open_color_config)
        menu.addSeparator()
        
        act_save = menu.addAction("💾 Save Map as Image")
        act_save.triggered.connect(lambda: self.save_canvas_as_image(self.figure_map, "Grid_Map"))
        menu.addSeparator()
        
        layout_menu = menu.addMenu("🗓️ Calendar Layout Mode")
        modes = [
            "Compact Matrix (31 Days)", 
            "Segmented Years (31 Days)", 
            "True Calendar Standard (3x4 Grids)", 
            "True Calendar Widescreen (4x3 Grids)", 
            "GitHub Contribution (52 Weeks)"
        ]
        for mode in modes:
            act = layout_menu.addAction(mode)
            act.setCheckable(True)
            act.setChecked(self.map_layout_mode == mode)
            act.triggered.connect(lambda checked=False, m=mode: self.change_map_layout(m))
            
        act_dates = layout_menu.addAction("Show Dates in Tiles")
        act_dates.setCheckable(True)
        act_dates.setChecked(self.map_show_dates)
        act_dates.triggered.connect(self.toggle_map_dates)
            
        hl_menu = menu.addMenu("🔦 Highlight Settings")
        fld_menu = hl_menu.addMenu("Match Field")
        for f in ["Name or Path", "Name Only", "Path Only"]:
            act = fld_menu.addAction(f)
            act.setCheckable(True)
            act.setChecked(self.map_highlight_field == f)
            act.triggered.connect(lambda checked=False, val=f: self.change_map_highlight_setting('field', val))
            
        typ_menu = hl_menu.addMenu("Target Type")
        for t in ["Files & Folders", "Files Only", "Folders Only"]:
            act = typ_menu.addAction(t)
            act.setCheckable(True)
            act.setChecked(self.map_highlight_type == t)
            act.triggered.connect(lambda checked=False, val=t: self.change_map_highlight_setting('type', val))
            
        hl_menu.addSeparator()
        act_hl_col = hl_menu.addAction("🎨 Border Color...")
        act_hl_col.triggered.connect(self.change_map_highlight_color)
        act_hl_size = hl_menu.addAction("📏 Border Size...")
        act_hl_size.triggered.connect(self.change_map_highlight_size)
            
        menu.addSeparator()
        
        sort_menu = menu.addMenu("🔄 Sort Direction")
        for order in ["Top to Bottom (Newest First)", "Top to Bottom (Oldest First)"]:
            act = sort_menu.addAction(order)
            act.setCheckable(True)
            act.setChecked(self.map_sort_order == order)
            act.triggered.connect(lambda checked=False, o=order: self.change_map_sort(o))
            
        menu.addSeparator()
        
        size_menu = menu.addMenu("🔍 Tile Size")
        for size in ["Small (0.7x)", "Medium (1.0x)", "Large (1.2x)", "Custom..."]:
            act = size_menu.addAction(size)
            act.setCheckable(True)
            act.setChecked(self.map_tile_size == size)
            act.triggered.connect(lambda checked=False, s=size: self.change_map_size(s))
            
        act_margin = size_menu.addAction("📐 Adjust Top Margin...")
        act_margin.triggered.connect(self.adjust_top_margin)
            
        menu.addSeparator()
        self._build_color_menu(menu, 'map_color_mode', self.change_map_mode)
            
        menu.addSeparator()
        grad_menu = menu.addMenu("🌈 Gradient Color (Intensity Mode)")
        for grad in ["Fire", "Green", "Blue", "Yellow", "Excel (Green-Yellow-Red)", "Custom..."]:
            act = grad_menu.addAction(grad)
            act.setCheckable(True)
            act.setChecked(self.map_gradient == grad)
            act.triggered.connect(lambda checked=False, g=grad: self.change_map_gradient(g))
            
        menu.exec(QCursor.pos())
        
    def toggle_map_dates(self, checked):
        self.map_show_dates = checked
        self.settings.setValue("map_show_dates", checked)
        self.safe_render_map()
        
    def toggle_time_lines(self, checked):
        self.time_show_lines = checked
        self.settings.setValue("time_show_lines", checked)
        self.safe_render_time_heatmap()

    def change_time_layout(self, layout):
        self.time_layout_mode = layout
        self.settings.setValue("time_layout_mode", layout)
        self.safe_render_time_heatmap()

    def change_map_highlight_setting(self, stype, val):
        if stype == 'field':
            self.map_highlight_field = val
            self.settings.setValue("map_highlight_field", val)
        elif stype == 'type':
            self.map_highlight_type = val
            self.settings.setValue("map_highlight_type", val)
        self.safe_render_map()
        self.safe_render_time_heatmap()
        
    def change_map_highlight_color(self):
        color = QColorDialog.getColor(QColor(self.map_highlight_color), self, "Select Border Color")
        if color.isValid():
            self.map_highlight_color = color.name()
            self.settings.setValue("map_highlight_color", color.name())
            self.safe_render_map()
            self.safe_render_time_heatmap()
            
    def change_map_highlight_size(self):
        val, ok = QInputDialog.getDouble(self, "Border Size", "Enter border thickness:", self.map_highlight_size, 0.5, 10.0, 1)
        if ok:
            self.map_highlight_size = val
            self.settings.setValue("map_highlight_size", val)
            self.safe_render_map()
            self.safe_render_time_heatmap()
        
    def open_color_config(self):
        dlg = MapColorConfigDialog(self.current_results, self)
        if dlg.exec() == QDialog.Accepted:
            self.safe_render_map()
            self.safe_render_analytics()
            self.safe_render_time_heatmap()
            
    def adjust_top_margin(self):
        val, ok = QInputDialog.getDouble(self, "Adjust Top Margin", "Enter top padding value (0 to obliterate space, 78 is default):", self.map_top_margin, 0, 200, 1)
        if ok:
            self.map_top_margin = val
            self.settings.setValue("map_top_margin", val)
            self.safe_render_map()
            self.safe_render_time_heatmap()
        
    def save_canvas_as_image(self, figure, prefix):
        if not figure: return
        path, _ = QFileDialog.getSaveFileName(self, "Save Image", f"VMan_{prefix}.png", "PNG Images (*.png);;JPEG Images (*.jpg)", options=QFileDialog.DontUseNativeDialog)
        if path:
            prog = QProgressDialog("Rendering High-Resolution Image...\nThis may take a moment for massive maps.", "Cancel", 0, 0, self)
            prog.setWindowTitle("Saving Image")
            prog.setWindowModality(Qt.WindowModal)
            prog.show()
            QApplication.processEvents(); QThread.msleep(50); QApplication.processEvents()
            try:
                figure.savefig(path, bbox_inches='tight', dpi=300)
                prog.close()
                QMessageBox.information(self, "Success", f"Image saved successfully to:\n{path}")
            except Exception as e:
                prog.close()
                QMessageBox.critical(self, "Error", f"Failed to save image:\n{e}")

    def change_map_layout(self, layout):
        self.map_layout_mode = layout
        self.settings.setValue("map_layout_mode", layout)
        self.lbl_status.setText(f"✅ Map layout changed to: {layout}")
        self.safe_render_map()
        
    def change_map_sort(self, order):
        self.map_sort_order = order
        self.settings.setValue("map_sort_order", order)
        self.lbl_status.setText(f"✅ Sort order changed to: {order}")
        self.safe_render_map()
        self.safe_render_time_heatmap()
        
    def change_map_size(self, size):
        if size == "Custom...":
            val, ok = QInputDialog.getDouble(self, "Custom Tile Size", "Enter tile scale multiplier (0.1 to 5.0):", self.map_custom_scale, 0.1, 5.0, 2)
            if ok:
                self.map_custom_scale = val
                self.map_tile_size = "Custom..."
                self.settings.setValue("map_custom_scale", val)
                self.settings.setValue("map_tile_size", "Custom...")
                self.lbl_status.setText(f"✅ Map tile size scaled to: {val}x")
                self.safe_render_map()
                self.safe_render_time_heatmap()
        else:
            self.map_tile_size = size
            self.settings.setValue("map_tile_size", size)
            self.lbl_status.setText(f"✅ Map tile size changed to: {size}")
            self.safe_render_map()
            self.safe_render_time_heatmap()
        
    def change_map_mode(self, mode):
        self.map_color_mode = mode
        self.settings.setValue("map_color_mode", mode)
        self.lbl_status.setText(f"✅ Map color mode changed to: {mode}")
        self.safe_render_map()
        
    def change_time_mode(self, mode):
        self.time_color_mode = mode
        self.settings.setValue("time_color_mode", mode)
        self.lbl_status.setText(f"✅ Time color mode changed to: {mode}")
        self.safe_render_time_heatmap()
        
    def change_map_gradient(self, grad):
        if grad == "Custom...":
            QMessageBox.information(self, "Custom Gradient", "Please select Base and Peak colors in the Customize Colors menu.")
            self.open_color_config()
        self.map_gradient = grad
        self.settings.setValue("map_gradient", grad)
        if "Intensity" in self.map_color_mode or self.map_color_mode == "By Duplicates":
            self.lbl_status.setText(f"✅ Map intensity gradient changed to: {grad}")
            self.safe_render_map()
            self.safe_render_time_heatmap()
            
    def toggle_fast_mode(self, checked):
        self.fast_mode = checked
        self.settings.setValue("fast_mode", checked)
        if checked: self.lbl_status.setText("⚡ Fast Mode ON: Visual Maps & Analytics will be skipped for raw speed.")
        else: self.lbl_status.setText("✅ Fast Mode OFF: Visual Maps & Analytics restored.")

    def trigger_search(self):
        if not self.active_db or not os.path.exists(self.active_db): return QMessageBox.warning(self, "No Database", "No active database found.")
        self.is_searching = True
        self.is_rendering = False
        self._abort_render = False
        self.set_button_style("running")
        QApplication.processEvents() 
        
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        self.local_filter_input.clear()
        self.local_tag_filter_input.clear()
        self.map_highlight_input.clear()
        self.map_tag_highlight_input.clear()
        
        self.progress.setVisible(True)
        self.progress.setValue(0)
        self.lbl_status.setText("Searching...")
        self.txt_tele_log.clear()

        unit_mult = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3}
        mult = unit_mult.get(self.combo_sz_unit.currentText(), 1024**2)

        params = {
            'name': self.txt_name.text().strip(), 'path': self.txt_path.text().strip(),
            'tags': [t.strip().lower() for t in self.txt_search_tags.text().split(',') if t.strip()],
            'match': self.combo_match.currentText(), 'look_for': self.combo_look_for.currentText(),
            'case_sensitive': self.chk_case_sensitive.isChecked(), # <-- ADDED THIS
            'exts': [e.strip().lower() for e in self.txt_ext.text().split(',') if e.strip()],
            'ex_exts': [e.strip().lower() for e in self.txt_exclude_ext.text().split(',') if e.strip()],
            'ex_names': [n.strip().lower() for n in self.txt_exclude_name.text().split(',') if n.strip()],
            'category': getattr(self, 'combo_category', QComboBox()).currentText() if hasattr(self, 'combo_category') else "All",
            'sz_min': self.spin_size_min.value() * mult, 'sz_max': self.spin_size_max.value() * mult,
            'use_date': self.chk_use_date.isChecked(), 'date_start': self.date_start.date(), 'date_end': self.date_end.date(),
            'date_type': self.combo_date_type.currentText(),
            'use_time': self.chk_use_time.isChecked(),
            'time_start': self.time_start.time().toString("HH:mm"),
            'time_end': self.time_end.time().toString("HH:mm"),
            'use_day_range': self.chk_day_range.isChecked(),
            'day_start': self.combo_day_start.currentText(),
            'day_end': self.combo_day_end.currentText(),
            'lvl_min': self.spin_lvl_min.value(),
            'lvl_max': self.spin_lvl_max.value(),
            'len_min': self.spin_len_min.value(),
            'len_max': self.spin_len_max.value(),
            'name_pattern': getattr(self, 'combo_name_pattern', QComboBox()).currentText(), # <-- Add this line
            'unique_patterns': getattr(self.chk_unique, 'isChecked', lambda: False)(),
            'verbose': self.verbose_telemetry,
            'db': self.active_db,
            'db_list': getattr(self, 'active_db_list', [self.active_db])  # <-- Pass selected DBs
        }
        self.worker = SearchWorker(params)
        self.worker.progress_update.connect(self.progress.setValue)
        self.worker.results_ready.connect(self.store_and_populate)
        self.worker.start()

    def store_and_populate(self, results):
        self.progress.setVisible(False)
        self.current_results = results
        total = len(results)
        start_idx, end_idx = 0, total
        self._lazy_enabled = False 
        
        unique_exts = set(r[3] for r in results)
        if unique_exts and not self._abort_render:
            dlg = ExtFilterDialog(unique_exts, self)
            if dlg.exec() == QDialog.Accepted:
                allowed = dlg.get_allowed()
                results = [r for r in results if r[3] in allowed]
                self.current_results = results
                total = len(results)
                end_idx = total
        
        if total > 1500 and not self._abort_render:
            dlg = RowLimitDialogSearch(total, self)
            if dlg.exec() == QDialog.Accepted:
                start_idx, end_idx, self._lazy_enabled = dlg.get_values()
            else:
                self.lbl_status.setText("Search Aborted by User.")
                self.is_searching = False
                self.set_button_style("idle")
                return
                
        # Safely connect lazy loader to scrollbar without triggering Qt RuntimeWarnings
        if getattr(self, '_lazy_connected', False):
            try: self.table.verticalScrollBar().valueChanged.disconnect(self.check_lazy_load)
            except: pass
            
        if getattr(self, '_lazy_enabled', False):
            self.table.verticalScrollBar().valueChanged.connect(self.check_lazy_load)
            self._lazy_connected = True
        else:
            self._lazy_connected = False
            
        if not self.fast_mode and not self._abort_render:
            self._build_matrix_cache(results)
            self.safe_render_map()
            self.safe_render_time_heatmap()
            self.safe_render_analytics()
        else:
            if self.figure_map:
                self.figure_map.clear()
                self.figure_map.add_subplot(111).text(0.5, 0.5, "⚡ Fast Mode is ON\nMap rendering is disabled for maximum speed.", ha='center', va='center', color='gray')
                self.canvas_map.draw()
            if self.figure_chart:
                self.figure_chart.clear()
                self.figure_chart.add_subplot(111).text(0.5, 0.5, "⚡ Fast Mode is ON\nAnalytics rendering is disabled for maximum speed.", ha='center', va='center', color='gray')
                self.canvas_chart.draw()
            if self.figure_time:
                self.figure_time.clear()
                self.figure_time.add_subplot(111).text(0.5, 0.5, "⚡ Fast Mode is ON", ha='center', va='center', color='gray')
                self.canvas_time.draw()
                
        self.is_rendering = True       
        self.populate_table(results[start_idx:end_idx], total)

    def update_telemetry(self, p_path):
        self.txt_tele_log.appendPlainText(f"Scanning -> {p_path}")
        if self.txt_tele_log.document().blockCount() > 100:
            cursor = self.txt_tele_log.textCursor()
            cursor.movePosition(QTextCursor.Start)
            cursor.select(QTextCursor.BlockUnderCursor)
            cursor.removeSelectedText()
            cursor.deleteChar()
        self.txt_tele_log.verticalScrollBar().setValue(self.txt_tele_log.verticalScrollBar().maximum())

    def closeEvent(self, event):
        self.settings.setValue("table_state", self.table.horizontalHeader().saveState())
        self.settings.setValue("show_icons", self.show_icons)
        self.settings.setValue("map_layout_mode", self.map_layout_mode)
        self.settings.setValue("time_layout_mode", self.time_layout_mode)
        self.settings.setValue("time_color_mode", getattr(self, 'time_color_mode', "Intensity (Count)"))
        self.settings.setValue("time_show_lines", getattr(self, 'time_show_lines', False))
        self.settings.setValue("map_color_mode", self.map_color_mode)
        self.settings.setValue("map_gradient", self.map_gradient)
        self.settings.setValue("map_sort_order", self.map_sort_order)
        self.settings.setValue("map_tile_size", self.map_tile_size)
        self.settings.setValue("map_custom_scale", self.map_custom_scale)
        self.settings.setValue("map_top_margin", self.map_top_margin)
        self.settings.setValue("map_highlight_field", self.map_highlight_field)
        self.settings.setValue("map_highlight_type", self.map_highlight_type)
        self.settings.setValue("map_highlight_color", self.map_highlight_color)
        self.settings.setValue("map_highlight_size", self.map_highlight_size)
        self.settings.setValue("map_show_dates", self.map_show_dates)
        self.settings.setValue("dup_bg_mode", getattr(self, 'dup_bg_mode', "Muted Blue Intensity"))
        self.settings.setValue("fast_mode", self.fast_mode)
        event.accept()

    def apply_theme(self, theme_name=None):
        is_dark = True
        if self.main_app and hasattr(self.main_app, 'theme_combo'):
            self.setStyleSheet(THEMES.get(self.main_app.theme_combo.currentText(), THEMES["Dark"]))
            is_dark = self.main_app.theme_combo.currentText() == "Dark"
            
        bg_col = "#161b22" if is_dark else "#f6f8fa"
        brd_col = "#30363d" if is_dark else "#d0d7de"
        lbl_col = "#c9d1d9" if is_dark else "#24292f"
        
        input_bg = "#0d1117" if is_dark else "#ffffff"
        input_text = "#c9d1d9" if is_dark else "#24292f"
        
        card_css = f"""
            QFrame#FilterCard {{ background: {bg_col}; border: 1px solid {brd_col}; border-radius: 8px; }}
            QFrame#FilterCard QLabel {{ background: transparent; border: none; color: {lbl_col}; }}
            
            QCheckBox::indicator {{ border: 1px solid #58a6ff; width: 14px; height: 14px; border-radius: 3px; background: transparent; }}
            QCheckBox::indicator:checked {{ background: #58a6ff; image: url(none); }} 
            QCheckBox {{ color: {lbl_col}; background: transparent; outline: none; }}
            
            QComboBox, QSpinBox, QDoubleSpinBox, QDateEdit, QTimeEdit, QLineEdit {{ 
                background: {input_bg}; color: {input_text}; border: 1px solid {brd_col}; padding: 2px 4px; border-radius: 4px; min-height: 20px; font-size: 12px;
            }}
            QComboBox::drop-down {{ width: 15px; border: none; }}
        """
        self.card_scope.setStyleSheet(card_css)
        self.card_type.setStyleSheet(card_css)
        self.card_metrics.setStyleSheet(card_css)
        
        self.tabs.setStyleSheet(f"""
            QTabBar::tab {{ background: {bg_col}; color: {lbl_col}; padding: 10px 15px; border: 1px solid {brd_col}; border-top-left-radius: 4px; border-top-right-radius: 4px; }}
            QTabBar::tab:selected {{ background: {input_bg}; color: #58a6ff; font-weight: bold; border-bottom: 2px solid #58a6ff; }}
            QTabWidget::pane {{ border: 1px solid {brd_col}; top: -1px; border-radius: 8px; border-top-left-radius: 0px; }}
        """)

        self.progress.setStyleSheet(f"""
            QProgressBar {{ border: 1px solid {brd_col}; border-radius: 4px; background: {input_bg}; color: {input_text}; font-weight: bold; text-align: center; }}
            QProgressBar::chunk {{ background: #1f6feb; border-radius: 3px; }}
        """)

        self.txt_tele_log.setStyleSheet(f"""
            QPlainTextEdit {{ background-color: {input_bg}; color: {input_text}; font-family: Consolas, monospace; border: 1px solid {brd_col}; border-radius: 6px; padding: 10px; }}
            QScrollBar:vertical {{ border: none; background: {input_bg}; width: 8px; margin: 0px; }}
            QScrollBar::handle:vertical {{ background: {brd_col}; border-radius: 4px; min-height: 20px; }}
            QScrollBar::handle:vertical:hover {{ background: #58a6ff; }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ border: none; background: none; height: 0px; }}
        """)
        
        css_inputs = f"background: {input_bg}; color: {input_text}; border: 1px solid {brd_col}; padding: 6px; border-radius: 4px; font-size: 13px;"
        self.local_filter_input.setStyleSheet(css_inputs)
        self.local_tag_filter_input.setStyleSheet(css_inputs)
        self.map_highlight_input.setStyleSheet(css_inputs)
        self.map_tag_highlight_input.setStyleSheet(css_inputs)

        glass_btn = f"""
            QPushButton {{ 
                background-color: {'rgba(255, 255, 255, 0.05)' if is_dark else 'rgba(0, 0, 0, 0.03)'}; 
                border: 1px solid {'rgba(255, 255, 255, 0.15)' if is_dark else 'rgba(0, 0, 0, 0.1)'}; 
                border-radius: 6px; 
                font-size: 16px; font-weight: bold; 
                color: {lbl_col}; 
            }}
            QPushButton:hover {{ background-color: {'rgba(255, 255, 255, 0.1)' if is_dark else 'rgba(0, 0, 0, 0.08)'}; }}
            QPushButton:checked {{ background-color: {'rgba(88, 166, 255, 0.15)' if is_dark else 'rgba(9, 105, 218, 0.1)'}; border-color: {'#58a6ff' if is_dark else '#0969da'}; }}
        """
        for btn in [self.btn_scope, self.btn_type, self.btn_metrics, self.btn_reset]:
            btn.setStyleSheet(glass_btn)
            
        self.set_button_style("idle" if not self.is_searching else "running")

    def set_button_style(self, state):
        is_dark = True
        if self.main_app and hasattr(self.main_app, 'theme_combo'):
            is_dark = self.main_app.theme_combo.currentText() == "Dark"

        btn_bg = "#1c1c1e" if is_dark else "#e5e7eb"
        btn_fg = "#ffffff" if is_dark else "#1f2937"
        btn_border = "#333333" if is_dark else "#d1d5db"
        btn_hover = "#2c2c2e" if is_dark else "#d1d5db"
        btn_hover_border = "#58a6ff" if is_dark else "#2563eb"

        if state == "idle":
            self.btn_search.setText("⚡ SEARCH")
            self.btn_search.setStyleSheet(f"""
                QPushButton {{ background-color: {btn_bg}; color: {btn_fg}; border: 2px solid {btn_border}; border-radius: 6px; font-weight: 900; font-size: 13px; }} 
                QPushButton:hover {{ background-color: {btn_hover}; border-color: {btn_hover_border}; }}
            """)
        elif state == "running":
            self.btn_search.setText("🛑 ABORT")
            self.btn_search.setStyleSheet("""
                QPushButton { background-color: #4a0000; color: #ff7b72; border: 2px solid #f85149; border-radius: 6px; font-weight: 900; font-size: 13px; } 
                QPushButton:hover { background-color: #6a0000; }
            """)
            
    def update_filter_visibility(self):
        self.card_scope.setVisible(self.btn_scope.isChecked())
        self.card_type.setVisible(self.btn_type.isChecked())
        self.card_metrics.setVisible(self.btn_metrics.isChecked())
        self.filters_container.setVisible(self.btn_scope.isChecked() or self.btn_type.isChecked() or self.btn_metrics.isChecked())

    def handle_button_action(self):
        if self.is_searching or getattr(self, 'is_rendering', False):
            self._abort_render = True
            if hasattr(self, 'worker'): self.worker.abort()
            self.lbl_status.setText("Aborting...")
            self.btn_search.setEnabled(False)
        else: 
            self.trigger_search()

    def get_icon(self, is_folder, name, ext, db_id):
        if not self.show_icons: return QIcon()
        if is_folder: return self.style().standardIcon(QStyle.SP_DirIcon)
        
        if db_id != -1:
            from main import get_thumb_dir_for_db
            target_dir = get_thumb_dir_for_db(self.active_db)
            thumb_path = target_dir / f"{db_id}.png"
            cache_key = f"thumb_{Path(self.active_db).stem}_{db_id}"
            
            if cache_key in self.icon_cache: return self.icon_cache[cache_key]
            if thumb_path.exists():
                self.icon_cache[cache_key] = QIcon(str(thumb_path))
                return self.icon_cache[cache_key]

        ext_clean = str(ext).lower().strip('.')
        if not ext_clean: return self.style().standardIcon(QStyle.SP_FileIcon)

        if ext_clean not in self.icon_cache:
            e_path = self.ICONS_DIR / f"{ext_clean}.png"
            if e_path.exists():
                self.icon_cache[ext_clean] = QIcon(str(e_path))
            else:
                self.icon_cache[ext_clean] = self.icon_provider.icon(QFileInfo(name))
                
        return self.icon_cache[ext_clean]

    def human_size(self, size_in_bytes):
        try: size = float(size_in_bytes)
        except: return "0 B"
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0: return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"

    def set_table_view_mode(self, mode):
        self.table_view_mode = mode
        self.apply_local_filter()

    def populate_table(self, display_results, total_matches):
        self.table.setSortingEnabled(False) 
        self.table.setUpdatesEnabled(False)
        self.table.setRowCount(0)
        
        if not display_results:
            
            # Sync Grid View
            self.search_grid.clear()
            for item_row in display_results:
                db_id, name, p_path, ext, size, mod, is_fldr, real_path, tags, color_tag, cat_val, sha256_val = item_row[:12]
                db_label = item_row[12] if len(item_row) > 12 else Path(self.active_db).stem
                
                icon = self.get_icon(is_fldr, name, ext, db_id)
                g_item = QListWidgetItem(name)
                g_item.setIcon(icon)
                g_item.setData(Qt.UserRole, {'id': db_id, 'is_fldr': is_fldr, 'real_path': real_path, 'tags': tags, 'size': size, 'mod': mod, 'name': name, 'ext': ext, 'cat': cat_val, 'db': db_label})
                self.search_grid.addItem(g_item)
            
            self.table.setUpdatesEnabled(True)
            self.table.setSortingEnabled(True)
            self.lbl_status.setText(f"Search complete. 0 rows loaded into Table. ({total_matches} available in Maps).")
            self.is_rendering = False
            self.is_searching = False
            self.btn_search.setEnabled(True)
            self.set_button_style("idle")
            return
            
        display_total = len(display_results)
        
        self.progress.setVisible(True)
        self.progress.setMaximum(display_total)
        self.progress.setValue(0)
        QApplication.processEvents()
        
        i = 0
        for i, item_row in enumerate(display_results):
            if self._abort_render: break 
            db_id, name, p_path, ext, size, mod, is_fldr, real_path, tags, color_tag, cat_val, sha256_val = item_row[:12]
            db_origin = item_row[12] if len(item_row) > 12 else Path(self.active_db).stem
            
            self.table.insertRow(i)
            
            sno_item = NumericTableItem(str(i + 1)); sno_item.setData(Qt.UserRole, i + 1)
            self.table.setItem(i, 0, sno_item)
            
            name_item = QTableWidgetItem(name)
            name_item.setIcon(self.get_icon(is_fldr, name, ext, db_id))
            if color_tag and color_tag in self.VMAN_COLORS: 
                name_item.setBackground(QBrush(self.VMAN_COLORS[color_tag])) 
                name_item.setForeground(QColor("#ffffff")) 
            self.table.setItem(i, 1, name_item)
            
            # Col 2: Database origin
            db_item = QTableWidgetItem(db_origin)
            db_item.setForeground(QBrush(QColor("#58a6ff")))
            self.table.setItem(i, 2, db_item)

            self.table.setItem(i, 3, QTableWidgetItem(p_path))
            
            type_str = "Folder" if is_fldr else (ext[1:].upper() + " File" if ext.startswith(".") else (ext.upper() + " File" if ext else "File"))
            self.table.setItem(i, 4, QTableWidgetItem(type_str))
            
            sz_str = "--" if is_fldr else self.human_size(size)
            sz_item = NumericTableItem(sz_str); sz_item.setData(Qt.UserRole, size if not is_fldr else -1)
            self.table.setItem(i, 5, sz_item)
            
            self.table.setItem(i, 6, QTableWidgetItem(str(mod)))
            
            eff_tag = tags if tags else self.inherited_tags.get(db_id, "")
            self.table.setItem(i, 7, QTableWidgetItem(str(eff_tag) if eff_tag else ""))
            
            sha_item = QTableWidgetItem(str(sha256_val) if sha256_val else "--")
            sha_item.setForeground(QColor("#8b949e"))
            self.table.setItem(i, 8, sha_item)
            
            meta_item = QTableWidgetItem("")
            meta_item.setData(Qt.UserRole, {'id': db_id, 'is_fldr': is_fldr, 'real_path': real_path, 'tags': eff_tag, 'size': size, 'mod': mod, 'name': name, 'ext': ext, 'cat': cat_val, 'db': db_origin})
            self.table.setItem(i, 9, meta_item)
            
            if i % 100 == 0:
                self.progress.setValue(i)
                QApplication.processEvents()
                
        self.progress.setValue(display_total)
        self.progress.setVisible(False)
        self.table.setUpdatesEnabled(True)
        self.table.setSortingEnabled(True)
        
        self.apply_local_filter() 
        
        self.is_rendering = False
        self.is_searching = False
        self.btn_search.setEnabled(True)
        self.set_button_style("idle")
        
        if self._abort_render:
            self.lbl_status.setText(f"Rendering aborted. Displaying {i} rows.")
        else:
            self.lbl_status.setText(f"Search complete. Displaying {len(display_results)} out of {total_matches} total matches.")

    def search_complete(self, was_aborted):
        pass

    def show_header_menu(self, pos):
        menu = QMenu(self)
        for col in range(self.table.columnCount()):
            col_name = self.table.horizontalHeaderItem(col).text()
            action = menu.addAction(f"Show {col_name}")
            action.setCheckable(True)
            action.setChecked(not self.table.isColumnHidden(col))
            action.toggled.connect(lambda checked, c=col: self.table.setColumnHidden(c, not checked))
            
        menu.addSeparator()
        act_icons = menu.addAction("Show File Icons")
        act_icons.setCheckable(True)
        act_icons.setChecked(self.show_icons)
        act_icons.toggled.connect(self.toggle_icons)
        
        menu.exec(self.table.horizontalHeader().mapToGlobal(pos))
        
    def toggle_icons(self, checked):
        self.show_icons = checked
        self.trigger_search() 

    def handle_double_click(self, item):
        if not item: return
        meta_item = self.table.item(item.row(), 9) # <-- Changed from 8 to 9
        if not meta_item: return 
        
        meta = meta_item.data(Qt.UserRole)
        if not meta: return
        
        if meta.get('real_path') and os.path.exists(meta['real_path']): 
            self.open_in_os(meta['real_path'])
        else: 
            QMessageBox.warning(self, "Virtual Only", "This item has no physical OS file mapped to it.")

    def show_context_menu(self, pos):
        item = self.table.itemAt(pos)
        if not item: return
        
        row = item.row()
        i_name = self.table.item(row, 1); i_vpath = self.table.item(row, 3); i_meta = self.table.item(row, 9) 
        if not i_name or not i_vpath or not i_meta: return
        
        name = i_name.text(); v_path = i_vpath.text()
        meta = i_meta.data(Qt.UserRole)
        if not meta: return

        selected_rows = self.table.selectionModel().selectedRows()
        
        menu = QMenu(self)
        
        # --- RESTORED FILTER VIEW ---
        view_menu = menu.addMenu("👁️ Filter View")
        for mode in ["Files & Folders", "Files Only", "Folders Only"]:
            act = view_menu.addAction(mode)
            act.setCheckable(True)
            act.setChecked(self.table_view_mode == mode)
            act.triggered.connect(lambda checked=False, m=mode: self.set_table_view_mode(m))
        menu.addSeparator()
        # ----------------------------
        # --- THUMBNAILS SUBMENU IN SEARCH RESULTS ---
        thumb_menu = menu.addMenu("🖼️ Thumbnails")
        sz_menu = thumb_menu.addMenu("📐 Thumbnail Size")
        current_sz = self.settings.value("search_grid_size", "Medium")
        for sz in ["Small", "Medium", "Large", "Extra Large"]:
            act_sz = sz_menu.addAction(sz)
            act_sz.setCheckable(True)
            act_sz.setChecked(current_sz == sz)
            act_sz.triggered.connect(lambda chk=False, s=sz: self.set_search_grid_size(s))
            
        thumb_menu.addSeparator()
        thumb_menu.addAction("⚡ Generate Thumbnails for Search Results", self.generate_thumbnails_search_results)
        # --------------------------------------------
               
        act_open_os = menu.addAction("🚀 Open Native OS Default")
        act_show_os = menu.addAction("📂 Show in OS Explorer")
        
        if len(selected_rows) > 1:
            act_vman = menu.addAction(f"🎞 Open {len(selected_rows)} Highlighted in VMan Viewer")
        elif not meta['is_fldr']:
            act_vman = menu.addAction("🎞 Open in VMan Viewer")
        else: act_vman = None
            
        menu.addSeparator()
        
        # --- BULK COLORS AND TAGS ---
        color_menu = menu.addMenu("🎨 Set Color Tag (Highlighted)")
        for color in ["None", "Red", "Orange", "Gold", "Green", "Cyan", "Blue", "Purple", "Pink"]:
            color_menu.addAction(color).triggered.connect(lambda checked=False, c=color: self.bulk_set_color(c, selected_rows))
            
        menu.addAction("🏷️ Assign Custom Text Tags (Highlighted)").triggered.connect(lambda: self.bulk_set_tags(selected_rows))
        
        menu.addSeparator()
        
        # --- SEND TO DATABASE ---
        send_menu = menu.addMenu("📤 Send Highlighted To Database...")
        db_list = list(Path("vman_data/compiled_views").glob("*.db"))
        for db_file in db_list:
            if str(db_file.resolve()) != str(Path(self.active_db).resolve()):
                send_menu.addAction(db_file.stem).triggered.connect(lambda checked=False, tgt=db_file: self.send_to_database(tgt, selected_rows))
        
        menu.addSeparator()
        act_copy_names = menu.addAction("📋 Copy Selected Names")
        act_csv = menu.addAction("📥 Export Selected to CSV")
        menu.addSeparator()
        
        act_nav = menu.addAction("🎯 Locate in Virtual File Manager")
        act_copy = menu.addAction("📋 Copy Virtual Path")
        if len(selected_rows) > 1:
            act_props = menu.addAction("ℹ️ Multi-Item Properties")
        else:
            act_props = menu.addAction("ℹ️ Properties")
        
        menu.addSeparator()
        menu.addAction("🕰️ Fix Timestamps (Forensic Extractor)").triggered.connect(self.run_timestamp_corrector)
        
        action = menu.exec(self.table.viewport().mapToGlobal(pos))
        
        if action == act_open_os: self.open_in_os(meta['real_path'])
        elif action == act_show_os: self.show_in_os_explorer(meta['real_path'])
        elif action == act_vman:
            playlist = []
            for idx in selected_rows:
                m = self.table.item(idx.row(), 9).data(Qt.UserRole)
                if not m or not m.get('is_fldr', True) and m.get('real_path') and os.path.exists(m['real_path']):
                    playlist.append({'path': m['real_path'], 'name': self.table.item(idx.row(), 1).text(), 'ext': os.path.splitext(m['real_path'])[1].lower()})
            if playlist:
                try:
                    from main import vmanViewer
                    parent = self.main_app if self.main_app else self
                    if not hasattr(parent, 'active_viewers'): parent.active_viewers = []
                    viewer = vmanViewer(playlist, 0, parent)
                    parent.active_viewers.append(viewer)
                    viewer.show()
                except Exception as e: print(e)
            else: QMessageBox.warning(self, "Viewer", "No valid physical files selected.")
        elif action == act_copy_names:
            names = [self.table.item(idx.row(), 1).text() for idx in selected_rows if self.table.item(idx.row(), 1)]
            QApplication.clipboard().setText("\n".join(names))
        elif action == act_csv: self.export_table_to_csv(selected_rows)
        elif action == act_nav: self.navigate_to_item(row)
        elif action == act_copy: QApplication.clipboard().setText(f"{v_path}{name}/" if meta['is_fldr'] else f"{v_path}{name}")
        elif action == act_props: 
            if len(selected_rows) > 1:
                self.show_multi_properties(selected_rows)
            else:
                self.show_properties(name, v_path, meta)

    def bulk_set_color(self, color, selected_rows):
        for idx in selected_rows:
            meta = self.table.item(idx.row(), 9).data(Qt.UserRole)
            db = meta.get('db')
            if not db: continue
            db_path = str(Path("vman_data/compiled_views") / f"{db}.db") if db != "vman_vfs" else "vman_data/vman_vfs.db"
            try:
                with sqlite3.connect(db_path) as conn:
                    conn.cursor().execute("UPDATE virtual_fs SET color_tag=? WHERE id=?", ("" if color=="None" else color, meta['id']))
                    conn.commit()
                # Update UI instantly
                item = self.table.item(idx.row(), 1)
                if color in self.VMAN_COLORS:
                    item.setBackground(QBrush(self.VMAN_COLORS[color]))
                    item.setForeground(QColor("#ffffff"))
                else:
                    item.setData(Qt.BackgroundRole, None)
                    item.setData(Qt.ForegroundRole, None)
            except Exception as e: print(f"DB Error: {e}")
        if self.main_app: self.main_app.refresh_all()

    def bulk_set_tags(self, selected_rows):
        tags, ok = QInputDialog.getText(self, "Apply Tags", "Enter custom tags (comma separated):")
        if not ok or not tags.strip(): return
        
        for idx in selected_rows:
            meta = self.table.item(idx.row(), 9).data(Qt.UserRole)
            db = meta.get('db')
            if not db: continue
            db_path = str(Path("vman_data/compiled_views") / f"{db}.db") if db != "vman_vfs" else "vman_data/vman_vfs.db"
            try:
                with sqlite3.connect(db_path) as conn:
                    old_tags = conn.cursor().execute("SELECT custom_tags FROM virtual_fs WHERE id=?", (meta['id'],)).fetchone()[0]
                    new_val = f"{old_tags}, {tags.strip()}".strip(", ") if old_tags else tags.strip()
                    conn.cursor().execute("UPDATE virtual_fs SET custom_tags=? WHERE id=?", (new_val, meta['id']))
                    conn.commit()
                # Update UI
                meta['tags'] = new_val
                self.table.item(idx.row(), 9).setData(Qt.UserRole, meta)
                self.table.item(idx.row(), 7).setText(new_val)
            except Exception as e: print(e)
        if self.main_app: self.main_app.refresh_all()

    def send_to_database(self, target_db_path, selected_rows):
        if not self.main_app or not hasattr(self.main_app, 'send_to_database'):
            return QMessageBox.warning(self, "Error", "Main application engine not linked.")
        
        items_to_send = []
        for idx in selected_rows:
            meta = self.table.item(idx.row(), 9).data(Qt.UserRole)
            typ = "folder" if meta['is_fldr'] else "file"
            v_path = self.table.item(idx.row(), 3).text()
            items_to_send.append((typ, v_path, meta['id']))
            
        self.main_app.send_to_database(target_db_path, items_to_send)

    def run_timestamp_corrector(self):
        selected_rows = self.table.selectionModel().selectedRows()
        if not selected_rows: return
        items_data = []
        
        with sqlite3.connect(self.active_db) as conn:
            cur = conn.cursor()
            for idx in selected_rows:
                meta = self.table.item(idx.row(), 9).data(Qt.UserRole) # <-- Changed from 8 to 9
                if not meta: continue
                
                # Fetch Virtual Path from Table
                v_path = self.table.item(idx.row(), 3).text() # <-- Changed from 2 to 3
                
                if meta.get('is_fldr'):
                    fldr_name = self.table.item(idx.row(), 1).text()
                    full_v_path = f"{v_path}{fldr_name}/"
                    
                    cur.execute("SELECT id, name, modified, real_path, custom_tags, creation_date, parent_path FROM virtual_fs WHERE is_folder=0 AND parent_path LIKE ?", (f"{full_v_path}%",))
                    for r in cur.fetchall():
                        items_data.append({'id': r[0], 'name': r[1], 'mod': r[2], 'real_path': r[3], 'tags': r[4], 'creation_date': r[5] or r[2], 'p_path': r[6]})
                else:
                    meta['p_path'] = v_path
                    items_data.append(meta)
                    
        unique_items = {item['id']: item for item in items_data}.values()
        
        if not unique_items:
            return QMessageBox.warning(self, "No Files Found", "No valid files found inside the selected items/folders.")
            
        dlg = TimestampCorrectorDialog(list(unique_items), self.active_db, self)
        if dlg.exec() == QDialog.Accepted:
            self.trigger_search()

    def export_table_to_csv(self, selected_rows):
        path, _ = QFileDialog.getSaveFileName(self, "Export to CSV", "VMan_Search_Results.csv", "CSV Files (*.csv)", options=QFileDialog.DontUseNativeDialog)
        if not path: return
        try:
            with open(path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                headers = [self.table.horizontalHeaderItem(c).text() for c in range(1, 9)] # <-- Change to 9
                writer.writerow(headers)
                for idx in selected_rows:
                    r = idx.row()
                    writer.writerow([self.table.item(r, c).text() for c in range(1, 9)]) # <-- Change to 9
            QMessageBox.information(self, "Success", "Exported successfully.")
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def set_color_tag(self, row, db_id, color_name, is_fldr, name):
        try:
            item = self.table.item(row, 1) 
            if color_name in self.VMAN_COLORS:
                item.setBackground(QBrush(self.VMAN_COLORS[color_name]))
                item.setForeground(QColor("#ffffff"))
            else:
                item.setData(Qt.BackgroundRole, None)
                item.setData(Qt.ForegroundRole, None)
                
            if self.main_app and hasattr(self.main_app, 'bulk_tag_items'):
                typ = "folder" if is_fldr else "file"
                sel_items = [(typ, name, db_id)]
                self.main_app.bulk_tag_items(color_name, sel_items)
            else:
                with sqlite3.connect(self.active_db) as conn:
                    conn.cursor().execute("UPDATE virtual_fs SET color_tag=? WHERE id=?", ("" if color_name=="None" else color_name, db_id))
                    conn.commit()
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def navigate_to_item(self, row):
        name = self.table.item(row, 1).text()
        v_path = self.table.item(row, 2).text()
        if self.main_app:
            self.main_app.nav_to_path(v_path)
            if hasattr(self.main_app, 'local_filter'): self.main_app.local_filter.setText(name)
            self.main_app.raise_(); self.main_app.activateWindow()

    def open_in_os(self, real_path):
        if not real_path or not os.path.exists(real_path): return QMessageBox.warning(self, "Cannot Open", "File missing or deleted.")
        try:
            if sys.platform == "win32": os.startfile(real_path)
            elif sys.platform == "darwin": subprocess.Popen(["open", real_path])
            else: subprocess.Popen(["xdg-open", real_path])
        except Exception as e: QMessageBox.critical(self, "Error", str(e))

    def show_in_os_explorer(self, real_path):
        if not real_path or not os.path.exists(real_path): return QMessageBox.warning(self, "Missing", "File missing.")
        try:
            if sys.platform == "win32": subprocess.run(['explorer', '/select,', os.path.normpath(real_path)])
            elif sys.platform == "darwin": subprocess.run(['open', '-R', real_path])
            else: subprocess.run(['xdg-open', os.path.dirname(real_path)])
        except Exception as e: QMessageBox.critical(self, "Error", str(e))

    def edit_tags(self, row, name, meta):
        new_tags, ok = QInputDialog.getText(self, "Edit Tags", f"Tags for {name}:", QLineEdit.Normal, meta['tags'] or "")
        if ok:
            try:
                with sqlite3.connect(self.active_db) as conn:
                    conn.cursor().execute("UPDATE virtual_fs SET custom_tags=? WHERE id=?", (new_tags.strip(), meta['id']))
                    conn.commit()
                meta['tags'] = new_tags.strip()
                self.table.item(row, 9).setData(Qt.UserRole, meta) # <-- Changed from 8 to 9
                self.table.item(row, 7).setText(meta['tags'])      # <-- Changed from 6 to 7
                if self.main_app: self.main_app.refresh_all()
            except Exception as e: QMessageBox.critical(self, "Error", str(e))

    def show_properties(self, name, v_path, meta):
        dlg = QDialog(self); dlg.setWindowTitle(f"Properties: {name}"); dlg.setMinimumWidth(500)
        if self.main_app and hasattr(self.main_app, 'theme_combo'): dlg.setStyleSheet(THEMES.get(self.main_app.theme_combo.currentText(), THEMES["Dark"]))
        layout = QFormLayout(dlg)
        layout.addRow("Name:", QLabel(name)); layout.addRow("Virtual Path:", QLineEdit(v_path))
        layout.addRow("Type:", QLabel("Directory (Virtual Folder)" if meta['is_fldr'] else "Virtual File"))
        if not meta['is_fldr']: layout.addRow("Size:", QLabel(self.human_size(meta['size'])))
        layout.addRow("Modified:", QLabel(str(meta['mod'])))
        txt_real = QLineEdit(str(meta['real_path']) if meta['real_path'] else "Virtual Only"); txt_real.setReadOnly(True)
        layout.addRow("Local OS Path:", txt_real); layout.addRow("Tags:", QLabel(str(meta['tags']) if meta['tags'] else "None"))
        btn = QPushButton("Close"); btn.clicked.connect(dlg.accept); layout.addRow("", btn)
        dlg.exec()

    # --- Async Render Wrappers ---
    def safe_render_map(self):
        prog = QProgressDialog("Rendering Visual Grid Map...\nThis may take a moment for massive maps.", None, 0, 0, self)
        prog.setWindowTitle("Processing"); prog.setWindowModality(Qt.WindowModal); prog.show()
        QApplication.processEvents(); QThread.msleep(50); QApplication.processEvents()
        try: self.render_grid_map()
        finally: prog.close()

    def safe_render_analytics(self):
        prog = QProgressDialog("Rendering Search Analytics...\nAggregating large datasets.", None, 0, 0, self)
        prog.setWindowTitle("Processing"); prog.setWindowModality(Qt.WindowModal); prog.show()
        QApplication.processEvents(); QThread.msleep(50); QApplication.processEvents()
        try: self.render_analytics()
        finally: prog.close()

    def safe_render_time_heatmap(self):
        prog = QProgressDialog("Rendering 24-Hour Time Heatmap...\nAggregating timeframes.", None, 0, 0, self)
        prog.setWindowTitle("Processing"); prog.setWindowModality(Qt.WindowModal); prog.show()
        QApplication.processEvents(); QThread.msleep(50); QApplication.processEvents()
        try: self.render_time_heatmap()
        finally: prog.close()

    # --- MAP CLICK INTERACTION ---
    def on_map_click(self, event):
        if not hasattr(self, 'figure_map') or not self.figure_map or event.inaxes != self.figure_map.axes[0] or event.xdata is None or event.ydata is None: return
        if not hasattr(self, 'map_coords_dict') or not self.map_coords_dict: return
        
        scale = 1.0
        if self.map_tile_size == "Small (0.7x)": scale = 0.7
        elif self.map_tile_size == "Medium (1.0x)": scale = 1.0
        elif self.map_tile_size == "Large (1.2x)": scale = 1.2
        elif self.map_tile_size == "Custom...": scale = getattr(self, 'map_custom_scale', 1.0)
        box_w, box_h, gap = 1.0 * scale, 1.0 * scale, 0.2 * scale
        
        col = int(math.floor(event.xdata / (box_w + gap)))
        row = int(math.floor(event.ydata / (box_h + gap)))
        
        if (col, row) in self.map_coords_dict:
            target_ym, target_day = self.map_coords_dict[(col, row)]
        else: return
            
        target_date_prefix = f"{target_ym}-{target_day:02d}"
        filtered_results = []
        
        if "Tag" in self.map_color_mode:
            is_flat = "Flat" in self.map_color_mode
            is_size = "Size" in self.map_color_mode
            for r in self.current_results:
                if not str(r[5]).startswith(target_date_prefix): continue
                eff_tag = r[8] if (is_flat or not is_size) else (r[8] if r[8] else self.inherited_tags.get(r[0]))
                if eff_tag: filtered_results.append(r)
        elif self.map_color_mode == "By Duplicates":
            for r in self.current_results:
                if not str(r[5]).startswith(target_date_prefix): continue
                if self.duplicate_map.get(r[0], "Unique") in ["Original", "Duplicate"]:
                    filtered_results.append(r)
        else:
            filtered_results = [r for r in self.current_results if str(r[5]).startswith(target_date_prefix)]
        
        if filtered_results:
            self._abort_render = False 
            self.populate_table(filtered_results, len(self.current_results))
            self.tabs.setCurrentIndex(0) 
            self.lbl_status.setText(f"Viewing {len(filtered_results)} files modified exactly on {target_date_prefix}.")
        else:
            self.lbl_status.setText(f"No files found exactly on {target_date_prefix}.")

    def on_time_click(self, event):
        if not hasattr(self, 'figure_time') or not self.figure_time or event.inaxes != self.figure_time.axes[0] or event.xdata is None or event.ydata is None: return
        if not hasattr(self, 'time_coords_dict') or not self.time_coords_dict: return
        
        scale = 1.0
        if self.map_tile_size == "Small (0.7x)": scale = 0.7
        elif self.map_tile_size == "Medium (1.0x)": scale = 1.0
        elif self.map_tile_size == "Large (1.2x)": scale = 1.2
        elif self.map_tile_size == "Custom...": scale = getattr(self, 'map_custom_scale', 1.0)
        box_w, box_h, gap = 1.0 * scale, 1.0 * scale, 0.2 * scale
        
        col = int(math.floor(event.xdata / (box_w + gap)))
        row = int(math.floor(event.ydata / (box_h + gap)))
        
        if (col, row) in self.time_coords_dict:
            target_ym, target_b = self.time_coords_dict[(col, row)]
        else: return
            
        hh = int(target_b)
        mm_start = 30 if target_b - hh >= 0.5 else 0
        mm_end = 59 if mm_start == 30 else 29
        
        filtered_results = []
        
        if "Tag" in self.time_color_mode:
            is_flat = "Flat" in self.time_color_mode
            is_size = "Size" in self.time_color_mode
            for r in self.current_results:
                dt_str = str(r[5])
                if dt_str.startswith(target_ym) and len(dt_str) >= 16:
                    try:
                        r_hh = int(dt_str[11:13]); r_mm = int(dt_str[14:16])
                        if r_hh == hh and mm_start <= r_mm <= mm_end:
                            eff_tag = r[8] if (is_flat or not is_size) else (r[8] if r[8] else self.inherited_tags.get(r[0]))
                            if eff_tag: filtered_results.append(r)
                    except: pass
        elif self.time_color_mode == "By Duplicates":
            for r in self.current_results:
                dt_str = str(r[5])
                if dt_str.startswith(target_ym) and len(dt_str) >= 16:
                    try:
                        r_hh = int(dt_str[11:13]); r_mm = int(dt_str[14:16])
                        if r_hh == hh and mm_start <= r_mm <= mm_end:
                            if self.duplicate_map.get(r[0], "Unique") in ["Original", "Duplicate"]:
                                filtered_results.append(r)
                    except: pass
        else:
            for r in self.current_results:
                dt_str = str(r[5])
                if dt_str.startswith(target_ym) and len(dt_str) >= 16:
                    try:
                        r_hh = int(dt_str[11:13]); r_mm = int(dt_str[14:16])
                        if r_hh == hh and mm_start <= r_mm <= mm_end:
                            filtered_results.append(r)
                    except: pass
        
        if filtered_results:
            self._abort_render = False 
            self.populate_table(filtered_results, len(self.current_results))
            self.tabs.setCurrentIndex(0) 
            self.lbl_status.setText(f"Viewing {len(filtered_results)} files modified in {target_ym} between {hh:02d}:{mm_start:02d} - {hh:02d}:{mm_end:02d}.")
        else:
            self.lbl_status.setText(f"No files found in {target_ym} between {hh:02d}:{mm_start:02d} - {hh:02d}:{mm_end:02d}.")

    # --- PERFECT HIGH-SPEED ZERO-MARGIN RENDERING ---
    def get_map_value(self, items, mode):
        is_size = "Size" in mode
        is_flat = "Flat" in mode
        total_val = sum((i['size'] for i in items)) if is_size else len(items)
        if total_val == 0: return 0, None
        
        if "Category" in mode:
            counts = defaultdict(float)
            for i in items: counts[i['cat']] += (i['size'] if is_size else 1)
            return total_val, max(counts, key=counts.get)
        elif "Extension" in mode:
            counts = defaultdict(float)
            for i in items: counts[i['ext']] += (i['size'] if is_size else 1)
            return total_val, max(counts, key=counts.get)
        elif "Tag" in mode:
            counts = defaultdict(float)
            has_tag = False
            for i in items:
                t_str = i['tag_exact'] if (is_flat or not is_size) else i['tag_inherited']
                if t_str:
                    for t in t_str.split(','):
                        if t.strip(): 
                            counts[t.strip()] += (i['size'] if is_size else 1)
                            has_tag = True
            if not has_tag: return total_val, "Untagged"
            return total_val, max(counts, key=counts.get)
        elif mode == "By Duplicates":
            orig_count = sum(1 for i in items if i.get('dup_status') == "Original")
            dup_count = sum(1 for i in items if i.get('dup_status') == "Duplicate")
            if orig_count + dup_count == 0: return total_val, None
            ratio = dup_count / float(orig_count + dup_count)
            return total_val, ratio
            
        return total_val, None
        
    def get_cell_color(self, val, max_val, dom, is_dark, saved_colors, color_mode):
        if val == 0: return ("#21262d" if is_dark else "#ebecf0", "black" if not is_dark else "white")
        
        def get_background_hex():
            dup_bg = getattr(self, 'dup_bg_mode', 'Muted Blue Intensity')
            if dup_bg == "Hidden (Empty)": return "#21262d" if is_dark else "#ebecf0"
            elif dup_bg == "Solid Dark Blue": return "#1e293b"
            elif dup_bg == "Solid Dark Gray": return "#30363d"
            elif dup_bg == "Gray Intensity":
                intensity = max(0.2, min(1.0, math.pow(val / max_val, 0.5))) if max_val > 0 else 0
                r = g = b = int(33 + 100 * intensity)
                return f"#{min(255,max(0,r)):02x}{min(255,max(0,g)):02x}{min(255,max(0,b)):02x}"
            elif dup_bg == "Custom Intensity...":
                intensity = max(0.2, min(1.0, math.pow(val / max_val, 0.5))) if max_val > 0 else 0
                low = QColor(saved_colors.get("Gradient_Low", "#21262d" if is_dark else "#ebecf0"))
                high = QColor(saved_colors.get("Gradient_High", "#f85149"))
                r = int(low.red() + (high.red() - low.red()) * intensity)
                g = int(low.green() + (high.green() - low.green()) * intensity)
                b = int(low.blue() + (high.blue() - low.blue()) * intensity)
                return f"#{min(255,max(0,r)):02x}{min(255,max(0,g)):02x}{min(255,max(0,b)):02x}"
            else: 
                intensity = max(0.2, min(1.0, math.pow(val / max_val, 0.5))) if max_val > 0 else 0
                r, g, b = int(33 + 50*intensity), int(38 + 50*intensity), int(45 + 100*intensity)
                return f"#{min(255,max(0,r)):02x}{min(255,max(0,g)):02x}{min(255,max(0,b)):02x}"
        
        if color_mode == "By Duplicates":
            if dom is None:
                bg_hex = get_background_hex()
                bg_c = QColor(bg_hex)
                lum = 0.299 * bg_c.red() + 0.587 * bg_c.green() + 0.114 * bg_c.blue()
                return (bg_hex, "black" if lum > 140 else "white")
                
            ratio = float(dom)
            if ratio <= 0.5:
                pct = ratio * 2.0
                r = int(46 + (227 - 46) * pct); g = int(160 + (179 - 160) * pct); b = int(67 + (65 - 67) * pct)
            else:
                pct = (ratio - 0.5) * 2.0
                r = int(227 + (248 - 227) * pct); g = int(179 + (81 - 179) * pct); b = int(65 + (73 - 65) * pct)
            
            bg_hex = f"#{min(255,max(0,r)):02x}{min(255,max(0,g)):02x}{min(255,max(0,b)):02x}"
            lum = 0.299 * r + 0.587 * g + 0.114 * b
            return (bg_hex, "black" if lum > 140 else "white")
        
        is_gradient = "Gradient" in color_mode or "Intensity" in color_mode
        intensity = math.pow(float(val) / float(max_val), 0.5) if (max_val > 0 and is_gradient) else 1.0
        intensity = max(0.15, min(1.0, intensity)) 
        
        default_cat_colors = GLOBAL_CAT_COLORS
        
        if "Category" in color_mode:
            base_hex = saved_colors.get(f"Category_{dom}", default_cat_colors.get(dom, "#8b949e"))
        elif "Extension" in color_mode:
            base_hex = saved_colors.get(f"Extension_{dom}", f"#{min(255, max(100, int(hashlib.md5(dom.encode()).hexdigest()[:6], 16) & 0xFFFFFF)):06x}")
        elif "Tag" in color_mode:
            if dom == "Untagged":
                bg_hex = get_background_hex()
                bg_c = QColor(bg_hex)
                lum = 0.299 * bg_c.red() + 0.587 * bg_c.green() + 0.114 * bg_c.blue()
                return (bg_hex, "black" if lum > 140 else "white")
            else:
                base_hex = saved_colors.get(f"Tag_{dom}", f"#{min(255, max(100, int(hashlib.md5(dom.encode()).hexdigest()[:6], 16) & 0xFFFFFF)):06x}")
        else:
            grad = getattr(self, 'map_gradient', 'Excel (Green-Yellow-Red)')
            if grad == "Excel (Green-Yellow-Red)":
                if intensity <= 0.5:
                    pct = intensity * 2.0
                    r = int(99 + (255 - 99) * pct); g = int(190 + (235 - 190) * pct); b = int(123 + (132 - 123) * pct)
                else:
                    pct = (intensity - 0.5) * 2.0
                    r = int(255 + (248 - 255) * pct); g = int(235 + (105 - 235) * pct); b = int(132 + (107 - 132) * pct)
            elif grad == "Fire":
                r, g, b = int(40 + 215*intensity), int(20 + 130*(intensity**2)), int(20)
            elif grad == "Green":
                r, g, b = int(20), int(50 + 205*intensity), int(40)
            elif grad == "Blue":
                r, g, b = int(10 + 100*(intensity**2)), int(20 + 150*(intensity**2)), int(40 + 215*intensity)
            elif grad == "Yellow":
                r, g, b = int(50 + 205*intensity), int(50 + 180*intensity), int(20)
            elif grad == "Custom...":
                low = QColor(saved_colors.get("Gradient_Low", "#21262d"))
                high = QColor(saved_colors.get("Gradient_High", "#f85149"))
                r = int(low.red() + (high.red() - low.red()) * intensity)
                g = int(low.green() + (high.green() - low.green()) * intensity)
                b = int(low.blue() + (high.blue() - low.blue()) * intensity)
            else:
                r, g, b = int(40 + 215*intensity), int(20 + 130*intensity), int(20)
            
            bg_hex = f"#{min(255,max(0,r)):02x}{min(255,max(0,g)):02x}{min(255,max(0,b)):02x}"
            lum = 0.299 * r + 0.587 * g + 0.114 * b
            return (bg_hex, "black" if lum > 140 else "white")
            
        base_c = QColor(base_hex)
        if not is_gradient:
            lum = 0.299 * base_c.red() + 0.587 * base_c.green() + 0.114 * base_c.blue()
            return (base_hex, "black" if lum > 140 else "white")
            
        bg_c = QColor("#21262d" if is_dark else "#ebecf0")
        r = int(bg_c.red() + (base_c.red() - bg_c.red()) * intensity)
        g = int(bg_c.green() + (base_c.green() - bg_c.green()) * intensity)
        b = int(bg_c.blue() + (base_c.blue() - bg_c.blue()) * intensity)
        
        bg_hex = f"#{min(255,max(0,r)):02x}{min(255,max(0,g)):02x}{min(255,max(0,b)):02x}"
        lum = 0.299 * r + 0.587 * g + 0.114 * b
        return (bg_hex, "black" if lum > 140 else "white")

    def render_grid_map(self):
        if not hasattr(self, 'figure_map') or not self.figure_map: return
        self.figure_map.clear()
        ax = self.figure_map.add_subplot(111)
        
        is_dark = True
        if self.main_app and hasattr(self.main_app, 'theme_combo'):
            is_dark = self.main_app.theme_combo.currentText() == "Dark"
        else:
            is_dark = QApplication.palette().window().color().lightness() < 128
            
        bg_c, txt_c = ('#0d1117', '#c9d1d9') if is_dark else ('#ffffff', '#24292f')
        self.figure_map.patch.set_facecolor(bg_c)
        ax.set_facecolor(bg_c)
        ax.tick_params(colors=txt_c, labelsize=9)
        
        ax.set_anchor('N') 

        if not hasattr(self, 'matrix_cache') or not self.matrix_cache:
            ax.text(0.5, 0.5, "No search results to visualize", color=txt_c, ha='center', va='center')
            ax.axis('off'); self.canvas_map.draw(); return
            
        saved_colors = QSettings("vmanOS", "HeatmapColors").value("custom_colors", {})
        if not isinstance(saved_colors, dict): saved_colors = {}

        scale = 1.0
        if self.map_tile_size == "Small (0.7x)": scale = 0.7
        elif self.map_tile_size == "Medium (1.0x)": scale = 1.0
        elif self.map_tile_size == "Large (1.2x)": scale = 1.2
        elif self.map_tile_size == "Custom...": scale = getattr(self, 'map_custom_scale', 1.2)
        
        box_w, box_h, gap = 1.0 * scale, 1.0 * scale, 0.2 * scale 
        pixel_scale = 16 * scale 
        
        patches, facecolors, edgecolors, linewidths = [], [], [], []
        self.map_coords_dict = {} 
        
        is_reverse_sort = (self.map_sort_order == "Top to Bottom (Newest First)")
        matrix = self.matrix_cache
        
        name_filter = self.map_highlight_input.text().lower()
        tag_highlight = self.map_tag_highlight_input.text().lower()
        
        hl_field = getattr(self, 'map_highlight_field', 'Name or Path')
        hl_type = getattr(self, 'map_highlight_type', 'Files & Folders')
        hl_color = getattr(self, 'map_highlight_color', '#b8860b')
        hl_size = getattr(self, 'map_highlight_size', 2.0)
        show_dates = getattr(self, 'map_show_dates', True)
        
        highlighted_years = set()
        
        max_val = 0.1
        for ym, days in matrix.items():
            for d, items in days.items():
                val, _ = self.get_map_value(items, self.map_color_mode)
                if val > max_val: max_val = val

        ax.set_aspect('equal')

        if self.map_layout_mode == "Compact Matrix (31 Days)":
            ym_keys = sorted(matrix.keys(), reverse=is_reverse_sort)
            for row_idx, ym in enumerate(ym_keys):
                for day in range(1, 32):
                    items = matrix[ym].get(day, [])
                    has_highlight = False
                    if name_filter or tag_highlight:
                        for i in items:
                            if hl_type == "Files Only" and i.get('is_fldr'): continue
                            if hl_type == "Folders Only" and not i.get('is_fldr'): continue
                            
                            match_n = not name_filter
                            if name_filter:
                                n_match = name_filter in i['name']
                                p_match = name_filter in i['path']
                                if hl_field == "Name or Path": match_n = n_match or p_match
                                elif hl_field == "Name Only": match_n = n_match
                                elif hl_field == "Path Only": match_n = p_match
                            
                            match_t = not tag_highlight
                            if tag_highlight:
                                match_t = tag_highlight in (i['tag_exact'] or "").lower() or tag_highlight in (i['tag_inherited'] or "").lower()
                                
                            if match_n and match_t:
                                has_highlight = True; highlighted_years.add(ym.split('-')[0])
                                break
                    
                    val, dom = self.get_map_value(items, self.map_color_mode)
                    x_pos = (day - 1) * (box_w + gap); y_pos = row_idx * (box_h + gap)
                    self.map_coords_dict[(day-1, row_idx)] = (ym, day)
                    c_hex, font_color = self.get_cell_color(val, max_val, dom, is_dark, saved_colors, self.map_color_mode)
                            
                    rect = mpatches.Rectangle((x_pos, y_pos), box_w, box_h)
                    patches.append(rect); facecolors.append(c_hex)
                    if (name_filter or tag_highlight) and has_highlight:
                        edgecolors.append(hl_color); linewidths.append(hl_size)
                    else:
                        edgecolors.append("none"); linewidths.append(0.0)
                        
                    if show_dates:
                        ax.text(x_pos + box_w/2, y_pos + box_h/2, str(day), ha='center', va='center', color=font_color, fontsize=int(7 * scale))
                    
            max_x = 31 * (box_w + gap); max_y = len(ym_keys) * (box_h + gap)
            ax.set_xlim(-gap, max_x); ax.set_ylim(max_y, -gap)
            x_ticks = [(d - 1) * (box_w + gap) + (box_w / 2) for d in range(1, 32)]
            ax.set_xticks(x_ticks); ax.set_xticklabels([str(d) for d in range(1, 32)])
            ax.xaxis.tick_top()
            y_ticks = [r * (box_h + gap) + (box_h / 2) for r in range(len(ym_keys))]
            ax.set_yticks(y_ticks); ax.set_yticklabels(ym_keys, fontweight="bold")
            top_pad = getattr(self, 'map_top_margin', 78.0) / max(100, max_y * pixel_scale)
            self.figure_map.subplots_adjust(top=1.0 - top_pad, bottom=0.01, left=0.10, right=0.98)
            self.canvas_map.setFixedSize(int(max_x * pixel_scale + 100), int(max_y * pixel_scale + 80))

        elif self.map_layout_mode == "Segmented Years (31 Days)":
            ym_keys = sorted(matrix.keys(), reverse=is_reverse_sort)
            current_row = 0; y_ticks_pos = []; y_ticks_labels = []; last_year = None
            
            for ym in ym_keys:
                y, m = ym.split('-')
                if last_year and last_year != y:
                    year_rect = mpatches.Rectangle((-gap, (current_row-1) * (box_h + gap) + box_h), 31 * (box_w + gap), 0, edgecolor="#b8860b", linewidth=2)
                    ax.add_patch(year_rect)
                    current_row += 1 
                
                for day in range(1, 32):
                    items = matrix[ym].get(day, [])
                    has_highlight = False
                    if name_filter or tag_highlight:
                        for i in items:
                            if hl_type == "Files Only" and i.get('is_fldr'): continue
                            if hl_type == "Folders Only" and not i.get('is_fldr'): continue
                            
                            match_n = not name_filter
                            if name_filter:
                                n_match = name_filter in i['name']; p_match = name_filter in i['path']
                                if hl_field == "Name or Path": match_n = n_match or p_match
                                elif hl_field == "Name Only": match_n = n_match
                                elif hl_field == "Path Only": match_n = p_match
                            match_t = not tag_highlight
                            if tag_highlight:
                                match_t = tag_highlight in (i['tag_exact'] or "").lower() or tag_highlight in (i['tag_inherited'] or "").lower()
                                
                            if match_n and match_t:
                                has_highlight = True; highlighted_years.add(y)
                                break
                            
                    val, dom = self.get_map_value(items, self.map_color_mode)
                    x_pos = (day - 1) * (box_w + gap); y_pos = current_row * (box_h + gap)
                    self.map_coords_dict[(day-1, current_row)] = (ym, day)
                    c_hex, font_color = self.get_cell_color(val, max_val, dom, is_dark, saved_colors, self.map_color_mode)
                            
                    rect = mpatches.Rectangle((x_pos, y_pos), box_w, box_h)
                    patches.append(rect); facecolors.append(c_hex)
                    if (name_filter or tag_highlight) and has_highlight:
                        edgecolors.append(hl_color); linewidths.append(hl_size)
                    else:
                        edgecolors.append("none"); linewidths.append(0.0)
                        
                    if show_dates:
                        ax.text(x_pos + box_w/2, y_pos + box_h/2, str(day), ha='center', va='center', color=font_color, fontsize=int(7 * scale))
                    
                y_ticks_pos.append(current_row * (box_h + gap) + (box_h / 2))
                y_ticks_labels.append(f"{y} {calendar.month_abbr[int(m)]}")
                current_row += 1; last_year = y
                
            max_x = 31 * (box_w + gap); max_y = current_row * (box_h + gap)
            ax.set_xlim(-gap, max_x); ax.set_ylim(max_y, -gap) 
            x_ticks = [(d - 1) * (box_w + gap) + (box_w / 2) for d in range(1, 32)]
            ax.set_xticks(x_ticks); ax.set_xticklabels([str(d) for d in range(1, 32)])
            ax.xaxis.tick_top()
            ax.set_yticks(y_ticks_pos); ax.set_yticklabels(y_ticks_labels, fontweight="bold")
            top_pad = getattr(self, 'map_top_margin', 78.0) / max(100, max_y * pixel_scale)
            self.figure_map.subplots_adjust(top=1.0 - top_pad, bottom=0.01, left=0.12, right=0.98)
            self.canvas_map.setFixedSize(int(max_x * pixel_scale + 120), int(max_y * pixel_scale + 60))

        elif "True Calendar" in self.map_layout_mode:
            years = set(int(ym[:4]) for ym in matrix.keys())
            if not years: years.add(datetime.datetime.now().year)
            years = sorted(list(years), reverse=is_reverse_sort)
            
            cols_per_row = 4 if "Widescreen" in self.map_layout_mode else 3
            total_cols_width = cols_per_row * 9 - 2
            current_row = 0
            
            for y in years:
                current_row += 3 
                ax.text((total_cols_width / 2) * (box_w + gap), current_row * (box_h + gap) - (0.8 * scale), str(y), ha='center', va='bottom', color='#8a6306', fontweight='bold', fontsize=int(18 * scale))
                year_start_row = current_row
                
                for m_idx in range(1, 13):
                    grid_col = (m_idx - 1) % cols_per_row
                    if m_idx > 1 and grid_col == 0: current_row += 9 
                    
                    x_offset = grid_col * 9 
                    ax.text(x_offset * (box_w + gap) + 3.5 * (box_w + gap), current_row * (box_h + gap) - 0.2, calendar.month_abbr[m_idx], ha='center', va='bottom', color='#b8860b', fontweight='bold', fontsize=int(10 * scale))
                    day_labels = ["M", "T", "W", "T", "F", "S", "S"]
                    for i, d in enumerate(day_labels):
                        ax.text((x_offset + i) * (box_w + gap) + box_w/2, (current_row + 0.8) * (box_h + gap), d, ha='center', va='bottom', color='#8b949e', fontsize=int(8 * scale))
                        
                    ym = f"{y}-{m_idx:02d}"
                    _, days_in_month = calendar.monthrange(y, m_idx)
                    start_idx = calendar.weekday(y, m_idx, 1) 
                    
                    for day in range(1, days_in_month + 1):
                        col = x_offset + ((start_idx + day - 1) % 7)
                        r_idx = current_row + 1 + ((start_idx + day - 1) // 7)
                        x_pos = col * (box_w + gap); y_pos = r_idx * (box_h + gap)
                        self.map_coords_dict[(col, r_idx)] = (ym, day)
                        items = matrix[ym].get(day, [])
                        
                        has_highlight = False
                        if name_filter or tag_highlight:
                            for i in items:
                                if hl_type == "Files Only" and i.get('is_fldr'): continue
                                if hl_type == "Folders Only" and not i.get('is_fldr'): continue
                                match_n = not name_filter
                                if name_filter:
                                    n_match = name_filter in i['name']; p_match = name_filter in i['path']
                                    if hl_field == "Name or Path": match_n = n_match or p_match
                                    elif hl_field == "Name Only": match_n = n_match
                                    elif hl_field == "Path Only": match_n = p_match
                                match_t = not tag_highlight
                                if tag_highlight: match_t = tag_highlight in (i['tag_exact'] or "").lower() or tag_highlight in (i['tag_inherited'] or "").lower()
                                if match_n and match_t: has_highlight = True; highlighted_years.add(str(y)); break
                                
                        val, dom = self.get_map_value(items, self.map_color_mode)
                        c_hex, font_color = self.get_cell_color(val, max_val, dom, is_dark, saved_colors, self.map_color_mode)
                                
                        rect = mpatches.Rectangle((x_pos, y_pos), box_w, box_h)
                        patches.append(rect); facecolors.append(c_hex)
                        if (name_filter or tag_highlight) and has_highlight:
                            edgecolors.append(hl_color); linewidths.append(hl_size)
                        else:
                            edgecolors.append("none"); linewidths.append(0.0)
                        
                        if show_dates:
                            ax.text(x_pos + box_w/2, y_pos + box_h/2, str(day), ha='center', va='center', color=font_color, fontsize=int(7 * scale))

                current_row += 8 
                y_box_h = (current_row - year_start_row - 0.5) * (box_h + gap)
                year_rect = mpatches.Rectangle((-gap, year_start_row * (box_h + gap) - gap*2), total_cols_width * (box_w + gap) + gap, y_box_h, fill=False, edgecolor="#b8860b", linewidth=2)
                ax.add_patch(year_rect)
                
            max_x = total_cols_width * (box_w + gap); max_y = current_row * (box_h + gap)
            ax.set_xlim(-gap*2, max_x + gap); ax.set_ylim(max_y, -gap*3)
            ax.set_xticks([]); ax.set_yticks([])
            top_pad = getattr(self, 'map_top_margin', 78.0) / max(100, max_y * pixel_scale)
            self.figure_map.subplots_adjust(top=1.0 - top_pad, bottom=0.01, left=0.02, right=0.98)
            self.canvas_map.setFixedSize(int(max_x * pixel_scale + 50), int(max_y * pixel_scale + 60))

        elif self.map_layout_mode == "GitHub Contribution (52 Weeks)":
            years = set(int(ym[:4]) for ym in matrix.keys())
            if not years: years.add(datetime.datetime.now().year)
            years = sorted(list(years), reverse=is_reverse_sort)
            
            current_row = 0; y_ticks_pos = []; y_ticks_labels = []
            
            for y in years:
                y_ticks_pos.append((current_row + 3) * (box_h + gap))
                y_ticks_labels.append(str(y))
                try: start_date = datetime.date(y, 1, 1)
                except: continue
                
                start_wday = start_date.weekday() 
                for day_offset in range(365 + (1 if calendar.isleap(y) else 0)):
                    curr_date = start_date + datetime.timedelta(days=day_offset)
                    week = (day_offset + start_wday) // 7
                    wday = curr_date.weekday()
                    ym = curr_date.strftime("%Y-%m"); day = curr_date.day
                    x_pos = week * (box_w + gap); y_pos = (current_row + wday) * (box_h + gap)
                    self.map_coords_dict[(week, current_row + wday)] = (ym, day)
                    items = matrix[ym].get(day, [])
                    
                    has_highlight = False
                    if name_filter or tag_highlight:
                        for i in items:
                            if hl_type == "Files Only" and i.get('is_fldr'): continue
                            if hl_type == "Folders Only" and not i.get('is_fldr'): continue
                            match_n = not name_filter
                            if name_filter:
                                n_match = name_filter in i['name']; p_match = name_filter in i['path']
                                if hl_field == "Name or Path": match_n = n_match or p_match
                                elif hl_field == "Name Only": match_n = n_match
                                elif hl_field == "Path Only": match_n = p_match
                            match_t = not tag_highlight
                            if tag_highlight: match_t = tag_highlight in (i['tag_exact'] or "").lower() or tag_highlight in (i['tag_inherited'] or "").lower()
                            if match_n and match_t: has_highlight = True; highlighted_years.add(str(y)); break
                                
                    val, dom = self.get_map_value(items, self.map_color_mode)
                    c_hex, font_color = self.get_cell_color(val, max_val, dom, is_dark, saved_colors, self.map_color_mode)
                            
                    rect = mpatches.Rectangle((x_pos, y_pos), box_w, box_h)
                    patches.append(rect); facecolors.append(c_hex)
                    if (name_filter or tag_highlight) and has_highlight:
                        edgecolors.append(hl_color); linewidths.append(hl_size)
                    else:
                        edgecolors.append("none"); linewidths.append(0.0)
                        
                    if show_dates:
                        ax.text(x_pos + box_w/2, y_pos + box_h/2, str(day), ha='center', va='center', color=font_color, fontsize=int(5 * scale))
                    
                current_row += 8 
                
            max_x = 54 * (box_w + gap); max_y = current_row * (box_h + gap)
            ax.set_xlim(-gap, max_x); ax.set_ylim(max_y, -gap)
            ax.set_xticks([]); ax.set_yticks(y_ticks_pos)
            ax.set_yticklabels(y_ticks_labels, fontweight="bold")
            top_pad = getattr(self, 'map_top_margin', 78.0) / max(100, max_y * pixel_scale)
            self.figure_map.subplots_adjust(top=1.0 - top_pad, bottom=0.01, left=0.08, right=0.98)
            self.canvas_map.setFixedSize(int(max_x * pixel_scale + 100), int(max_y * pixel_scale + 60))

        collection = PatchCollection(patches, facecolors=facecolors, edgecolors=edgecolors, linewidths=linewidths)
        ax.add_collection(collection)
        ax.set_title(f"Visual Grid Map [{self.map_layout_mode}] - ({self.map_color_mode})", color=txt_c, fontweight='bold', pad=15)
        for spine in ['top', 'right', 'bottom', 'left']: ax.spines[spine].set_visible(False)
        ax.tick_params(axis='both', which='major', length=0)
        self.canvas_map.draw()
        
        if getattr(self, '_show_highlight_dialog', False) and (name_filter or tag_highlight):
            if highlighted_years: QMessageBox.information(self, "Highlight Found", f"Matched tiles dynamically bordered in: {', '.join(sorted(list(highlighted_years)))}")
            else: QMessageBox.warning(self, "Not Found", "No tiles matched your highlight criteria.")

    def render_time_heatmap(self):
        if not hasattr(self, 'figure_time') or not self.figure_time: return
        self.figure_time.clear()
        ax = self.figure_time.add_subplot(111)
        
        is_dark = True
        if self.main_app and hasattr(self.main_app, 'theme_combo'):
            is_dark = self.main_app.theme_combo.currentText() == "Dark"
        else:
            is_dark = QApplication.palette().window().color().lightness() < 128
            
        bg_c, txt_c = ('#0d1117', '#c9d1d9') if is_dark else ('#ffffff', '#24292f')
        self.figure_time.patch.set_facecolor(bg_c)
        ax.set_facecolor(bg_c)
        ax.tick_params(colors=txt_c, labelsize=9)
        ax.set_anchor('N') 

        if not hasattr(self, 'time_matrix_cache') or not self.time_matrix_cache:
            ax.text(0.5, 0.5, "No specific time data to visualize", color=txt_c, ha='center', va='center')
            ax.axis('off'); self.canvas_time.draw(); return

        scale = 1.0
        if self.map_tile_size == "Small (0.7x)": scale = 0.7
        elif self.map_tile_size == "Medium (1.0x)": scale = 1.0
        elif self.map_tile_size == "Large (1.2x)": scale = 1.2
        elif self.map_tile_size == "Custom...": scale = getattr(self, 'map_custom_scale', 1.2)
        
        box_w, box_h, gap = 1.0 * scale, 1.0 * scale, 0.2 * scale 
        pixel_scale = 16 * scale 
        
        patches, facecolors, edgecolors, linewidths = [], [], [], []
        self.time_coords_dict = {} 
        
        is_reverse_sort = (self.map_sort_order == "Top to Bottom (Newest First)")
        matrix = self.time_matrix_cache
        ym_keys = sorted(matrix.keys(), reverse=is_reverse_sort)
        
        saved_colors = QSettings("vmanOS", "HeatmapColors").value("custom_colors", {})
        if not isinstance(saved_colors, dict): saved_colors = {}
        
        name_filter = self.map_highlight_input.text().lower()
        tag_highlight = self.map_tag_highlight_input.text().lower()
        hl_field = getattr(self, 'map_highlight_field', 'Name or Path')
        hl_type = getattr(self, 'map_highlight_type', 'Files & Folders')
        hl_color = getattr(self, 'map_highlight_color', '#b8860b')
        hl_size = getattr(self, 'map_highlight_size', 2.0)
        
        max_val = 0.1
        for ym in ym_keys:
            for b_idx in range(48):
                items = matrix[ym].get(b_idx/2.0, [])
                val, _ = self.get_map_value(items, self.time_color_mode)
                if val > max_val: max_val = val
                
        ax.set_aspect('equal')
        
        if self.time_layout_mode == "Segmented Years":
            current_row = 0; y_ticks_pos = []; y_ticks_labels = []; last_year = None
            
            for ym in ym_keys:
                y, m = ym.split('-')
                if last_year and last_year != y:
                    year_rect = mpatches.Rectangle((-gap, (current_row-1) * (box_h + gap) + box_h), 48 * (box_w + gap), 0, edgecolor="#b8860b", linewidth=2)
                    ax.add_patch(year_rect)
                    current_row += 1 
                
                for b_idx in range(48):
                    b_val = b_idx / 2.0
                    items = matrix[ym].get(b_val, [])
                    x_pos = b_idx * (box_w + gap)
                    y_pos = current_row * (box_h + gap)
                    self.time_coords_dict[(b_idx, current_row)] = (ym, b_val)
                    
                    has_highlight = False
                    if name_filter or tag_highlight:
                        for i in items:
                            if hl_type == "Files Only" and i.get('is_fldr'): continue
                            if hl_type == "Folders Only" and not i.get('is_fldr'): continue
                            match_n = not name_filter
                            if name_filter:
                                n_match = name_filter in i['name']; p_match = name_filter in i['path']
                                if hl_field == "Name or Path": match_n = n_match or p_match
                                elif hl_field == "Name Only": match_n = n_match
                                elif hl_field == "Path Only": match_n = p_match
                            match_t = not tag_highlight
                            if tag_highlight: match_t = tag_highlight in (i['tag_exact'] or "").lower() or tag_highlight in (i['tag_inherited'] or "").lower()
                            if match_n and match_t: has_highlight = True; break
                    
                    val, dom = self.get_map_value(items, self.time_color_mode)
                    c_hex, _ = self.get_cell_color(val, max_val, dom, is_dark, saved_colors, self.time_color_mode)
                            
                    rect = mpatches.Rectangle((x_pos, y_pos), box_w, box_h)
                    patches.append(rect); facecolors.append(c_hex)
                    if (name_filter or tag_highlight) and has_highlight:
                        edgecolors.append(hl_color); linewidths.append(hl_size)
                    else:
                        edgecolors.append("none"); linewidths.append(0.0)
                
                y_ticks_pos.append(current_row * (box_h + gap) + (box_h / 2))
                y_ticks_labels.append(f"{y} {calendar.month_abbr[int(m)]}")
                current_row += 1; last_year = y
                
            max_x = 48 * (box_w + gap); max_y = current_row * (box_h + gap)
            ax.set_xlim(-gap, max_x); ax.set_ylim(max_y, -gap)
            x_ticks = [i * (box_w + gap) + (box_w / 2) for i in range(48)]
            ax.set_xticks(x_ticks)
            ax.set_xticklabels([f"{int(i/2)}:30" if i%2!=0 else f"{int(i/2)}:00" for i in range(48)], rotation=90, fontsize=int(6*scale))
            ax.xaxis.tick_top()
            ax.set_yticks(y_ticks_pos); ax.set_yticklabels(y_ticks_labels, fontweight="bold")
            top_pad = getattr(self, 'map_top_margin', 78.0) / max(100, max_y * pixel_scale)
            self.figure_time.subplots_adjust(top=1.0 - top_pad, bottom=0.01, left=0.10, right=0.98)
            self.canvas_time.setFixedSize(int(max_x * pixel_scale + 100), int(max_y * pixel_scale + 100))
        
        else:
            for row_idx, ym in enumerate(ym_keys):
                for b_idx in range(48):
                    b_val = b_idx / 2.0
                    items = matrix[ym].get(b_val, [])
                    x_pos = b_idx * (box_w + gap)
                    y_pos = row_idx * (box_h + gap)
                    self.time_coords_dict[(b_idx, row_idx)] = (ym, b_val)
                    
                    has_highlight = False
                    if name_filter or tag_highlight:
                        for i in items:
                            if hl_type == "Files Only" and i.get('is_fldr'): continue
                            if hl_type == "Folders Only" and not i.get('is_fldr'): continue
                            match_n = not name_filter
                            if name_filter:
                                n_match = name_filter in i['name']; p_match = name_filter in i['path']
                                if hl_field == "Name or Path": match_n = n_match or p_match
                                elif hl_field == "Name Only": match_n = n_match
                                elif hl_field == "Path Only": match_n = p_match
                            match_t = not tag_highlight
                            if tag_highlight: match_t = tag_highlight in (i['tag_exact'] or "").lower() or tag_highlight in (i['tag_inherited'] or "").lower()
                            if match_n and match_t: has_highlight = True; break
                    
                    val, dom = self.get_map_value(items, self.time_color_mode)
                    c_hex, _ = self.get_cell_color(val, max_val, dom, is_dark, saved_colors, self.time_color_mode)
                            
                    rect = mpatches.Rectangle((x_pos, y_pos), box_w, box_h)
                    patches.append(rect); facecolors.append(c_hex)
                    if (name_filter or tag_highlight) and has_highlight:
                        edgecolors.append(hl_color); linewidths.append(hl_size)
                    else:
                        edgecolors.append("none"); linewidths.append(0.0)
                    
            max_x = 48 * (box_w + gap); max_y = len(ym_keys) * (box_h + gap)
            ax.set_xlim(-gap, max_x); ax.set_ylim(max_y, -gap)
            x_ticks = [i * (box_w + gap) + (box_w / 2) for i in range(48)]
            ax.set_xticks(x_ticks)
            ax.set_xticklabels([f"{int(i/2)}:30" if i%2!=0 else f"{int(i/2)}:00" for i in range(48)], rotation=90, fontsize=int(6*scale))
            ax.xaxis.tick_top()
            y_ticks = [r * (box_h + gap) + (box_h / 2) for r in range(len(ym_keys))]
            ax.set_yticks(y_ticks); ax.set_yticklabels(ym_keys, fontweight="bold")
            top_pad = getattr(self, 'map_top_margin', 78.0) / max(100, max_y * pixel_scale)
            self.figure_time.subplots_adjust(top=1.0 - top_pad, bottom=0.01, left=0.10, right=0.98)
            self.canvas_time.setFixedSize(int(max_x * pixel_scale + 100), int(max_y * pixel_scale + 100))

        collection = PatchCollection(patches, facecolors=facecolors, edgecolors=edgecolors, linewidths=linewidths)
        ax.add_collection(collection)
        
        if self.time_show_lines:
            for div in [12, 24, 36]: 
                xl = div * (box_w + gap) - (gap / 2.0)
                ax.axvline(x=xl, color='#b8860b' if is_dark else 'gray', linestyle='--', linewidth=1.5, alpha=0.8)

        ax.set_title(f"24-Hour Time Heatmap [{self.time_layout_mode}] - ({self.time_color_mode})", color=txt_c, fontweight='bold', pad=25)
        for spine in ['top', 'right', 'bottom', 'left']: ax.spines[spine].set_visible(False)
        ax.tick_params(axis='both', which='major', length=0)
        self.canvas_time.draw()

    def render_analytics(self):
        if not hasattr(self, 'figure_chart') or not self.figure_chart: return
        self.figure_chart.clear()
        ax = self.figure_chart.add_subplot(111)
        
        is_dark = True
        if self.main_app and hasattr(self.main_app, 'theme_combo'):
            is_dark = self.main_app.theme_combo.currentText() == "Dark"
        else:
            is_dark = QApplication.palette().window().color().lightness() < 128
            
        bg_c, txt_c = ('#0d1117', '#c9d1d9') if is_dark else ('#ffffff', '#24292f')
        grid_c = '#30363d' if is_dark else '#e1e4e8'
        self.figure_chart.patch.set_facecolor(bg_c)
        ax.set_facecolor(bg_c)
        ax.tick_params(colors=txt_c, labelsize=9)

        if not self.current_results:
            ax.text(0.5, 0.5, "No search results to visualize", color=txt_c, ha='center', va='center'); ax.axis('off')
            self.canvas_chart.draw(); return

        metric = self.combo_chart_metric.currentText()
        is_size = "Size" in metric
        is_flat = "Flat" in metric
        
        data_map = defaultdict(float)
        
        for r in self.current_results:
            is_fldr = r[6]
            size_mb = (r[4] or 0) / (1024*1024)
            val = size_mb if is_size else 1.0
            
            if is_size and is_fldr and not is_flat: continue 
            
            if "By Database" in metric:
                db_name = r[12] if len(r) > 12 else Path(self.active_db).stem
                data_map[db_name] += val
            
            if "Extension" in metric:
                k = r[3].upper() if r[3] else "NONE"
                data_map[k] += val
            elif "Category" in metric:
                k = r[10] if r[10] else "Others" 
                data_map[k] += val
            elif "By Tags" in metric:
                tags_str = r[8] if (is_flat or not is_size) else (r[8] if r[8] else self.inherited_tags.get(r[0]))
                if tags_str:
                    for t in tags_str.split(','):
                        if t.strip(): data_map[t.strip()] += val
                else:
                    data_map["Untagged"] += val
            elif "Virtual Path" in metric:
                k = r[2].split('/')[1] if len(r[2].split('/')) > 1 else "Root"
                data_map[k] += val
            elif "Year" in metric:
                mod_str = str(r[5])
                k = mod_str[:4] if len(mod_str)>=4 else "Unknown"
                data_map[k] += val
            elif "Month" in metric:
                mod_str = str(r[5])
                k = mod_str[:7] if len(mod_str)>=7 else "Unknown"
                data_map[k] += val
            elif "Week" in metric:
                mod_str = str(r[5])
                if len(mod_str) >= 10:
                    try:
                        dto = datetime.datetime.strptime(mod_str[:10], "%Y-%m-%d")
                        y, w, _ = dto.isocalendar()
                        k = f"{y}-W{w:02d}"
                        data_map[k] += val
                    except: pass
            elif "Day" in metric:
                mod_str = str(r[5])
                if len(mod_str) >= 10:
                    try:
                        dto = datetime.datetime.strptime(mod_str[:10], "%Y-%m-%d")
                        k = dto.strftime("%A")
                        data_map[k] += val
                    except: pass
            elif "By Date" in metric:
                mod_str = str(r[5])
                if len(mod_str) >= 10:
                    k = mod_str[8:10] 
                    data_map[k] += val
            elif "24-Hour Time" in metric:
                dt_str = str(r[5])
                if len(dt_str) >= 16:
                    try:
                        k = dt_str[11:13] + ":00"
                        data_map[k] += val
                    except: pass

        if not data_map:
            ax.text(0.5, 0.5, "Insufficient data for this metric", color=txt_c, ha='center', va='center'); ax.axis('off')
            self.canvas_chart.draw(); return

        sort_mode = self.combo_chart_sort.currentText()
        day_order = {"Monday": 1, "Tuesday": 2, "Wednesday": 3, "Thursday": 4, "Friday": 5, "Saturday": 6, "Sunday": 7}
        
        # Exact Custom Category Sorting implementation
        cat_order = {cat: i for i, cat in enumerate(list(CAT_MAP.keys()) + ["Others"])}
        
        if "Category" in metric:
            if sort_mode == "Sort: Name/Time (Descending)":
                sorted_items = sorted(data_map.items(), key=lambda x: cat_order.get(x[0], 99), reverse=True)
            elif sort_mode == "Sort: Name/Time (Ascending)":
                sorted_items = sorted(data_map.items(), key=lambda x: cat_order.get(x[0], 99), reverse=False)
            elif sort_mode == "Sort: Value (High to Low)":
                sorted_items = sorted(data_map.items(), key=lambda x: x[1], reverse=True)
            else:
                sorted_items = sorted(data_map.items(), key=lambda x: x[1], reverse=False)
        else:
            if sort_mode == "Sort: Value (High to Low)":
                sorted_items = sorted(data_map.items(), key=lambda x: x[1], reverse=True)
            elif sort_mode == "Sort: Value (Low to High)":
                sorted_items = sorted(data_map.items(), key=lambda x: x[1], reverse=False)
            elif sort_mode == "Sort: Name/Time (Descending)":
                if "By Day" in metric:
                    sorted_items = sorted(data_map.items(), key=lambda x: day_order.get(x[0], 99), reverse=True)
                else:
                    sorted_items = sorted(data_map.items(), key=lambda x: str(x[0]), reverse=True)
            else: # Ascending
                if "By Day" in metric:
                    sorted_items = sorted(data_map.items(), key=lambda x: day_order.get(x[0], 99), reverse=False)
                else:
                    sorted_items = sorted(data_map.items(), key=lambda x: str(x[0]), reverse=False)
        
        sorted_items = sorted_items[:31]
        
        labels = [x[0] for x in sorted_items]
        values = [x[1] for x in sorted_items]
        
        saved_colors = QSettings("vmanOS", "HeatmapColors").value("custom_colors", {})
        if not isinstance(saved_colors, dict): saved_colors = {}
        default_cat_colors = GLOBAL_CAT_COLORS
        
        colors = []
        for lab in labels:
            if "Category" in metric: colors.append(saved_colors.get(f"Category_{lab}", default_cat_colors.get(lab, "#8b949e")))
            elif "Extension" in metric: colors.append(saved_colors.get(f"Extension_{lab}", f"#{min(255, max(100, int(hashlib.md5(lab.encode()).hexdigest()[:6], 16) & 0xFFFFFF)):06x}"))
            elif "Tags" in metric: colors.append(saved_colors.get(f"Tag_{lab}", f"#{min(255, max(100, int(hashlib.md5(lab.encode()).hexdigest()[:6], 16) & 0xFFFFFF)):06x}"))
            else: colors.append("#58a6ff")
        
        # --- REPLACE ax.bar(...) with this block ---
        self.chart_bars = ax.bar(labels, values, color=colors, edgecolor=bg_c)
        self.chart_labels = labels
        self.chart_values = values
        
        # Create Tooltip Annotation
        self.chart_annot = ax.annotate("", xy=(0,0), xytext=(0, 10), textcoords="offset points",
                                       bbox=dict(boxstyle="round,pad=0.5", fc=bg_c, ec=txt_c, lw=1.5, alpha=0.95),
                                       arrowprops=dict(arrowstyle="->", color=txt_c),
                                       color=txt_c, zorder=10, fontsize=10, fontweight='bold', ha='center')
        self.chart_annot.set_visible(False)
        # ---------------------------------------------
        
        ax.set_title(f"Search Results Analytics {metric}", color=txt_c, fontweight='bold', pad=15)
        ax.set_ylabel("Size (MB)" if is_size else "Count", color=txt_c, fontweight='bold')
        ax.tick_params(axis='x', rotation=30)
        ax.grid(axis='y', color=grid_c, linestyle='--', alpha=0.5)
        for spine in ['top', 'right']: ax.spines[spine].set_visible(False)
        for spine in ['bottom', 'left']: ax.spines[spine].set_color(grid_c)
        
        self.figure_chart.tight_layout(pad=1.5)
        self.canvas_chart.draw()
