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
from collections import defaultdict
from pathlib import Path

from PySide6.QtCore import Qt, QDate, QThread, Signal, QSize, QFileInfo, QSettings
from PySide6.QtGui import QAction, QFont, QIcon, QColor, QBrush, QTextCursor, QCursor
from PySide6.QtWidgets import (QMainWindow, QVBoxLayout, QHBoxLayout, QGridLayout, 
                               QLabel, QPushButton, QWidget, QLineEdit, QComboBox, 
                               QCheckBox, QDoubleSpinBox, QDateEdit, QTableWidget, 
                               QTableWidgetItem, QHeaderView, QMessageBox, QMenu, 
                               QApplication, QProgressBar, QProgressDialog, QStyle, QFrame, 
                               QFormLayout, QDialog, QFileIconProvider, QSizePolicy, 
                               QScrollArea, QPlainTextEdit, QTabWidget, QButtonGroup, QRadioButton, QFileDialog, QColorDialog, QInputDialog)
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


# --- Color Customization Dialog ---
class MapColorConfigDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("⚙️ Customize Map Colors")
        self.resize(400, 650)
        if parent and hasattr(parent, 'styleSheet'): self.setStyleSheet(parent.styleSheet())
        
        self.settings = QSettings("vmanOS", "HeatmapColors")
        self.custom_colors = self.settings.value("custom_colors", {})
        if not isinstance(self.custom_colors, dict): self.custom_colors = {}
        
        layout = QVBoxLayout(self)
        
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.NoFrame)
        container = QWidget()
        self.form = QFormLayout(container)
        
        self.color_btns = {}
        
        # Custom Gradient for Intensity Mode
        self.form.addRow(QLabel("<b style='color:#58a6ff; font-size:14px;'>Custom Intensity Gradient</b>"), QLabel(""))
        self.add_color_row("Low Intensity (Empty/Min):", "Gradient_Low", self.custom_colors.get("Gradient_Low", "#21262d"))
        self.add_color_row("High Intensity (Peak):", "Gradient_High", self.custom_colors.get("Gradient_High", "#f85149"))
        self.form.addRow(QLabel(" "), QLabel(" "))
        
        # Categories
        self.form.addRow(QLabel("<b style='color:#58a6ff; font-size:14px;'>Category Colors</b>"), QLabel(""))
        cats = ["Images", "Videos", "Audio", "Documents", "Code", "Archives", "Others"]
        default_cat_colors = {"Images": "#a371f7", "Videos": "#f85149", "Audio": "#ff7b72", "Documents": "#d2a8ff", "Code": "#79c0ff", "Archives": "#e3b341", "Others": "#8b949e"}
        
        for c in cats:
            key = f"Category_{c}"
            cur_color = self.custom_colors.get(key, default_cat_colors.get(c, "#8b949e"))
            self.add_color_row(c, key, cur_color)
            
        # Extensions & Tags (Load existing)
        self.form.addRow(QLabel(" "), QLabel(" "))
        self.form.addRow(QLabel("<b style='color:#58a6ff; font-size:14px;'>Extension Colors</b>"), QLabel(""))
        for k, v in self.custom_colors.items():
            if k.startswith("Extension_"): self.add_color_row(k.replace("Extension_", "Ext: "), k, v)
            
        btn_add_ext = QPushButton("+ Add Extension Color")
        btn_add_ext.clicked.connect(lambda: self.add_new_mapping("Extension"))
        self.form.addRow("", btn_add_ext)
        
        self.form.addRow(QLabel(" "), QLabel(" "))
        self.form.addRow(QLabel("<b style='color:#58a6ff; font-size:14px;'>Tag Colors</b>"), QLabel(""))
        for k, v in self.custom_colors.items():
            if k.startswith("Tag_"): self.add_color_row(k.replace("Tag_", "Tag: "), k, v)
            
        btn_add_tag = QPushButton("+ Add Tag Color")
        btn_add_tag.clicked.connect(lambda: self.add_new_mapping("Tag"))
        self.form.addRow("", btn_add_tag)
            
        layout.addWidget(scroll)
        scroll.setWidget(container)
        
        btn_box = QHBoxLayout()
        btn_save = QPushButton("Save & Apply")
        btn_save.setStyleSheet("background-color: #2ea043; color: white; font-weight: bold; padding: 8px; border-radius: 4px;")
        btn_save.clicked.connect(self.save_and_close)
        btn_box.addStretch()
        btn_box.addWidget(btn_save)
        layout.addLayout(btn_box)
        
    def add_color_row(self, display_name, key, color_hex):
        btn = QPushButton()
        btn.setCursor(Qt.PointingHandCursor)
        btn.setStyleSheet(f"background-color: {color_hex}; border: 1px solid #30363d; border-radius: 4px; min-width: 60px; min-height: 25px;")
        btn.clicked.connect(lambda checked=False, k=key, b=btn: self.pick_color(k, b))
        self.color_btns[key] = color_hex
        self.form.addRow(display_name, btn)

    def add_new_mapping(self, prefix):
        name, ok = QInputDialog.getText(self, f"Add {prefix}", f"Enter {prefix} name (e.g. {'jpg' if prefix=='Extension' else 'work'}):")
        if ok and name.strip():
            key = f"{prefix}_{name.strip().lower()}"
            if key not in self.color_btns:
                color = QColorDialog.getColor(Qt.white, self, "Select Color")
                if color.isValid():
                    self.add_color_row(f"{prefix[:3]}: {name.strip().lower()}", key, color.name())
        
    def pick_color(self, key, btn):
        color = QColorDialog.getColor(QColor(self.color_btns[key]), self, "Select Color")
        if color.isValid():
            self.color_btns[key] = color.name()
            btn.setStyleSheet(f"background-color: {color.name()}; border: 1px solid #30363d; border-radius: 4px; min-width: 60px; min-height: 25px;")
            
    def save_and_close(self):
        for k, v in self.color_btns.items():
            self.custom_colors[k] = v
        self.settings.setValue("custom_colors", self.custom_colors)
        self.accept()


class RowLimitDialogSearch(QDialog):
    def __init__(self, total_rows, parent=None):
        super().__init__(parent)
        self.total_rows = total_rows
        self.setWindowTitle("Massive Dataset Found")
        self.resize(450, 250)
        if parent and hasattr(parent, 'styleSheet'): self.setStyleSheet(parent.styleSheet())
        
        layout = QVBoxLayout(self)
        lbl = QLabel(f"<b>Found {total_rows:,} matching records.</b><br><br>Loading huge amounts of rows into the UI grid will consume RAM and reduce performance. How many rows would you like to render in the table?<br><br><span style='color:#e3b341;'><i>(Note: Maps and Analytics process 100% of the data instantly if Fast Mode is OFF).</i></span>")
        lbl.setWordWrap(True)
        layout.addWidget(lbl)
        
        self.radio_group = QButtonGroup(self)
        self.rb_rec = QRadioButton(f"Recommended (First {min(1000, total_rows)} rows)")
        self.rb_rec.setChecked(True)
        self.rb_first_half = QRadioButton(f"First Half ({total_rows // 2} rows)")
        self.rb_last_half = QRadioButton(f"Last Half ({total_rows - (total_rows // 2)} rows)")
        self.rb_all = QRadioButton(f"All Rows ({total_rows}) - ⚠️ May cause UI stutter")
        self.rb_none = QRadioButton("0 Rows (Maximum Speed - Maps & Charts Only)")
        
        for i, rb in enumerate([self.rb_rec, self.rb_first_half, self.rb_last_half, self.rb_all, self.rb_none]):
            self.radio_group.addButton(rb, i)
            layout.addWidget(rb)
            
        btn_box = QHBoxLayout()
        btn_ok = QPushButton("Render Data")
        btn_ok.setStyleSheet("background-color: #2ea043; color: white; font-weight: bold; padding: 8px; border-radius: 4px;")
        btn_ok.clicked.connect(self.accept)
        btn_box.addWidget(btn_ok)
        layout.addLayout(btn_box)

    def get_values(self):
        idx = self.radio_group.checkedId()
        if idx == 0: return 0, min(1000, self.total_rows)
        elif idx == 1: return 0, self.total_rows // 2
        elif idx == 2: return self.total_rows // 2, self.total_rows
        elif idx == 3: return 0, self.total_rows
        elif idx == 4: return 0, 0


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
        db_path = self.p['db']
        
        if not os.path.exists(db_path):
            self.finished_search.emit(False)
            return
            
        try:
            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()
                cur.execute("PRAGMA table_info(virtual_fs)")
                cols = [row[1] for row in cur.fetchall()]
                
                selects = ["id", "name", "parent_path", "is_folder", "size", "extension", "modified", "real_path", "custom_tags"]
                if "color_tag" in cols: selects.append("color_tag")
                else: selects.append("'' as color_tag")
                if "category" in cols: selects.append("category")
                else: selects.append("'Others' as category")
                
                # --- NEW: Safely query SHA-256 ---
                if "sha256" in cols: selects.append("sha256")
                elif "hash" in cols: selects.append("hash as sha256")
                else: selects.append("'' as sha256")
                
                query = f"SELECT {', '.join(selects)} FROM virtual_fs WHERE 1=1"
                sql_params = []
                
                if self.p['look_for'] == "Files Only": query += " AND is_folder=0"
                elif self.p['look_for'] == "Folders Only": query += " AND is_folder=1"
                
                if self.p['name']:
                    if self.p['match'] == "Contains": query += " AND name LIKE ?"; sql_params.append(f"%{self.p['name']}%")
                    elif self.p['match'] == "Exact": query += " AND name = ?"; sql_params.append(self.p['name'])
                    elif self.p['match'] == "Starts With": query += " AND name LIKE ?"; sql_params.append(f"{self.p['name']}%")
                    elif self.p['match'] == "Ends With": query += " AND name LIKE ?"; sql_params.append(f"%{self.p['name']}")
                        
                if self.p['path']: query += " AND parent_path LIKE ?"; sql_params.append(f"%{self.p['path']}%")
                
                if self.p.get('category', 'All') != "All":
                    query += " AND category = ?"
                    sql_params.append(self.p['category'])
                
                if self.p.get('ex_names'):
                    for ex_n in self.p['ex_names']:
                        query += " AND LOWER(name) NOT LIKE ?"
                        sql_params.append(f"%{ex_n}%")
                    
                cur.execute(query, sql_params)
                raw_results = cur.fetchall()
                
                total = len(raw_results)
                last_dir = "" 
                
                for idx, row_data in enumerate(raw_results):
                    if not self.is_running:
                        self.results_ready.emit(all_results)
                        self.finished_search.emit(True)
                        return
                        
                    # UPDATE to 12 items:
                    db_id, name, p_path, is_fldr, size, ext, modified, real_path, tags, color_tag, category_val, sha256_val = row_data
                    
                    if self.p.get('verbose', False) and p_path != last_dir:
                        self.telemetry_update.emit(p_path)
                        last_dir = p_path
                        
                    ext_val = str(ext).lower() if ext else ""
                    size_val = size or 0
                    
                    is_match = True
                    if self.p['exts'] and ext_val not in self.p['exts']: is_match = False
                    elif self.p['ex_exts'] and ext_val in self.p['ex_exts']: is_match = False
                    elif not is_fldr and not (self.p['sz_min'] <= size_val <= self.p['sz_max']): is_match = False
                    elif self.p['use_date'] and modified:
                        try:
                            mod_date = QDate.fromString(modified.split()[0], "yyyy-MM-dd")
                            if mod_date < self.p['date_start'] or mod_date > self.p['date_end']: is_match = False
                        except: pass
                        
                    if is_match:
                        # UPDATE to 12 items:
                        all_results.append((db_id, name, p_path, ext_val, size_val, modified, is_fldr, real_path, tags, color_tag, category_val, sha256_val))
                        
                    if idx % 800 == 0 or idx == total - 1:
                        if total > 0: self.progress_update.emit(int((idx / total) * 100))
                        
        except Exception as e:
            print(f"Search Error: {e}")
            
        self.progress_update.emit(100)
        self.results_ready.emit(all_results)
        self.finished_search.emit(False)


class AdvancedSearchWindow(QMainWindow):
    def __init__(self, active_db, parent=None):
        super().__init__(parent)
        self.active_db = active_db
        self.main_app = parent
        self.is_searching = False
        self._abort_render = False 
        
        # Load Persistent Settings
        self.settings = QSettings("VirtualMan", "AdvancedSearchUI")
        self.show_icons = self.settings.value("show_icons", True, type=bool)
        
        self.verbose_telemetry = self.settings.value("verbose_telemetry", False, type=bool) 
        self.fast_mode = self.settings.value("fast_mode", False, type=bool)
        
        self.map_color_mode = self.settings.value("map_color_mode", "Intensity (Count)")
        self.map_gradient = self.settings.value("map_gradient", "Excel (Green-Yellow-Red)")
        self.map_layout_mode = self.settings.value("map_layout_mode", "True Calendar Standard (3x4 Grids)")
        self.map_sort_order = self.settings.value("map_sort_order", "Top to Bottom (Newest First)")
        self.map_tile_size = self.settings.value("map_tile_size", "Large (1.2x)") 
        self.map_custom_scale = float(self.settings.value("map_custom_scale", 1.2))
        self.map_top_margin = float(self.settings.value("map_top_margin", 78.0)) # Default changed to 78.0
        
        self.current_results = []
        self.matrix_cache = defaultdict(lambda: defaultdict(list))
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
        
        self.setWindowTitle("Search It!")
        self.resize(1050, 650)
        self.setAttribute(Qt.WA_DeleteOnClose, False)
        
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setSpacing(8)
        main_layout.setContentsMargins(10, 10, 10, 10)

        # --- TOP AREA (GLASS BUTTONS) ---
        top_bar = QHBoxLayout()
        self.txt_name = QLineEdit()
        self.txt_name.setPlaceholderText("Search file or folder name...")
        self.txt_name.setStyleSheet("font-size: 15px; padding: 10px;")
        
        self.btn_scope = QPushButton("Location & Target")
        self.btn_type = QPushButton("File Types")
        self.btn_metrics = QPushButton("Attributes & Time")
        
        for btn in [self.btn_scope, self.btn_type, self.btn_metrics]:
            btn.setCheckable(True)
            btn.toggled.connect(self.update_filter_visibility)

        self.btn_search = QPushButton("⚡ SEARCH")
        self.btn_search.setFixedWidth(150)
        self.btn_search.clicked.connect(self.handle_button_action)

        top_bar.addWidget(self.txt_name, stretch=1)
        top_bar.addWidget(self.btn_scope)
        top_bar.addWidget(self.btn_type)
        top_bar.addWidget(self.btn_metrics)
        top_bar.addWidget(self.btn_search)
        main_layout.addLayout(top_bar)

        # --- DYNAMIC HORIZONTAL FILTER CARDS ---
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
        f1.addRow("Match Mode:", self.combo_match); f1.addRow("Data Type:", self.combo_look_for); f1.addRow("Virtual Path:", self.txt_path)
        filters_h_layout.addWidget(self.card_scope)

        self.card_type = QFrame(); self.card_type.setObjectName("FilterCard"); self.card_type.setVisible(False)
        self.card_type.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        f2 = QFormLayout(self.card_type); f2.setContentsMargins(15, 15, 15, 15); f2.setVerticalSpacing(10)
        self.txt_ext = QLineEdit(); self.txt_ext.setPlaceholderText(".jpg, .pdf")
        self.txt_exclude_ext = QLineEdit(); self.txt_exclude_ext.setPlaceholderText(".tmp, .bak")
        self.txt_exclude_name = QLineEdit(); self.txt_exclude_name.setPlaceholderText("e.g. backup, temp")
        f2.addRow("Include Ext:", self.txt_ext); f2.addRow("Exclude Ext:", self.txt_exclude_ext); f2.addRow("Skip Names:", self.txt_exclude_name)
        filters_h_layout.addWidget(self.card_type)

        self.card_metrics = QFrame(); self.card_metrics.setObjectName("FilterCard"); self.card_metrics.setVisible(False)
        self.card_metrics.setFixedWidth(420) 
        f3 = QFormLayout(self.card_metrics); f3.setContentsMargins(15, 15, 15, 15); f3.setVerticalSpacing(10)
        
        self.combo_category = QComboBox()
        self.combo_category.addItems(["All", "Images", "Videos", "Audio", "Documents", "Code", "Archives", "Others"])
        f3.addRow("Category:", self.combo_category)

        size_lay = QHBoxLayout()
        self.spin_size_min = QDoubleSpinBox(); self.spin_size_min.setRange(0, 999999); self.spin_size_min.setDecimals(2); self.spin_size_min.setButtonSymbols(QDoubleSpinBox.NoButtons)
        self.spin_size_max = QDoubleSpinBox(); self.spin_size_max.setRange(0, 999999); self.spin_size_max.setValue(999999); self.spin_size_max.setDecimals(2); self.spin_size_max.setButtonSymbols(QDoubleSpinBox.NoButtons)
        self.combo_sz_unit = QComboBox(); self.combo_sz_unit.addItems(["MB", "B", "KB", "GB"])
        self.combo_sz_unit.setFixedWidth(55)
        size_lay.addWidget(self.spin_size_min); size_lay.addWidget(QLabel("to")); size_lay.addWidget(self.spin_size_max); size_lay.addWidget(self.combo_sz_unit)

        date_lay = QHBoxLayout()
        self.chk_use_date = QCheckBox("")
        self.date_start = QDateEdit(QDate.currentDate().addYears(-1)); self.date_start.setCalendarPopup(True); self.date_start.setEnabled(False)
        self.date_end = QDateEdit(QDate.currentDate()); self.date_end.setCalendarPopup(True); self.date_end.setEnabled(False)
        self.chk_use_date.toggled.connect(self.date_start.setEnabled); self.chk_use_date.toggled.connect(self.date_end.setEnabled)
        date_lay.addWidget(self.chk_use_date); date_lay.addWidget(self.date_start); date_lay.addWidget(QLabel("to")); date_lay.addWidget(self.date_end)
        
        f3.addRow("File Size:", size_lay); f3.addRow("Modified:", date_lay)
        filters_h_layout.addWidget(self.card_metrics)
        main_layout.addWidget(self.filters_container)

        for box in [self.txt_name, self.txt_path, self.txt_ext, self.txt_exclude_ext, self.txt_exclude_name]:
            box.returnPressed.connect(self.handle_button_action)

        self.progress = QProgressBar(); self.progress.setVisible(False)
        self.progress.setFixedHeight(18) 
        self.progress.setTextVisible(True) 
        self.progress.setAlignment(Qt.AlignCenter)
        self.progress.setStyleSheet("""
            QProgressBar { border: 1px solid #30363d; border-radius: 4px; background-color: #0d1117; color: white; font-weight: bold; }
            QProgressBar::chunk { background-color: #1f6feb; border-radius: 3px; }
        """)
        main_layout.addWidget(self.progress)
        
        self.lbl_status = QLabel("Ready."); self.lbl_status.setStyleSheet("color: #8b949e; font-style: italic; padding-left: 5px;")
        main_layout.addWidget(self.lbl_status)

        self.tabs = QTabWidget()
        self.tabs.setStyleSheet("""
            QTabBar::tab { background: #161b22; color: #8b949e; padding: 10px 15px; border: 1px solid #30363d; border-top-left-radius: 4px; border-top-right-radius: 4px; }
            QTabBar::tab:selected { background: #0d1117; color: #58a6ff; font-weight: bold; border-bottom: 2px solid #58a6ff; }
            QTabWidget::pane { border: 1px solid #30363d; top: -1px; border-radius: 8px; border-top-left-radius: 0px; }
        """)
        
        # --- TAB 1: RESULTS TABLE ---
        tab1_container = QWidget()
        tab1_layout = QVBoxLayout(tab1_container)
        tab1_layout.setContentsMargins(0, 0, 0, 0)
        tab1_layout.setSpacing(5)
        
        self.local_filter_input = QLineEdit()
        self.local_filter_input.setPlaceholderText(" Quick Filter: Type to instantly filter current results by Name or Virtual Path...")
        self.local_filter_input.textChanged.connect(self.apply_local_filter)
        tab1_layout.addWidget(self.local_filter_input)
        
        self.table = QTableWidget(0, 9)
        self.table.setHorizontalHeaderLabels(["S.No.", "Name", "Virtual Path", "Type", "Size", "Modified", "Labels", "SHA-256", "HiddenMeta"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.setColumnWidth(0, 50); self.table.setColumnWidth(1, 280); self.table.setColumnWidth(2, 280)
        self.table.setColumnWidth(3, 120); self.table.setColumnWidth(4, 90); self.table.setColumnWidth(5, 140); self.table.setColumnWidth(6, 120)
        self.table.setColumnWidth(7, 200)
        
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSortingEnabled(True) 
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setShowGrid(False)
        self.table.verticalHeader().setVisible(False)
        self.table.setIconSize(QSize(20, 20))
        
        self.table.itemDoubleClicked.connect(self.handle_double_click)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)
        self.table.horizontalHeader().setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.horizontalHeader().customContextMenuRequested.connect(self.show_header_menu)
        
        tab1_layout.addWidget(self.table)
        self.tabs.addTab(tab1_container, "📋 Search Results")

        # --- TAB 2: VISUAL GRID MAP ---
        self.map_scroll = QScrollArea()
        self.map_scroll.setWidgetResizable(True)
        self.map_scroll.setFrameShape(QScrollArea.NoFrame)
        self.map_scroll.setAlignment(Qt.AlignTop | Qt.AlignHCenter)
        self.map_scroll.setContextMenuPolicy(Qt.CustomContextMenu)
        self.map_scroll.customContextMenuRequested.connect(self.show_map_context_menu)
        
        map_widget = QWidget()
        map_lay = QVBoxLayout(map_widget)
        map_lay.setContentsMargins(0, 0, 0, 0)
        
        if MATPLOTLIB_AVAILABLE:
            self.figure_map = Figure(dpi=100)
            self.canvas_map = FigureCanvas(self.figure_map)
            self.canvas_map.setContextMenuPolicy(Qt.CustomContextMenu)
            self.canvas_map.customContextMenuRequested.connect(self.show_map_context_menu)
            self.canvas_map.mpl_connect('button_press_event', self.on_map_click)
            self.canvas_map.wheelEvent = lambda event: self.map_scroll.wheelEvent(event)
            map_lay.addWidget(self.canvas_map, alignment=Qt.AlignTop | Qt.AlignHCenter)
        else:
            self.figure_map = None
            lbl = QLabel("Matplotlib is required.")
            map_lay.addWidget(lbl)
            
        self.map_scroll.setWidget(map_widget)
        self.tabs.addTab(self.map_scroll, "🗺️ Visual Grid Map")

        # --- TAB 3: ADVANCED SEARCH ANALYTICS ---
        chart_widget = QWidget()
        chart_lay = QVBoxLayout(chart_widget)
        chart_lay.setContentsMargins(10, 10, 10, 10)
        
        c_top = QHBoxLayout()
        self.combo_chart_metric = QComboBox()
        self.combo_chart_metric.addItems([
            "By Extension (Count)", "By Extension (Size MB)", 
            "By Category (Count)", "By Category (Size MB)", 
            "By Tags (Count)", "By Tags (Size MB)",
            "By Virtual Path (Count)", "By Virtual Path (Size MB)", 
            "By Year (Count)", "By Year (Size MB)"
        ])
        self.combo_chart_metric.currentTextChanged.connect(self.render_analytics)
        c_top.addWidget(QLabel("<b>Chart Metric:</b>"))
        c_top.addWidget(self.combo_chart_metric, stretch=1)
        chart_lay.addLayout(c_top)
        
        if MATPLOTLIB_AVAILABLE:
            self.figure_chart = Figure(dpi=100)
            self.canvas_chart = FigureCanvas(self.figure_chart)
            self.canvas_chart.setContextMenuPolicy(Qt.CustomContextMenu)
            self.canvas_chart.customContextMenuRequested.connect(self.show_chart_context_menu)
            self.canvas_chart.wheelEvent = lambda event: event.ignore()
            chart_lay.addWidget(self.canvas_chart, stretch=1)
        else:
            self.figure_chart = None
            
        self.tabs.addTab(chart_widget, "📊 Search Analytics")

        # --- TAB 4: SIMPLE DIRECTORY SCANNER ---
        telemetry_widget = QWidget()
        tele_lay = QVBoxLayout(telemetry_widget)
        tele_lay.setContentsMargins(10, 10, 10, 10)
        
        self.txt_tele_log = QPlainTextEdit()
        self.txt_tele_log.setReadOnly(True)
        self.txt_tele_log.setContextMenuPolicy(Qt.CustomContextMenu)
        self.txt_tele_log.customContextMenuRequested.connect(self.show_telemetry_context_menu)
        
        self.txt_tele_log.setStyleSheet("""
            QPlainTextEdit { background-color: #0d1117; color: #8b949e; font-family: Consolas, monospace; border: 1px solid #30363d; border-radius: 6px; padding: 10px; }
            QScrollBar:vertical { border: none; background: #0d1117; width: 8px; margin: 0px; }
            QScrollBar::handle:vertical { background: #30363d; border-radius: 4px; min-height: 20px; }
            QScrollBar::handle:vertical:hover { background: #58a6ff; }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { border: none; background: none; height: 0px; }
        """)
        
        tele_lay.addWidget(QLabel("Live Directory Path Tracing:"))
        tele_lay.addWidget(self.txt_tele_log)
        self.tabs.addTab(telemetry_widget, "📡 Live Scanner")

        main_layout.addWidget(self.tabs, stretch=1)

        self.apply_theme()
        
        saved_state = self.settings.value("table_state")
        if saved_state:
            self.table.horizontalHeader().restoreState(saved_state)
        else:
            self.table.setColumnHidden(0, True) 
            self.table.setColumnHidden(7, True) # Hide SHA-256 by default to save screen space
            self.table.setColumnHidden(8, True) # HiddenMeta moved to index 8

    def _build_matrix_cache(self, files):
        self.matrix_cache = defaultdict(lambda: defaultdict(list))
        self.max_hits = 1
        for r in files:
            if r[6]: continue 
            mod_str = str(r[5])
            if len(mod_str) >= 10:
                ym = mod_str[:7]
                try: day = int(mod_str[8:10])
                except: continue
                
                meta = {'ext': r[3], 'cat': r[10] if r[10] else "Others", 'tag': r[8], 'size': r[4] or 0} 
                self.matrix_cache[ym][day].append(meta)
                if len(self.matrix_cache[ym][day]) > self.max_hits: 
                    self.max_hits = len(self.matrix_cache[ym][day])

    def apply_local_filter(self, text):
        search_text = text.lower()
        self.table.setUpdatesEnabled(False) # Prevents UI stutter while filtering
        
        for row in range(self.table.rowCount()):
            item_name = self.table.item(row, 1)
            item_path = self.table.item(row, 2)
            
            if item_name and item_path:
                match = search_text in item_name.text().lower() or search_text in item_path.text().lower()
                self.table.setRowHidden(row, not match)
                
        self.table.setUpdatesEnabled(True)

    # --- Context Menus ---
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
        
        color_menu = menu.addMenu("🎨 Map Color Mode")
        color_modes = [
            "Intensity (Count)", "Intensity (Size)", 
            "By Category Color (Count)", "By Category Color (Size)",
            "By Extension Color (Count)", "By Extension Color (Size)",
            "By Tag Color (Count)", "By Tag Color (Size)"
        ]
        for mode in color_modes:
            act = color_menu.addAction(mode)
            act.setCheckable(True)
            act.setChecked(self.map_color_mode == mode)
            act.triggered.connect(lambda checked=False, m=mode: self.change_map_mode(m))
            
        menu.addSeparator()
        grad_menu = menu.addMenu("🌈 Gradient Color (Intensity Mode)")
        for grad in ["Fire", "Green", "Blue", "Yellow", "Excel (Green-Yellow-Red)", "Custom..."]:
            act = grad_menu.addAction(grad)
            act.setCheckable(True)
            act.setChecked(self.map_gradient == grad)
            act.triggered.connect(lambda checked=False, g=grad: self.change_map_gradient(g))
            
        menu.exec(QCursor.pos())
        
    def open_color_config(self):
        dlg = MapColorConfigDialog(self)
        if dlg.exec() == QDialog.Accepted:
            self.render_grid_map()
            
    def adjust_top_margin(self):
        val, ok = QInputDialog.getDouble(self, "Adjust Top Margin", "Enter top padding value (0 to obliterate space, 78 is default):", self.map_top_margin, 0, 200, 1)
        if ok:
            self.map_top_margin = val
            self.settings.setValue("map_top_margin", val)
            self.render_grid_map()
        
    def save_canvas_as_image(self, figure, prefix):
        if not figure: return
        path, _ = QFileDialog.getSaveFileName(self, "Save Image", f"VMan_{prefix}.png", "PNG Images (*.png);;JPEG Images (*.jpg)", options=QFileDialog.DontUseNativeDialog)
        if path:
            prog = QProgressDialog("Rendering High-Resolution Image...\nThis may take a moment for massive maps.", "Cancel", 0, 0, self)
            prog.setWindowTitle("Saving Image")
            prog.setWindowModality(Qt.WindowModal)
            prog.show()
            for _ in range(5): QApplication.processEvents() 
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
        self.render_grid_map()
        
    def change_map_sort(self, order):
        self.map_sort_order = order
        self.settings.setValue("map_sort_order", order)
        self.lbl_status.setText(f"✅ Sort order changed to: {order}")
        self.render_grid_map()
        
    def change_map_size(self, size):
        if size == "Custom...":
            val, ok = QInputDialog.getDouble(self, "Custom Tile Size", "Enter tile scale multiplier (0.1 to 5.0):", self.map_custom_scale, 0.1, 5.0, 2)
            if ok:
                self.map_custom_scale = val
                self.map_tile_size = "Custom..."
                self.settings.setValue("map_custom_scale", val)
                self.settings.setValue("map_tile_size", "Custom...")
                self.lbl_status.setText(f"✅ Map tile size scaled to: {val}x")
                self.render_grid_map()
        else:
            self.map_tile_size = size
            self.settings.setValue("map_tile_size", size)
            self.lbl_status.setText(f"✅ Map tile size changed to: {size}")
            self.render_grid_map()
        
    def change_map_mode(self, mode):
        self.map_color_mode = mode
        self.settings.setValue("map_color_mode", mode)
        self.lbl_status.setText(f"✅ Map color mode changed to: {mode}")
        self.render_grid_map()
        
    def change_map_gradient(self, grad):
        if grad == "Custom...":
            QMessageBox.information(self, "Custom Gradient", "Please select Base and Peak colors in the Customize Colors menu.")
            self.open_color_config()
        self.map_gradient = grad
        self.settings.setValue("map_gradient", grad)
        if "Intensity" in self.map_color_mode:
            self.lbl_status.setText(f"✅ Map intensity gradient changed to: {grad}")
            self.render_grid_map()
            
    def toggle_fast_mode(self, checked):
        self.fast_mode = checked
        self.settings.setValue("fast_mode", checked)
        if checked: self.lbl_status.setText("⚡ Fast Mode ON: Visual Maps & Analytics will be skipped for raw speed.")
        else: self.lbl_status.setText("✅ Fast Mode OFF: Visual Maps & Analytics restored.")

    def trigger_search(self):
        if not self.active_db or not os.path.exists(self.active_db): return QMessageBox.warning(self, "No Database", "No active database found.")
        self.is_searching = True
        self._abort_render = False
        self.set_button_style("running")
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        self.local_filter_input.clear() # --- ADD THIS LINE ---
        self.progress.setVisible(True)
        self.progress.setValue(0)
        self.lbl_status.setText("Searching...")
        self.txt_tele_log.clear()

        unit_mult = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3}
        mult = unit_mult.get(self.combo_sz_unit.currentText(), 1024**2)

        params = {
            'name': self.txt_name.text().strip(), 'path': self.txt_path.text().strip(),
            'match': self.combo_match.currentText(), 'look_for': self.combo_look_for.currentText(),
            'exts': [e.strip().lower() for e in self.txt_ext.text().split(',') if e.strip()],
            'ex_exts': [e.strip().lower() for e in self.txt_exclude_ext.text().split(',') if e.strip()],
            'ex_names': [n.strip().lower() for n in self.txt_exclude_name.text().split(',') if n.strip()],
            'category': getattr(self, 'combo_category', QComboBox()).currentText() if hasattr(self, 'combo_category') else "All",
            'sz_min': self.spin_size_min.value() * mult, 'sz_max': self.spin_size_max.value() * mult,
            'use_date': self.chk_use_date.isChecked(), 'date_start': self.date_start.date(), 'date_end': self.date_end.date(),
            'verbose': self.verbose_telemetry,
            'db': self.active_db
        }
        self.worker = SearchWorker(params)
        self.worker.progress_update.connect(self.progress.setValue)
        self.worker.results_ready.connect(self.store_and_populate)
        self.worker.finished_search.connect(self.search_complete)
        self.worker.telemetry_update.connect(self.update_telemetry)
        self.worker.start()

    def store_and_populate(self, results):
        self.progress.setVisible(False)
        self.current_results = results
        total = len(results)
        start_idx, end_idx = 0, total
        
        if total > 1500:
            dlg = RowLimitDialogSearch(total, self)
            if dlg.exec() == QDialog.Accepted:
                start_idx, end_idx = dlg.get_values()
            else:
                start_idx, end_idx = 0, 0 
                
        if not self.fast_mode:
            self._build_matrix_cache(results)
            self.render_grid_map()
            self.render_analytics()
        else:
            if self.figure_map:
                self.figure_map.clear()
                self.figure_map.add_subplot(111).text(0.5, 0.5, "⚡ Fast Mode is ON\nMap rendering is disabled for maximum speed.", ha='center', va='center', color='gray')
                self.canvas_map.draw()
            if self.figure_chart:
                self.figure_chart.clear()
                self.figure_chart.add_subplot(111).text(0.5, 0.5, "⚡ Fast Mode is ON\nAnalytics rendering is disabled for maximum speed.", ha='center', va='center', color='gray')
                self.canvas_chart.draw()
                
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
        self.settings.setValue("map_color_mode", self.map_color_mode)
        self.settings.setValue("map_gradient", self.map_gradient)
        self.settings.setValue("map_sort_order", self.map_sort_order)
        self.settings.setValue("map_tile_size", self.map_tile_size)
        self.settings.setValue("map_custom_scale", self.map_custom_scale)
        self.settings.setValue("map_top_margin", self.map_top_margin)
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
            
            QDateEdit, QSpinBox, QDoubleSpinBox, QComboBox {{ 
                background: {input_bg}; color: {input_text}; border: 1px solid {brd_col}; padding: 4px; border-radius: 4px;
            }}
        """
        self.card_scope.setStyleSheet(card_css)
        self.card_type.setStyleSheet(card_css)
        self.card_metrics.setStyleSheet(card_css)

        # --- NEW: MAKE TABS, PROGRESS BAR, AND TELEMETRY LOG 100% LIGHT-MODE COMPATIBLE ---
        self.tabs.setStyleSheet(f"""
            QTabBar::tab {{ background: {bg_col}; color: {lbl_col}; padding: 10px 15px; border: 1px solid {brd_col}; border-top-left-radius: 4px; border-top-right-radius: 4px; }}
            QTabBar::tab:selected {{ background: {input_bg}; color: #58a6ff; font-weight: bold; border-bottom: 2px solid #58a6ff; }}
            QTabWidget::pane {{ border: 1px solid {brd_col}; top: -1px; border-radius: 8px; border-top-left-radius: 0px; }}
        """)

        self.progress.setStyleSheet(f"""
            QProgressBar {{ border: 1px solid {brd_col}; border-radius: 4px; background-color: {input_bg}; color: {input_text}; font-weight: bold; }}
            QProgressBar::chunk {{ background-color: #1f6feb; border-radius: 3px; }}
        """)

        self.txt_tele_log.setStyleSheet(f"""
            QPlainTextEdit {{ background-color: {input_bg}; color: {input_text}; font-family: Consolas, monospace; border: 1px solid {brd_col}; border-radius: 6px; padding: 10px; }}
            QScrollBar:vertical {{ border: none; background: {input_bg}; width: 8px; margin: 0px; }}
            QScrollBar::handle:vertical {{ background: {brd_col}; border-radius: 4px; min-height: 20px; }}
            QScrollBar::handle:vertical:hover {{ background: #58a6ff; }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ border: none; background: none; height: 0px; }}
        """)
        self.local_filter_input.setStyleSheet(f"background: {input_bg}; color: {input_text}; border: 1px solid {brd_col}; padding: 6px; border-radius: 4px; font-size: 13px;")
        # ----------------------------------------------------------------------------------
        
        glass_btn = f"""
            QPushButton {{ 
                background-color: {'rgba(255, 255, 255, 0.05)' if is_dark else 'rgba(0, 0, 0, 0.03)'}; 
                border: 1px solid {'rgba(255, 255, 255, 0.15)' if is_dark else 'rgba(0, 0, 0, 0.1)'}; 
                border-radius: 6px; 
                padding: 10px; font-size: 13px; font-weight: bold; 
                color: {lbl_col}; 
            }}
            QPushButton:hover {{ background-color: {'rgba(255, 255, 255, 0.1)' if is_dark else 'rgba(0, 0, 0, 0.08)'}; }}
            QPushButton:checked {{ background-color: {'rgba(88, 166, 255, 0.15)' if is_dark else 'rgba(9, 105, 218, 0.1)'}; border-color: {'#58a6ff' if is_dark else '#0969da'}; }}
        """
        for btn in [self.btn_scope, self.btn_type, self.btn_metrics]:
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
            self.btn_search.setText("EXECUTE")
            self.btn_search.setStyleSheet(f"""
                QPushButton {{ background-color: {btn_bg}; color: {btn_fg}; border: 2px solid {btn_border}; border-radius: 6px; font-weight: 900; font-size: 14px; padding: 10px; }} 
                QPushButton:hover {{ background-color: {btn_hover}; border-color: {btn_hover_border}; }}
            """)
        elif state == "running":
            self.btn_search.setText("🛑 ABORT")
            self.btn_search.setStyleSheet("""
                QPushButton { background-color: #4a0000; color: #ff7b72; border: 2px solid #f85149; border-radius: 6px; font-weight: 900; font-size: 14px; padding: 10px; } 
                QPushButton:hover { background-color: #6a0000; }
            """)
            
    def update_filter_visibility(self):
        self.card_scope.setVisible(self.btn_scope.isChecked())
        self.card_type.setVisible(self.btn_type.isChecked())
        self.card_metrics.setVisible(self.btn_metrics.isChecked())
        self.filters_container.setVisible(self.btn_scope.isChecked() or self.btn_type.isChecked() or self.btn_metrics.isChecked())

    def handle_button_action(self):
        if self.is_searching:
            self._abort_render = True
            self.worker.abort()
            self.lbl_status.setText("Aborting search...")
            self.btn_search.setEnabled(False)
        else: self.trigger_search()

    def get_icon(self, is_folder, name, ext, db_id):
        if not self.show_icons: return QIcon()
        if is_folder: return self.style().standardIcon(QStyle.SP_DirIcon)
        
        if db_id != -1:
            cache_key = f"thumb_{db_id}"
            if cache_key in self.icon_cache: return self.icon_cache[cache_key]
            thumb_path = self.THUMBS_DIR / f"{db_id}.png"
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

    def populate_table(self, display_results, total_matches):
        # FIX: Forcefully disable sorting BEFORE adding rows to prevent data scrambling!
        self.table.setSortingEnabled(False) 
        self.table.setUpdatesEnabled(False)
        self.table.setRowCount(0)
        
        if not display_results:
            self.table.setUpdatesEnabled(True)
            self.table.setSortingEnabled(True)
            self.lbl_status.setText(f"Search complete. 0 rows loaded into Table. ({total_matches} available in Maps).")
            return
            
        display_total = len(display_results)
        
        self.progress.setVisible(True)
        self.progress.setMaximum(display_total)
        self.progress.setValue(0)
        QApplication.processEvents()
        
        
        for i, (db_id, name, p_path, ext, size, mod, is_fldr, real_path, tags, color_tag, cat_val, sha256_val) in enumerate(display_results):
            if self._abort_render: break
            
            self.table.insertRow(i)
            
            sno_item = NumericTableItem(str(i + 1)); sno_item.setData(Qt.UserRole, i + 1)
            self.table.setItem(i, 0, sno_item)
            
            name_item = QTableWidgetItem(name)
            name_item.setIcon(self.get_icon(is_fldr, name, ext, db_id))
            if color_tag and color_tag in self.VMAN_COLORS: 
                name_item.setBackground(QBrush(self.VMAN_COLORS[color_tag])) 
                name_item.setForeground(QColor("#ffffff")) 
            self.table.setItem(i, 1, name_item)
            
            self.table.setItem(i, 2, QTableWidgetItem(p_path))
            
            type_str = "Folder" if is_fldr else (ext[1:].upper() + " File" if ext.startswith(".") else (ext.upper() + " File" if ext else "File"))
            self.table.setItem(i, 3, QTableWidgetItem(type_str))
            
            sz_str = "--" if is_fldr else self.human_size(size)
            sz_item = NumericTableItem(sz_str); sz_item.setData(Qt.UserRole, size if not is_fldr else -1)
            self.table.setItem(i, 4, sz_item)
            
            self.table.setItem(i, 5, QTableWidgetItem(str(mod)))
            self.table.setItem(i, 6, QTableWidgetItem(str(tags) if tags else ""))
            
            # --- NEW: SHA-256 Column (Index 7) ---
            sha_item = QTableWidgetItem(str(sha256_val) if sha256_val else "--")
            sha_item.setForeground(QColor("#8b949e")) # Muted color for hashes
            self.table.setItem(i, 7, sha_item)
            
            # Moved HiddenMeta to Index 8
            meta_item = QTableWidgetItem("")
            meta_item.setData(Qt.UserRole, {'id': db_id, 'is_fldr': is_fldr, 'real_path': real_path, 'tags': tags, 'size': size, 'mod': mod})
            self.table.setItem(i, 8, meta_item)
            
            if i % 100 == 0:
                self.progress.setValue(i)
                QApplication.processEvents()
                
        self.progress.setValue(display_total)
        self.progress.setVisible(False)
        self.table.setUpdatesEnabled(True)
        # Re-enable sorting only AFTER all rows and properties are safely mapped
        self.table.setSortingEnabled(True)
        
        self.lbl_status.setText(f"Search complete. Displaying {len(display_results)} out of {total_matches} total matches.")
        
    def search_complete(self, was_aborted):
        self.is_searching = False
        self.btn_search.setEnabled(True)
        self.set_button_style("idle")
        if was_aborted:
            self.progress.setVisible(False)
            self.lbl_status.setText(f"Search aborted. Displaying partial results.")

    def show_header_menu(self, pos):
        menu = QMenu(self)
        for col in range(self.table.columnCount()):
            col_name = self.table.horizontalHeaderItem(col).text()
            
            # (Removed the line that was hiding "HiddenMeta" from the menu)
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
        meta_item = self.table.item(item.row(), 8) # Changed from 7 to 8
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
        
        i_name = self.table.item(row, 1)
        i_vpath = self.table.item(row, 2)
        i_meta = self.table.item(row, 8) # Changed from 7 to 8
        
        if not i_name or not i_vpath or not i_meta: return
        
        name = i_name.text()
        v_path = i_vpath.text()
        meta = i_meta.data(Qt.UserRole)
        
        if not meta: return

        selected_rows = self.table.selectionModel().selectedRows()
        
        menu = QMenu(self)
        act_open_os = menu.addAction("🚀 Open Native OS Default")
        act_show_os = menu.addAction("📂 Show in OS Explorer")
        
        act_vman = None
        if len(selected_rows) > 1:
            act_vman = menu.addAction(f"🎞 Open {len(selected_rows)} Highlighted in VMan Viewer")
        elif not meta['is_fldr']:
            act_vman = menu.addAction("🎞 Open in VMan Viewer")
            
        menu.addSeparator()
        
        color_menu = menu.addMenu("🎨 Set Color Tag")
        for color in ["None", "Red", "Orange", "Gold", "Green", "Cyan", "Blue", "Purple", "Pink"]:
            act = color_menu.addAction(color)
            act.triggered.connect(lambda checked=False, c=color, r=row, mid=meta['id'], is_f=meta['is_fldr'], nm=name: self.set_color_tag(r, mid, c, is_f, nm))
            
        menu.addSeparator()
        
        act_copy_names = menu.addAction("📋 Copy Selected Names")
        act_csv = menu.addAction("📥 Export Selected to CSV")
        menu.addSeparator()
        
        act_nav = menu.addAction("🎯 Locate in Virtual File Manager")
        act_copy = menu.addAction("📋 Copy Virtual Path")
        menu.addSeparator()
        act_tags = menu.addAction("🏷️ Edit Text Tags")
        act_props = menu.addAction("ℹ️ Properties")
        
        menu.addSeparator()
        act_fast = menu.addAction("⚡ Fast Mode (Disable Visual Maps)")
        act_fast.setCheckable(True)
        act_fast.setChecked(self.fast_mode)
        act_fast.toggled.connect(self.toggle_fast_mode)
        
        action = menu.exec(self.table.viewport().mapToGlobal(pos))
        
        if action == act_open_os: self.open_in_os(meta['real_path'])
        elif action == act_show_os: self.show_in_os_explorer(meta['real_path'])
        elif action == act_vman:
            playlist = []
            for idx in selected_rows:
                m = self.table.item(idx.row(), 8).data(Qt.UserRole) # Changed from 7 to 8
                if not m or not m.get('is_fldr', True) and m.get('real_path') and os.path.exists(m['real_path']):
                    n = self.table.item(idx.row(), 1).text()
                    e = os.path.splitext(m['real_path'])[1].lower()
                    playlist.append({'path': m['real_path'], 'name': n, 'ext': e})
            if playlist:
                try:
                    from main import vmanViewer
                    parent = self.main_app if self.main_app else self
                    if not hasattr(parent, 'active_viewers'): parent.active_viewers = []
                    viewer = vmanViewer(playlist, 0, parent)
                    parent.active_viewers.append(viewer)
                    viewer.show()
                except Exception as e: print(e)
            else:
                QMessageBox.warning(self, "Viewer", "No valid physical files selected.")
        elif action == act_copy_names:
            names = [self.table.item(idx.row(), 1).text() for idx in selected_rows if self.table.item(idx.row(), 1)]
            QApplication.clipboard().setText("\n".join(names))
            QMessageBox.information(self, "Copied", f"Copied {len(names)} names to clipboard.")
        elif action == act_csv:
            self.export_table_to_csv(selected_rows)
        elif action == act_nav: self.navigate_to_item(row)
        elif action == act_copy: QApplication.clipboard().setText(f"{v_path}{name}/" if meta['is_fldr'] else f"{v_path}{name}")
        elif action == act_tags: self.edit_tags(row, name, meta)
        elif action == act_props: self.show_properties(name, v_path, meta)
        
    def export_table_to_csv(self, selected_rows):
        path, _ = QFileDialog.getSaveFileName(self, "Export to CSV", "VMan_Search_Results.csv", "CSV Files (*.csv)", options=QFileDialog.DontUseNativeDialog)
        if not path: return
        try:
            with open(path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                headers = [self.table.horizontalHeaderItem(c).text() for c in range(1, 7)]
                writer.writerow(headers)
                for idx in selected_rows:
                    r = idx.row()
                    writer.writerow([self.table.item(r, c).text() for c in range(1, 7)])
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
                self.table.item(row, 7).setData(Qt.UserRole, meta)
                self.table.item(row, 6).setText(meta['tags'])
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

    # --- MAP CLICK INTERACTION ---
    def on_map_click(self, event):
        if not hasattr(self, 'figure_map') or not self.figure_map or event.inaxes != self.figure_map.axes[0] or event.xdata is None or event.ydata is None: return
        if not hasattr(self, 'map_coords_dict') or not self.map_coords_dict: return
        
        # FIX: The internal Matplotlib data coordinates are ALWAYS 1.0 base, 
        # regardless of UI scaling! We must not multiply by scale here.
        box_w, box_h, gap = 1.0, 1.0, 0.2
        
        col = int(event.xdata / (box_w + gap))
        row = int(event.ydata / (box_h + gap))
        
        if (col, row) in self.map_coords_dict:
            target_ym, target_day = self.map_coords_dict[(col, row)]
        else: return
            
        target_date_prefix = f"{target_ym}-{target_day:02d}"
        filtered_results = [r for r in self.current_results if str(r[5]).startswith(target_date_prefix)]
        
        if filtered_results:
            self._abort_render = False 
            self.populate_table(filtered_results, len(self.current_results))
            self.tabs.setCurrentIndex(0) 
            self.lbl_status.setText(f"Viewing {len(filtered_results)} files modified exactly on {target_date_prefix}.")
        else:
            self.lbl_status.setText(f"No files found exactly on {target_date_prefix}.")
            
    # --- PERFECT HIGH-SPEED ZERO-MARGIN RENDERING ---
    def get_map_value(self, items, mode):
        is_size = "(Size)" in mode
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
                t_str = i['tag']
                if t_str:
                    for t in t_str.split(','):
                        if t.strip(): 
                            counts[t.strip()] += (i['size'] if is_size else 1)
                            has_tag = True
            if not has_tag: return total_val, "Untagged"
            return total_val, max(counts, key=counts.get)
            
        return total_val, None
        
    def get_intensity_hex(self, val, max_val, grad, saved_colors):
        if max_val <= 0: return "#21262d"
        intensity = math.pow(float(val) / float(max_val), 0.5) 
        intensity = max(0.15, min(1.0, intensity)) 
        
        if grad == "Excel (Green-Yellow-Red)":
            if intensity <= 0.5:
                pct = intensity * 2.0
                r = int(99 + (255 - 99) * pct)
                g = int(190 + (235 - 190) * pct)
                b = int(123 + (132 - 123) * pct)
            else:
                pct = (intensity - 0.5) * 2.0
                r = int(255 + (248 - 255) * pct)
                g = int(235 + (105 - 235) * pct)
                b = int(132 + (107 - 132) * pct)
        elif grad == "Fire":
            r, g, b = int(40 + 215*intensity), int(20 + 130*(intensity**2)), int(20)
        elif grad == "Green":
            r, g, b = int(20), int(50 + 205*intensity), int(40)
        elif grad == "Blue":
            r, g, b = int(20), int(60 + 150*(intensity**2)), int(40 + 215*intensity)
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
            
        return f"#{min(255,max(0,r)):02x}{min(255,max(0,g)):02x}{min(255,max(0,b)):02x}"

    def render_grid_map(self):
        if not hasattr(self, 'figure_map') or not self.figure_map: return
        self.figure_map.clear()
        ax = self.figure_map.add_subplot(111)
        
        is_dark = True
        if self.main_app and hasattr(self.main_app, 'theme_combo'):
            is_dark = self.main_app.theme_combo.currentText() == "Dark"
            
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
        cat_colors = {"Images": "#a371f7", "Videos": "#f85149", "Audio": "#ff7b72", "Documents": "#d2a8ff", "Code": "#79c0ff", "Archives": "#e3b341", "Others": "#8b949e"}

        scale = 1.0
        if self.map_tile_size == "Small (0.7x)": scale = 0.7
        elif self.map_tile_size == "Medium (1.0x)": scale = 1.0
        elif self.map_tile_size == "Large (1.2x)": scale = 1.2
        elif self.map_tile_size == "Custom...": scale = getattr(self, 'map_custom_scale', 1.2)
        
        box_w, box_h, gap = 1.0, 1.0, 0.2 
        pixel_scale = 16 * scale 
        
        patches, facecolors = [], []
        self.map_coords_dict = {} 
        
        is_reverse_sort = (self.map_sort_order == "Top to Bottom (Newest First)")
        matrix = self.matrix_cache
        
        max_val = 0.1
        for ym, days in matrix.items():
            for d, items in days.items():
                val, _ = self.get_map_value(items, self.map_color_mode)
                if val > max_val: max_val = val

        ax.set_aspect('equal')

        # -----------------------------------------------------
        # MODE 1: COMPACT MATRIX (31 Days)
        # -----------------------------------------------------
        if self.map_layout_mode == "Compact Matrix (31 Days)":
            ym_keys = sorted(matrix.keys(), reverse=is_reverse_sort)
            
            for row_idx, ym in enumerate(ym_keys):
                for day in range(1, 32):
                    items = matrix[ym].get(day, [])
                    val, dom = self.get_map_value(items, self.map_color_mode)
                    
                    x_pos = (day - 1) * (box_w + gap)
                    y_pos = row_idx * (box_h + gap)
                    
                    self.map_coords_dict[(day-1, row_idx)] = (ym, day)
                    
                    if val == 0: c_hex = "#21262d" if is_dark else "#ebecf0"
                    else:
                        if "Category" in self.map_color_mode: c_hex = saved_colors.get(f"Category_{dom}", cat_colors.get(dom, "#8b949e"))
                        elif "Extension" in self.map_color_mode: c_hex = saved_colors.get(f"Extension_{dom}", f"#{min(255, max(100, int(hashlib.md5(dom.encode()).hexdigest()[:6], 16) & 0xFFFFFF)):06x}")
                        elif "Tag" in self.map_color_mode: 
                            c_hex = "#30363d" if dom == "Untagged" else saved_colors.get(f"Tag_{dom}", f"#{min(255, max(100, int(hashlib.md5(dom.encode()).hexdigest()[:6], 16) & 0xFFFFFF)):06x}")
                        else: c_hex = self.get_intensity_hex(val, max_val, getattr(self, 'map_gradient', 'Fire'), saved_colors)
                            
                    rect = mpatches.Rectangle((x_pos, y_pos), box_w, box_h)
                    patches.append(rect)
                    facecolors.append(c_hex)
                    
            max_x = 31 * (box_w + gap)
            max_y = len(ym_keys) * (box_h + gap)
            ax.set_xlim(-gap, max_x)
            ax.set_ylim(max_y, -gap)
            
            x_ticks = [(d - 1) * (box_w + gap) + (box_w / 2) for d in range(1, 32)]
            ax.set_xticks(x_ticks)
            ax.set_xticklabels([str(d) for d in range(1, 32)])
            ax.xaxis.tick_top()
            
            y_ticks = [r * (box_h + gap) + (box_h / 2) for r in range(len(ym_keys))]
            ax.set_yticks(y_ticks)
            ax.set_yticklabels(ym_keys, fontweight="bold")
            
            top_pad = getattr(self, 'map_top_margin', 40.0) / max(100, max_y * pixel_scale)
            self.figure_map.subplots_adjust(top=1.0 - top_pad, bottom=0.01, left=0.10, right=0.98)
            self.canvas_map.setFixedSize(int(max_x * pixel_scale + 100), int(max_y * pixel_scale + 80))

        # -----------------------------------------------------
        # MODE 2: SEGMENTED YEARS (31 Days)
        # -----------------------------------------------------
        elif self.map_layout_mode == "Segmented Years (31 Days)":
            ym_keys = sorted(matrix.keys(), reverse=is_reverse_sort)
            current_row = 0
            y_ticks_pos = []
            y_ticks_labels = []
            last_year = None
            
            for ym in ym_keys:
                y, m = ym.split('-')
                
                if last_year and last_year != y:
                    year_rect = mpatches.Rectangle((-gap, (current_row-1) * (box_h + gap) + box_h), 31 * (box_w + gap), 0, edgecolor="#b8860b", linewidth=2)
                    ax.add_patch(year_rect)
                    current_row += 1 
                
                for day in range(1, 32):
                    items = matrix[ym].get(day, [])
                    val, dom = self.get_map_value(items, self.map_color_mode)
                    
                    x_pos = (day - 1) * (box_w + gap)
                    y_pos = current_row * (box_h + gap)
                    self.map_coords_dict[(day-1, current_row)] = (ym, day)
                    
                    if val == 0: c_hex = "#21262d" if is_dark else "#ebecf0"
                    else:
                        if "Category" in self.map_color_mode: c_hex = saved_colors.get(f"Category_{dom}", cat_colors.get(dom, "#8b949e"))
                        elif "Extension" in self.map_color_mode: c_hex = saved_colors.get(f"Extension_{dom}", f"#{min(255, max(100, int(hashlib.md5(dom.encode()).hexdigest()[:6], 16) & 0xFFFFFF)):06x}")
                        elif "Tag" in self.map_color_mode: 
                            c_hex = "#30363d" if dom == "Untagged" else saved_colors.get(f"Tag_{dom}", f"#{min(255, max(100, int(hashlib.md5(dom.encode()).hexdigest()[:6], 16) & 0xFFFFFF)):06x}")
                        else: c_hex = self.get_intensity_hex(val, max_val, getattr(self, 'map_gradient', 'Fire'), saved_colors)
                            
                    rect = mpatches.Rectangle((x_pos, y_pos), box_w, box_h)
                    patches.append(rect)
                    facecolors.append(c_hex)
                    
                y_ticks_pos.append(current_row * (box_h + gap) + (box_h / 2))
                y_ticks_labels.append(f"{y} {calendar.month_abbr[int(m)]}")
                current_row += 1
                last_year = y
                
            max_x = 31 * (box_w + gap)
            max_y = current_row * (box_h + gap)
            ax.set_xlim(-gap, max_x)
            ax.set_ylim(max_y, -gap) 
            
            x_ticks = [(d - 1) * (box_w + gap) + (box_w / 2) for d in range(1, 32)]
            ax.set_xticks(x_ticks)
            ax.set_xticklabels([str(d) for d in range(1, 32)])
            ax.xaxis.tick_top()
            
            ax.set_yticks(y_ticks_pos)
            ax.set_yticklabels(y_ticks_labels, fontweight="bold")
            
            top_pad = getattr(self, 'map_top_margin', 40.0) / max(100, max_y * pixel_scale)
            self.figure_map.subplots_adjust(top=1.0 - top_pad, bottom=0.01, left=0.12, right=0.98)
            self.canvas_map.setFixedSize(int(max_x * pixel_scale + 120), int(max_y * pixel_scale + 60))

        # -----------------------------------------------------
        # MODE 3: TRUE CALENDAR (Dynamic Widescreen/Standard)
        # -----------------------------------------------------
        elif "True Calendar" in self.map_layout_mode:
            years = set(int(ym[:4]) for ym in matrix.keys())
            if not years: years.add(datetime.datetime.now().year)
            years = sorted(list(years), reverse=is_reverse_sort)
            
            cols_per_row = 4 if "Widescreen" in self.map_layout_mode else 3
            # --- FIX: Subtract 2 unused trailing columns to perfectly align right edge ---
            total_cols_width = cols_per_row * 9 - 2
            
            current_row = 0
            
            for y in years:
                current_row += 3 
                # Changed from "- 2.5" to "- (0.8 * scale)" to pull the Year text perfectly downwards
                ax.text((total_cols_width / 2) * (box_w + gap), current_row * (box_h + gap) - (0.8 * scale), str(y), ha='center', va='bottom', color='#8a6306', fontweight='bold', fontsize=int(18 * scale))
                year_start_row = current_row
                
                for m_idx in range(1, 13):
                    grid_col = (m_idx - 1) % cols_per_row
                    if m_idx > 1 and grid_col == 0: current_row += 9 
                    
                    x_offset = grid_col * 9 
                    
                    ax.text(x_offset * (box_w + gap) + 3.5 * (box_w + gap), current_row * (box_h + gap) - 0.2, calendar.month_abbr[m_idx], ha='center', va='bottom', color=txt_c, fontweight='bold', fontsize=int(10 * scale))
                    day_labels = ["M", "T", "W", "T", "F", "S", "S"]
                    for i, d in enumerate(day_labels):
                        ax.text((x_offset + i) * (box_w + gap) + box_w/2, (current_row + 0.8) * (box_h + gap), d, ha='center', va='bottom', color='#8b949e', fontsize=int(8 * scale))
                        
                    ym = f"{y}-{m_idx:02d}"
                    _, days_in_month = calendar.monthrange(y, m_idx)
                    start_idx = calendar.weekday(y, m_idx, 1) 
                    
                    for day in range(1, days_in_month + 1):
                        col = x_offset + ((start_idx + day - 1) % 7)
                        r_idx = current_row + 1 + ((start_idx + day - 1) // 7)
                        
                        x_pos = col * (box_w + gap)
                        y_pos = r_idx * (box_h + gap)
                        
                        self.map_coords_dict[(col, r_idx)] = (ym, day)
                        
                        items = matrix[ym].get(day, [])
                        val, dom = self.get_map_value(items, self.map_color_mode)
                        
                        if val == 0: c_hex = "#21262d" if is_dark else "#ebecf0"
                        else:
                            if "Category" in self.map_color_mode: c_hex = saved_colors.get(f"Category_{dom}", cat_colors.get(dom, "#8b949e"))
                            elif "Extension" in self.map_color_mode: c_hex = saved_colors.get(f"Extension_{dom}", f"#{min(255, max(100, int(hashlib.md5(dom.encode()).hexdigest()[:6], 16) & 0xFFFFFF)):06x}")
                            elif "Tag" in self.map_color_mode: 
                                c_hex = "#30363d" if dom == "Untagged" else saved_colors.get(f"Tag_{dom}", f"#{min(255, max(100, int(hashlib.md5(dom.encode()).hexdigest()[:6], 16) & 0xFFFFFF)):06x}")
                            else: c_hex = self.get_intensity_hex(val, max_val, getattr(self, 'map_gradient', 'Fire'), saved_colors)
                                
                        rect = mpatches.Rectangle((x_pos, y_pos), box_w, box_h)
                        patches.append(rect)
                        facecolors.append(c_hex)
                        
                        font_color = "black" if (not is_dark and val==0) else "white"
                        ax.text(x_pos + box_w/2, y_pos + box_h/2, str(day), ha='center', va='center', color=font_color, fontsize=int(7 * scale))

                current_row += 8 
                y_box_h = (current_row - year_start_row - 0.5) * (box_h + gap)
                
                # --- FIX: Balanced Border width exactly symmetrically ---
                year_rect = mpatches.Rectangle((-gap, year_start_row * (box_h + gap) - gap*2), total_cols_width * (box_w + gap) + gap, y_box_h, fill=False, edgecolor="#b8860b", linewidth=2)
                ax.add_patch(year_rect)
                
            max_x = total_cols_width * (box_w + gap)
            max_y = current_row * (box_h + gap)
            
            # --- FIX: Added extra 'gap' breathing room outside the X limits so border isn't clipped ---
            ax.set_xlim(-gap*2, max_x + gap)
            ax.set_ylim(max_y, -gap*3)
            ax.set_xticks([]); ax.set_yticks([])
            
            top_pad = getattr(self, 'map_top_margin', 40.0) / max(100, max_y * pixel_scale)
            self.figure_map.subplots_adjust(top=1.0 - top_pad, bottom=0.01, left=0.02, right=0.98)
            self.canvas_map.setFixedSize(int(max_x * pixel_scale + 50), int(max_y * pixel_scale + 60))

        # -----------------------------------------------------
        # MODE 4: GITHUB CONTRIBUTION (52 Weeks Horizontal)
        # -----------------------------------------------------
        elif self.map_layout_mode == "GitHub Contribution (52 Weeks)":
            years = set(int(ym[:4]) for ym in matrix.keys())
            if not years: years.add(datetime.datetime.now().year)
            years = sorted(list(years), reverse=is_reverse_sort)
            
            current_row = 0
            y_ticks_pos = []
            y_ticks_labels = []
            
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
                    
                    ym = curr_date.strftime("%Y-%m")
                    day = curr_date.day
                    
                    x_pos = week * (box_w + gap)
                    y_pos = (current_row + wday) * (box_h + gap)
                    
                    self.map_coords_dict[(week, current_row + wday)] = (ym, day)
                    
                    items = matrix[ym].get(day, [])
                    val, dom = self.get_map_value(items, self.map_color_mode)
                    
                    if val == 0: c_hex = "#21262d" if is_dark else "#ebecf0"
                    else:
                        if "Category" in self.map_color_mode: c_hex = saved_colors.get(f"Category_{dom}", cat_colors.get(dom, "#8b949e"))
                        elif "Extension" in self.map_color_mode: c_hex = saved_colors.get(f"Extension_{dom}", f"#{min(255, max(100, int(hashlib.md5(dom.encode()).hexdigest()[:6], 16) & 0xFFFFFF)):06x}")
                        elif "Tag" in self.map_color_mode: 
                            c_hex = "#30363d" if dom == "Untagged" else saved_colors.get(f"Tag_{dom}", f"#{min(255, max(100, int(hashlib.md5(dom.encode()).hexdigest()[:6], 16) & 0xFFFFFF)):06x}")
                        else: c_hex = self.get_intensity_hex(val, max_val, getattr(self, 'map_gradient', 'Fire'), saved_colors)
                            
                    rect = mpatches.Rectangle((x_pos, y_pos), box_w, box_h)
                    patches.append(rect)
                    facecolors.append(c_hex)
                    
                current_row += 8 
                
            max_x = 54 * (box_w + gap)
            max_y = current_row * (box_h + gap)
            ax.set_xlim(-gap, max_x)
            ax.set_ylim(max_y, -gap)
            ax.set_xticks([])
            ax.set_yticks(y_ticks_pos)
            ax.set_yticklabels(y_ticks_labels, fontweight="bold")
            
            top_pad = getattr(self, 'map_top_margin', 40.0) / max(100, max_y * pixel_scale)
            self.figure_map.subplots_adjust(top=1.0 - top_pad, bottom=0.01, left=0.08, right=0.98)
            self.canvas_map.setFixedSize(int(max_x * pixel_scale + 100), int(max_y * pixel_scale + 60))

        collection = PatchCollection(patches, facecolors=facecolors, edgecolors="none")
        ax.add_collection(collection)
        
        ax.set_title(f"Visual Grid Map [{self.map_layout_mode}] - ({self.map_color_mode})\nClick any tile to view files", color=txt_c, fontweight='bold', pad=15)
        
        for spine in ['top', 'right', 'bottom', 'left']: ax.spines[spine].set_visible(False)
        ax.tick_params(axis='both', which='major', length=0)
        
        self.canvas_map.draw()
        
    def render_analytics(self):
        if not hasattr(self, 'figure_chart') or not self.figure_chart: return
        self.figure_chart.clear()
        ax = self.figure_chart.add_subplot(111)
        
        is_dark = True
        if self.main_app and hasattr(self.main_app, 'theme_combo'):
            is_dark = self.main_app.theme_combo.currentText() == "Dark"
            
        bg_c, txt_c = ('#0d1117', '#c9d1d9') if is_dark else ('#ffffff', '#24292f')
        grid_c = '#30363d' if is_dark else '#e1e4e8'
        self.figure_chart.patch.set_facecolor(bg_c)
        ax.set_facecolor(bg_c)
        ax.tick_params(colors=txt_c, labelsize=9)

        if not self.current_results:
            ax.text(0.5, 0.5, "No search results to visualize", color=txt_c, ha='center', va='center'); ax.axis('off')
            self.canvas_chart.draw(); return

        metric = self.combo_chart_metric.currentText()
        is_size = "(Size MB)" in metric
        
        data_map = defaultdict(float)
        
        for r in self.current_results:
            is_fldr = r[6]
            size_mb = (r[4] or 0) / (1024*1024)
            val = size_mb if is_size else 1.0
            
            if is_size and is_fldr: continue 
            
            if "Extension" in metric:
                k = r[3].upper() if r[3] else "NONE"
                data_map[k] += val
            elif "Category" in metric:
                k = r[10] if r[10] else "Others" 
                data_map[k] += val
            elif "By Tags" in metric:
                tags_str = r[8]
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

        if not data_map:
            ax.text(0.5, 0.5, "Insufficient data for this metric", color=txt_c, ha='center', va='center'); ax.axis('off')
            self.canvas_chart.draw(); return

        sorted_items = sorted(data_map.items(), key=lambda x: x[1], reverse=True)[:12]
        labels = [x[0] for x in sorted_items]
        values = [x[1] for x in sorted_items]
        
        colors = ["#58a6ff", "#3fb950", "#e3b341", "#a371f7", "#f85149", "#d2a8ff", "#79c0ff", "#2ea043", "#ff7b72", "#bc8cff"]
        
        ax.bar(labels, values, color=colors, edgecolor=bg_c)
        
        ax.set_title(f"Search Results Analytics {metric}", color=txt_c, fontweight='bold', pad=15)
        ax.set_ylabel("Size (MB)" if is_size else "Count", color=txt_c, fontweight='bold')
        ax.tick_params(axis='x', rotation=30)
        ax.grid(axis='y', color=grid_c, linestyle='--', alpha=0.5)
        for spine in ['top', 'right']: ax.spines[spine].set_visible(False)
        for spine in ['bottom', 'left']: ax.spines[spine].set_color(grid_c)
        
        self.figure_chart.tight_layout(pad=1.5)
        self.canvas_chart.draw()
