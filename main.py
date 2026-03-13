#!/usr/bin/env python3
"""
Nexus OS Virtual File Manager — Master Professional Edition
Timeline Diary, Deep Smart Views, Zero-Lag Async Engine, and Custom Tags.
"""
from __future__ import annotations
import hashlib, os, shutil, sqlite3, sys, zipfile, time, csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict

from PySide6.QtCore import (
    Qt, QThread, Signal, QModelIndex, QAbstractTableModel, 
    QFileInfo, QUrl, QSize, QMimeData, QPropertyAnimation, QEasingCurve, QTimer, QDate
)
from PySide6.QtGui import (
    QFont, QPixmap, QImage, QAction, QPainter, QIcon, QDragEnterEvent, QDropEvent, 
    QColor, QBrush, QKeySequence, QShortcut, QDrag, QKeyEvent, QTextCursor, QTextCharFormat
)
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QToolBar, QFileDialog, QMessageBox,
    QWidget, QVBoxLayout, QLabel, QPushButton, QHBoxLayout, QInputDialog, 
    QProgressDialog, QTreeWidget, QTreeWidgetItem, QPlainTextEdit, 
    QLineEdit, QComboBox, QTableView, QHeaderView, QMenu, QAbstractItemView, 
    QStatusBar, QSizePolicy, QFormLayout, QDockWidget, QToolButton,
    QStackedWidget, QListWidget, QListWidgetItem, QListView, QTabWidget, 
    QSlider, QStyle, QGraphicsOpacityEffect, QScrollArea, QDialog,
    QGraphicsView, QGraphicsScene, QTextBrowser, QTableWidget, QTableWidgetItem, QCheckBox,
    QCalendarWidget
)

try:
    from PySide6.QtWidgets import QFileIconProvider
    HAS_ICON_PROVIDER = True
except ImportError:
    try:
        from PySide6.QtGui import QAbstractFileIconProvider as QFileIconProvider
        HAS_ICON_PROVIDER = True
    except ImportError: HAS_ICON_PROVIDER = False

try:
    from PySide6.QtMultimedia import QMediaPlayer, QAudioOutput
    from PySide6.QtMultimediaWidgets import QVideoWidget
    HAS_MULTIMEDIA = True
except ImportError: HAS_MULTIMEDIA = False

try: import pandas as pd; PANDAS_AVAILABLE = True
except Exception: PANDAS_AVAILABLE = False

try:
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.figure import Figure
    import matplotlib.dates as mdates
    MATPLOTLIB_AVAILABLE = True
except Exception: MATPLOTLIB_AVAILABLE = False

# ---------------- Constants & Global Utilities ----------------
APP_TITLE = "Nexus OS Data Engine"
DATA_DIR = Path("nexus_data")
VIEWS_DIR = DATA_DIR / "compiled_views"
DB_FILE = DATA_DIR / "nexus_vfs.db"
MAX_VIRTUAL_STORAGE = 100 * 1024 * 1024 * 1024  
CHUNK_SIZE = 150 

FILE_CATEGORIES = {
    "Images": ['.png', '.jpg', '.jpeg', '.bmp', '.gif', '.webp', '.svg'],
    "Videos": ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv'],
    "Audio": ['.mp3', '.wav', '.ogg', '.flac', '.aac'],
    "Documents": ['.pdf', '.doc', '.docx', '.txt', '.csv', '.xlsx', '.xls', '.ppt', '.pptx', '.md'],
    "Code": ['.py', '.js', '.html', '.css', '.cpp', '.c', '.java', '.json', '.xml', '.sh']
}

def get_category_for_ext(ext):
    for cat, exts in FILE_CATEGORIES.items():
        if ext in exts: return cat
    return "Others"

SMART_PROTOCOLS = {
    "tags://": ["custom_tags"],
    "y_c_m_e://": ["year", "category", "month", "extension"],
    "c_y_m://": ["category", "year", "month"],
    "y_m://": ["year", "month"],
    "y_m_c://": ["year", "month", "category"],
    "y_c_m://": ["year", "category", "month"]
}

FUTURISTIC_THEME = """
QMainWindow, QDialog { background-color: #0d1117; }
QWidget { color: #c9d1d9; font-family: 'Segoe UI', Helvetica, sans-serif; font-size: 13px; }
QDockWidget::title { background: #161b22; padding: 8px; border-top-left-radius: 8px; border-top-right-radius: 8px; font-weight: bold; }
QLineEdit, QComboBox { background-color: #0d1117; border: 1px solid #30363d; border-radius: 6px; padding: 6px 10px; color: #58a6ff; }
QLineEdit:focus, QComboBox:focus { border: 1px solid #58a6ff; }
QPushButton { background-color: #21262d; border: 1px solid #30363d; border-radius: 6px; padding: 6px 12px; font-weight: bold; }
QPushButton:hover { background-color: #30363d; border: 1px solid #8b949e; }
QTableView, QTreeView, QListWidget, QPlainTextEdit, QTextBrowser, QTableWidget, QScrollArea { background-color: #0d1117; border: 1px solid #30363d; border-radius: 8px; gridline-color: #21262d; outline: none; }
QHeaderView::section { background-color: #161b22; padding: 6px; border: none; border-right: 1px solid #30363d; font-weight: bold; }
QScrollBar:vertical { background: #0d1117; width: 14px; margin: 0px; }
QScrollBar::handle:vertical { background: #30363d; min-height: 20px; border-radius: 7px; margin: 2px; }
QScrollBar::handle:vertical:hover { background: #58a6ff; }
QListWidget::item { background: transparent; border: 1px solid transparent; border-radius: 8px; padding: 8px; margin: 5px; }
QListWidget::item:hover { background: #161b22; border: 1px solid #30363d; }
QListWidget::item:selected { background: rgba(88, 166, 255, 0.15); border: 1px solid #58a6ff; }
QProgressBar { border: 1px solid #30363d; border-radius: 5px; text-align: center; color: white; background: #0d1117;}
QProgressBar::chunk { background-color: #2ea043; border-radius: 5px; }
QTableView::indicator, QTreeWidget::indicator, QTableWidget::indicator { width: 16px; height: 16px; }
QTableView::indicator:unchecked, QTableWidget::indicator:unchecked { background-color: #0d1117; border: 2px solid #58a6ff; border-radius: 3px; }
QTableView::indicator:checked, QTableWidget::indicator:checked { background-color: #2ea043; border: 2px solid #2ea043; border-radius: 3px; }
QCalendarWidget QWidget { alternate-background-color: #161b22; }
QCalendarWidget QAbstractItemView:enabled { color: #c9d1d9; background-color: #0d1117; selection-background-color: #58a6ff; selection-color: white; border: none; }
QCalendarWidget QToolButton { color: #c9d1d9; background-color: #21262d; border-radius: 4px; padding: 5px; }
QCalendarWidget QToolButton:hover { background-color: #30363d; }
QCalendarWidget QMenu { background-color: #161b22; color: #c9d1d9; }
QCalendarWidget QSpinBox { background-color: #0d1117; color: #c9d1d9; }
"""

def ensure_dirs(): 
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    VIEWS_DIR.mkdir(parents=True, exist_ok=True)

def now_ts(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def human_size(num_bytes: int) -> str:
    try: n = int(num_bytes)
    except Exception: return "0 B"
    if n < 1024: return f"{n} B"
    n_kb = n / 1024.0
    if n_kb < 1024: return f"{n_kb:.1f} KB"
    n_mb = n_kb / 1024.0
    if n_mb < 1024: return f"{n_mb:.2f} MB" if n_mb < 10 else f"{n_mb:.1f} MB"
    n_gb = n_mb / 1024.0
    return f"{n_gb:.2f} GB" if n_gb < 10 else f"{n_gb:.1f} GB"

# ---------------- Database Engine ----------------
class NexusDB:
    def __init__(self, path: Path):
        self.path = path
        self.conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self._ensure_schema()

    def _ensure_schema(self):
        c = self.conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS virtual_fs (
                id INTEGER PRIMARY KEY, parent_path TEXT, name TEXT, is_folder INTEGER,
                real_path TEXT, size INTEGER, extension TEXT, modified TEXT,
                color_tag TEXT DEFAULT '', secondary_name TEXT DEFAULT '',
                is_hidden INTEGER DEFAULT 0, in_trash INTEGER DEFAULT 0,
                is_favorite INTEGER DEFAULT 0, sha256 TEXT DEFAULT '',
                category TEXT DEFAULT 'Others', year TEXT DEFAULT '', month TEXT DEFAULT '',
                custom_tags TEXT DEFAULT ''
            );
        """)
        for col in [
            "color_tag TEXT DEFAULT ''", "secondary_name TEXT DEFAULT ''", 
            "is_hidden INTEGER DEFAULT 0", "in_trash INTEGER DEFAULT 0", 
            "is_favorite INTEGER DEFAULT 0", "sha256 TEXT DEFAULT ''",
            "category TEXT DEFAULT 'Others'", "year TEXT DEFAULT ''", "month TEXT DEFAULT ''",
            "custom_tags TEXT DEFAULT ''"
        ]:
            try: c.execute(f"ALTER TABLE virtual_fs ADD COLUMN {col};")
            except sqlite3.OperationalError: pass

        c.execute("CREATE INDEX IF NOT EXISTS idx_vfs_parent ON virtual_fs(parent_path);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_vfs_ycme ON virtual_fs(year, category, month, extension);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_vfs_tags ON virtual_fs(custom_tags);")
        self.conn.commit()

    def get_stats(self, current_prefix=""):
        c = self.conn.cursor()
        where_clause = "is_folder=0 AND in_trash=0"
        params = []
        
        if current_prefix and current_prefix != "/":
            if current_prefix.startswith("tags://"):
                parts = [p for p in current_prefix.replace("tags://", "").split("/") if p]
                if len(parts) >= 1:
                    where_clause += " AND custom_tags LIKE ?"
                    params.append(f"%{parts[0]}%")
            elif current_prefix.startswith("y_m_f://"):
                parts = [p for p in current_prefix.replace("y_m_f://", "").split("/") if p]
                if len(parts) >= 1:
                    where_clause += " AND year=?"
                    params.append(parts[0])
                if len(parts) >= 2:
                    where_clause += " AND month=?"
                    params.append(parts[1])
            elif "://" in current_prefix:
                proto = current_prefix.split("://")[0] + "://"
                if proto in SMART_PROTOCOLS:
                    cols = SMART_PROTOCOLS[proto]
                    parts = [p for p in current_prefix.replace(proto, "").split("/") if p]
                    for i in range(min(len(parts), len(cols))):
                        where_clause += f" AND {cols[i]}=?"
                        params.append(parts[i])
            else:
                where_clause += " AND parent_path LIKE ?"
                params.append(f"{current_prefix}%")

        c.execute(f"SELECT COUNT(*), COALESCE(SUM(size), 0) FROM virtual_fs WHERE {where_clause}", params)
        res = c.fetchone()
        f_cnt = res[0] if res else 0
        total_sz = res[1] if res else 0
        
        c.execute(f"SELECT COUNT(*) FROM virtual_fs WHERE is_folder=1 AND in_trash=0 AND parent_path LIKE ?", (f"{current_prefix}%",))
        d_cnt = c.fetchone()[0]
        
        c.execute(f"SELECT extension, COUNT(*), COALESCE(SUM(size),0) FROM virtual_fs WHERE {where_clause} GROUP BY extension", params)
        dist = c.fetchall()
        
        c.execute(f"SELECT name, size FROM virtual_fs WHERE {where_clause} ORDER BY size DESC LIMIT 10", params)
        top_files = c.fetchall()
        
        c.execute(f"SELECT COUNT(*) FROM virtual_fs WHERE is_hidden=1 AND in_trash=0")
        hidden_cnt = c.fetchone()[0]
        
        c.execute("SELECT COALESCE(SUM(size), 0) FROM virtual_fs WHERE in_trash=1")
        trash_sz = c.fetchone()[0]
        
        c.execute(f"SELECT year || '-' || month as dt, COUNT(*), SUM(size) FROM virtual_fs WHERE {where_clause} AND year != '' GROUP BY dt ORDER BY dt", params)
        time_series = c.fetchall()
        
        return {"files": f_cnt, "folders": d_cnt, "used_bytes": total_sz, "distribution": dist, "top_files": top_files, "hidden": hidden_cnt, "trash_size": trash_sz, "time_series": time_series}

    def close(self):
        try: self.conn.close()
        except Exception: pass

# ---------------- Full Screen Timeline Diary Engine ----------------
class TimelineDiaryDialog(QDialog):
    def __init__(self, db_path, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.setWindowTitle("Nexus Timeline Diary")
        self.setStyleSheet(FUTURISTIC_THEME)
        self.showMaximized()
        
        layout = QHBoxLayout(self)
        
        # Left Side: Custom Calendar
        cal_layout = QVBoxLayout()
        self.calendar = QCalendarWidget()
        self.calendar.setGridVisible(True)
        self.calendar.setVerticalHeaderFormat(QCalendarWidget.NoVerticalHeader)
        self.calendar.currentPageChanged.connect(self.highlight_days)
        self.calendar.clicked.connect(self.load_diary)
        
        # Style Calendar slightly larger
        font = self.calendar.font()
        font.setPointSize(12)
        self.calendar.setFont(font)
        
        cal_layout.addWidget(self.calendar)
        layout.addLayout(cal_layout, 1)
        
        # Right Side: Feed/Diary
        self.diary_browser = QTextBrowser()
        layout.addWidget(self.diary_browser, 2)
        
        # Initialize
        today = QDate.currentDate()
        self.highlight_days(today.year(), today.month())
        self.calendar.setSelectedDate(today)
        self.load_diary(today)
        
    def highlight_days(self, year, month):
        self.calendar.setDateTextFormat(QDate(), QTextCharFormat())
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT SUBSTR(modified, 9, 2) FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND modified LIKE ?", (f"{year}-{month:02d}-%",))
        days = [r[0] for r in cur.fetchall()]
        conn.close()
        
        fmt = QTextCharFormat()
        fmt.setBackground(QColor("#2ea043"))
        fmt.setForeground(QColor("white"))
        fmt.setFontWeight(QFont.Bold)
        
        for d in days:
            try:
                day_int = int(d)
                self.calendar.setDateTextFormat(QDate(year, month, day_int), fmt)
            except ValueError:
                pass

    def load_diary(self, date):
        dt_str = date.toString("yyyy-MM-dd")
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("SELECT SUBSTR(modified, 12, 8), name, parent_path, size, category FROM virtual_fs WHERE modified LIKE ? AND is_folder=0 AND in_trash=0 ORDER BY modified ASC", (f"{dt_str}%",))
        entries = cur.fetchall()
        conn.close()
        
        html = f"<h1 style='color:#58a6ff; text-align:center;'>📖 System Timeline: {date.toString('dddd, MMMM d, yyyy')}</h1><hr>"
        
        if not entries:
            html += "<h3 style='color:#8b949e; text-align:center;'><br><br>No system activity recorded on this day.</h3>"
        else:
            html += f"<p style='color:#c9d1d9; text-align:center;'><b>{len(entries)}</b> files were modified or logged.</p><br>"
            html += "<ul style='list-style-type: none; padding-left: 0;'>"
            
            cat_colors = {
                "Images": "#a371f7",
                "Videos": "#f85149",
                "Audio": "#ff7b72",
                "Documents": "#d2a8ff",
                "Code": "#79c0ff",
                "Others": "#8b949e"
            }
            
            for time_str, name, pp, size, cat in entries:
                c_color = cat_colors.get(cat, "#8b949e")
                html += f"""
                <li style='margin-bottom: 15px; background-color: rgba(33, 38, 45, 0.6); padding: 12px; border-left: 5px solid {c_color}; border-radius: 6px;'>
                    <span style='color: #58a6ff; font-size: 15px;'><b>🕒 {time_str}</b></span><br>
                    <span style='font-size: 16px; color: white;'>Action registered on <b style='color: {c_color};'>{name}</b></span> 
                    <span style='color: #8b949e; font-size: 13px;'>({human_size(size)})</span><br>
                    <span style='color: #8b949e; font-size: 13px;'>Path: {pp}</span>
                </li>
                """
            html += "</ul>"
            
        self.diary_browser.setHtml(html)

# ---------------- Background Engine Threads ----------------
class SpaceScannerThread(QThread):
    progress = Signal(int, int, str)
    found = Signal(str, str, int, int)
    finished_scan = Signal()
    
    def __init__(self, db_path, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.is_cancelled = False
        
    def cancel(self): 
        self.is_cancelled = True
    
    def run(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            
            self.progress.emit(10, 100, "Scanning for Junk Files...")
            if self.is_cancelled: return
            cur.execute("SELECT id, name, parent_path, size FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND (extension IN ('.tmp', '.bak', '.log', '.cache') OR name LIKE '%cache%')")
            for r in cur.fetchall():
                if self.is_cancelled: return
                self.found.emit("Junk File", f"[{r[2]}] {r[1]}", r[3], r[0])
                
            self.progress.emit(40, 100, "Scanning for Huge Files...")
            if self.is_cancelled: return
            cur.execute("SELECT id, name, parent_path, size FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND size > 524288000 ORDER BY size DESC")
            for r in cur.fetchall():
                if self.is_cancelled: return
                self.found.emit("Huge File (>500MB)", f"[{r[2]}] {r[1]}", r[3], r[0])
                
            self.progress.emit(70, 100, "Scanning for Duplicates...")
            if self.is_cancelled: return
            cur.execute("SELECT size, extension, COUNT(*) as c FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND size > 0 GROUP BY size, extension HAVING c > 1")
            dupes = cur.fetchall()
            for size, ext, count in dupes:
                if self.is_cancelled: return
                cur.execute("SELECT id, name, parent_path FROM virtual_fs WHERE size=? AND extension=? AND is_folder=0 AND in_trash=0", (size, ext))
                files = cur.fetchall()
                for f in files[1:]:
                    self.found.emit("Duplicate File", f"[{f[2]}] {f[1]}", size, f[0])
                    
            self.progress.emit(100, 100, "Scan Complete.")
        except Exception as e: 
            print(e)
        finally:
            if 'conn' in locals(): conn.close()
            self.finished_scan.emit()

class ImportFilesThread(QThread):
    progress = Signal(int, int, str)
    finished_import = Signal(int, int)
    error = Signal(str)
    
    def __init__(self, db_path, target_prefix, paths, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.target_prefix = target_prefix
        self.paths = paths
        self.is_cancelled = False
        
    def cancel(self): 
        self.is_cancelled = True
        
    def run(self):
        try:
            conn = sqlite3.connect(str(self.db_path))
            cur = conn.cursor()
            added_f, added_d = 0, 0
            
            all_files_to_process = []
            for p in self.paths:
                if os.path.isdir(p):
                    for root, _, files in os.walk(p):
                        for f in files: all_files_to_process.append(os.path.join(root, f))
                else: 
                    all_files_to_process.append(p)
            total = len(all_files_to_process)
            
            for p in self.paths:
                if self.is_cancelled: break
                if os.path.isdir(p):
                    folder_name = os.path.basename(p)
                    cur.execute("INSERT OR IGNORE INTO virtual_fs (parent_path, name, is_folder, modified) VALUES (?,?,1,?)", (self.target_prefix, folder_name, now_ts()))
                    added_d += 1
                    for root, dirs, files in os.walk(p):
                        if self.is_cancelled: break
                        curr_parent = self.target_prefix + folder_name + "/" + os.path.relpath(root, p).replace("\\", "/") + "/" if os.path.relpath(root, p) != "." else self.target_prefix + folder_name + "/"
                        for d in dirs: 
                            cur.execute("INSERT OR IGNORE INTO virtual_fs (parent_path, name, is_folder, modified) VALUES (?,?,1,?)", (curr_parent, d, now_ts()))
                            added_d += 1
                        
                        records = []
                        for f in files:
                            fp = os.path.join(root, f)
                            ext = os.path.splitext(f)[1].lower()
                            cat = get_category_for_ext(ext)
                            mod = datetime.fromtimestamp(os.path.getmtime(fp)).strftime("%Y-%m-%d %H:%M:%S")
                            records.append((curr_parent, f, 0, fp, os.path.getsize(fp), ext, mod, cat, mod[0:4], mod[5:7]))
                            added_f += 1
                        
                        cur.executemany("INSERT INTO virtual_fs (parent_path, name, is_folder, real_path, size, extension, modified, category, year, month) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", records)
                        self.progress.emit(added_f, total, f"Imported {added_f}/{total} files...")
                else:
                    ext = os.path.splitext(p)[1].lower()
                    cat = get_category_for_ext(ext)
                    mod = datetime.fromtimestamp(os.path.getmtime(p)).strftime("%Y-%m-%d %H:%M:%S")
                    cur.execute("INSERT INTO virtual_fs (parent_path, name, is_folder, real_path, size, extension, modified, category, year, month) VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?)", (self.target_prefix, os.path.basename(p), p, os.path.getsize(p), ext, mod, cat, mod[0:4], mod[5:7]))
                    added_f += 1
                    self.progress.emit(added_f, total, f"Imported {added_f}/{total} files...")
            
            conn.commit()
            conn.close()
            self.finished_import.emit(added_f, added_d)
        except Exception as e:
            self.error.emit(str(e))

class DataLoaderThread(QThread):
    data_ready = Signal(list, list) 
    
    def __init__(self, db_path, target_path, show_hidden, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.target_path = target_path
        self.show_hidden = show_hidden
        self.is_cancelled = False
        
    def cancel(self): 
        self.is_cancelled = True

    def run(self):
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()
        h_q = "AND is_hidden = 0" if not self.show_hidden else ""
        folders, files = [], []
        try:
            if self.is_cancelled: return
            matched_proto = next((p for p in SMART_PROTOCOLS if self.target_path.startswith(p)), None)
            
            # --- CUSTOM TAG ENGINE (tags://) ---
            if self.target_path.startswith("tags://"):
                parts = [p for p in self.target_path.replace("tags://", "").split("/") if p]
                if len(parts) == 0:
                    cur.execute("SELECT custom_tags, size FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND custom_tags IS NOT NULL AND custom_tags != ''")
                    all_tags = cur.fetchall()
                    tag_stats = defaultdict(lambda: [0, 0])
                    for tags_str, sz in all_tags:
                        for t in [x.strip() for x in tags_str.split(",") if x.strip()]:
                            tag_stats[t][0] += 1
                            tag_stats[t][1] += sz or 0
                    folders = [(-1, "tags://", t, "", "", 0, stats[0], stats[1]) for t, stats in tag_stats.items()]
                elif len(parts) >= 1:
                    target_tag = parts[0]
                    cur.execute(f"SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden, custom_tags FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND custom_tags LIKE ? {h_q}", (f"%{target_tag}%",))
                    for row in cur.fetchall():
                        t_list = [x.strip() for x in str(row[9]).split(",") if x.strip()]
                        if target_tag in t_list: files.append(row[:9])

            # --- STATISTICAL FOLDER CLUSTERING (y_m_f://) ---
            elif self.target_path.startswith("y_m_f://"):
                parts = [p for p in self.target_path.replace("y_m_f://", "").split("/") if p]
                cur.execute("SELECT parent_path, year, month, COUNT(id) FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND year IS NOT NULL AND year != '' AND month IS NOT NULL AND month != '' GROUP BY parent_path, year, month")
                stats = cur.fetchall()
                folder_age = {}; temp_tracker = {}
                for pp, y, m, c in stats:
                    if pp not in temp_tracker or c > temp_tracker[pp]:
                        temp_tracker[pp] = c; folder_age[pp] = (y, m)
                cur.execute("SELECT parent_path, COUNT(id), SUM(size) FROM virtual_fs WHERE is_folder=0 AND in_trash=0 GROUP BY parent_path")
                totals = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

                if len(parts) == 0:
                    years = sorted(list(set([v[0] for v in folder_age.values()])), reverse=True)
                    folders = [(-1, "y_m_f://", yr, "", "", 0, 0, 0) for yr in years]
                elif len(parts) == 1:
                    target_year = parts[0]
                    months = sorted(list(set([v[1] for pp, v in folder_age.items() if v[0] == target_year])))
                    folders = [(-1, f"y_m_f://{target_year}/", mo, "", "", 0, 0, 0) for mo in months]
                elif len(parts) == 2:
                    target_year, target_month = parts[0], parts[1]
                    matched_folders = [pp for pp, age in folder_age.items() if age == (target_year, target_month)]
                    for pp in matched_folders:
                        folder_name = pp.strip("/").split("/")[-1] if pp.strip("/") else "Root_Files"
                        cnt, sz = totals.get(pp, (0,0))
                        folders.append((-1, f"y_m_f://{target_year}/{target_month}/", folder_name, "", "", 0, cnt, sz))
                elif len(parts) >= 3:
                    folder_name = parts[2]; target_year, target_month = parts[0], parts[1]
                    matched_pp = None
                    for pp, age in folder_age.items():
                        if age == (target_year, target_month) and (pp.strip("/").split("/")[-1] if pp.strip("/") else "Root_Files") == folder_name:
                            matched_pp = pp; break
                    if matched_pp:
                        cur.execute(f"SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE parent_path=? AND is_folder=0 AND in_trash=0 {h_q}", (matched_pp,))
                        files = cur.fetchall()

            # --- STANDARD SMART VIEWS ---
            elif matched_proto:
                cols = SMART_PROTOCOLS[matched_proto]
                parts = [p for p in self.target_path.replace(matched_proto, "").split("/") if p]
                depth = len(parts)
                
                if depth < len(cols):
                    target_col = cols[depth]
                    where_clauses = ["is_folder=0", "in_trash=0", f"{target_col} != ''"]
                    params = []
                    for i in range(depth):
                        where_clauses.append(f"{cols[i]}=?"); params.append(parts[i])
                        
                    query = f"SELECT {target_col}, COUNT(id), SUM(size) FROM virtual_fs WHERE {' AND '.join(where_clauses)} GROUP BY {target_col}"
                    cur.execute(query, tuple(params))
                    base_path = matched_proto + "/".join(parts) + "/" if parts else matched_proto
                    folders = [(-1, base_path, r[0], "", "", 0, r[1], r[2] or 0) for r in cur.fetchall() if r[0]]
                else:
                    where_clauses = ["is_folder=0", "in_trash=0"] + [f"{cols[i]}=?" for i in range(len(cols))]
                    if not self.show_hidden: where_clauses.append("is_hidden=0")
                    query = f"SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE {' AND '.join(where_clauses)}"
                    cur.execute(query, tuple(parts))
                    files = cur.fetchall()

            # --- PHYSICAL VIEWS ---
            elif self.target_path == "trash://":
                cur.execute("SELECT id, parent_path, name, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE is_folder=1 AND in_trash=1")
                folders = [(r[0], r[1], r[2], r[3], r[4], r[5], 0, 0) for r in cur.fetchall()]
                cur.execute("SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE is_folder=0 AND in_trash=1")
                files = cur.fetchall()
            elif self.target_path == "fav://":
                cur.execute(f"SELECT id, parent_path, name, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE is_folder=1 AND is_favorite=1 AND in_trash=0 {h_q}")
                folders = [(r[0], r[1], r[2], r[3], r[4], r[5], 0, 0) for r in cur.fetchall()]
                cur.execute(f"SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE is_folder=0 AND is_favorite=1 AND in_trash=0 {h_q}")
                files = cur.fetchall()
            else:
                folder_q = f"""
                SELECT id, name, color_tag, secondary_name, is_hidden,
                (SELECT COUNT(id) FROM virtual_fs f2 WHERE f2.parent_path LIKE virtual_fs.parent_path || virtual_fs.name || '/%' AND f2.is_folder=0 AND f2.in_trash=0),
                (SELECT SUM(size) FROM virtual_fs f2 WHERE f2.parent_path LIKE virtual_fs.parent_path || virtual_fs.name || '/%' AND f2.is_folder=0 AND f2.in_trash=0)
                FROM virtual_fs WHERE parent_path=? AND is_folder=1 AND in_trash=0 {h_q}
                """
                cur.execute(folder_q, (self.target_path,))
                folders = [(r[0], self.target_path, r[1], r[2], r[3], r[4], r[5] or 0, r[6] or 0) for r in cur.fetchall()] 
                cur.execute(f"SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE parent_path=? AND is_folder=0 AND in_trash=0 {h_q}", (self.target_path,))
                files = cur.fetchall()
                
            if not self.is_cancelled:
                self.data_ready.emit(folders, files)
        except Exception as e: print("DB Load Error:", e)
        finally: conn.close()

class CompilerThread(QThread):
    progress = Signal(int, int, str)
    finished = Signal(str)
    error = Signal(str)
    
    def __init__(self, source_db, target_db, source_prefix, query, params, parent=None):
        super().__init__(parent)
        self.source_db = source_db
        self.target_db = target_db
        self.source_prefix = source_prefix
        self.query = query
        self.params = params
        self.is_cancelled = False
        
    def cancel(self): 
        self.is_cancelled = True

    def run(self):
        try:
            if self.is_cancelled: return
            self.progress.emit(0, 100, "Initializing compiler...")
            if os.path.exists(self.target_db): os.remove(self.target_db)
            
            tgt_db = NexusDB(Path(self.target_db))
            tgt_conn = tgt_db.conn
            tgt_cur = tgt_conn.cursor()
            src_conn = sqlite3.connect(self.source_db)
            src_cur = src_conn.cursor()

            self.progress.emit(20, 100, "Executing extraction query...")
            src_cur.execute(self.query, self.params)
            rows = src_cur.fetchall()
            total = len(rows)

            if total == 0: 
                self.error.emit("View is empty. Compilation cancelled.")
                return
                
            self.progress.emit(40, 100, f"Pathing and writing {total} records...")
            
            is_smart_view = "://" in self.source_prefix
            modified_rows = []
            for r in rows:
                if self.is_cancelled: 
                    src_conn.close()
                    tgt_conn.close()
                    return
                r_list = list(r)
                if is_smart_view: 
                    r_list[1] = "/"
                else:
                    if r_list[1].startswith(self.source_prefix):
                        r_list[1] = "/" + r_list[1][len(self.source_prefix):]
                        if not r_list[1].startswith("/"): r_list[1] = "/" + r_list[1]
                modified_rows.append(tuple(r_list))

            insert_q = f"INSERT INTO virtual_fs VALUES ({','.join(['?']*18)})"
            batch_size = 1000
            for i in range(0, total, batch_size):
                if self.is_cancelled: 
                    src_conn.close()
                    tgt_conn.close()
                    return
                tgt_cur.executemany(insert_q, modified_rows[i:i+batch_size])
                tgt_conn.commit()
                self.progress.emit(int(40 + (i/total)*60), 100, f"Compiled {min(i+batch_size, total)}/{total} records...")

            tgt_cur.execute("CREATE INDEX idx_vfs_parent ON virtual_fs(parent_path);")
            tgt_conn.commit()
            src_conn.close()
            tgt_conn.close()
            self.finished.emit(self.target_db)
        except Exception as e: 
            self.error.emit(str(e))

class MaterializeThread(QThread):
    progress = Signal(int, int, str)
    finished = Signal(str)
    error = Signal(str)
    
    def __init__(self, db_path, virtual_root, physical_dest, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.virtual_root = virtual_root
        self.physical_dest = physical_dest
        self.is_cancelled = False
        
    def cancel(self): 
        self.is_cancelled = True
        
    def run(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("SELECT parent_path, name, real_path, is_folder FROM virtual_fs WHERE parent_path LIKE ? AND in_trash=0", (f"{self.virtual_root}%",))
            items = cur.fetchall()
            total = len(items)
            
            if total == 0: 
                self.error.emit("No files found to export.")
                return
            
            for i, (pp, name, real_path, is_folder) in enumerate(items):
                if self.is_cancelled: 
                    conn.close()
                    return
                    
                rel_path = pp[len(self.virtual_root):]
                dest_dir = os.path.join(self.physical_dest, rel_path.lstrip('/').replace('/', os.sep))
                os.makedirs(dest_dir, exist_ok=True)
                
                if is_folder:
                    os.makedirs(os.path.join(dest_dir, name), exist_ok=True)
                elif real_path and os.path.exists(real_path):
                    dest_file = os.path.join(dest_dir, name)
                    if not os.path.exists(dest_file):
                        shutil.copy2(real_path, dest_file)
                self.progress.emit(i+1, total, f"Exporting: {name}")
                
            conn.close()
            self.finished.emit(self.physical_dest)
        except Exception as e: 
            self.error.emit(str(e))

class ExportZipThread(QThread):
    progress = Signal(int, int, str)
    finished = Signal(str)
    error = Signal(str)
    
    def __init__(self, db_path, items_to_export, zip_filepath, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.items_to_export = items_to_export
        self.zip_filepath = zip_filepath
        self.is_cancelled = False
        
    def cancel(self): 
        self.is_cancelled = True
        
    def run(self):
        try:
            conn = sqlite3.connect(str(self.db_path))
            cur = conn.cursor()
            all_files_to_zip = []
            
            for typ, v_path, real_path, db_id in self.items_to_export:
                if self.is_cancelled: return
                if typ == "file" and real_path and os.path.exists(real_path):
                    all_files_to_zip.append((real_path, v_path.split("/")[-1] if v_path else os.path.basename(real_path)))
                elif typ == "folder":
                    cur.execute("SELECT parent_path, name, real_path, is_folder FROM virtual_fs WHERE parent_path LIKE ?", (f"{v_path}%",))
                    for pp, n, rp, is_f in cur.fetchall():
                        if not is_f and rp and os.path.exists(rp):
                            zip_internal_path = f"{v_path.strip('/').split('/')[-1]}/{pp[len(v_path):]}{n}".replace("//", "/")
                            all_files_to_zip.append((rp, zip_internal_path))
                            
            total = len(all_files_to_zip)
            if total == 0: 
                self.error.emit("No physical files found to zip.")
                return
                
            with zipfile.ZipFile(self.zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zf:
                for i, (real_os_path, zip_virtual_path) in enumerate(all_files_to_zip):
                    if self.is_cancelled: return
                    self.progress.emit(i+1, total, f"Compressing: {zip_virtual_path}")
                    zf.write(real_os_path, arcname=zip_virtual_path)
                    
            conn.close()
            self.finished.emit(self.zip_filepath)
        except Exception as e: 
            self.error.emit(str(e))

class ImageLoader(QThread):
    finished = Signal(str, object)
    def __init__(self, path: str, max_size=(1920, 1080), parent=None): 
        super().__init__(parent)
        self.path = path
        self.max_size = max_size
        
    def run(self):
        try:
            img = QImage(self.path)
            if not img.isNull() and img.width() > 0:
                if img.width() > self.max_size[0] or img.height() > self.max_size[1]: 
                    img = img.scaled(*self.max_size, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            self.finished.emit(self.path, img)
        except Exception: 
            self.finished.emit(self.path, QImage())

class HashCalculator(QThread):
    finished = Signal(str)
    def __init__(self, path, parent=None): 
        super().__init__(parent)
        self.path = path
        
    def run(self):
        try:
            sha256 = hashlib.sha256()
            with open(self.path, 'rb') as f:
                for block in iter(lambda: f.read(4096), b""): sha256.update(block)
            self.finished.emit(sha256.hexdigest())
        except Exception as e: 
            self.finished.emit(f"Error: {str(e)}")

# ---------------- Smart Analyzers & Deleters ----------------
class SpaceAnalyzerDialog(QDialog):
    def __init__(self, db_path, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.setWindowTitle("Advanced Space & Junk Analyzer")
        self.resize(800, 500)
        self.setStyleSheet(FUTURISTIC_THEME)
        layout = QVBoxLayout(self)
        
        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(["Select", "Type", "Name / Location", "Size", "ID"])
        self.table.setColumnWidth(0, 60)
        self.table.setColumnWidth(1, 120)
        self.table.setColumnWidth(2, 450)
        self.table.setColumnWidth(3, 100)
        self.table.setColumnHidden(4, True)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table)
        
        btn_lay = QHBoxLayout()
        self.btn_select_all = QPushButton("☑ Select All")
        self.btn_select_all.clicked.connect(self.toggle_select_all)
        self.btn_refresh = QPushButton("🔄 Scan Database")
        self.btn_delete = QPushButton("🗑️ Delete Selected")
        self.btn_delete.setStyleSheet("background-color: #8b0000; font-weight: bold;")
        self.btn_refresh.clicked.connect(self.scan)
        self.btn_delete.clicked.connect(self.delete_selected)
        btn_lay.addWidget(self.btn_select_all)
        btn_lay.addWidget(self.btn_refresh)
        btn_lay.addStretch()
        btn_lay.addWidget(self.btn_delete)
        layout.addLayout(btn_lay)
        self.scan()

    def toggle_select_all(self):
        self.select_all_state = not getattr(self, 'select_all_state', False)
        state = Qt.Checked if self.select_all_state else Qt.Unchecked
        for r in range(self.table.rowCount()):
            item = self.table.item(r, 0)
            if item: item.setCheckState(state)

    def scan(self):
        self.table.setRowCount(0)
        self.btn_refresh.setEnabled(False)
        self.btn_delete.setEnabled(False)
        self.scanner = SpaceScannerThread(self.db_path, self)
        self.scanner.found.connect(self._add_row)
        
        self.prog_dlg = QProgressDialog("Scanning Database for Junk...", "Cancel", 0, 100, self)
        self.prog_dlg.setWindowModality(Qt.WindowModal)
        self.scanner.progress.connect(lambda v, t, txt: (self.prog_dlg.setValue(v), self.prog_dlg.setLabelText(txt)))
        self.prog_dlg.canceled.connect(self.scanner.cancel)
        
        self.scanner.finished_scan.connect(self.scan_finished)
        self.scanner.start()
        self.prog_dlg.show()

    def scan_finished(self):
        self.prog_dlg.close()
        self.btn_refresh.setEnabled(True)
        self.btn_delete.setEnabled(True)

    def _add_row(self, typ, desc, size, db_id):
        row = self.table.rowCount()
        self.table.insertRow(row)
        chk = QTableWidgetItem()
        chk.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
        chk.setCheckState(Qt.Unchecked)
        self.table.setItem(row, 0, chk)
        self.table.setItem(row, 1, QTableWidgetItem(typ))
        self.table.setItem(row, 2, QTableWidgetItem(desc))
        self.table.setItem(row, 3, QTableWidgetItem(human_size(size)))
        self.table.setItem(row, 4, QTableWidgetItem(str(db_id)))

    def delete_selected(self):
        ids_to_delete = [int(self.table.item(r, 4).text()) for r in range(self.table.rowCount()) if self.table.item(r, 0).checkState() == Qt.Checked]
        if not ids_to_delete: return
        if QMessageBox.question(self, "Confirm", f"Permanently delete {len(ids_to_delete)} flagged items from Nexus DB?", QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.executemany("DELETE FROM virtual_fs WHERE id=?", [(i,) for i in ids_to_delete])
            conn.commit()
            conn.close()
            self.parent().clear_cache()
            self.parent().refresh_all()
            self.scan()
            QMessageBox.information(self, "Success", "Items deleted successfully.")

class BulkDeleterDialog(QDialog):
    def __init__(self, db_path, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.setWindowTitle("Bulk Operation Engine")
        self.resize(400, 200)
        self.setStyleSheet(FUTURISTIC_THEME)
        lay = QFormLayout(self)
        self.combo_type = QComboBox()
        self.combo_type.addItems(["Extension is (e.g. .tmp)", "Name contains (e.g. copy)"])
        self.txt_val = QLineEdit()
        self.chk_perm = QCheckBox("Delete Permanently (Otherwise send to Virtual Trash)")
        self.btn_exec = QPushButton("Execute Bulk Delete")
        self.btn_exec.setStyleSheet("background-color: #8b0000; font-weight:bold;")
        self.btn_exec.clicked.connect(self.execute)
        lay.addRow("Condition:", self.combo_type)
        lay.addRow("Value:", self.txt_val)
        lay.addRow("", self.chk_perm)
        lay.addRow("", self.btn_exec)

    def execute(self):
        val = self.txt_val.text().strip()
        if not val: return
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        sql_base = "DELETE FROM virtual_fs" if self.chk_perm.isChecked() else "UPDATE virtual_fs SET in_trash=1"
        
        if "Extension" in self.combo_type.currentText():
            if not val.startswith('.'): val = '.' + val
            cur.execute(f"SELECT COUNT(*) FROM virtual_fs WHERE extension=? AND is_folder=0 AND in_trash=0", (val,))
            cnt = cur.fetchone()[0]
            if cnt == 0: return QMessageBox.information(self, "Result", "No matching files found.")
            if QMessageBox.question(self, "Confirm", f"Are you sure you want to {'permanently ' if self.chk_perm.isChecked() else ''}delete {cnt} files with extension '{val}'?") == QMessageBox.Yes:
                cur.execute(f"{sql_base} WHERE extension=? AND is_folder=0 AND in_trash=0", (val,))
                conn.commit()
                self.parent().clear_cache()
                self.parent().refresh_all()
                self.close()
        else:
            cur.execute(f"SELECT COUNT(*) FROM virtual_fs WHERE name LIKE ? AND is_folder=0 AND in_trash=0", (f"%{val}%",))
            cnt = cur.fetchone()[0]
            if cnt == 0: return QMessageBox.information(self, "Result", "No matching files found.")
            if QMessageBox.question(self, "Confirm", f"Are you sure you want to {'permanently ' if self.chk_perm.isChecked() else ''}delete {cnt} files containing '{val}'?") == QMessageBox.Yes:
                cur.execute(f"{sql_base} WHERE name LIKE ? AND is_folder=0 AND in_trash=0", (f"%{val}%",))
                conn.commit()
                self.parent().clear_cache()
                self.parent().refresh_all()
                self.close()
        conn.close()

# ---------------- Advanced Popup Viewer ----------------
class AdvancedImageViewer(QGraphicsView):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.scene = QGraphicsScene(self)
        self.setScene(self.scene)
        self.setRenderHints(QPainter.Antialiasing | QPainter.SmoothPixmapTransform)
        self.setDragMode(QGraphicsView.ScrollHandDrag)
        self.setStyleSheet("background: transparent; border: none;")
        self._pixmap_item = None
        self.zoom_factor = 1.15
        
    def set_image(self, pixmap):
        self.scene.clear()
        self._pixmap_item = self.scene.addPixmap(pixmap)
        self.setSceneRect(self._pixmap_item.boundingRect())
        self.fitInView(self.sceneRect(), Qt.KeepAspectRatio)
        
    def wheelEvent(self, event):
        if event.modifiers() == Qt.ControlModifier:
            if event.angleDelta().y() > 0: self.scale(self.zoom_factor, self.zoom_factor)
            else: self.scale(1 / self.zoom_factor, 1 / self.zoom_factor)
        else: super().wheelEvent(event)

class NexusViewer(QDialog):
    def __init__(self, playlist, start_index, parent=None):
        super().__init__(parent)
        self.playlist = playlist
        self.current_index = start_index
        self.setWindowTitle("Nexus Media Engine")
        self.resize(1000, 700)
        self.setStyleSheet(FUTURISTIC_THEME)
        layout = QVBoxLayout(self)
        
        self.header_layout = QHBoxLayout()
        self.btn_prev = QPushButton("◀ Prev")
        self.btn_prev.clicked.connect(self._prev_item)
        self.lbl_title = QLabel()
        self.lbl_title.setFont(QFont("Segoe UI", 12, QFont.Bold))
        self.lbl_title.setAlignment(Qt.AlignCenter)
        self.lbl_counter = QLabel()
        self.lbl_counter.setFixedWidth(80)
        self.lbl_counter.setAlignment(Qt.AlignCenter)
        self.btn_next = QPushButton("Next ▶")
        self.btn_next.clicked.connect(self._next_item)
        
        self.header_layout.addWidget(self.btn_prev)
        self.header_layout.addWidget(self.lbl_counter)
        self.header_layout.addWidget(self.lbl_title, 1)
        self.header_layout.addWidget(self.btn_next)
        layout.addLayout(self.header_layout)

        self.stack = QStackedWidget()
        layout.addWidget(self.stack)
        
        self.txt_view = QPlainTextEdit()
        self.txt_view.setReadOnly(True)
        self.txt_view.setFont(QFont("Consolas", 11))
        self.stack.addWidget(self.txt_view)
        
        self.img_view = AdvancedImageViewer()
        self.stack.addWidget(self.img_view)

        self.media_widget = QWidget()
        m_layout = QVBoxLayout(self.media_widget)
        if HAS_MULTIMEDIA:
            self.video_widget = QVideoWidget()
            m_layout.addWidget(self.video_widget, 1)
            m_controls = QHBoxLayout()
            self.btn_play = QPushButton("⏯ Play/Pause")
            self.slider = QSlider(Qt.Horizontal)
            self.vol_slider = QSlider(Qt.Horizontal)
            self.vol_slider.setMaximumWidth(100)
            self.vol_slider.setValue(100)
            m_controls.addWidget(self.btn_play)
            m_controls.addWidget(self.slider)
            m_controls.addWidget(QLabel("Vol:"))
            m_controls.addWidget(self.vol_slider)
            m_layout.addLayout(m_controls)
            
            self.player = QMediaPlayer()
            self.audio = QAudioOutput()
            self.player.setAudioOutput(self.audio)
            self.player.setVideoOutput(self.video_widget)
            self.btn_play.clicked.connect(self._toggle_playback)
            self.player.positionChanged.connect(self.slider.setValue)
            self.player.durationChanged.connect(self.slider.setMaximum)
            self.slider.sliderMoved.connect(self.player.setPosition)
            self.vol_slider.valueChanged.connect(lambda v: self.audio.setVolume(v/100.0))
        self.stack.addWidget(self.media_widget)
        
        self._setup_shortcuts()
        self._load_current_item()

    def _setup_shortcuts(self):
        QShortcut(QKeySequence(Qt.Key_Right), self, self._next_item)
        QShortcut(QKeySequence(Qt.Key_Left), self, self._prev_item)
        QShortcut(QKeySequence(Qt.Key_Space), self, self._toggle_playback)
        QShortcut(QKeySequence(Qt.Key_Escape), self, self.close)
        QShortcut(QKeySequence(Qt.Key_Up), self, self._vol_up)
        QShortcut(QKeySequence(Qt.Key_Down), self, self._vol_down)

    def _vol_up(self):
        if HAS_MULTIMEDIA and hasattr(self, 'vol_slider'): self.vol_slider.setValue(min(100, self.vol_slider.value() + 10))
    def _vol_down(self):
        if HAS_MULTIMEDIA and hasattr(self, 'vol_slider'): self.vol_slider.setValue(max(0, self.vol_slider.value() - 10))

    def _toggle_playback(self):
        if hasattr(self, 'player'):
            if self.player.playbackState() == QMediaPlayer.PlayingState: self.player.pause()
            else: self.player.play()
            
    def _update_nav_buttons(self): 
        self.btn_prev.setEnabled(self.current_index > 0)
        self.btn_next.setEnabled(self.current_index < len(self.playlist) - 1)
        
    def _load_current_item(self):
        if not self.playlist: return
        self._update_nav_buttons()
        item = self.playlist[self.current_index]
        filepath = item['path']
        filename = item['name']
        ext = item['ext']
        
        self.lbl_title.setText(filename)
        self.lbl_counter.setText(f"{self.current_index + 1} / {len(self.playlist)}")
        self.setWindowTitle(f"Nexus Viewer - {filename}")
        
        if hasattr(self, 'player'): self.player.stop()
        
        if ext in ['.png', '.jpg', '.jpeg', '.bmp', '.gif', '.webp']: 
            self.stack.setCurrentIndex(1)
            self.img_view.set_image(QPixmap(filepath))
        elif ext in ['.txt', '.py', '.json', '.csv', '.md', '.log', '.xml', '.js', '.html', '.ini']:
            self.stack.setCurrentIndex(0)
            try:
                with open(filepath, 'r', encoding='utf-8', errors='replace') as f: self.txt_view.setPlainText(f.read())
            except Exception as e: 
                self.txt_view.setPlainText(str(e))
        elif ext in ['.mp3', '.wav', '.ogg', '.mp4', '.avi', '.mkv', '.mov'] and HAS_MULTIMEDIA: 
            self.stack.setCurrentIndex(2)
            self.player.setSource(QUrl.fromLocalFile(filepath))
            self.player.play()
        else: 
            self.stack.setCurrentIndex(0)
            self.txt_view.setPlainText("Format unsupported natively.")

    def _next_item(self):
        if self.current_index < len(self.playlist) - 1: 
            self.current_index += 1
            self._load_current_item()
            
    def _prev_item(self):
        if self.current_index > 0: 
            self.current_index -= 1
            self._load_current_item()
            
    def closeEvent(self, ev):
        if hasattr(self, 'player'): self.player.stop()
        super().closeEvent(ev)

# ---------------- View Engine Widgets ----------------
class InteractiveBreadcrumb(QWidget):
    pathClicked = Signal(str)
    def __init__(self, parent=None):
        super().__init__(parent)
        self.layout = QHBoxLayout(self)
        self.layout.setContentsMargins(2, 2, 2, 2)
        self.layout.setSpacing(2)
        self.layout.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.setStyleSheet("background-color: transparent;")

    def set_path(self, path: str):
        while self.layout.count():
            item = self.layout.takeAt(0)
            widget = item.widget()
            if widget: widget.deleteLater()
            
        def add_btn(text, target):
            if self.layout.count() > 0:
                lbl = QLabel("❯")
                lbl.setStyleSheet("color: #888; font-size: 10px; font-weight: bold; margin: 0 4px;")
                self.layout.addWidget(lbl)
            btn = QToolButton()
            btn.setText(text)
            btn.setCursor(Qt.PointingHandCursor)
            btn.clicked.connect(lambda checked=False, p=target: self.pathClicked.emit(p))
            self.layout.addWidget(btn)

        add_btn("💽 Root", "/")
        actual_path = path.split("] ")[-1] if "] " in path else path

        def build_smart(proto, label):
            add_btn(label, proto)
            p_str = actual_path.replace(proto, "").strip("/")
            if p_str:
                parts = p_str.split("/")
                curr = proto
                for part in parts: 
                    curr += part + "/"
                    add_btn(part, curr)

        if actual_path == "/": pass
        elif actual_path.startswith("trash://"): add_btn("🗑 Trash", "trash://")
        elif actual_path.startswith("fav://"): add_btn("⭐ Favorites", "fav://")
        else:
            matched = False
            for proto in SMART_PROTOCOLS.keys():
                if actual_path.startswith(proto):
                    build_smart(proto, f"💡 {proto.replace('://', '').upper().replace('_', ' ➔ ')}")
                    matched = True
                    break
            if not matched:
                parts = [p for p in actual_path.split("/") if p]
                current_build = "/"
                for part in parts: 
                    current_build += part + "/"
                    add_btn(part, current_build)
        self.layout.addStretch()

class NexusTableModel(QAbstractTableModel):
    def __init__(self, headers: List[str], rows: List[Dict], parent=None):
        super().__init__(parent)
        self.headers = headers
        self.all_rows = rows
        self.display_limit = CHUNK_SIZE
        self.colors = {"Red": QColor("#5c2121"), "Blue": QColor("#213c5c"), "Green": QColor("#215c2b"), "Gold": QColor("#5c4c21")}
        
    def rowCount(self, parent=QModelIndex()): return min(len(self.all_rows), self.display_limit)
    def columnCount(self, parent=QModelIndex()): return len(self.headers)
    def data(self, index: QModelIndex, role=Qt.DisplayRole):
        if not index.isValid(): return None
        row = self.all_rows[index.row()]
        col = index.column()
        if role == Qt.DisplayRole: return row["display"][col]
        elif role == Qt.UserRole: return row.get("user_data")
        elif role == Qt.UserRole + 1: return row.get("color_tag")
        elif role == Qt.DecorationRole and col == 0: return row.get("icon")
        elif role == Qt.TextAlignmentRole: return int(Qt.AlignRight | Qt.AlignVCenter) if self.headers[col] == "Size" else int(Qt.AlignLeft | Qt.AlignVCenter)
        elif role == Qt.ForegroundRole: return QBrush(QColor("#888888")) if row.get("is_hidden") else None
        elif role == Qt.BackgroundRole: return QBrush(self.colors[row.get("color_tag")]) if row.get("color_tag") in self.colors else None
        return None
        
    def headerData(self, section: int, orientation, role=Qt.DisplayRole): 
        return self.headers[section] if role == Qt.DisplayRole and orientation == Qt.Horizontal else None
        
    def sort(self, column: int, order=Qt.AscendingOrder):
        self.layoutAboutToBeChanged.emit()
        self.all_rows.sort(key=lambda x: x["sort_keys"][column], reverse=(order == Qt.DescendingOrder))
        self.display_limit = CHUNK_SIZE
        self.layoutChanged.emit()
        
    def canFetchMore(self, parent=QModelIndex()): return self.display_limit < len(self.all_rows)
    def fetchMore(self, parent=QModelIndex()):
        items = min(CHUNK_SIZE, len(self.all_rows) - self.display_limit)
        if items > 0: 
            self.beginInsertRows(parent, self.display_limit, self.display_limit + items - 1)
            self.display_limit += items
            self.endInsertRows()

class ScaledImageLabel(QLabel):
    def __init__(self):
        super().__init__()
        self.setMinimumHeight(200)
        self.setAlignment(Qt.AlignCenter)
        self.setStyleSheet("background-color: transparent; border: 1px solid #30363d; border-radius: 8px; padding: 5px;")
        self._pixmap = None
        self.eff = QGraphicsOpacityEffect(self)
        self.setGraphicsEffect(self.eff)
        self.anim = QPropertyAnimation(self.eff, b"opacity")
        self.anim.setDuration(400)
        self.anim.setEasingCurve(QEasingCurve.InOutQuad)
        
    def setPixmap(self, pm): 
        self._pixmap = pm
        self.update()
        self.eff.setOpacity(0.0)
        self.anim.setStartValue(0.0)
        self.anim.setEndValue(1.0)
        self.anim.start()
        
    def clear(self): 
        self._pixmap = None
        self.update()
        
    def paintEvent(self, event):
        super().paintEvent(event)
        if self._pixmap is not None and not self._pixmap.isNull() and self._pixmap.width() > 0:
            painter = QPainter(self)
            painter.setRenderHint(QPainter.Antialiasing)
            painter.setRenderHint(QPainter.SmoothPixmapTransform)
            scaled = self._pixmap.scaled(self.size(), Qt.KeepAspectRatio, Qt.SmoothTransformation)
            painter.drawPixmap((self.width() - scaled.width()) // 2, (self.height() - scaled.height()) // 2, scaled)

class SandboxTableView(QTableView):
    filesDroppedOS = Signal(list)
    internalDrop = Signal(str, bool)
    openRequest = Signal()
    def __init__(self, parent=None): 
        super().__init__(parent)
        self.setAcceptDrops(True)
        self.setDragEnabled(True)
        self.setDragDropMode(QAbstractItemView.DragDrop)
        
    def keyPressEvent(self, event: QKeyEvent): 
        if event.key() in (Qt.Key_Return, Qt.Key_Enter): self.openRequest.emit()
        else: super().keyPressEvent(event)
        
    def startDrag(self, supportedActions):
        sel = self.selectionModel().selectedRows()
        if not sel: return
        self.window()._current_drag_items = [self.model().data(self.model().index(idx.row(), 0), Qt.UserRole) for idx in sel if self.model().data(self.model().index(idx.row(), 0), Qt.UserRole)]
        drag = QDrag(self)
        mime = QMimeData()
        mime.setText("nexus_internal_drag")
        drag.setMimeData(mime)
        drag.exec(Qt.MoveAction | Qt.CopyAction)
        
    def dragEnterEvent(self, event: QDragEnterEvent): 
        if event.mimeData().hasUrls() or event.mimeData().text() == "nexus_internal_drag": event.acceptProposedAction()
        else: super().dragEnterEvent(event)
        
    def dragMoveEvent(self, event): 
        if event.mimeData().hasUrls() or event.mimeData().text() == "nexus_internal_drag": event.acceptProposedAction()
        else: super().dragMoveEvent(event)
        
    def dropEvent(self, event: QDropEvent):
        if event.mimeData().hasUrls(): 
            self.filesDroppedOS.emit([url.toLocalFile() for url in event.mimeData().urls()])
            event.acceptProposedAction()
        elif event.mimeData().text() == "nexus_internal_drag":
            idx = self.indexAt(event.position().toPoint())
            dest_path = self.window().current_prefix
            if idx.isValid():
                data = self.model().data(self.model().index(idx.row(), 0), Qt.UserRole)
                if data and data[0] == "folder": dest_path = data[1]
            self.internalDrop.emit(dest_path, bool(event.keyboardModifiers() & (Qt.ControlModifier | Qt.ShiftModifier)))
            event.acceptProposedAction()
        else: super().dropEvent(event)

class SandboxListWidget(QListWidget):
    filesDroppedOS = Signal(list)
    internalDrop = Signal(str, bool)
    openRequest = Signal()
    def __init__(self, parent=None): 
        super().__init__(parent)
        self.setAcceptDrops(True)
        self.setDragEnabled(True)
        self.setDragDropMode(QAbstractItemView.DragDrop)
        
    def keyPressEvent(self, event: QKeyEvent): 
        if event.key() in (Qt.Key_Return, Qt.Key_Enter): self.openRequest.emit()
        else: super().keyPressEvent(event)
        
    def startDrag(self, supportedActions):
        sel = self.selectedItems()
        if not sel: return
        self.window()._current_drag_items = [item.data(Qt.UserRole) for item in sel if item.data(Qt.UserRole)]
        drag = QDrag(self)
        mime = QMimeData()
        mime.setText("nexus_internal_drag")
        drag.setMimeData(mime)
        drag.exec(Qt.MoveAction | Qt.CopyAction)
        
    def dragEnterEvent(self, event: QDragEnterEvent): 
        if event.mimeData().hasUrls() or event.mimeData().text() == "nexus_internal_drag": event.acceptProposedAction()
        else: super().dragEnterEvent(event)
        
    def dragMoveEvent(self, event): 
        if event.mimeData().hasUrls() or event.mimeData().text() == "nexus_internal_drag": event.acceptProposedAction()
        else: super().dragMoveEvent(event)
        
    def dropEvent(self, event: QDropEvent):
        if event.mimeData().hasUrls(): 
            self.filesDroppedOS.emit([url.toLocalFile() for url in event.mimeData().urls()])
            event.acceptProposedAction()
        elif event.mimeData().text() == "nexus_internal_drag":
            item = self.itemAt(event.position().toPoint())
            dest_path = self.window().current_prefix
            if item and item.data(Qt.UserRole) and item.data(Qt.UserRole)[0] == "folder": dest_path = item.data(Qt.UserRole)[1]
            self.internalDrop.emit(dest_path, bool(event.keyboardModifiers() & (Qt.ControlModifier | Qt.ShiftModifier)))
            event.acceptProposedAction()
        else: super().dropEvent(event)

class InternalTreeWidget(QTreeWidget):
    itemDropped = Signal(str, bool)
    def __init__(self, parent=None): 
        super().__init__(parent)
        self.setAcceptDrops(True)
        
    def dragEnterEvent(self, event: QDragEnterEvent): 
        if event.mimeData().text() == "nexus_internal_drag": event.acceptProposedAction()
        else: super().dragEnterEvent(event)
        
    def dragMoveEvent(self, event): 
        if event.mimeData().text() == "nexus_internal_drag": event.acceptProposedAction()
        else: super().dragMoveEvent(event)
        
    def dropEvent(self, event: QDropEvent):
        if event.mimeData().text() == "nexus_internal_drag" and self.itemAt(event.position().toPoint()):
            self.itemDropped.emit(self.itemAt(event.position().toPoint()).data(0, Qt.UserRole), bool(event.keyboardModifiers() & (Qt.ControlModifier | Qt.ShiftModifier)))
            event.acceptProposedAction()

# ---------------- Main Application Window ----------------
class NexusVirtualManager(QMainWindow):
    def __init__(self):
        super().__init__()
        self.is_dark_mode = True 
        self.show_hidden = False
        self.current_prefix = "/"
        self.active_db_path = str(DB_FILE)
        
        self.history_back, self.history_forward = [], []
        self.v_clipboard = {"action": None, "items": []}
        self._current_drag_items, self._icon_cache, self._workers = [], {}, []
        self.icon_provider = QFileIconProvider() if HAS_ICON_PROVIDER else None
        
        self.loader_thread = None
        self.render_queue = []
        self.table_rows_buffer = []
        self.view_cache = {} 
        
        self.render_timer = QTimer(self)
        self.render_timer.timeout.connect(self._render_chunk)
        self.render_progress = None

        ensure_dirs()
        self.db = NexusDB(DB_FILE)
        self.setWindowTitle(APP_TITLE)
        self.resize(1600, 950)
        self.setFont(QFont("Segoe UI", 10))
        
        self._build_ui()
        self._setup_shortcuts()
        self.toggle_theme()
        self.refresh_all()

    def sys_log(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_console.appendPlainText(f"[{timestamp}] {message}")
        self.log_console.moveCursor(QTextCursor.End)

    def clear_cache(self): 
        self.view_cache.clear()

    def _build_ui(self):
        tb = QToolBar("Navigation")
        tb.setMovable(False)
        tb.setIconSize(QSize(20, 20))
        self.addToolBar(tb)
        
        self.act_back = QAction("◀", self)
        self.act_back.triggered.connect(self.nav_back)
        self.act_back.setEnabled(False)
        
        self.act_fwd = QAction("▶", self)
        self.act_fwd.triggered.connect(self.nav_forward)
        self.act_fwd.setEnabled(False)
        
        act_up = QAction("⬆ Up", self)
        act_up.triggered.connect(self.nav_up)
        
        act_new_folder = QAction("📂 Folder", self)
        act_new_folder.triggered.connect(self.create_folder)
        
        act_new_file = QAction("📄 File", self)
        act_new_file.triggered.connect(self.create_virtual_file)
        
        act_timeline = QAction("📅 Timeline Diary", self)
        act_timeline.triggered.connect(lambda: TimelineDiaryDialog(self.active_db_path, self).exec())
        
        act_analyzer = QAction("🧹 Space Analyzer", self)
        act_analyzer.triggered.connect(lambda: SpaceAnalyzerDialog(self.active_db_path, self).exec())
        
        act_bulk_del = QAction("🗑️ Bulk Delete", self)
        act_bulk_del.triggered.connect(lambda: BulkDeleterDialog(self.active_db_path, self).exec())
        
        act_load_ext = QAction("📂 Load DB...", self)
        act_load_ext.triggered.connect(self.load_external_db)
        
        self.act_view_mode = QAction("🖼 Grid View", self)
        self.act_view_mode.triggered.connect(self.toggle_view_mode)
        
        self.act_toggle_sidebar = QAction("📊 Inspector", self)
        self.act_toggle_sidebar.triggered.connect(self.toggle_sidebar)
        
        act_toggle_log = QAction("📝 Console", self)
        act_toggle_log.triggered.connect(lambda: self.log_dock.setVisible(not self.log_dock.isVisible()))
        
        act_help = QAction("❓ Help", self)
        act_help.triggered.connect(self.show_help)
        
        tb.addActions([self.act_back, self.act_fwd, act_up])
        tb.addSeparator()
        tb.addActions([act_new_folder, act_new_file])
        tb.addSeparator()
        tb.addActions([act_timeline, act_analyzer, act_bulk_del, act_load_ext])
        tb.addSeparator()
        tb.addActions([self.act_view_mode, self.act_toggle_sidebar, act_toggle_log, act_help])
        
        empty = QWidget()
        empty.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        tb.addWidget(empty)
        
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        vbox = QVBoxLayout(central_widget)
        vbox.setContentsMargins(8, 8, 8, 8)
        
        nav_row = QHBoxLayout()
        self.breadcrumb = InteractiveBreadcrumb()
        self.breadcrumb.pathClicked.connect(self.nav_to_path)
        
        self.breadcrumb_scroll = QScrollArea()
        self.breadcrumb_scroll.setWidgetResizable(True)
        self.breadcrumb_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.breadcrumb_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.breadcrumb_scroll.setFrameShape(QScrollArea.NoFrame)
        self.breadcrumb_scroll.setStyleSheet("background: transparent; border: none;")
        self.breadcrumb_scroll.setFixedHeight(40)
        self.breadcrumb_scroll.setWidget(self.breadcrumb)
        
        self.local_filter = QLineEdit()
        self.local_filter.setPlaceholderText("Filter current view instantly...")
        self.local_filter.setMaximumWidth(300)
        self.local_filter.textChanged.connect(self.filter_current_view)
        
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Global Search (Ctrl+F)...")
        self.search_box.setMaximumWidth(300)
        self.search_box.returnPressed.connect(self.run_global_search)
        
        nav_row.addWidget(self.breadcrumb_scroll, stretch=1)
        nav_row.addWidget(self.local_filter)
        nav_row.addWidget(self.search_box)
        vbox.addLayout(nav_row)

        self.view_stack = QStackedWidget()
        
        self.file_table = SandboxTableView()
        self.file_table.filesDroppedOS.connect(self.on_files_dropped)
        self.file_table.internalDrop.connect(self.execute_internal_drop)
        self.file_table.verticalHeader().setVisible(False)
        self.file_table.horizontalHeader().setStretchLastSection(True)
        self.file_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.file_table.setSortingEnabled(True)
        self.file_table.clicked.connect(self.on_file_click)
        self.file_table.doubleClicked.connect(self.open_selected)
        self.file_table.openRequest.connect(self.open_selected)
        self.file_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.file_table.customContextMenuRequested.connect(self.context_menu)
        self.view_stack.addWidget(self.file_table)

        self.file_grid = SandboxListWidget()
        self.file_grid.filesDroppedOS.connect(self.on_files_dropped)
        self.file_grid.internalDrop.connect(self.execute_internal_drop)
        self.file_grid.setViewMode(QListView.IconMode)
        self.file_grid.setGridSize(QSize(140, 160))
        self.file_grid.setIconSize(QSize(80, 80))
        self.file_grid.setUniformItemSizes(True)
        self.file_grid.setWordWrap(True)
        self.file_grid.setTextElideMode(Qt.ElideRight)
        self.file_grid.setSpacing(10)
        self.file_grid.setResizeMode(QListView.Adjust)
        self.file_grid.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.file_grid.setContextMenuPolicy(Qt.CustomContextMenu)
        self.file_grid.customContextMenuRequested.connect(lambda pos: self.context_menu(pos, is_grid=True))
        self.file_grid.itemDoubleClicked.connect(self.open_selected)
        self.file_grid.itemClicked.connect(self.on_grid_click)
        self.file_grid.openRequest.connect(self.open_selected)
        self.view_stack.addWidget(self.file_grid)
        
        vbox.addWidget(self.view_stack)

        # Log Console Dock
        self.log_dock = QDockWidget("Live System Console", self)
        self.log_dock.setAllowedAreas(Qt.BottomDockWidgetArea)
        self.log_console = QPlainTextEdit()
        self.log_console.setReadOnly(True)
        self.log_console.setFont(QFont("Consolas", 9))
        self.log_dock.setWidget(self.log_console)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.log_dock)
        self.log_dock.hide()

        self.tree_dock = QDockWidget("Data Engine & Views", self)
        self.tree_dock.setAllowedAreas(Qt.LeftDockWidgetArea)
        self.folder_tree = InternalTreeWidget()
        self.folder_tree.setHeaderHidden(True)
        self.folder_tree.itemExpanded.connect(self.on_folder_expand)
        self.folder_tree.itemClicked.connect(self.on_tree_click)
        self.tree_dock.setWidget(self.folder_tree)
        self.addDockWidget(Qt.LeftDockWidgetArea, self.tree_dock)

        self.right_dock = QDockWidget("Inspector", self)
        self.right_dock.setAllowedAreas(Qt.RightDockWidgetArea)
        self.right_tabs = QTabWidget()
        
        # Preview Tab
        preview_container = QWidget()
        prev_layout = QVBoxLayout(preview_container)
        self.preview_stack = QStackedWidget()
        self.preview_image = ScaledImageLabel()
        self.preview_stack.addWidget(self.preview_image)
        self.preview_text = QPlainTextEdit()
        self.preview_text.setReadOnly(True)
        self.preview_stack.addWidget(self.preview_text)
        
        self.media_container = QWidget()
        media_layout = QVBoxLayout(self.media_container)
        self.lbl_media_title = QLabel("Audio Engine")
        self.lbl_media_title.setAlignment(Qt.AlignCenter)
        media_controls = QHBoxLayout()
        self.btn_play = QPushButton("▶")
        self.btn_pause = QPushButton("⏸")
        self.btn_stop = QPushButton("⏹")
        media_controls.addWidget(self.btn_play)
        media_controls.addWidget(self.btn_pause)
        media_controls.addWidget(self.btn_stop)
        self.media_slider = QSlider(Qt.Horizontal)
        media_layout.addWidget(self.lbl_media_title)
        media_layout.addLayout(media_controls)
        media_layout.addWidget(self.media_slider)
        self.preview_stack.addWidget(self.media_container)
        
        if HAS_MULTIMEDIA:
            self.player = QMediaPlayer()
            self.audio_output = QAudioOutput()
            self.player.setAudioOutput(self.audio_output)
            self.btn_play.clicked.connect(self.player.play)
            self.btn_pause.clicked.connect(self.player.pause)
            self.btn_stop.clicked.connect(self.player.stop)
            self.player.positionChanged.connect(self.media_slider.setValue)
            self.player.durationChanged.connect(self.media_slider.setMaximum)
            self.media_slider.sliderMoved.connect(self.player.setPosition)
            
        prev_layout.addWidget(self.preview_stack)
        self.right_tabs.addTab(preview_container, "Preview")

        # Props Tab
        props_container = QWidget()
        ed_layout = QFormLayout(props_container)
        self.ed_name = QLineEdit()
        self.ed_target = QLineEdit()
        self.ed_target.setReadOnly(True)
        self.ed_custom_tags = QLineEdit()
        self.ed_custom_tags.setPlaceholderText("tag1, tag2")
        self.ed_secondary = QLineEdit()
        self.ed_tag = QComboBox()
        self.ed_tag.addItems(["None", "Red", "Green", "Blue", "Gold"])
        self.btn_save_ed = QPushButton("Apply Properties")
        self.btn_save_ed.clicked.connect(self.save_properties_editor)
        self.btn_calc_hash = QPushButton("Compute SHA-256")
        self.btn_calc_hash.clicked.connect(self.compute_checksum)
        
        ed_layout.addRow("Name:", self.ed_name)
        ed_layout.addRow("Sec Name:", self.ed_secondary)
        ed_layout.addRow("Target:", self.ed_target)
        ed_layout.addRow("Labels:", self.ed_custom_tags)
        ed_layout.addRow("Color:", self.ed_tag)
        ed_layout.addRow("", self.btn_save_ed)
        ed_layout.addRow("", self.btn_calc_hash)
        self.right_tabs.addTab(props_container, "Properties")

        # Analytics Tab
        stats_container = QWidget()
        st_layout = QVBoxLayout(stats_container)
        self.lbl_stats_txt = QTextBrowser()
        st_layout.addWidget(self.lbl_stats_txt)
        self.right_tabs.addTab(stats_container, "Analytics")

        # Charts Tab
        charts_container = QWidget()
        ch_layout = QVBoxLayout(charts_container)
        ctrl_lay = QHBoxLayout()
        self.stat_combo = QComboBox()
        self.stat_combo.addItems(["Distribution by Extension (Size)", "Distribution by Extension (Count)", "Top 10 Largest Files", "Storage Ratio (Pie Chart)", "File Count Over Time"])
        self.stat_combo.currentIndexChanged.connect(self.update_statistics)
        ctrl_lay.addWidget(QLabel("Investigation:"))
        ctrl_lay.addWidget(self.stat_combo, 1)
        ch_layout.addLayout(ctrl_lay)

        self.chart_scroll = QScrollArea()
        self.chart_scroll.setWidgetResizable(True)
        self.chart_container = QWidget()
        self.chart_lay = QVBoxLayout(self.chart_container)
        
        if MATPLOTLIB_AVAILABLE: 
            self.figure = Figure(figsize=(5, 6))
            self.canvas = FigureCanvas(self.figure)
            self.canvas.setMinimumHeight(500)
            self.chart_lay.addWidget(self.canvas)
        else: 
            self.figure = self.canvas = None
            self.chart_lay.addWidget(QLabel("Matplotlib not installed. Please pip install matplotlib."))
            
        self.chart_scroll.setWidget(self.chart_container)
        ch_layout.addWidget(self.chart_scroll)
        self.right_tabs.addTab(charts_container, "Charts")

        self.right_dock.setWidget(self.right_tabs)
        self.addDockWidget(Qt.RightDockWidgetArea, self.right_dock)
        self.status = QStatusBar()
        self.setStatusBar(self.status)
        self.sys_log("Nexus OS Master Edition Initialized.")

    def _setup_shortcuts(self):
        QShortcut(QKeySequence("Ctrl+C"), self, self.cmd_copy)
        QShortcut(QKeySequence("Ctrl+X"), self, self.cmd_cut)
        QShortcut(QKeySequence("Ctrl+V"), self, self.cmd_paste)
        QShortcut(QKeySequence("Delete"), self, self.cmd_delete)
        QShortcut(QKeySequence("F2"), self, self.cmd_rename)
        QShortcut(QKeySequence("Ctrl+Shift+N"), self, self.create_folder)
        QShortcut(QKeySequence("Ctrl+N"), self, self.create_virtual_file)
        QShortcut(QKeySequence("Ctrl+F"), self, self.search_box.setFocus)
        QShortcut(QKeySequence("Ctrl+O"), self, self.open_selected_nexus)
        
        QShortcut(QKeySequence("Backspace"), self, self.nav_back)
        QShortcut(QKeySequence("Shift+Backspace"), self, self.nav_forward)
        QShortcut(QKeySequence("Alt+Up"), self, self.nav_up)

    def show_help(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("Nexus OS Instructions")
        dlg.setStyleSheet(FUTURISTIC_THEME)
        dlg.resize(600, 400)
        lay = QVBoxLayout(dlg)
        txt = QPlainTextEdit()
        txt.setPlainText("""NEXUS OS - KEYBOARD & FEATURE GUIDE

[Keyboard Navigation]
Backspace   : Go Back
Shift+Back  : Go Forward
Alt+Up      : Go Up One Directory
Ctrl+F      : Focus Global Search
Enter       : Open selected folder or OS File

[Viewer Controls (Ctrl+O)]
Spacebar    : Play/Pause Media
Right/Left  : Next/Previous Item
Up/Down     : Volume Control
Escape      : Close Viewer

[Operations]
F2          : Rename (Select multiple files to Bulk Rename sequentially!)
Delete      : Send to Trash / Permanently Delete
Ctrl+C/V/X  : Copy, Paste, Cut
""")
        txt.setReadOnly(True)
        lay.addWidget(txt)
        dlg.exec()

    def filter_current_view(self, text):
        term = text.lower()
        if self.view_stack.currentIndex() == 0:
            for row in range(self.file_table.model().rowCount()):
                name = self.file_table.model().data(self.file_table.model().index(row, 0), Qt.DisplayRole)
                self.file_table.setRowHidden(row, term not in str(name).lower())
        else:
            for i in range(self.file_grid.count()):
                item = self.file_grid.item(i)
                item.setHidden(term not in item.text().lower())

    def toggle_theme(self): 
        self.setStyleSheet(FUTURISTIC_THEME)
        
    def toggle_view_mode(self):
        self.view_stack.setCurrentIndex(1 if self.view_stack.currentIndex() == 0 else 0)
        self.act_view_mode.setText("📄 List View" if self.view_stack.currentIndex() == 1 else "🖼 Grid View")
        
    def toggle_sidebar(self): 
        self.right_dock.setVisible(not self.right_dock.isVisible())
        
    def toggle_hidden(self): 
        self.show_hidden = self.act_toggle_hidden.isChecked()
        self.clear_cache()
        self.load_directory(self.current_prefix)

    def _get_native_icon(self, real_path: str, is_folder: bool, ext: str = "") -> QIcon:
        if is_folder: return self.style().standardIcon(QStyle.SP_DirIcon)
        ext = str(ext).lower()
        if ext in ['.png', '.jpg', '.jpeg', '.bmp', '.gif', '.webp'] and real_path and os.path.exists(real_path):
            if real_path not in self._icon_cache:
                try: self._icon_cache[real_path] = QIcon(QPixmap(real_path).scaled(96, 96, Qt.KeepAspectRatio, Qt.SmoothTransformation))
                except Exception: pass
            return self._icon_cache.get(real_path, self.style().standardIcon(QStyle.SP_FileIcon))
        if ext not in self._icon_cache:
            self._icon_cache[ext] = self.icon_provider.icon(QFileInfo(real_path)) if HAS_ICON_PROVIDER and self.icon_provider and real_path and os.path.exists(real_path) else self.style().standardIcon(QStyle.SP_FileIcon)
        return self._icon_cache[ext]

    def _is_smart_path(self, p):
        if p.startswith("y_m_f://"): return True
        for proto in SMART_PROTOCOLS:
            if p.startswith(proto): return True
        return False

    def load_external_db(self):
        path, _ = QFileDialog.getOpenFileName(self, "Load Nexus DB", "", "SQLite DB (*.db)")
        if path:
            dest = VIEWS_DIR / Path(path).name
            if str(Path(path).resolve()) != str(dest.resolve()):
                shutil.copy2(path, dest)
            self.refresh_all()
            self.status.showMessage(f"Imported DB to Compiled Views: {Path(path).name}")
            self.sys_log(f"Imported External Database: {Path(path).name}")

    # ---------- Navigation & View Construction ----------
    def refresh_tree(self):
        self.folder_tree.clear()
        sys_root = QTreeWidgetItem(self.folder_tree, ["💽 Main System DB"])
        sys_root.setData(0, Qt.UserRole, "/")
        sys_root.setIcon(0, self.style().standardIcon(QStyle.SP_DirHomeIcon))
        sys_root.setExpanded(True)
        
        QTreeWidgetItem(sys_root, ["⭐ Favorites"]).setData(0, Qt.UserRole, "fav://")
        QTreeWidgetItem(sys_root, ["🗑 Trash Bin"]).setData(0, Qt.UserRole, "trash://")
        
        smart = QTreeWidgetItem(sys_root, ["💡 Dynamic Smart Views"])
        smart.setIcon(0, self.style().standardIcon(QStyle.SP_FileDialogDetailedView))
        smart.setExpanded(True)
        
        QTreeWidgetItem(smart, ["🏷️ By Custom Tags"]).setData(0, Qt.UserRole, "tags://")
        QTreeWidgetItem(smart, ["🗂 Stat: Year ➔ Month ➔ Folder"]).setData(0, Qt.UserRole, "y_m_f://")
        for proto, cols in SMART_PROTOCOLS.items():
            if proto == "tags://": continue
            name = " ➔ ".join([c.capitalize() for c in cols])
            QTreeWidgetItem(smart, [f"🗂 {name}"]).setData(0, Qt.UserRole, proto)

        compiled_root = QTreeWidgetItem(self.folder_tree, ["📦 Compiled Databases"])
        compiled_root.setIcon(0, self.style().standardIcon(QStyle.SP_DriveHDIcon))
        compiled_root.setExpanded(True)
        for view_db in VIEWS_DIR.glob("*.db"):
            node = QTreeWidgetItem(compiled_root, [view_db.stem])
            node.setData(0, Qt.UserRole, f"db://{view_db.name}")
            node.setIcon(0, self.style().standardIcon(QStyle.SP_FileIcon))

    def refresh_all(self):
        self.refresh_tree()
        self.load_directory(self.current_prefix)

    def on_tree_click(self, item: QTreeWidgetItem, col: int):
        path = item.data(0, Qt.UserRole)
        if path and path.startswith("db://"):
            db_name = path.replace("db://", "")
            self.active_db_path = str(VIEWS_DIR / db_name)
            self.db.close()
            self.db = NexusDB(Path(self.active_db_path))
            self.clear_cache()
            self.status.showMessage(f"Connected to isolated DB: {db_name}")
            self.sys_log(f"Switched context to Isolated DB: {db_name}")
            self.nav_to_path("/")
        elif path:
            if self.active_db_path != str(DB_FILE):
                self.active_db_path = str(DB_FILE)
                self.db.close()
                self.db = NexusDB(DB_FILE)
                self.clear_cache()
                self.status.showMessage("Reconnected to Main System DB")
                self.sys_log("Reconnected to Main System DB.")
            self.nav_to_path(path)

    def on_folder_expand(self, item: QTreeWidgetItem):
        if item.data(0, Qt.UserRole + 1) or item.data(0, Qt.UserRole) in ["fav://", "trash://"] or self._is_smart_path(str(item.data(0, Qt.UserRole))) or str(item.data(0, Qt.UserRole)).startswith("db://"): return
        cur = self.db.conn.cursor()
        cur.execute(f"SELECT name FROM virtual_fs WHERE parent_path = ? AND is_folder = 1 AND in_trash = 0 {'AND is_hidden=0' if not self.show_hidden else ''} ORDER BY name", (item.data(0, Qt.UserRole),))
        for sf in [str(r[0]) for r in cur.fetchall() if r[0]]:
            child = QTreeWidgetItem(item, [sf])
            child.setData(0, Qt.UserRole, f"{item.data(0, Qt.UserRole)}{sf}/")
            child.setIcon(0, self.style().standardIcon(QStyle.SP_DirIcon))
            child.setChildIndicatorPolicy(QTreeWidgetItem.ShowIndicator)
        item.setData(0, Qt.UserRole + 1, True)

    def nav_to_path(self, path: str, record_history=True):
        if record_history and self.current_prefix != path: 
            self.history_back.append(self.current_prefix)
            self.history_forward.clear()
        self.load_directory(path)

    def nav_back(self):
        if self.history_back: 
            self.history_forward.append(self.current_prefix)
            self.nav_to_path(self.history_back.pop(), False)
            self.act_back.setEnabled(len(self.history_back) > 0)
            self.act_fwd.setEnabled(len(self.history_forward) > 0)
            
    def nav_forward(self):
        if self.history_forward: 
            self.history_back.append(self.current_prefix)
            self.nav_to_path(self.history_forward.pop(), False)
            self.act_back.setEnabled(len(self.history_back) > 0)
            self.act_fwd.setEnabled(len(self.history_forward) > 0)
            
    def nav_up(self):
        if self.current_prefix not in ["/", "trash://", "fav://"] and not self._is_smart_path(self.current_prefix): 
            self.nav_to_path("/" if len(self.current_prefix.strip("/").split("/")) <= 1 else "/" + "/".join(self.current_prefix.strip("/").split("/")[:-1]) + "/")
        elif self._is_smart_path(self.current_prefix):
            parts = [p for p in self.current_prefix.split("/") if p]
            if len(parts) <= 1: self.nav_to_path("/")
            else: self.nav_to_path("/".join(parts[:-1]) + "/")

    def load_directory(self, target_path: str):
        self.current_prefix = target_path
        self.breadcrumb.set_path(f"[{Path(self.active_db_path).stem}] " + target_path)
        QTimer.singleShot(100, lambda: self.breadcrumb_scroll.horizontalScrollBar().setValue(self.breadcrumb_scroll.horizontalScrollBar().maximum()))
        
        self.search_box.clear()
        self.local_filter.clear()
        self.act_back.setEnabled(len(self.history_back) > 0)
        self.act_fwd.setEnabled(len(self.history_forward) > 0)
        
        if target_path in self.view_cache: 
            self._start_ui_render(*self.view_cache[target_path], cached=True)
            return
            
        if self.loader_thread and self.loader_thread.isRunning():
            self.loader_thread.cancel()
            self.loader_thread.quit()
            self.loader_thread.wait()

        self.status.showMessage("Computing View...")
        self.loader_thread = DataLoaderThread(self.active_db_path, target_path, self.show_hidden, self)
        self.loader_thread.data_ready.connect(lambda f, fl: self._start_ui_render(f, fl, cached=False))
        self.loader_thread.start()

    def _start_ui_render(self, folders, files, cached=False):
        if not cached: self.view_cache[self.current_prefix] = (folders, files) 
        self.file_grid.clear()
        self.render_queue = folders + files
        total = len(self.render_queue)
        self.table_rows_buffer = []
        
        if total > CHUNK_SIZE:
            self.render_progress = QProgressDialog(f"{'Loading from RAM Cache' if cached else 'Rendering'} {total} items...", "Cancel", 0, total, self)
            self.render_progress.setWindowModality(Qt.WindowModal)
            self.render_progress.show()
        else: 
            self.render_progress = None

        self.file_grid.setUpdatesEnabled(False)
        self.render_timer.start(1) 

    def _render_chunk(self):
        if not self.render_queue:
            self.render_timer.stop()
            self.file_grid.setUpdatesEnabled(True)
            self.file_table.setModel(NexusTableModel(["Name", "Size", "Type", "Location", "Tag"], self.table_rows_buffer))
            self.file_table.setColumnWidth(0, 350); self.file_table.setColumnWidth(1, 100); self.file_table.setColumnWidth(2, 100); self.file_table.setColumnWidth(3, 400)
            if self.render_progress: self.render_progress.close()
            self.status.showMessage("Rendering complete.", 3000)
            self.update_statistics()
            return

        if self.render_progress and self.render_progress.wasCanceled(): 
            self.render_queue.clear()
            return

        chunk = self.render_queue[:CHUNK_SIZE]
        self.render_queue = self.render_queue[CHUNK_SIZE:]
        dir_icon = self.style().standardIcon(QStyle.SP_DirIcon)

        for item in chunk:
            if len(item) == 8: # Folder with count/size
                db_id, pp, f_name, c_tag, sec_n, is_h, count, size = item
                v_path = f"{pp}{f_name}/" if not f_name.endswith("/") else f"{pp}{f_name}"
                disp_name = f"{f_name}\n({count} items, {human_size(size)})" if count > 0 else f"{f_name}"
                self.table_rows_buffer.append({"display": [f_name, human_size(size), f"Virtual Folder ({count})", pp, c_tag], "sort_keys": [(0, f_name.lower()), (0, size), (0, count), (0, pp.lower()), (0, c_tag)], "user_data": ("folder", v_path, db_id), "color_tag": c_tag, "is_hidden": is_h, "icon": dir_icon})
                list_item = QListWidgetItem(dir_icon, disp_name)
                list_item.setData(Qt.UserRole, ("folder", v_path, db_id))
                self.file_grid.addItem(list_item)
            else: # File
                db_id, n, s, ext, rp, mod, c_tag, sec_n, is_h = item[:9]
                icon = self._get_native_icon(rp, False, ext)
                s_val = s if s else 0
                ext_str = str(ext) if ext else "file"
                disp_name = f"{n}\n({sec_n})" if sec_n else str(n)
                self.table_rows_buffer.append({"display": [str(n), human_size(s_val), ext_str, str(rp) if rp else "", c_tag], "sort_keys": [(1, str(n).lower()), (1, s_val), (1, ext_str.lower()), (1, str(rp).lower() if rp else ""), (1, c_tag)], "user_data": ("file", str(rp), db_id), "color_tag": c_tag, "is_hidden": is_h, "icon": icon})
                list_item = QListWidgetItem(icon, disp_name)
                list_item.setData(Qt.UserRole, ("file", str(rp), db_id))
                self.file_grid.addItem(list_item)

        if self.render_progress: 
            self.render_progress.setValue(self.render_progress.maximum() - len(self.render_queue))

    def run_global_search(self):
        term = self.search_box.text().strip()
        if not term: return self.load_directory(self.current_prefix)
        cur = self.db.conn.cursor()
        cur.execute(f"SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE (name LIKE ? OR secondary_name LIKE ? OR custom_tags LIKE ?) AND is_folder=0 AND in_trash=0", (f"%{term}%", f"%{term}%", f"%{term}%"))
        self._start_ui_render([], cur.fetchall(), cached=False)
        self.sys_log(f"Global Search executed for: {term}")

    def _trigger_preview(self, typ, path, db_id, name):
        if db_id == -1: return 
        self.btn_save_ed.setProperty("db_id", db_id)
        self.btn_save_ed.setProperty("is_folder", typ == "folder")
        
        if HAS_MULTIMEDIA and hasattr(self, 'player'): self.player.stop()
        self.preview_image.clear()
        self.preview_text.clear()
        
        if typ == "folder": 
            self.preview_stack.setCurrentIndex(1)
            self.ed_target.setText(f"Virtual Container: {path}")
            self.preview_text.setPlainText(f"Directory Data:\n{path}")
            return
            
        cur = self.db.conn.cursor()
        cur.execute("SELECT real_path, size, extension, modified, secondary_name, custom_tags FROM virtual_fs WHERE id = ?", (db_id,))
        row = cur.fetchone()
        
        if not row: return
        real_path, size, ext, mod, sec_n, custom_tags = row
        self.ed_target.setText(real_path)
        ext = str(ext).lower()
        self.ed_secondary.setText(str(sec_n) if sec_n else "")
        self.ed_custom_tags.setText(str(custom_tags) if custom_tags else "")
        
        if real_path and os.path.exists(real_path):
            if ext in [".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp"]:
                self.preview_stack.setCurrentIndex(0)
                loader = ImageLoader(real_path, parent=self)
                loader.finished.connect(self.on_image_loaded)
                self._register_worker(loader)
                loader.start()
            elif ext in [".txt", ".csv", ".json", ".xml", ".py", ".md", ".log", ".ini"]:
                self.preview_stack.setCurrentIndex(1)
                try:
                    with open(real_path, 'r', encoding='utf-8', errors='replace') as f: 
                        self.preview_text.setPlainText(f.read(5000))
                except Exception as e: 
                    self.preview_text.setPlainText(f"Error reading file: {e}")
            elif ext in [".mp3", ".wav", ".ogg"] and HAS_MULTIMEDIA:
                self.preview_stack.setCurrentIndex(2)
                self.lbl_media_title.setText(name)
                self.player.setSource(QUrl.fromLocalFile(real_path))
            else: 
                self.preview_stack.setCurrentIndex(1)
                self.preview_text.setPlainText(f"File: {name}\nTarget: {real_path}\nSize: {human_size(size)}\nModified: {mod}")
        else: 
            self.preview_stack.setCurrentIndex(1)
            self.preview_text.setPlainText(f"Virtual File: {name}\nDisconnected or missing local file.")

    def on_image_loaded(self, path, image):
        if image and not image.isNull() and image.width() > 0: 
            self.preview_image.setPixmap(QPixmap.fromImage(image))
        else: 
            self.preview_image.clear()
            self.preview_text.setPlainText("Image preview failed to load.")
            self.preview_stack.setCurrentIndex(1)

    def on_file_click(self, index: QModelIndex):
        if self.file_table.model() and self.file_table.model().data(self.file_table.model().index(index.row(), 0), Qt.UserRole):
            data = self.file_table.model().data(self.file_table.model().index(index.row(), 0), Qt.UserRole)
            if data[2] == -1: return 
            name = self.file_table.model().data(self.file_table.model().index(index.row(), 0), Qt.DisplayRole)
            self.ed_name.setText(str(name))
            self.ed_tag.setCurrentIndex(max(0, self.ed_tag.findText(self.file_table.model().data(self.file_table.model().index(index.row(), 0), Qt.UserRole + 1))))
            self._trigger_preview(data[0], data[1], data[2], name)

    def on_grid_click(self, item: QListWidgetItem):
        if item.data(Qt.UserRole): 
            if item.data(Qt.UserRole)[2] == -1: return
            self.ed_name.setText(str(item.text().split("\n")[0]))
            self._trigger_preview(item.data(Qt.UserRole)[0], item.data(Qt.UserRole)[1], item.data(Qt.UserRole)[2], item.text())

    def save_properties_editor(self):
        db_id = self.btn_save_ed.property("db_id")
        if not db_id or db_id == -1 or not self.ed_name.text().strip(): return
        cur = self.db.conn.cursor()
        new_name = self.ed_name.text().strip()
        new_sec = self.ed_secondary.text().strip()
        new_tag = "" if self.ed_tag.currentText() == "None" else self.ed_tag.currentText()
        new_custom_tags = self.ed_custom_tags.text().strip()
        
        if self.btn_save_ed.property("is_folder"):
            cur.execute("SELECT parent_path, name FROM virtual_fs WHERE id = ?", (db_id,))
            pp, old_name = cur.fetchone()
            cur.execute("UPDATE virtual_fs SET name=?, secondary_name=?, color_tag=?, custom_tags=? WHERE id=?", (new_name, new_sec, new_tag, new_custom_tags, db_id))
            cur.execute("UPDATE virtual_fs SET parent_path = ? || SUBSTR(parent_path, LENGTH(?) + 1) WHERE parent_path LIKE ?", (f"{pp}{new_name}/", f"{pp}{old_name}/", f"{pp}{old_name}/%"))
        else: 
            cur.execute("UPDATE virtual_fs SET name=?, secondary_name=?, color_tag=?, custom_tags=? WHERE id=?", (new_name, new_sec, new_tag, new_custom_tags, db_id))
            
        self.db.conn.commit()
        self.clear_cache()
        self.load_directory(self.current_prefix)
        self.status.showMessage("Virtual Properties Saved.", 3000)
        self.sys_log(f"Properties updated for DB ID: {db_id}")

    def compute_checksum(self):
        rp = self.ed_target.text()
        if not rp or not os.path.exists(rp) or os.path.isdir(rp): 
            return QMessageBox.warning(self, "Error", "Invalid or missing physical file.")
        self.status.showMessage("Computing SHA-256 Hash...")
        calc = HashCalculator(rp, self)
        calc.finished.connect(lambda h: (self.sys_log(f"Calculated Hash for {os.path.basename(rp)}: {h}"), QMessageBox.information(self, "SHA-256 Checksum", f"File: {os.path.basename(rp)}\n\nHash:\n{h}")))
        self._register_worker(calc)
        calc.start()

    # ---------- Operations & Integrations ----------
    def open_selected(self):
        sel = self._get_selected_items()
        if not sel: return
        data = sel[0]
        if data[0] == "folder" or data[2] == -1: 
            self.nav_to_path(data[1])
        elif data[1] and os.path.exists(data[1]):
            try: os.startfile(data[1]) if sys.platform=="win32" else os.system(f"open '{data[1]}'" if sys.platform=="darwin" else f"xdg-open '{data[1]}'")
            except Exception as e: QMessageBox.warning(self, "Open", str(e))
        else: 
            QMessageBox.warning(self, "Not Found", "Target file is missing locally.")

    def open_selected_nexus(self):
        sel = self._get_selected_items()
        if not sel or sel[0][2] == -1: return
        target_typ, target_rp, target_db_id = sel[0]
        if target_typ != "file" or not target_rp or not os.path.exists(target_rp): 
            return QMessageBox.warning(self, "Nexus Viewer", "Cannot open virtual folder or missing local file.")
            
        playlist = []
        start_index = 0
        if self.view_stack.currentIndex() == 0:  
            model = self.file_table.model()
            for r in range(model.rowCount()):
                data = model.data(model.index(r, 0), Qt.UserRole)
                name = model.data(model.index(r, 0), Qt.DisplayRole)
                if data and data[0] == "file" and data[1] and os.path.exists(data[1]):
                    playlist.append({'path': data[1], 'name': name, 'ext': os.path.splitext(data[1])[1].lower()})
                    if data[2] == target_db_id: start_index = len(playlist) - 1
        else: 
            for i in range(self.file_grid.count()):
                item = self.file_grid.item(i)
                data = item.data(Qt.UserRole)
                if data and data[0] == "file" and data[1] and os.path.exists(data[1]):
                    playlist.append({'path': data[1], 'name': item.text().split("\n")[0], 'ext': os.path.splitext(data[1])[1].lower()})
                    if data[2] == target_db_id: start_index = len(playlist) - 1

        self.viewer = NexusViewer(playlist, start_index, self)
        self.viewer.show()

    def export_virtual_to_os(self):
        items_to_export = self._get_selected_items()
        if not items_to_export:
            if self.view_stack.currentIndex() == 0:
                model = self.file_table.model()
                for r in range(model.rowCount()): items_to_export.append(model.data(model.index(r, 0), Qt.UserRole))
            else:
                for i in range(self.file_grid.count()): items_to_export.append(self.file_grid.item(i).data(Qt.UserRole))

        if not items_to_export: return QMessageBox.warning(self, "Export", "The current view is empty.")
            
        dest_dir = QFileDialog.getExistingDirectory(self, "Select OS Destination to Materialize")
        if not dest_dir: return
        
        self.export_dlg = QProgressDialog("Materializing view to physical OS...", "Cancel", 0, len(items_to_export), self)
        self.export_dlg.setWindowModality(Qt.WindowModal)
        self.export_dlg.show()
        
        self.mat_thread = MaterializeThread(str(self.db.path), self.current_prefix, dest_dir, self)
        self.mat_thread.progress.connect(lambda c,t,m: (self.export_dlg.setValue(int((c/max(1,t))*100)), self.export_dlg.setLabelText(m)))
        self.export_dlg.canceled.connect(self.mat_thread.cancel)
        self.mat_thread.finished.connect(lambda p: (self.export_dlg.close(), QMessageBox.information(self, "Success", f"Structure materialized at:\n{p}"), self.sys_log("Materialized Virtual Structure to OS.")))
        self.mat_thread.error.connect(lambda e: (self.export_dlg.close(), QMessageBox.critical(self, "Error", f"Failed:\n{e}")))
        self._register_worker(self.mat_thread)
        self.mat_thread.start()

    def export_csv(self, sel_items):
        csv_path, _ = QFileDialog.getSaveFileName(self, "Export to CSV", "", "CSV Files (*.csv)")
        if not csv_path: return
        try:
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Type", "Virtual Path", "Database ID", "Real Physical Path"])
                for typ, path, db_id in sel_items:
                    rp = path if typ == "file" else ""
                    writer.writerow([typ, path, db_id, rp])
            QMessageBox.information(self, "Success", "CSV Exported successfully.")
            self.sys_log(f"Exported selection to CSV: {csv_path}")
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _get_selected_items(self):
        if self.view_stack.currentIndex() == 0:
            model = self.file_table.model()
            return [model.data(model.index(idx.row(), 0), Qt.UserRole) for idx in self.file_table.selectionModel().selectedRows() if model.data(model.index(idx.row(), 0), Qt.UserRole)]
        return [item.data(Qt.UserRole) for item in self.file_grid.selectedItems() if item.data(Qt.UserRole)]

    def context_menu(self, pos, is_grid=False):
        menu = QMenu(self)
        if not self._is_smart_path(self.current_prefix) and self.active_db_path == str(DB_FILE) and not self.current_prefix.startswith("trash://"):
            menu.addAction("📂 Create Virtual Folder (Ctrl+Shift+N)", self.create_folder)
            menu.addAction("📥 Import Real Files", self.import_real_files)
            menu.addAction("📁 Import Real Folder", self.import_real_folder)
        
        sel_items = [i for i in self._get_selected_items() if i[2] != -1] 
        if sel_items:
            menu.addSeparator()
            if len(sel_items) == 1:
                if sel_items[0][0] == "file": 
                    menu.addAction("📂 Open Natively (Enter)", self.open_selected)
                    menu.addAction("🎞 Open in Nexus Viewer (Ctrl+O)", self.open_selected_nexus)
                menu.addAction("🏷 Set Secondary Name", self.cmd_set_secondary_name)
                res = self.db.conn.cursor().execute("SELECT is_hidden, is_favorite FROM virtual_fs WHERE id=?", (sel_items[0][2],)).fetchone()
                if res: 
                    menu.addAction("Unhide" if res[0] else "Hide", lambda: self.toggle_item_hidden(sel_items[0][2], not res[0]))
                    menu.addAction("Remove Favorite" if res[1] else "Add Favorite", lambda: self.toggle_item_fav(sel_items[0][2], not res[1]))
            menu.addAction("Copy (Ctrl+C)", self.cmd_copy)
            menu.addAction("Cut (Ctrl+X)", self.cmd_cut)
            menu.addAction("Trash (Delete)", self.cmd_delete)
            menu.addAction("Rename (F2)", self.cmd_rename)
            menu.addSeparator()
            tag_menu = menu.addMenu("🏷 Set Color Tag")
            for color in ["None", "Red", "Green", "Blue", "Gold"]: 
                tag_menu.addAction(color, lambda checked=False, c=color: self.bulk_tag_items(c, sel_items))
            menu.addAction("🏷️ Bulk Add Custom Tags...", lambda: self.bulk_add_custom_tags(sel_items))
            menu.addSeparator()
            menu.addAction("📦 Export Selected to ZIP...", lambda: self.export_to_zip(sel_items))
            menu.addAction("📊 Export View to CSV", lambda: self.export_csv(sel_items))
            
        menu.addSeparator()
        menu.addAction("📤 Materialize View to OS", self.export_virtual_to_os)
        menu.addAction("⚙️ Compile View to Isolated DB", self.compile_current_view)
        act_paste = QAction("📋 Paste (Ctrl+V)", self)
        act_paste.triggered.connect(self.cmd_paste)
        act_paste.setEnabled(bool(self.v_clipboard["items"]) and not self._is_smart_path(self.current_prefix))
        menu.addAction(act_paste)
        
        menu.exec(self.file_grid.viewport().mapToGlobal(pos) if is_grid else self.file_table.viewport().mapToGlobal(pos))

    def toggle_item_hidden(self, db_id, hide: bool): 
        self.db.conn.cursor().execute("UPDATE virtual_fs SET is_hidden = ? WHERE id = ?", (1 if hide else 0, db_id))
        self.db.conn.commit()
        self.clear_cache()
        self.load_directory(self.current_prefix)
        self.sys_log(f"Item visibility toggled for DB_ID {db_id}")

    def toggle_item_fav(self, db_id, fav: bool): 
        self.db.conn.cursor().execute("UPDATE virtual_fs SET is_favorite = ? WHERE id = ?", (1 if fav else 0, db_id))
        self.db.conn.commit()
        self.clear_cache()
        self.load_directory(self.current_prefix)

    def bulk_tag_items(self, color, sel_items):
        for typ, path, db_id in sel_items: 
            self.db.conn.cursor().execute("UPDATE virtual_fs SET color_tag = ? WHERE id = ?", ("" if color=="None" else color, db_id))
        self.db.conn.commit()
        self.clear_cache()
        self.load_directory(self.current_prefix)
        self.sys_log(f"Applied Color Tag: {color} to {len(sel_items)} items.")

    def bulk_add_custom_tags(self, sel_items):
        tags, ok = QInputDialog.getText(self, "Bulk Apply Tags", "Enter tags separated by comma (e.g. urgent, work, vacation):")
        if not ok or not tags.strip(): return
        cur = self.db.conn.cursor()
        for typ, path, db_id in sel_items:
            old_tags = cur.execute("SELECT custom_tags FROM virtual_fs WHERE id=?", (db_id,)).fetchone()[0]
            new_val = f"{old_tags}, {tags.strip()}".strip(", ") if old_tags else tags.strip()
            cur.execute("UPDATE virtual_fs SET custom_tags = ? WHERE id = ?", (new_val, db_id))
        self.db.conn.commit()
        self.clear_cache()
        self.load_directory(self.current_prefix)
        self.sys_log(f"Bulk applied custom tags '{tags}' to {len(sel_items)} items.")

    def cmd_copy(self):
        items = self._get_selected_items()
        if items: 
            self.v_clipboard = {"action": "copy", "items": items}
            self.status.showMessage(f"Copied {len(items)} items virtually.", 3000)

    def cmd_cut(self):
        items = self._get_selected_items()
        if items: 
            self.v_clipboard = {"action": "cut", "items": items}
            self.status.showMessage(f"Cut {len(items)} items virtually.", 3000)

    def cmd_paste(self):
        if not self.v_clipboard["items"]: return
        if self._is_smart_path(self.current_prefix): 
            return QMessageBox.warning(self, "Error", "Cannot paste into dynamic Smart Views.")
        self._current_drag_items = self.v_clipboard["items"]
        is_copy = (self.v_clipboard["action"] == "copy")
        self.execute_internal_drop(self.current_prefix, is_copy)
        if not is_copy: self.v_clipboard = {"action": None, "items": []} 

    def cmd_delete(self):
        items = self._get_selected_items()
        if not items: return
        clean_items = [i for i in items if i[2] != -1] 
        if not clean_items: return
        
        is_permanent = self.current_prefix.startswith("trash://") or self.active_db_path != str(DB_FILE)
        if QMessageBox.question(self, "Delete", "Permanently delete from Nexus OS?" if is_permanent else "Move selected to Virtual Trash?", QMessageBox.Yes|QMessageBox.No) != QMessageBox.Yes: return
        cur = self.db.conn.cursor()
        
        prog = QProgressDialog(f"Deleting {len(clean_items)} items...", "Cancel", 0, len(clean_items), self)
        prog.setWindowModality(Qt.WindowModal)
        prog.show()
        
        for i, (typ, path, db_id) in enumerate(clean_items):
            if prog.wasCanceled(): break
            if is_permanent:
                if typ == "file": cur.execute("DELETE FROM virtual_fs WHERE id = ?", (db_id,))
                else: cur.execute("DELETE FROM virtual_fs WHERE parent_path LIKE ? OR id = ?", (f"{path}%", db_id))
            else:
                if typ == "file": cur.execute("UPDATE virtual_fs SET in_trash = 1 WHERE id = ?", (db_id,))
                else: cur.execute("UPDATE virtual_fs SET in_trash = 1 WHERE parent_path LIKE ? OR id = ?", (f"{path}%", db_id))
            prog.setValue(i+1)
            
        self.db.conn.commit()
        self.clear_cache()
        self.refresh_all()
        self.sys_log(f"Deleted {len(clean_items)} items.")

    def cmd_rename(self):
        items = self._get_selected_items()
        if not items: return
        
        if len(items) == 1 and items[0][2] != -1: 
            typ, path, db_id = items[0]
            old_name = self.db.conn.cursor().execute("SELECT name FROM virtual_fs WHERE id = ?", (db_id,)).fetchone()[0]
            if typ == "file":
                base, ext = os.path.splitext(old_name)
                new_base, ok = QInputDialog.getText(self, "Rename File", "New Name:", QLineEdit.Normal, base)
                if not ok or not new_base.strip() or new_base.strip() == base: return
                new_name = new_base.strip() + ext
                self.db.conn.cursor().execute("UPDATE virtual_fs SET name = ? WHERE id = ?", (new_name, db_id))
            else:
                new_name, ok = QInputDialog.getText(self, "Rename Folder", "New Name:", QLineEdit.Normal, old_name)
                if not ok or not new_name.strip() or new_name.strip() == old_name: return
                self.db.conn.cursor().execute("UPDATE virtual_fs SET name = ? WHERE id = ?", (new_name.strip(), db_id))
                self.db.conn.cursor().execute("UPDATE virtual_fs SET parent_path = ? || SUBSTR(parent_path, LENGTH(?) + 1) WHERE parent_path LIKE ?", (f"{self.current_prefix}{new_name.strip()}/", path, f"{path}%"))
            self.db.conn.commit()
            self.clear_cache()
            self.refresh_tree()
            self.load_directory(self.current_prefix)
            self.sys_log(f"Renamed item to '{new_name}'")
            
        elif len(items) > 1:
            base_name, ok = QInputDialog.getText(self, "Bulk Rename", f"Enter base name to serialize {len(items)} items:")
            if ok and base_name.strip():
                prog = QProgressDialog(f"Bulk Renaming {len(items)} items...", "Cancel", 0, len(items), self)
                prog.setWindowModality(Qt.WindowModal)
                prog.show()
                cur = self.db.conn.cursor()
                for i, (typ, path, db_id) in enumerate(items):
                    if prog.wasCanceled(): break
                    if db_id == -1: continue
                    if typ == "file":
                        ext = cur.execute("SELECT extension FROM virtual_fs WHERE id=?", (db_id,)).fetchone()[0]
                        new_name = f"{base_name.strip()} ({i+1}){ext}"
                        cur.execute("UPDATE virtual_fs SET name=? WHERE id=?", (new_name, db_id))
                    else:
                        new_name = f"{base_name.strip()} ({i+1})"
                        cur.execute("UPDATE virtual_fs SET name=? WHERE id=?", (new_name, db_id))
                        cur.execute("UPDATE virtual_fs SET parent_path = ? || SUBSTR(parent_path, LENGTH(?) + 1) WHERE parent_path LIKE ?", (f"{self.current_prefix}{new_name}/", path, f"{path}%"))
                    prog.setValue(i+1)
                self.db.conn.commit()
                self.clear_cache()
                self.refresh_tree()
                self.load_directory(self.current_prefix)
                self.sys_log(f"Bulk Renamed {len(items)} items to base '{base_name.strip()}'")

    def cmd_set_secondary_name(self):
        items = self._get_selected_items()
        if len(items) == 1 and items[0][2] != -1:
            db_id = items[0][2]
            old_sec = self.db.conn.cursor().execute("SELECT secondary_name FROM virtual_fs WHERE id = ?", (db_id,)).fetchone()[0]
            new_sec, ok = QInputDialog.getText(self, "Secondary Name", "Enter Secondary Name / Description:", QLineEdit.Normal, str(old_sec))
            if ok: 
                self.db.conn.cursor().execute("UPDATE virtual_fs SET secondary_name = ? WHERE id = ?", (new_sec.strip(), db_id))
                self.db.conn.commit()
                self.clear_cache()
                self.load_directory(self.current_prefix)

    def execute_internal_drop(self, dest_path, is_copy):
        if not self._current_drag_items or dest_path.startswith("trash://") or self._is_smart_path(dest_path) or self.active_db_path != str(DB_FILE): return
        cur = self.db.conn.cursor()
        for typ, path, db_id in self._current_drag_items:
            if db_id == -1: continue 
            if typ == "file":
                if is_copy:
                    cur.execute("SELECT name, is_folder, real_path, size, extension, modified, color_tag, is_hidden, category, year, month, custom_tags FROM virtual_fs WHERE id = ?", (db_id,))
                    row = cur.fetchone()
                    if row: cur.execute("INSERT INTO virtual_fs (parent_path, name, is_folder, real_path, size, extension, modified, color_tag, is_hidden, category, year, month, custom_tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (dest_path, f"{row[0]} - Copy", *row[1:]))
                else: cur.execute("UPDATE virtual_fs SET parent_path = ? WHERE id = ?", (dest_path, db_id))
            else:
                cur.execute("SELECT name FROM virtual_fs WHERE id = ?", (db_id,))
                row = cur.fetchone()
                if not row: continue
                if is_copy:
                    new_base_name = f"{row[0]} - Copy"
                    cur.execute("INSERT INTO virtual_fs (parent_path, name, is_folder) VALUES (?, ?, 1)", (dest_path, new_base_name))
                    cur.execute("SELECT name, is_folder, real_path, size, extension, modified, color_tag, is_hidden, parent_path, category, year, month, custom_tags FROM virtual_fs WHERE parent_path LIKE ?", (f"{path}%",))
                    for r in cur.fetchall(): cur.execute("INSERT INTO virtual_fs (parent_path, name, is_folder, real_path, size, extension, modified, color_tag, is_hidden, category, year, month, custom_tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (f"{dest_path}{new_base_name}/" + r[8][len(path):], *r[:8], r[9], r[10], r[11], r[12]))
                else:
                    cur.execute("UPDATE virtual_fs SET parent_path = ? WHERE id = ?", (dest_path, db_id))
                    cur.execute("UPDATE virtual_fs SET parent_path = ? || SUBSTR(parent_path, LENGTH(?) + 1) WHERE parent_path LIKE ?", (f"{dest_path}{row[0]}/", path, f"{path}%"))
        self.db.conn.commit()
        self._current_drag_items = []
        self.clear_cache()
        self.refresh_all()
        self.sys_log(f"Internal Drag/Drop executed to '{dest_path}'")

    def create_folder(self):
        if self.current_prefix.startswith("trash://") or self.current_prefix.startswith("fav://") or self._is_smart_path(self.current_prefix): return
        name, ok = QInputDialog.getText(self, "New Virtual Folder", "Folder Name:")
        if ok and name.strip() and not self.db.conn.cursor().execute("SELECT id FROM virtual_fs WHERE parent_path=? AND name=? AND is_folder=1", (self.current_prefix, name.strip())).fetchone():
            self.db.conn.cursor().execute("INSERT INTO virtual_fs (parent_path, name, is_folder, modified) VALUES (?, ?, 1, ?)", (self.current_prefix, name.strip(), now_ts()))
            self.db.conn.commit()
            self.clear_cache()
            self.refresh_tree()
            self.load_directory(self.current_prefix)

    def create_virtual_file(self):
        if self.current_prefix.startswith("trash://") or self.current_prefix.startswith("fav://") or self._is_smart_path(self.current_prefix): return
        name, ok = QInputDialog.getText(self, "New Virtual File", "File Name:")
        if ok and name.strip():
            self.db.conn.cursor().execute("INSERT INTO virtual_fs (parent_path, name, is_folder, real_path, size, extension, modified) VALUES (?, ?, 0, '', 0, ?, ?)", (self.current_prefix, name.strip(), os.path.splitext(name.strip())[1].lower(), now_ts()))
            self.db.conn.commit()
            self.clear_cache()
            self.load_directory(self.current_prefix)

    def import_real_files(self):
        if not self.current_prefix.startswith("trash://") and not self.current_prefix.startswith("fav://") and not self._is_smart_path(self.current_prefix):
            files, _ = QFileDialog.getOpenFileNames(self, "Import Real Files")
            self.on_files_dropped(files) if files else None

    def import_real_folder(self):
        if not self.current_prefix.startswith("trash://") and not self.current_prefix.startswith("fav://") and not self._is_smart_path(self.current_prefix):
            folder = QFileDialog.getExistingDirectory(self, "Import Real Folder")
            self.on_files_dropped([folder]) if folder else None

    def on_files_dropped(self, paths):
        if self._is_smart_path(self.current_prefix) or self.current_prefix.startswith("trash://"): return QMessageBox.warning(self, "Error", "Cannot import directly into Smart Views.")
        
        self.import_dlg = QProgressDialog("Importing files into Virtual Sandbox...", "Cancel", 0, 100, self)
        self.import_dlg.setWindowModality(Qt.WindowModal)
        self.import_dlg.show()
        
        self.import_thread = ImportFilesThread(str(self.db.path), self.current_prefix, paths, self)
        self.import_thread.progress.connect(lambda c,t,m: (self.import_dlg.setMaximum(t), self.import_dlg.setValue(c), self.import_dlg.setLabelText(m)))
        self.import_dlg.canceled.connect(self.import_thread.cancel)
        
        def on_import_finished(f_cnt, d_cnt):
            self.import_dlg.close()
            self.clear_cache()
            self.refresh_all()
            self.status.showMessage(f"Imported {f_cnt} files and {d_cnt} folders.", 5000)
            self.sys_log(f"Successfully Sandbox Imported {f_cnt} files.")
            
        self.import_thread.finished_import.connect(on_import_finished)
        self.import_thread.error.connect(lambda e: (self.import_dlg.close(), QMessageBox.critical(self, "Import Error", e)))
        self._register_worker(self.import_thread)
        self.import_thread.start()

    def compile_current_view(self):
        name, ok = QInputDialog.getText(self, "Compile DB View", "Enter name for new separate database (e.g., 'Project_Backup'):")
        if not ok or not name.strip(): return
        target_path = VIEWS_DIR / f"{name.strip().replace(' ', '_')}.db"
        
        query = ""
        params = ()
        matched_proto = next((p for p in SMART_PROTOCOLS if self.current_prefix.startswith(p)), None)
        
        if self.current_prefix.startswith("y_m_f://"):
            parts = [p for p in self.current_prefix.replace("y_m_f://", "").split("/") if p]
            if len(parts) >= 3:
                cur = self.db.conn.cursor()
                cur.execute("SELECT parent_path, year, month, COUNT(id) FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND year!='' AND month!='' GROUP BY parent_path, year, month")
                folder_age = {}
                temp_tracker = {}
                for pp, y, m, c in cur.fetchall():
                    if pp not in temp_tracker or c > temp_tracker[pp]:
                        temp_tracker[pp] = c
                        folder_age[pp] = (y, m)
                matched_pp = None
                for pp, age in folder_age.items():
                    if age == (parts[0], parts[1]) and (pp.strip("/").split("/")[-1] if pp.strip("/") else "Root_Files") == parts[2]:
                        matched_pp = pp
                        break
                if matched_pp:
                    query = "SELECT * FROM virtual_fs WHERE parent_path LIKE ? AND is_folder=0 AND in_trash=0"
                    params = (f"{matched_pp}%",)
                else: 
                    return QMessageBox.warning(self, "Compile Error", "Folder matching failed.")
            else:
                return QMessageBox.warning(self, "Compile Error", "You must navigate deeply into a folder to compile it from the Year/Month view.")
        
        elif matched_proto:
            cols = SMART_PROTOCOLS[matched_proto]
            parts = [p for p in self.current_prefix.replace(matched_proto, "").split("/") if p]
            where = ["is_folder=0", "in_trash=0"] + [f"{cols[i]}=?" for i in range(len(parts))]
            query = f"SELECT * FROM virtual_fs WHERE {' AND '.join(where)}"
            params = tuple(parts)
        else:
            query = "SELECT * FROM virtual_fs WHERE parent_path LIKE ? AND in_trash=0"
            params = (f"{self.current_prefix}%",)

        self.compile_dlg = QProgressDialog("Compiling standalone database...", "Cancel", 0, 100, self)
        self.compile_dlg.setWindowModality(Qt.WindowModal)
        self.compile_dlg.show()
        
        self.compiler = CompilerThread(str(self.db.path), str(target_path), self.current_prefix, query, params, self)
        self.compiler.progress.connect(lambda c, t, msg: (self.compile_dlg.setValue(c), self.compile_dlg.setLabelText(msg)))
        self.compile_dlg.canceled.connect(self.compiler.cancel)
        
        def on_compile_finished(db_res):
            self.compile_dlg.close()
            QMessageBox.information(self, "Success", f"DB compiled to:\n{db_res}")
            self.refresh_tree()
            self.sys_log(f"Compiled Isolated DB: {db_res}")
            
        self.compiler.finished.connect(on_compile_finished)
        self.compiler.error.connect(lambda e: (self.compile_dlg.close(), QMessageBox.critical(self, "Compile Error", e)))
        self._register_worker(self.compiler)
        self.compiler.start()

    def update_statistics(self):
        stats = self.db.get_stats(self.current_prefix if not self._is_smart_path(self.current_prefix) else "")
        usage_pct = (stats['used_bytes'] / MAX_VIRTUAL_STORAGE) * 100 if MAX_VIRTUAL_STORAGE else 0
        html = f"""
        <h3 style='color:#58a6ff;'>System Analytics ({Path(self.active_db_path).name})</h3><hr>
        <b>Total Virtual Files:</b> {stats['files']}<br>
        <b>Total Virtual Folders:</b> {stats['folders']}<br>
        <b>Simulated Storage Used:</b> {human_size(stats['used_bytes'])}<br>
        <b>System Allocation:</b> {usage_pct:.4f}%<br><hr>
        <h4 style='color:#58a6ff;'>Top Largest Managed Files:</h4>
        <ul style='list-style-type: square; margin-left: -20px;'>
        """
        for f in stats['top_files'][:5]: 
            html += f"<li>{f[0]} <span style='color:#8b949e;'>({human_size(f[1])})</span></li>"
        html += "</ul>"
        self.lbl_stats_txt.setHtml(html)
        
        if not self.figure: return
        self.figure.clear()
        mode = self.stat_combo.currentText()
        ax = self.figure.add_subplot(111)
        bg_c, txt_c = ('#0d1117', 'white') if self.is_dark_mode else ('#ffffff', 'black')
        self.figure.patch.set_facecolor(bg_c)
        ax.set_facecolor(bg_c)
        ax.tick_params(colors=txt_c)
        
        if "Line Chart" in mode or "Over Time" in mode:
            data = stats["time_series"]
            if not data: ax.text(0.5, 0.5, "No temporal data available", color=txt_c, ha='center')
            else:
                dates = [datetime.strptime(d[0], "%Y-%m") for d in data if len(d[0]) == 7]
                if "Count" in mode: 
                    vals = [d[1] for d in data if len(d[0]) == 7]
                    ylabel = "Total Files Modified"
                    color = '#00e5ff'
                else: 
                    vals = [d[2] / (1024*1024) for d in data if len(d[0]) == 7]
                    ylabel = "Storage Size (MB)"
                    color = '#d7ba7d'
                if dates and vals:
                    ax.plot(dates, vals, marker='o', linestyle='-', color=color, linewidth=2)
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
                    self.figure.autofmt_xdate()
                    ax.set_ylabel(ylabel, color=txt_c)
                    ax.set_title(mode, color=txt_c)
                    ax.grid(True, color='#30363d', linestyle='--')
                    
        elif "Distribution by Extension" in mode:
            data = stats["distribution"]
            if not data: ax.text(0.5, 0.5, "No distribution data available", color=txt_c, ha='center')
            else:
                df = pd.DataFrame(data, columns=['ext', 'count', 'size'])
                df['ext'] = df['ext'].replace('', 'none')
                if "Size" in mode: 
                    df = df.sort_values(by='size', ascending=False).head(15)
                    ax.bar(df['ext'], df['size'] / (1024*1024), color='#1e7145')
                    ax.set_ylabel("Storage Size (MB)", color=txt_c)
                else: 
                    df = df.sort_values(by='count', ascending=False).head(15)
                    ax.bar(df['ext'], df['count'], color='#58a6ff')
                    ax.set_ylabel("Total File Count", color=txt_c)
                ax.set_title(mode, color=txt_c)
                ax.tick_params(axis='x', rotation=45)
                ax.grid(axis='y', color='#30363d', linestyle='--')
                
        elif "Largest Files" in mode:
            data = stats["top_files"]
            if not data: ax.text(0.5, 0.5, "No files available", color=txt_c, ha='center')
            else:
                names = [d[0][:15] + ".." if len(d[0])>15 else d[0] for d in data]
                sizes = [d[1] / (1024*1024) for d in data]
                ax.barh(names, sizes, color='#a371f7')
                ax.set_xlabel("Size (MB)", color=txt_c)
                ax.set_title(mode, color=txt_c)
                ax.invert_yaxis()
                ax.grid(axis='x', color='#30363d', linestyle='--')
                
        elif "Ratio" in mode:
            labels = ['Used Storage', 'Free Space']
            sizes = [stats['used_bytes'], max(0, MAX_VIRTUAL_STORAGE - stats['used_bytes'])]
            colors = ['#d7ba7d', '#2ea043']
            ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, colors=colors, textprops={'color': txt_c})
            ax.set_title(mode, color=txt_c)

        self.figure.tight_layout()
        self.canvas.draw()

    def export_to_zip(self, items_to_export):
        zip_path, _ = QFileDialog.getSaveFileName(self, "Compile to ZIP", "", "ZIP Files (*.zip)")
        if zip_path:
            self.export_dlg = QProgressDialog("Compiling ZIP...", "Cancel", 0, 100, self)
            self.export_dlg.setWindowModality(Qt.WindowModal)
            self.export_dlg.show()
            self.zip_thread = ExportZipThread(str(self.db.path), items_to_export, zip_path, self)
            self.zip_thread.progress.connect(lambda c,t,m: self.export_dlg.setValue(int((c/max(1,t))*100)) if self.export_dlg else None)
            self.export_dlg.canceled.connect(self.zip_thread.cancel)
            self.zip_thread.finished.connect(lambda p: (self.export_dlg.close() if self.export_dlg else None, QMessageBox.information(self, "Success", f"ZIP created:\n{p}"), self.sys_log(f"Exported View to Zip: {Path(p).name}")))
            self.zip_thread.error.connect(lambda e: (self.export_dlg.close() if self.export_dlg else None, QMessageBox.critical(self, "Error", f"Failed:\n{e}")))
            self._register_worker(self.zip_thread)
            self.zip_thread.start()

    def _register_worker(self, worker: QThread): 
        self._workers.append(worker)
        worker.finished.connect(lambda: self._cleanup_worker(worker))
        
    def _cleanup_worker(self, worker: QThread):
        try: 
            self._workers.remove(worker)
            worker.deleteLater()
        except Exception: pass

    def closeEvent(self, ev):
        if HAS_MULTIMEDIA and hasattr(self, 'player'): self.player.stop()
        if self.render_timer.isActive(): self.render_timer.stop()
        for w in list(self._workers):
            try: 
                w.cancel() if hasattr(w, "cancel") else None
                w.wait(2000) if w.isRunning() else None
            except Exception: pass
        if self.loader_thread and self.loader_thread.isRunning(): 
            self.loader_thread.cancel()
            self.loader_thread.quit()
            self.loader_thread.wait()
        self.db.close()
        super().closeEvent(ev)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName(APP_TITLE)
    win = NexusVirtualManager()
    win.show()
    sys.exit(app.exec())
