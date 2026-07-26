"""
VMan OS Virtual File Manager
Timeline Diary, Deep Smart Views, Zero-Lag Async Engine, Custom Tags, Multi-Themes, and OS Hooks.
"""
from __future__ import annotations
import hashlib, os, shutil, sqlite3, sys, zipfile, time, csv, subprocess, math
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict

from PySide6.QtCore import (
    Qt, QThread, Signal, QModelIndex, QAbstractTableModel, 
    QFileInfo, QUrl, QSize, QMimeData, QPropertyAnimation, QEasingCurve, QTimer, QDate, QSettings
)
from PySide6.QtGui import (
    QFont, QPixmap, QImage, QAction, QPainter, QIcon, QDragEnterEvent, QDropEvent, 
    QColor, QBrush, QKeySequence, QShortcut, QDrag, QKeyEvent, QTextCursor, QTextCharFormat, QTransform
)
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QToolBar, QFileDialog, QMessageBox,
    QWidget, QVBoxLayout, QLabel, QPushButton, QHBoxLayout, QInputDialog, 
    QProgressDialog, QTreeWidget, QTreeWidgetItem, QPlainTextEdit, 
    QLineEdit, QComboBox, QTableView, QHeaderView, QMenu, QAbstractItemView, 
    QStatusBar, QSizePolicy, QFormLayout, QDockWidget, QToolButton,
    QStackedWidget, QListView, QTabWidget, QSlider, QStyle, QGraphicsOpacityEffect, 
    QScrollArea, QDialog, QGraphicsView, QGraphicsScene, QTextBrowser, 
    QTableWidget, QTableWidgetItem, QCheckBox, QCalendarWidget, QSpinBox, 
    QGridLayout, QFrame, QSplitter, QListWidget, QListWidgetItem, QGroupBox, QFormLayout
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
    from PySide6.QtMultimediaWidgets import QVideoWidget, QGraphicsVideoItem
    HAS_MULTIMEDIA = True
except ImportError: HAS_MULTIMEDIA = False

try:
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.figure import Figure
    import matplotlib.dates as mdates
    MATPLOTLIB_AVAILABLE = True
except Exception: MATPLOTLIB_AVAILABLE = False

from datetime import timedelta 
import re

import matplotlib
matplotlib.use('QtAgg')
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg
from matplotlib.figure import Figure
from PySide6.QtWidgets import QSplitter 

from PySide6.QtCore import QTimer, Qt
from PySide6.QtWidgets import QRadioButton

# ---------------- Constants & Themes ----------------
APP_TITLE = "VMan"
DATA_DIR = Path("vman_data")
VIEWS_DIR = DATA_DIR / "compiled_views"
DB_FILE = DATA_DIR / "vman_vfs.db"
MAX_VIRTUAL_STORAGE = 100 * 1024 * 1024 * 1024  
CHUNK_SIZE = 150 

FILE_CATEGORIES = {
    "Images": ['.png', '.jpg', '.jpeg', '.bmp', '.gif', '.webp', '.svg'],
    "Videos": ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv'],
    "Audio": ['.mp3', '.wav', '.ogg', '.flac', '.aac'],
    "Documents": ['.pdf', '.doc', '.docx', '.txt', '.csv', '.xlsx', '.xls', '.ppt', '.pptx', '.md'],
    "Code": ['.py', '.js', '.html', '.css', '.cpp', '.c', '.java', '.json', '.xml', '.sh']
}

SMART_PROTOCOLS = {
    "tags://": ["custom_tags"],
    "y_c_m_e://": ["year", "category", "month", "extension"],
    "c_y_m://": ["category", "year", "month"],
    "y_m://": ["year", "month"],
    "y_m_c://": ["year", "month", "category"],
    "y_c_m://": ["year", "category", "month"]
}

THEMES = {
    "Dark": """
        QMainWindow, QDialog, QDockWidget { background-color: #0d1117; color: #c9d1d9; font-family: 'Segoe UI'; font-size: 13px; }
        QWidget { color: #c9d1d9; }
        QDockWidget::title { background: #161b22; padding: 8px; border-top-left-radius: 8px; border-top-right-radius: 8px; font-weight: bold; }
        QLineEdit, QComboBox, QSpinBox { background-color: #0d1117; border: 1px solid #30363d; border-radius: 6px; padding: 6px 10px; color: #58a6ff; }
        QPushButton { background-color: #21262d; border: 1px solid #30363d; border-radius: 6px; padding: 6px 12px; font-weight: bold; color: #c9d1d9; }
        QPushButton:hover { background-color: #30363d; border: 1px solid #8b949e; }
        QTableView, QListView, QTreeView, QTableWidget, QScrollArea { background-color: #0d1117; color: #c9d1d9; border: 1px solid #30363d; border-radius: 8px; gridline-color: #21262d; }
        QPlainTextEdit, QTextBrowser, QListWidget { background-color: #0d1117; color: #c9d1d9; border: 1px solid #30363d; border-radius: 8px; }
        QHeaderView::section { background-color: #161b22; color: #c9d1d9; padding: 6px; border: none; border-right: 1px solid #30363d; font-weight: bold; }
        QTabWidget::pane { border: 1px solid #30363d; background: #0d1117; }
        QTabBar::tab { background: #161b22; color: #8b949e; padding: 8px 12px; border: 1px solid #30363d; border-bottom: none; border-top-left-radius: 4px; border-top-right-radius: 4px; }
        QTabBar::tab:selected { background: #0d1117; color: #58a6ff; font-weight: bold; border-bottom: 2px solid #58a6ff; }
        QListView::item, QListWidget::item { padding: 5px; border-radius: 5px; }
        QListView::item:selected, QListWidget::item:selected { background-color: #1f6feb; color: white; }
        QTableView::indicator { width: 18px; height: 18px; border: 1px solid #8b949e; border-radius: 3px; background: #21262d; }
        QTableView::indicator:checked { background-color: #58a6ff; }
    """,
    "Light": """
        QMainWindow, QDialog, QDockWidget { background-color: #f6f8fa; color: #24292f; font-family: 'Segoe UI'; font-size: 13px; }
        QWidget { color: #24292f; }
        QDockWidget::title { background: #e1e4e8; padding: 8px; border-top-left-radius: 8px; border-top-right-radius: 8px; font-weight: bold; color: #24292f; }
        QLineEdit, QComboBox, QSpinBox { background-color: #ffffff; border: 1px solid #d0d7de; border-radius: 6px; padding: 6px 10px; color: #0969da; }
        QPushButton { background-color: #f3f4f6; border: 1px solid #d0d7de; border-radius: 6px; padding: 6px 12px; font-weight: bold; color: #24292f; }
        QPushButton:hover { background-color: #ebecf0; border: 1px solid #8c959f; }
        QTableView, QListView, QTreeView, QTableWidget, QScrollArea { background-color: #ffffff; color: #24292f; border: 1px solid #d0d7de; border-radius: 8px; gridline-color: #e1e4e8; }
        QPlainTextEdit, QTextBrowser, QListWidget { background-color: #ffffff; color: #24292f; border: 1px solid #d0d7de; border-radius: 8px; }
        QHeaderView::section { background-color: #f6f8fa; color: #24292f; padding: 6px; border: none; border-right: 1px solid #d0d7de; font-weight: bold; }
        QTabWidget::pane { border: 1px solid #d0d7de; background: #ffffff; }
        QTabBar::tab { background: #f6f8fa; color: #57606a; padding: 8px 12px; border: 1px solid #d0d7de; border-bottom: none; border-top-left-radius: 4px; border-top-right-radius: 4px; }
        QTabBar::tab:selected { background: #ffffff; color: #0969da; font-weight: bold; border-bottom: 2px solid #0969da; }
        QListView::item, QListWidget::item { padding: 5px; border-radius: 5px; }
        QListView::item:selected, QListWidget::item:selected { background-color: #0969da; color: white; }
        QTableView::indicator { width: 18px; height: 18px; border: 1px solid #d0d7de; border-radius: 3px; background: #ffffff; }
        QTableView::indicator:checked { background-color: #0969da; }
    """
}

def get_category_for_ext(ext):
    for cat, exts in FILE_CATEGORIES.items():
        if ext in exts: return cat
    return "Others"

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

def ensure_dirs(): 
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    VIEWS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- Database Engine ----------------
class vmanDB:
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
        # Removed unused 'owner' column to keep schema clean
        for col in [
            "color_tag TEXT DEFAULT ''", "secondary_name TEXT DEFAULT ''", 
            "is_hidden INTEGER DEFAULT 0", "in_trash INTEGER DEFAULT 0", 
            "is_favorite INTEGER DEFAULT 0", "sha256 TEXT DEFAULT ''",
            "category TEXT DEFAULT 'Others'", "year TEXT DEFAULT ''", "month TEXT DEFAULT ''",
            "custom_tags TEXT DEFAULT ''", "hash_verified INTEGER DEFAULT 0",
            "creation_date TEXT DEFAULT ''"
        ]:
            try: c.execute(f"ALTER TABLE virtual_fs ADD COLUMN {col};")
            except sqlite3.OperationalError: pass
            
        c.execute("UPDATE virtual_fs SET creation_date = modified WHERE creation_date = '' OR creation_date IS NULL;")
        
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

        c.execute(f"SELECT COUNT(*), COALESCE(SUM(size), 0), MIN(modified), MAX(modified), COALESCE(AVG(size), 0) FROM virtual_fs WHERE {where_clause}", params)
        res = c.fetchone()
        f_cnt, total_sz, oldest, newest, avg_sz = (res[0] or 0, res[1] or 0, res[2] or "N/A", res[3] or "N/A", res[4] or 0)
        c.execute(f"SELECT COUNT(*) FROM virtual_fs WHERE is_folder=1 AND in_trash=0 AND parent_path LIKE ?", (f"{current_prefix}%",))
        d_cnt = c.fetchone()[0]
        c.execute(f"SELECT extension, COUNT(*), COALESCE(SUM(size),0) FROM virtual_fs WHERE {where_clause} GROUP BY extension", params)
        dist = c.fetchall()
        c.execute(f"SELECT name, size FROM virtual_fs WHERE {where_clause} ORDER BY size DESC LIMIT 10", params)
        top_files = c.fetchall()
        c.execute(f"SELECT year || '-' || month as dt, COUNT(*), SUM(size) FROM virtual_fs WHERE {where_clause} AND year != '' GROUP BY dt ORDER BY dt", params)
        time_series = c.fetchall()
        return {"files": f_cnt, "folders": d_cnt, "used_bytes": total_sz, "avg_bytes": avg_sz, "oldest": oldest, "newest": newest, "distribution": dist, "top_files": top_files, "time_series": time_series}

    def close(self):
        try: self.conn.close()
        except Exception: pass

# ---------------- Background Threads ----------------

class MaterializeThread(QThread):
    progress = Signal(int, int, str)
    finished = Signal(int)
    error = Signal(str)

    def __init__(self, db_path, items, dest_dir, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.items = items
        self.dest_dir = dest_dir
        self.is_cancelled = False

    def cancel(self): self.is_cancelled = True

    def sanitize_filename(self, name):
        return re.sub(r'[\\/*?:"<>|]', '_', str(name))

    def run(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            all_exports = [] 

            for typ, path_val, db_id in self.items:
                if self.is_cancelled: return

                if typ == "file" and db_id != -1:
                    res = cur.execute("SELECT name, real_path FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
                    if res: all_exports.append((self.sanitize_filename(res[0]), res[1]))
                
                elif typ == "folder":
                    if "://" in path_val:
                        prefix, data = path_val.split("://", 1)
                        parts = [p for p in data.split('/') if p]

                        if prefix == "y_m_f":
                            if len(parts) == 0:
                                res = cur.execute("SELECT year, month, name, real_path FROM virtual_fs WHERE is_folder=0 AND in_trash=0").fetchall()
                                for y, m, n, rp in res:
                                    dest = os.path.join(self.sanitize_filename(y or "Unknown_Year"), self.sanitize_filename(m or "Unknown_Month"), self.sanitize_filename(n))
                                    all_exports.append((dest, rp))
                            elif len(parts) == 1: 
                                yr = parts[0]
                                res = cur.execute("SELECT month, name, real_path FROM virtual_fs WHERE year=? AND is_folder=0 AND in_trash=0", (yr,)).fetchall()
                                for m, n, rp in res:
                                    dest = os.path.join(self.sanitize_filename(yr), self.sanitize_filename(m or "Unknown_Month"), self.sanitize_filename(n))
                                    all_exports.append((dest, rp))
                            elif len(parts) >= 2: 
                                yr, mo = parts[0], parts[1]
                                res = cur.execute("SELECT name, real_path FROM virtual_fs WHERE year=? AND month=? AND is_folder=0 AND in_trash=0", (yr, mo)).fetchall()
                                for n, rp in res:
                                    dest = os.path.join(self.sanitize_filename(yr), self.sanitize_filename(mo), self.sanitize_filename(n))
                                    all_exports.append((dest, rp))

                        elif prefix == "tags":
                            tag = parts[0] if parts else ""
                            if tag:
                                res = cur.execute("SELECT name, real_path FROM virtual_fs WHERE custom_tags LIKE ? AND is_folder=0 AND in_trash=0", (f"%{tag}%",)).fetchall()
                                for n, rp in res: all_exports.append((os.path.join(self.sanitize_filename(tag), self.sanitize_filename(n)), rp))
                            else:
                                res = cur.execute("SELECT custom_tags, name, real_path FROM virtual_fs WHERE custom_tags IS NOT NULL AND custom_tags != '' AND is_folder=0 AND in_trash=0").fetchall()
                                for tags, n, rp in res:
                                    for t in [x.strip() for x in tags.split(',') if x.strip()]:
                                        all_exports.append((os.path.join(self.sanitize_filename(t), self.sanitize_filename(n)), rp))
                        
                        elif prefix == "category":
                            cat = parts[0] if parts else ""
                            if cat:
                                res = cur.execute("SELECT name, real_path FROM virtual_fs WHERE category=? AND is_folder=0 AND in_trash=0", (cat,)).fetchall()
                                for n, rp in res: all_exports.append((os.path.join(self.sanitize_filename(cat), self.sanitize_filename(n)), rp))
                            else:
                                res = cur.execute("SELECT category, name, real_path FROM virtual_fs WHERE category IS NOT NULL AND category != '' AND is_folder=0 AND in_trash=0").fetchall()
                                for c, n, rp in res: all_exports.append((os.path.join(self.sanitize_filename(c), self.sanitize_filename(n)), rp))
                                
                        elif prefix == "search":
                            term = parts[0] if parts else ""
                            res = cur.execute("SELECT name, real_path FROM virtual_fs WHERE (name LIKE ? OR secondary_name LIKE ? OR custom_tags LIKE ?) AND is_folder=0 AND in_trash=0", (f"%{term}%", f"%{term}%", f"%{term}%")).fetchall()
                            for n, rp in res: all_exports.append((os.path.join("Search_Results", self.sanitize_filename(n)), rp))

                    elif path_val.startswith("/"):
                        res = cur.execute("SELECT parent_path, name, real_path FROM virtual_fs WHERE parent_path LIKE ? AND is_folder=0 AND in_trash=0", (f"{path_val}%",)).fetchall()
                        for pp, n, rp in res:
                            rel_p = pp[len(path_val):].lstrip('/') 
                            safe_parts = [self.sanitize_filename(p) for p in rel_p.split('/') if p]
                            safe_rel_p = os.path.join(*safe_parts) if safe_parts else ""
                            dest = os.path.join(safe_rel_p, self.sanitize_filename(n))
                            all_exports.append((dest, rp))

            total = len(all_exports)
            if total == 0:
                self.error.emit("No files found in this view to export. (The virtual folder or smart view might be empty).")
                conn.close()
                return

            count = 0
            for i, (rel_dest, source_rp) in enumerate(all_exports):
                if self.is_cancelled: return
                
                safe_rel_dest = rel_dest.replace('\\', '/').strip('/')
                final_dest = os.path.join(self.dest_dir, os.path.normpath(safe_rel_dest))
                
                os.makedirs(os.path.dirname(final_dest), exist_ok=True)
                self.progress.emit(i+1, total, f"Materializing: {safe_rel_dest}")

                try:
                    # FIX: Correctly skipping broken files instead of spamming ghost files
                    if source_rp and str(source_rp).strip() not in ("None", "") and os.path.exists(str(source_rp)):
                        shutil.copy2(str(source_rp), final_dest)
                        count += 1
                    else:
                        print(f"VMan Warning: Skipping export of {safe_rel_dest} because physical source is disconnected.")
                except Exception as e:
                    print(f"Materialize Error on {final_dest}: {e}")

            conn.close()
            self.finished.emit(count)
        except Exception as e:
            self.error.emit(str(e))

class BulkHashCalculator(QThread):
    progress = Signal(int, int, str)
    finished = Signal(int)
    def __init__(self, db_path, target_v_path, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.target_v_path = target_v_path
        self.is_cancelled = False
    def cancel(self): self.is_cancelled = True
    def run(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute("SELECT id, real_path, name FROM virtual_fs WHERE parent_path LIKE ? AND is_folder=0", (f"{self.target_v_path}%",))
                files = cur.fetchall()
                total = len(files)
                computed = 0
                for i, (db_id, rp, name) in enumerate(files):
                    if self.is_cancelled: break
                    if not rp or not os.path.exists(rp): continue
                    self.progress.emit(i+1, total, f"Hashing {name[:20]}...")
                    try:
                        sha = hashlib.sha256()
                        with open(rp, 'rb') as f:
                            for block in iter(lambda: f.read(4096), b""): sha.update(block)
                        cur.execute("UPDATE virtual_fs SET sha256=? WHERE id=?", (sha.hexdigest(), db_id))
                        computed += 1
                    except Exception: pass
                conn.commit()
            self.finished.emit(computed)
        except Exception: pass


class DataLoaderThread(QThread):
    data_ready = Signal(list, list) 
    def __init__(self, db_path, target_path, show_hidden, parent=None):
        super().__init__(parent)
        self.db_path, self.target_path, self.show_hidden = db_path, target_path, show_hidden
        self.is_cancelled = False
    def cancel(self): self.is_cancelled = True
    def run(self):
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()
        h_q = "AND is_hidden = 0" if not self.show_hidden else ""
        folders, files = [], []
        try:
            if self.is_cancelled: return
            matched_proto = next((p for p in SMART_PROTOCOLS if self.target_path.startswith(p)), None)
            
            if self.target_path.startswith("tags://"):
                parts = [p for p in self.target_path.replace("tags://", "").split("/") if p]
                if len(parts) == 0:
                    cur.execute("SELECT custom_tags, size FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND custom_tags IS NOT NULL AND custom_tags != ''")
                    tag_stats = defaultdict(lambda: [0, 0])
                    for tags_str, sz in cur.fetchall():
                        for t in [x.strip() for x in tags_str.split(",") if x.strip()]:
                            tag_stats[t][0] += 1
                            tag_stats[t][1] += sz or 0
                    folders = [(-1, "tags://", t, "", "", 0, stats[0], stats[1]) for t, stats in tag_stats.items()]
                elif len(parts) >= 1:
                    target_tag = parts[0]
                    cur.execute(f"SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden, custom_tags FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND custom_tags LIKE ? {h_q}", (f"%{target_tag}%",))
                    for row in cur.fetchall():
                        if target_tag in [x.strip() for x in str(row[9]).split(",") if x.strip()]: files.append(row[:9])

            elif self.target_path.startswith("y_m_f://"):
                parts = [p for p in self.target_path.replace("y_m_f://", "").split("/") if p]
                cur.execute("SELECT parent_path, year, month, COUNT(id) FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND year IS NOT NULL AND year != '' AND month IS NOT NULL AND month != '' GROUP BY parent_path, year, month")
                folder_age, temp_tracker = {}, {}
                for pp, y, m, c in cur.fetchall():
                    if pp not in temp_tracker or c > temp_tracker[pp]:
                        temp_tracker[pp], folder_age[pp] = c, (y, m)
                cur.execute("SELECT parent_path, COUNT(id), SUM(size) FROM virtual_fs WHERE is_folder=0 AND in_trash=0 GROUP BY parent_path")
                totals = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

                if len(parts) == 0:
                    folders = [(-1, "y_m_f://", yr, "", "", 0, 0, 0) for yr in sorted(list(set([v[0] for v in folder_age.values()])), reverse=True)]
                elif len(parts) == 1:
                    folders = [(-1, f"y_m_f://{parts[0]}/", mo, "", "", 0, 0, 0) for mo in sorted(list(set([v[1] for pp, v in folder_age.items() if v[0] == parts[0]])))]
                elif len(parts) == 2:
                    for pp in [pp for pp, age in folder_age.items() if age == (parts[0], parts[1])]:
                        cnt, sz = totals.get(pp, (0,0))
                        folders.append((-1, f"y_m_f://{parts[0]}/{parts[1]}/", pp.strip("/").split("/")[-1] if pp.strip("/") else "Root_Files", "", "", 0, cnt, sz))
                elif len(parts) >= 3:
                    for pp, age in folder_age.items():
                        if age == (parts[0], parts[1]) and (pp.strip("/").split("/")[-1] if pp.strip("/") else "Root_Files") == parts[2]:
                            cur.execute(f"SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE parent_path=? AND is_folder=0 AND in_trash=0 {h_q}", (pp,))
                            files = cur.fetchall()
                            break

            elif matched_proto:
                cols = SMART_PROTOCOLS[matched_proto]
                parts = [p for p in self.target_path.replace(matched_proto, "").split("/") if p]
                depth = len(parts)
                if depth < len(cols):
                    target_col = cols[depth]
                    where_clauses = ["is_folder=0", "in_trash=0", f"{target_col} != ''"] + [f"{cols[i]}=?" for i in range(depth)]
                    cur.execute(f"SELECT {target_col}, COUNT(id), SUM(size) FROM virtual_fs WHERE {' AND '.join(where_clauses)} GROUP BY {target_col}", tuple(parts))
                    base_path = matched_proto + "/".join(parts) + "/" if parts else matched_proto
                    folders = [(-1, base_path, r[0], "", "", 0, r[1], r[2] or 0) for r in cur.fetchall() if r[0]]
                else:
                    where_clauses = ["is_folder=0", "in_trash=0"] + [f"{cols[i]}=?" for i in range(len(cols))]
                    if not self.show_hidden: where_clauses.append("is_hidden=0")
                    cur.execute(f"SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE {' AND '.join(where_clauses)}", tuple(parts))
                    files = cur.fetchall()

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
                cur.execute(f"SELECT id, name, color_tag, secondary_name, is_hidden, (SELECT COUNT(id) FROM virtual_fs f2 WHERE f2.parent_path LIKE virtual_fs.parent_path || virtual_fs.name || '/%' AND f2.is_folder=0 AND f2.in_trash=0), (SELECT SUM(size) FROM virtual_fs f2 WHERE f2.parent_path LIKE virtual_fs.parent_path || virtual_fs.name || '/%' AND f2.is_folder=0 AND f2.in_trash=0) FROM virtual_fs WHERE parent_path=? AND is_folder=1 AND in_trash=0 {h_q}", (self.target_path,))
                folders = [(r[0], self.target_path, r[1], r[2], r[3], r[4], r[5] or 0, r[6] or 0) for r in cur.fetchall()] 
                cur.execute(f"SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE parent_path=? AND is_folder=0 AND in_trash=0 {h_q}", (self.target_path,))
                files = cur.fetchall()
                
            if not self.is_cancelled: self.data_ready.emit(folders, files)
        except Exception as e: print("DB Load Error:", e)
        finally: conn.close()


class SizeTableWidgetItem(QTableWidgetItem):
    def __init__(self, size_bytes):
        super().__init__(human_size(size_bytes))
        self.size_bytes = size_bytes
        self.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)

    def __lt__(self, other):
        if isinstance(other, SizeTableWidgetItem):
            return self.size_bytes < other.size_bytes
        return super().__lt__(other)

class SpaceScannerThread(QThread):
    progress = Signal(int, int, str)
    found = Signal(str, str, str, str, int, str, str, int) 
    finished_scan = Signal()
    
    def __init__(self, db_path, scan_roots=["/"], parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.scan_roots = scan_roots
        self.is_cancelled = False
        
    def cancel(self): self.is_cancelled = True
    
    def run(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            
            path_cond = " OR ".join(["parent_path LIKE ?"] * len(self.scan_roots))
            path_params = tuple(f"{p}%" for p in self.scan_roots)
            
            self.progress.emit(10, 100, "Scanning for Junk...")
            cur.execute(f"SELECT id, name, parent_path, extension, size, modified, sha256 FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND ({path_cond}) AND (extension IN ('.tmp', '.bak', '.log', '.cache') OR name LIKE '%cache%')", path_params)
            for r in cur.fetchall():
                if self.is_cancelled: return
                self.found.emit("Junk File", r[1], r[2], r[3] or "", r[4] or 0, r[5] or "Unknown", r[6] or "", r[0])
            
            self.progress.emit(40, 100, "Scanning for Huge Files...")
            cur.execute(f"SELECT id, name, parent_path, extension, size, modified, sha256 FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND ({path_cond}) AND size > 524288000 ORDER BY size DESC", path_params)
            for r in cur.fetchall():
                if self.is_cancelled: return
                self.found.emit("Huge File (>500MB)", r[1], r[2], r[3] or "", r[4] or 0, r[5] or "Unknown", r[6] or "", r[0])
            
            self.progress.emit(70, 100, "Scanning for Duplicates...")
            cur.execute(f"SELECT size, extension, COUNT(*) as c FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND ({path_cond}) AND size > 0 GROUP BY size, extension HAVING c > 1", path_params)
            for size, ext, count in cur.fetchall():
                if self.is_cancelled: return
                cur.execute(f"SELECT id, name, parent_path, extension, modified, sha256 FROM virtual_fs WHERE size=? AND extension=? AND is_folder=0 AND in_trash=0 AND ({path_cond})", (size, ext) + path_params)
                files = cur.fetchall()
                for f in files[1:]: 
                    self.found.emit("Duplicate File", f[1], f[2], f[3] or "", size or 0, f[4] or "Unknown", f[5] or "", f[0])
            
            self.progress.emit(100, 100, "Scan Complete.")
        finally:
            if 'conn' in locals(): conn.close()
            self.finished_scan.emit()

class ImportFilesThread(QThread):
    progress = Signal(int, int, str)
    finished_import = Signal(int, int)
    error = Signal(str)
    def __init__(self, db_path, target_prefix, paths, parent=None):
        super().__init__(parent)
        self.db_path, self.target_prefix, self.paths = db_path, target_prefix, paths
        self.is_cancelled = False
    def cancel(self): self.is_cancelled = True
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
                else: all_files_to_process.append(p)
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
                            mod = datetime.fromtimestamp(os.path.getmtime(fp)).strftime("%Y-%m-%d %H:%M:%S")
                            cre = datetime.fromtimestamp(os.path.getctime(fp)).strftime("%Y-%m-%d %H:%M:%S")
                            records.append((curr_parent, f, 0, fp, os.path.getsize(fp), ext, mod, get_category_for_ext(ext), mod[0:4], mod[5:7], cre))
                            added_f += 1
                        cur.executemany("INSERT INTO virtual_fs (parent_path, name, is_folder, real_path, size, extension, modified, category, year, month, creation_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", records)
                        self.progress.emit(added_f, total, f"Imported {added_f}/{total} files...")
                else:
                    ext = os.path.splitext(p)[1].lower()
                    mod = datetime.fromtimestamp(os.path.getmtime(p)).strftime("%Y-%m-%d %H:%M:%S")
                    cre = datetime.fromtimestamp(os.path.getctime(p)).strftime("%Y-%m-%d %H:%M:%S")
                    cur.execute("INSERT INTO virtual_fs (parent_path, name, is_folder, real_path, size, extension, modified, category, year, month, creation_date) VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?)", (self.target_prefix, os.path.basename(p), p, os.path.getsize(p), ext, mod, get_category_for_ext(ext), mod[0:4], mod[5:7], cre))
                    added_f += 1
                    self.progress.emit(added_f, total, f"Imported {added_f}/{total} files...")
            conn.commit()
            conn.close()
            self.finished_import.emit(added_f, added_d)
        except Exception as e: self.error.emit(str(e))

class CompilerThread(QThread):
    progress = Signal(int, int, str)
    finished = Signal(str)
    error = Signal(str)
    def __init__(self, source_db, target_db, source_prefix, query, params, parent=None):
        super().__init__(parent)
        self.source_db, self.target_db, self.source_prefix, self.query, self.params = source_db, target_db, source_prefix, query, params
        self.is_cancelled = False
    def cancel(self): self.is_cancelled = True
    def run(self):
        try:
            if self.is_cancelled: return
            self.progress.emit(0, 100, "Initializing compiler...")
            if os.path.exists(self.target_db): os.remove(self.target_db)
            tgt_db = vmanDB(Path(self.target_db))
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
                if self.is_cancelled: return
                r_list = list(r)
                if is_smart_view: r_list[1] = "/"
                else:
                    if r_list[1].startswith(self.source_prefix):
                        r_list[1] = "/" + r_list[1][len(self.source_prefix):]
                        if not r_list[1].startswith("/"): r_list[1] = "/" + r_list[1]
                modified_rows.append(tuple(r_list))
            tgt_conn = tgt_db.conn
            tgt_cur = tgt_conn.cursor()
            batch_size = 1000
            for i in range(0, total, batch_size):
                if self.is_cancelled: return
                tgt_cur.executemany(f"INSERT INTO virtual_fs VALUES ({','.join(['?']*18)})", modified_rows[i:i+batch_size])
                tgt_conn.commit()
                self.progress.emit(int(40 + (i/total)*60), 100, f"Compiled {min(i+batch_size, total)}/{total} records...")
            
            src_conn.close()
            tgt_conn.close()
            self.finished.emit(self.target_db)
        except Exception as e: self.error.emit(str(e))

class ExportZipThread(QThread):
    progress = Signal(int, int, str)
    finished = Signal(str)
    error = Signal(str)
    def __init__(self, db_path, items_to_export, zip_filepath, parent=None):
        super().__init__(parent)
        self.db_path, self.items_to_export, self.zip_filepath = db_path, items_to_export, zip_filepath
        self.is_cancelled = False
    def cancel(self): self.is_cancelled = True
    def run(self):
        try:
            conn = sqlite3.connect(str(self.db_path))
            cur = conn.cursor()
            all_files_to_zip = []
            
            for typ, path_val, db_id in self.items_to_export: 
                if self.is_cancelled: return
                
                if typ == "file":
                    cur.execute("SELECT real_path, name FROM virtual_fs WHERE id=?", (db_id,))
                    res = cur.fetchone()
                    if res and res[0] and os.path.exists(res[0]):
                        all_files_to_zip.append((res[0], res[1]))
                elif typ == "folder":
                    cur.execute("SELECT parent_path, name, real_path, is_folder FROM virtual_fs WHERE parent_path LIKE ? AND in_trash=0", (f"{path_val}%",))
                    for pp, n, rp, is_f in cur.fetchall():
                        if not is_f and rp and os.path.exists(rp):
                            rel_dest = f"{path_val.strip('/').split('/')[-1]}/{pp[len(path_val):]}{n}".replace("//", "/")
                            all_files_to_zip.append((rp, rel_dest))
                            
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
        except Exception as e: self.error.emit(str(e))

class ImageLoader(QThread):
    finished = Signal(str, object)
    def __init__(self, path: str, max_size=(1920, 1080), parent=None): 
        super().__init__(parent)
        self.path, self.max_size = path, max_size
    def run(self):
        try:
            img = QImage(self.path)
            if not img.isNull() and img.width() > 0:
                if img.width() > self.max_size[0] or img.height() > self.max_size[1]: 
                    # FIX: Double resizing bug avoided with FastTransformation
                    img = img.scaled(*self.max_size, Qt.KeepAspectRatio, Qt.FastTransformation)
            self.finished.emit(self.path, img)
        except Exception: self.finished.emit(self.path, QImage())

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
        except Exception as e: self.finished.emit(f"Error: {str(e)}")

# ---------------- UI Widgets ----------------
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
            if item.widget(): item.widget().deleteLater()
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

        if actual_path == "/": pass
        elif actual_path.startswith("trash://"): add_btn("🗑 Trash", "trash://")
        elif actual_path.startswith("fav://"): add_btn("⭐ Favorites", "fav://")
        else:
            matched = False
            for proto in SMART_PROTOCOLS.keys():
                if actual_path.startswith(proto):
                    add_btn(f"💡 {proto.replace('://', '').upper().replace('_', ' ➔ ')}", proto)
                    p_str = actual_path.replace(proto, "").strip("/")
                    if p_str:
                        curr = proto
                        for part in p_str.split("/"): 
                            curr += part + "/"
                            add_btn(part, curr)
                    matched = True; break
            if not matched:
                current_build = "/"
                for part in [p for p in actual_path.split("/") if p]: 
                    current_build += part + "/"
                    add_btn(part, current_build)
        self.layout.addStretch()

class vmanTableModel(QAbstractTableModel):
    def __init__(self, headers: List[str], rows: List[Dict], parent=None):
        super().__init__(parent)
        self.headers, self.all_rows, self.display_limit = headers, rows, CHUNK_SIZE
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
        self._pixmap = pm; self.update()
        self.eff.setOpacity(0.0); self.anim.setStartValue(0.0); self.anim.setEndValue(1.0); self.anim.start()
    def clear(self): self._pixmap = None; self.update()
    def paintEvent(self, event):
        super().paintEvent(event)
        if self._pixmap and not self._pixmap.isNull() and self._pixmap.width() > 0:
            painter = QPainter(self)
            painter.setRenderHint(QPainter.Antialiasing)
            painter.setRenderHint(QPainter.SmoothPixmapTransform)
            scaled = self._pixmap.scaled(self.size(), Qt.KeepAspectRatio, Qt.SmoothTransformation)
            painter.drawPixmap((self.width() - scaled.width()) // 2, (self.height() - scaled.height()) // 2, scaled)

class SandboxTableView(QTableView):
    filesDroppedOS = Signal(list); internalDrop = Signal(str, bool); openRequest = Signal()
    def __init__(self, parent=None): 
        super().__init__(parent); self.setAcceptDrops(True); self.setDragEnabled(True); self.setDragDropMode(QAbstractItemView.DragDrop)
    def keyPressEvent(self, event: QKeyEvent): 
        if event.key() in (Qt.Key_Return, Qt.Key_Enter): self.openRequest.emit()
        else: super().keyPressEvent(event)
    def startDrag(self, supportedActions):
        sel = self.selectionModel().selectedRows()
        if not sel: return
        self.window()._current_drag_items = [self.model().data(self.model().index(idx.row(), 0), Qt.UserRole) for idx in sel]
        drag = QDrag(self); mime = QMimeData(); mime.setText("vman_internal_drag"); drag.setMimeData(mime); drag.exec(Qt.MoveAction | Qt.CopyAction)
    def dragEnterEvent(self, event: QDragEnterEvent): 
        if event.mimeData().hasUrls() or event.mimeData().text() == "vman_internal_drag": event.acceptProposedAction()
        else: super().dragEnterEvent(event)
    def dragMoveEvent(self, event): 
        if event.mimeData().hasUrls() or event.mimeData().text() == "vman_internal_drag": event.acceptProposedAction()
        else: super().dragMoveEvent(event)
    def dropEvent(self, event: QDropEvent):
        if event.mimeData().hasUrls(): 
            self.filesDroppedOS.emit([url.toLocalFile() for url in event.mimeData().urls()]); event.acceptProposedAction()
        elif event.mimeData().text() == "vman_internal_drag":
            idx = self.indexAt(event.position().toPoint())
            dest_path = self.window().current_prefix
            if idx.isValid():
                data = self.model().data(self.model().index(idx.row(), 0), Qt.UserRole)
                if data and data[0] == "folder": dest_path = data[1]
            self.internalDrop.emit(dest_path, bool(event.keyboardModifiers() & (Qt.ControlModifier | Qt.ShiftModifier)))
            event.acceptProposedAction()
        else: super().dropEvent(event)

class SandboxListView(QListView):
    filesDroppedOS = Signal(list); internalDrop = Signal(str, bool); openRequest = Signal()
    def __init__(self, parent=None): 
        super().__init__(parent); self.setAcceptDrops(True); self.setDragEnabled(True); self.setDragDropMode(QAbstractItemView.DragDrop)
        self.setViewMode(QListView.IconMode); self.setGridSize(QSize(140, 160)); self.setIconSize(QSize(80, 80))
        self.setUniformItemSizes(True); self.setWordWrap(True); self.setSpacing(10)
        self.setResizeMode(QListView.Adjust); self.setSelectionMode(QAbstractItemView.ExtendedSelection)
    def keyPressEvent(self, event: QKeyEvent): 
        if event.key() in (Qt.Key_Return, Qt.Key_Enter): self.openRequest.emit()
        else: super().keyPressEvent(event)
    def startDrag(self, supportedActions):
        sel = self.selectionModel().selectedIndexes()
        if not sel: return
        self.window()._current_drag_items = [self.model().data(idx, Qt.UserRole) for idx in sel]
        drag = QDrag(self); mime = QMimeData(); mime.setText("vman_internal_drag"); drag.setMimeData(mime); drag.exec(Qt.MoveAction | Qt.CopyAction)
    def dragEnterEvent(self, event: QDragEnterEvent): 
        if event.mimeData().hasUrls() or event.mimeData().text() == "vman_internal_drag": event.acceptProposedAction()
        else: super().dragEnterEvent(event)
    def dragMoveEvent(self, event): 
        if event.mimeData().hasUrls() or event.mimeData().text() == "vman_internal_drag": event.acceptProposedAction()
        else: super().dragMoveEvent(event)
    def dropEvent(self, event: QDropEvent):
        if event.mimeData().hasUrls(): 
            self.filesDroppedOS.emit([url.toLocalFile() for url in event.mimeData().urls()]); event.acceptProposedAction()
        elif event.mimeData().text() == "vman_internal_drag":
            idx = self.indexAt(event.position().toPoint())
            dest_path = self.window().current_prefix
            if idx.isValid():
                data = self.model().data(idx, Qt.UserRole)
                if data and data[0] == "folder": dest_path = data[1]
            self.internalDrop.emit(dest_path, bool(event.keyboardModifiers() & (Qt.ControlModifier | Qt.ShiftModifier)))
            event.acceptProposedAction()
        else: super().dropEvent(event)

class InternalTreeWidget(QTreeWidget):
    def __init__(self, parent=None): 
        super().__init__(parent); self.setAcceptDrops(True)
    def dragEnterEvent(self, event: QDragEnterEvent): 
        if event.mimeData().text() == "vman_internal_drag": event.acceptProposedAction()
        else: super().dragEnterEvent(event)
    def dragMoveEvent(self, event): 
        if event.mimeData().text() == "vman_internal_drag": event.acceptProposedAction()
        else: super().dragMoveEvent(event)
    def dropEvent(self, event: QDropEvent):
        if event.mimeData().text() == "vman_internal_drag" and self.itemAt(event.position().toPoint()):
            self.window().execute_internal_drop(self.itemAt(event.position().toPoint()).data(0, Qt.UserRole), bool(event.keyboardModifiers() & (Qt.ControlModifier | Qt.ShiftModifier)))
            event.acceptProposedAction()

# ---------------- Dialogs ----------------
class DuplicateProofDialog(QDialog):
    def __init__(self, db_path, query, params, match_type, main_app, parent=None):
        super().__init__(parent)
        self.main_app = main_app
        self.setWindowTitle(f"Collision Proof: {match_type}")
        self.resize(850, 500)
        
        if main_app and hasattr(main_app, 'theme_combo'):
            self.setStyleSheet(THEMES.get(main_app.theme_combo.currentText(), THEMES["Dark"]))
        else:
            self.setStyleSheet(THEMES["Dark"])
        
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(f"<h3 style='color:#58a6ff;'>Analysis: {match_type}</h3>"))
        layout.addWidget(QLabel("The following files triggered this conflict rule based on matching parameters.<br>You can inspect or open them directly below to verify."))
        
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        container = QWidget()
        self.vbox = QVBoxLayout(container)
        
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            files = cur.fetchall()
            
            for f in files:
                db_id, name, pp, size, mod, sha, real_p = f
                card = QFrame()
                card.setStyleSheet("background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 10px; margin-bottom: 5px;")
                c_lay = QGridLayout(card)
                
                c_lay.addWidget(QLabel("<b>File Name:</b>"), 0, 0)
                c_lay.addWidget(QLabel(name), 0, 1)
                
                c_lay.addWidget(QLabel("<b>Virtual Path:</b>"), 1, 0)
                c_lay.addWidget(QLabel(pp), 1, 1)
                
                c_lay.addWidget(QLabel("<b>Physical OS Path:</b>"), 2, 0)
                txt_real = QLineEdit(real_p if real_p else "Disconnected")
                txt_real.setReadOnly(True)
                c_lay.addWidget(txt_real, 2, 1)
                
                c_lay.addWidget(QLabel("<b>Size & Modified:</b>"), 3, 0)
                c_lay.addWidget(QLabel(f"{human_size(size)} | {mod}"), 3, 1)
                
                c_lay.addWidget(QLabel("<b>SHA-256 Hash:</b>"), 4, 0)
                txt_hash = QLineEdit(sha if sha else "Not Computed")
                txt_hash.setReadOnly(True)
                c_lay.addWidget(txt_hash, 4, 1)
                
                btn_lay = QHBoxLayout()
                btn_open = QPushButton("🚀 Open File")
                btn_open.clicked.connect(lambda checked=False, id=db_id: self.main_app.open_local_file_system(id))
                
                btn_loc = QPushButton("📂 Open Location")
                btn_loc.clicked.connect(lambda checked=False, id=db_id: self.main_app.open_file_location(id))
                
                btn_lay.addWidget(btn_open)
                btn_lay.addWidget(btn_loc)
                btn_lay.addStretch()
                
                c_lay.addLayout(btn_lay, 5, 0, 1, 2)
                self.vbox.addWidget(card)
                
        self.vbox.addStretch()
        scroll.setWidget(container)
        layout.addWidget(scroll)
        
        btn_close = QPushButton("Close Proof")
        btn_close.clicked.connect(self.accept)
        layout.addWidget(btn_close)


class HierarchyConfigDialog(QDialog):
    def __init__(self, current_levels, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Configure Hierarchy Names")
        self.resize(500, 150)
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("<b>Define preferred column names (Comma separated):</b>"))
        layout.addWidget(QLabel("<small><i>Columns are spawned infinitely. These names apply to the first few levels.</i></small>"))
        self.txt_levels = QLineEdit(", ".join(current_levels))
        layout.addWidget(self.txt_levels)
        btn_save = QPushButton("Save Names")
        btn_save.clicked.connect(self.accept)
        layout.addWidget(btn_save)
        
    def get_levels(self):
        return [l.strip() for l in self.txt_levels.text().split(',') if l.strip()]

class PaginatingChartWidget(QWidget):
    def __init__(self, title, parent=None):
        super().__init__(parent)
        self.title = title
        self.full_data = []
        self.current_page = 0
        
        self.layout = QVBoxLayout(self)
        
        # --- Controls ---
        ctrl_lay = QHBoxLayout()
        self.type_cb = QComboBox()
        self.type_cb.addItems(["Horizontal Bar", "Pie", "Scatter"])
        self.type_cb.currentTextChanged.connect(self.draw_chart)
        
        self.per_page_spin = QSpinBox()
        self.per_page_spin.setRange(5, 100)
        self.per_page_spin.setValue(20) 
        self.per_page_spin.setPrefix("Per Page: ")
        self.per_page_spin.valueChanged.connect(self.on_per_page_changed)
        
        self.btn_prev = QPushButton("◀ Prev")
        self.btn_prev.clicked.connect(self.page_prev)
        self.lbl_page = QLabel("Page 1")
        self.lbl_page.setAlignment(Qt.AlignCenter)
        self.btn_next = QPushButton("Next ▶")
        self.btn_next.clicked.connect(self.page_next)
        
        ctrl_lay.addWidget(QLabel("Type:"))
        ctrl_lay.addWidget(self.type_cb)
        ctrl_lay.addWidget(self.per_page_spin)
        ctrl_lay.addStretch()
        ctrl_lay.addWidget(self.btn_prev)
        ctrl_lay.addWidget(self.lbl_page)
        ctrl_lay.addWidget(self.btn_next)
        self.layout.addLayout(ctrl_lay)
        
        # --- Canvas ---
        if MATPLOTLIB_AVAILABLE:
            self.fig = Figure(figsize=(8, 5), dpi=100)
            self.ax = self.fig.add_subplot(111)
            self.canvas = FigureCanvas(self.fig)
            self.layout.addWidget(self.canvas)
        else:
            self.layout.addWidget(QLabel("Matplotlib not installed. Please pip install matplotlib."))

    def update_data(self, data_dict):
        self.full_data = sorted([(str(k), v) for k, v in data_dict.items()], key=lambda x: x[1], reverse=True)
        self.current_page = 0
        self.draw_chart()

    def on_per_page_changed(self):
        self.current_page = 0
        self.draw_chart()

    def page_prev(self):
        if self.current_page > 0:
            self.current_page -= 1
            self.draw_chart()

    def page_next(self):
        max_page = math.ceil(len(self.full_data) / self.per_page_spin.value()) - 1
        if self.current_page < max_page:
            self.current_page += 1
            self.draw_chart()

    def draw_chart(self):
        if not MATPLOTLIB_AVAILABLE: return
        self.ax.clear()
        
        per_page = self.per_page_spin.value()
        total_pages = max(1, math.ceil(len(self.full_data) / per_page))
        self.current_page = min(self.current_page, total_pages - 1)
        
        start_idx = self.current_page * per_page
        end_idx = start_idx + per_page
        page_data = self.full_data[start_idx:end_idx]
        
        self.btn_prev.setEnabled(self.current_page > 0)
        self.btn_next.setEnabled(self.current_page < total_pages - 1)
        self.lbl_page.setText(f"Rank {start_idx + 1}-{min(end_idx, len(self.full_data))} (of {len(self.full_data)})")

        if not page_data:
            self.ax.text(0.5, 0.5, "No Data Available", color='white', ha='center')
            self.fig.patch.set_facecolor('#0d1117'); self.ax.set_facecolor('#0d1117')
            self.canvas.draw(); return

        keys = [k[:30] + '..' if len(k) > 30 else k for k, v in page_data]
        values = [v for k, v in page_data]
        
        chart_type = self.type_cb.currentText()
        
        # --- NEW: Vibrant Color Palette ---
        color_palette = ["#58a6ff", "#3fb950", "#e3b341", "#a371f7", "#f85149", "#d2a8ff", 
                         "#79c0ff", "#2ea043", "#ff7b72", "#bc8cff", "#f2cc60"]
        
        # Cycle through colors based on the number of items on the page
        chart_colors = [color_palette[i % len(color_palette)] for i in range(len(keys))]
        
        self.fig.patch.set_facecolor('#0d1117')
        self.ax.set_facecolor('#0d1117')
        self.ax.tick_params(colors='white')
        
        if chart_type == "Horizontal Bar":
            self.ax.barh(keys, values, color=chart_colors)
            self.ax.invert_yaxis() # Put highest value at the top
        elif chart_type == "Pie":
            self.ax.pie(values, labels=keys, autopct='%1.1f%%', colors=chart_colors, textprops={'color': "white"})
            self.ax.axis("equal")
        elif chart_type == "Scatter":
            self.ax.scatter(values, keys, c=chart_colors, s=100, edgecolors='white', linewidth=0.5, alpha=0.9)
            self.ax.invert_yaxis()

        if chart_type != "Pie":
            self.ax.grid(True, axis='x', linestyle='--', alpha=0.3, color='white')
            for spine in self.ax.spines.values(): spine.set_color('#30363d')
            
        self.ax.set_title(self.title, color='white', pad=15, fontweight='bold')
        self.fig.tight_layout()
        self.canvas.draw()


class TagListWidget(QListWidget):
    def __init__(self, parent_dialog):
        super().__init__()
        self.parent_dialog = parent_dialog

    def keyPressEvent(self, event):
        if event.key() == Qt.Key_Right:
            if self.parent_dialog.dynamic_lists:
                first_list = self.parent_dialog.dynamic_lists[0]['list']
                first_list.setFocus()
                if first_list.count() > 0 and first_list.currentRow() == -1:
                    first_list.setCurrentRow(0)
        elif event.key() in (Qt.Key_Return, Qt.Key_Enter):
            item = self.currentItem()
            if item: self.parent_dialog.on_tag_clicked(item)
        else:
            super().keyPressEvent(event)

class ColumnListWidget(QListWidget):
    def __init__(self, level_index, parent_dialog):
        super().__init__()
        self.level_index = level_index
        self.parent_dialog = parent_dialog

    def keyPressEvent(self, event):
        if event.key() == Qt.Key_Right:
            if self.level_index + 1 < len(self.parent_dialog.dynamic_lists):
                next_list = self.parent_dialog.dynamic_lists[self.level_index + 1]['list']
                next_list.setFocus()
                if next_list.count() > 0 and next_list.currentRow() == -1:
                    next_list.setCurrentRow(0)
                self.parent_dialog.column_scroll_area.ensureWidgetVisible(self.parent_dialog.dynamic_lists[self.level_index + 1]['widget'])
        elif event.key() == Qt.Key_Left:
            if self.level_index > 0:
                prev_list = self.parent_dialog.dynamic_lists[self.level_index - 1]['list']
                prev_list.setFocus()
                self.parent_dialog.column_scroll_area.ensureWidgetVisible(self.parent_dialog.dynamic_lists[self.level_index - 1]['widget'])
            else:
                self.parent_dialog.tag_list.setFocus()
        elif event.key() in (Qt.Key_Return, Qt.Key_Enter):
            item = self.currentItem()
            if item: self.parent_dialog.jump_to_virtual_or_real_path(item, force_real=False)
        else:
            super().keyPressEvent(event)

class vmanTagLibraryDialog(QDialog):
    def __init__(self, db_path, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.main_app = parent
        self.tag_cache = {}  
        self.base_v_path = "/" 
        

        # --- 1. Load Custom Names Persistently ---
        self.settings = QSettings("vmanOS", "TagLibraryConfig")
        saved_levels = self.settings.value("hierarchy_levels")
        if saved_levels:
            self.hierarchy_levels = list(saved_levels)
        else:
            self.hierarchy_levels = ["Parent Folder", "Level 1", "Level 2", "Level 3", "Level 4", "Level 5"]
            
        # Extract dynamic names for the UI (Expanded to 5 levels)
        self.l0_name = self.hierarchy_levels[0] if len(self.hierarchy_levels) > 0 else "Parent Folder"
        self.l1_name = self.hierarchy_levels[1] if len(self.hierarchy_levels) > 1 else "Level 1"
        self.l2_name = self.hierarchy_levels[2] if len(self.hierarchy_levels) > 2 else "Level 2"
        self.l3_name = self.hierarchy_levels[3] if len(self.hierarchy_levels) > 3 else "Level 3"
        self.l4_name = self.hierarchy_levels[4] if len(self.hierarchy_levels) > 4 else "Level 4"
        
        self.setWindowTitle("Universal Tag Engine & Analytics")
        self.resize(1300, 800)
        
        if self.main_app and hasattr(self.main_app, 'theme_combo'):
            self.setStyleSheet(THEMES.get(self.main_app.theme_combo.currentText(), THEMES["Dark"]))

        self._build_toolbar() 
        self.main_layout = QVBoxLayout(self)
        self.main_layout.addLayout(self.top_toolbar)
        
        # --- Master Tab Widget ---
        self.tabs = QTabWidget()
        self.tabs.setStyleSheet("""
            QTabBar::tab { background: #161b22; color: #8b949e; padding: 10px 15px; border: 1px solid #30363d; border-top-left-radius: 4px; border-top-right-radius: 4px; }
            QTabBar::tab:selected { background: #0d1117; color: #58a6ff; font-weight: bold; border-bottom: 2px solid #58a6ff; }
            QTabWidget::pane { border: 1px solid #30363d; top: -1px; }
        """)
        self.main_layout.addWidget(self.tabs, stretch=1)

        # Tab 1: Original Browser UI
        self.browser_tab = QWidget()
        self.browser_layout = QVBoxLayout(self.browser_tab)
        self.browser_layout.setContentsMargins(0,0,0,0)

        self.main_splitter = QSplitter(Qt.Horizontal)
        self.column_scroll_area = QScrollArea()
        self.column_scroll_area.setWidgetResizable(True)
        self.column_scroll_area.setFrameShape(QScrollArea.NoFrame)
        self.column_scroll_widget = QWidget()
        self.column_container_layout = QHBoxLayout(self.column_scroll_widget)
        self.column_container_layout.setAlignment(Qt.AlignLeft)
        self.column_scroll_area.setWidget(self.column_scroll_widget)

        self.tag_search_box = QLineEdit(); self.tag_search_box.setPlaceholderText("Search tags...")        
        self.tag_list = TagListWidget(self)        
        self.tag_list.itemClicked.connect(self.on_tag_clicked)
        self.tag_list.itemDoubleClicked.connect(lambda item: self.main_app.nav_to_path(f"tags://{item.text()}/") if self.main_app else None)
        self.tag_search_box.textChanged.connect(lambda text: self._filter_list(self.tag_list, text))
       
        
        tag_widget = QWidget()
        tag_layout = QVBoxLayout(tag_widget)
        tag_layout.setContentsMargins(0, 0, 0, 0)
        tag_layout.addWidget(QLabel("<b>🏷️ Tags</b>"))
        tag_layout.addWidget(self.tag_search_box)
        tag_layout.addWidget(self.tag_list)
        
        self.main_splitter.addWidget(self.column_scroll_area)
        self.main_splitter.addWidget(tag_widget)
        self.main_splitter.setStretchFactor(0, 4) 
        self.main_splitter.setStretchFactor(1, 1)
        self.browser_layout.addWidget(self.main_splitter)
        
        self.tabs.addTab(self.browser_tab, "📂 Library Browser")

        # Tabs 2-8: Built-in Analytics 
        self.dashboard_tab = QWidget()
        self.dashboard_layout = QGridLayout(self.dashboard_tab)
        
        self.tag_chart_tab = PaginatingChartWidget("Tag Frequency & Distribution", self)
        self.l0_chart_tab = PaginatingChartWidget(f"Volume by {self.l0_name}", self)
        self.l1_chart_tab = PaginatingChartWidget(f"Volume by {self.l1_name}", self)
        self.l2_chart_tab = PaginatingChartWidget(f"Volume by {self.l2_name}", self)
        self.l3_chart_tab = PaginatingChartWidget(f"Volume by {self.l3_name}", self)
        self.l4_chart_tab = PaginatingChartWidget(f"Volume by {self.l4_name}", self)
        
        self.tabs.addTab(self.dashboard_tab, "🏠 Executive Dashboard")
        self.tabs.addTab(self.tag_chart_tab, "🏷 Tag Analytics")
        self.tabs.addTab(self.l0_chart_tab, f"🗂 {self.l0_name} Analytics")
        self.tabs.addTab(self.l1_chart_tab, f"📂 {self.l1_name} Analytics")
        self.tabs.addTab(self.l2_chart_tab, f"📁 {self.l2_name} Analytics")
        self.tabs.addTab(self.l3_chart_tab, f"📄 {self.l3_name} Analytics")
        self.tabs.addTab(self.l4_chart_tab, f"📑 {self.l4_name} Analytics")

        self.dynamic_lists = [] 
        self._populate_base_contexts()
        self.refresh_memory_cache()
        self._setup_shortcuts()

    def _setup_shortcuts(self):
        QShortcut(QKeySequence("Ctrl+F"), self, self.global_search_box.setFocus)
        QShortcut(QKeySequence("Ctrl+T"), self, self.tag_search_box.setFocus)

    def _build_toolbar(self):
        self.top_toolbar = QHBoxLayout()

        self.top_toolbar.addWidget(QLabel("<b>Base View:</b>"))
        
        self.combo_base = QComboBox()
        self.combo_base.setEditable(True)
        self.combo_base.setMinimumWidth(180)
        self.combo_base.currentTextChanged.connect(self.change_base_context)
        self.top_toolbar.addWidget(self.combo_base)
        
        self.btn_map_global = QPushButton("🔗 Map View to OS Folder")
        self.btn_map_global.setStyleSheet("font-weight: bold; color: #58a6ff;")
        self.btn_map_global.clicked.connect(self.map_global_base_to_os)
        self.top_toolbar.addWidget(self.btn_map_global)
        
        self.top_toolbar.addStretch() 
        
        self.radio_folders = QRadioButton("Folders")
        self.radio_files = QRadioButton("Files")
        self.radio_folders.setChecked(True)
        
        self.global_search_box = QLineEdit()
        self.global_search_box.setPlaceholderText("Global Search ...")
        self.global_search_box.setFixedWidth(250)
        
        
        self.top_toolbar.addWidget(self.radio_folders)
        self.top_toolbar.addWidget(self.radio_files)
        self.top_toolbar.addWidget(self.global_search_box)
        
        self.search_timer = QTimer(self)
        self.search_timer.setSingleShot(True)
        self.search_timer.setInterval(500) 
        self.search_timer.timeout.connect(lambda: self.run_global_search(self.global_search_box.text()))
        
        self.global_search_box.textChanged.connect(self.search_timer.start)
        self.global_search_box.returnPressed.connect(self.search_timer.stop)
        self.global_search_box.returnPressed.connect(lambda: self.run_global_search(self.global_search_box.text()))
        self.radio_folders.toggled.connect(lambda: self.run_global_search(self.global_search_box.text()))
        
        btn_refresh = QPushButton("🔄 Refresh")
        btn_refresh.clicked.connect(self.full_refresh)
        
        self.btn_config = QPushButton("⚙️ Custom Names")
        self.btn_config.clicked.connect(self.configure_hierarchy)
        
        self.btn_import = QPushButton("📥 Import")
        self.btn_import.clicked.connect(self.import_csv)
        
        self.btn_export = QPushButton("📤 Export")
        self.btn_export.clicked.connect(self.export_csv)

        self.top_toolbar.addWidget(btn_refresh)
        self.top_toolbar.addWidget(self.btn_config)
        self.top_toolbar.addWidget(self.btn_import)
        self.top_toolbar.addWidget(self.btn_export)

    def full_refresh(self):
        while self.dynamic_lists:
            col = self.dynamic_lists.pop()
            col['widget'].setParent(None)
            col['widget'].deleteLater()
            
        self.tag_cache.clear()
        self.tag_list.blockSignals(True)
        self.tag_list.clear()
        self.tag_search_box.clear()
        
        self.refresh_memory_cache()
        self._populate_base_contexts()
        self.tag_list.blockSignals(False)
        QMessageBox.information(self, "Refreshed", "Tag Library data has been fully synced with the database.")

    def configure_hierarchy(self):
        dlg = HierarchyConfigDialog(self.hierarchy_levels, self)
        if dlg.exec():
            self.hierarchy_levels = dlg.get_levels()
            self.settings.setValue("hierarchy_levels", self.hierarchy_levels)
            self.refresh_memory_cache()
            QMessageBox.information(self, "Saved", "Hierarchy names saved for all future sessions.\n\nNote: Close and reopen the Tag Library to update the Analytics Tab names.")

    def refresh_memory_cache(self):
        self.tag_cache.clear()
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cur = conn.cursor()
                cur.execute("SELECT parent_path, name, custom_tags, is_folder FROM virtual_fs WHERE parent_path LIKE ?", (f"{self.base_v_path}%",))
                for pp, name, tags, is_folder in cur.fetchall():
                    full_path = f"{pp}{name}/" if is_folder else f"{pp}{name}"
                    full_path = full_path.replace("//", "/")
                    tag_list = [t.strip() for t in str(tags).split(',')] if tags else []
                    if tag_list or is_folder:
                        self.tag_cache[full_path] = tag_list
        except Exception as e: print(f"DB Load Error: {e}")
        
        self.update_analytics_data()

        while self.dynamic_lists:
            col_data = self.dynamic_lists.pop()
            col_data['widget'].setParent(None)
            col_data['widget'].deleteLater()
            
        self._add_column(0, self.l0_name)
        self._populate_level(0, self.base_v_path)
        self._populate_all_tags()

    def update_analytics_data(self):
        tags_data, l0_data, l1_data, l2_data, l3_data, l4_data = {}, {}, {}, {}, {}, {}
        total_items = 0
        total_folders = 0
        total_files = 0
        multi_tagged_items = 0
        total_tags_applied = 0
        
        base_depth = len([p for p in self.base_v_path.split('/') if p])
        
        for path, tags in self.tag_cache.items():
            total_items += 1
            
            if path.endswith('/'):
                total_folders += 1
            else:
                total_files += 1
                
            valid_tags = [t for t in tags if t]
            total_tags_applied += len(valid_tags)
            if len(valid_tags) > 1:
                multi_tagged_items += 1
                
            parts = [p for p in path.split('/') if p]
            
            relative_parts = parts[base_depth:]
            
            if len(relative_parts) > 0: 
                l0_data[relative_parts[0]] = l0_data.get(relative_parts[0], 0) + 1
            if len(relative_parts) > 1: 
                l1_data[relative_parts[1]] = l1_data.get(relative_parts[1], 0) + 1
            if len(relative_parts) > 2: 
                l2_data[relative_parts[2]] = l2_data.get(relative_parts[2], 0) + 1
            if len(relative_parts) > 3: 
                l3_data[relative_parts[3]] = l3_data.get(relative_parts[3], 0) + 1
            if len(relative_parts) > 4: 
                l4_data[relative_parts[4]] = l4_data.get(relative_parts[4], 0) + 1
            
            for t in valid_tags:
                tags_data[t] = tags_data.get(t, 0) + 1

        self.tag_chart_tab.update_data(tags_data)
        self.l0_chart_tab.update_data(l0_data)
        self.l1_chart_tab.update_data(l1_data)
        self.l2_chart_tab.update_data(l2_data)
        self.l3_chart_tab.update_data(l3_data)
        self.l4_chart_tab.update_data(l4_data)
        
        for i in reversed(range(self.dashboard_layout.count())): 
            item = self.dashboard_layout.itemAt(i)
            if item.widget(): item.widget().setParent(None)

        avg_tags = round(total_tags_applied / total_items, 2) if total_items > 0 else 0

        metrics = [
            ("TOTAL VIRTUAL ITEMS", total_items),
            ("VIRTUAL FOLDERS", total_folders),
            ("VIRTUAL FILES", total_files),
            ("UNIQUE TAGS", len(tags_data)),
            ("MULTI-TAGGED ITEMS", multi_tagged_items),
            ("AVG TAGS PER ITEM", avg_tags),
            (f"TOTAL {self.l0_name.upper()}S", len(l0_data)),
            (f"TOTAL {self.l1_name.upper()}S", len(l1_data)),
            (f"TOTAL {self.l2_name.upper()}S", len(l2_data)),
            (f"TOTAL {self.l3_name.upper()}S", len(l3_data)),
            (f"TOTAL {self.l4_name.upper()}S", len(l4_data))
        ]
        
        colors = ["#58a6ff", "#3fb950", "#e3b341", "#a371f7", "#f85149", "#d2a8ff", 
                  "#79c0ff", "#2ea043", "#ff7b72", "#bc8cff", "#f2cc60"]
  
        row, col = 0, 0
        for idx, (title, val) in enumerate(metrics):
            accent_color = colors[idx % len(colors)]
            
            card = QFrame()
            card.setMinimumSize(220, 130)
            card.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            
            card.setStyleSheet(f"""
                QFrame {{
                    background: #161b22; 
                    border: 1px solid #30363d; 
                    border-radius: 8px; 
                    border-top: 5px solid {accent_color};
                }}
            """)
            
            c_lay = QVBoxLayout(card)
            
            lbl_title = QLabel(title)
            lbl_title.setStyleSheet("color: #8b949e; font-size: 11px; font-weight: bold; border: none;")
            lbl_title.setWordWrap(True)
            lbl_title.setMinimumHeight(30)
            
            lbl_val = QLabel(str(val))
            lbl_val.setStyleSheet(f"color: {accent_color}; font-size: 34px; font-weight: bold; border: none;")
            lbl_val.setAlignment(Qt.AlignCenter)
            
            c_lay.addWidget(lbl_title, alignment=Qt.AlignTop | Qt.AlignLeft)
            c_lay.addStretch()
            c_lay.addWidget(lbl_val)
            c_lay.addStretch()
            
            self.dashboard_layout.addWidget(card, row, col)
            
            col += 1
            if col > 3: 
                col = 0
                row += 1
                
        for i in range(4):
            self.dashboard_layout.setColumnStretch(i, 1)
        for i in range(row + 1):
            self.dashboard_layout.setRowStretch(i, 1)

    def _add_column(self, level_index, title):
        search_box = QLineEdit()
        search_box.setPlaceholderText(f"Search {title}...")
        
        lst = ColumnListWidget(level_index, self)
        lst.setContextMenuPolicy(Qt.CustomContextMenu)
        lst.customContextMenuRequested.connect(lambda pos, l=lst: self.show_context_menu(l, pos))
        
        lst.currentItemChanged.connect(lambda current, previous, idx=level_index: self.on_level_clicked(idx, current) if current else None)
        
        lst.itemDoubleClicked.connect(lambda item: self.jump_to_virtual_or_real_path(item, force_real=False))
        search_box.textChanged.connect(lambda text, l=lst: self._filter_list(l, text))

        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 5, 0)
        layout.addWidget(QLabel(f"<b>{title}</b>"))
        layout.addWidget(search_box)
        layout.addWidget(lst)

        widget = QWidget()
        widget.setLayout(layout)
        widget.setMinimumWidth(220) 
        widget.setMaximumWidth(300)

        self.column_container_layout.addWidget(widget)

        self.dynamic_lists.append({
            'widget': widget,
            'list': lst,
            'search': search_box
        })

    def _populate_base_contexts(self):
        self.combo_base.blockSignals(True)
        self.combo_base.clear()
        self.combo_base.addItem("/") 
        
        existing_bases = set(["/"])
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute("SELECT name FROM virtual_fs WHERE is_folder=1 AND parent_path='/'")
                for (name,) in cur.fetchall():
                    path = f"/{name}/"
                    if path not in existing_bases:
                        self.combo_base.addItem(path)
                        existing_bases.add(path)
        except Exception: pass
        
        if self.base_v_path not in existing_bases:
            self.combo_base.addItem(self.base_v_path)
            
        self.combo_base.setCurrentText(self.base_v_path)
        self.combo_base.blockSignals(False)

    def change_base_context(self, text):
        if not text.endswith('/'): text += '/'
        if not text.startswith('/'): text = '/' + text
        self.base_v_path = text
        self.refresh_memory_cache()

    def _get_children(self, prefix_path):
        items = set()
        for path in self.tag_cache.keys():
            if path.startswith(prefix_path) and path != prefix_path:
                remainder = path[len(prefix_path):].strip('/')
                if remainder: items.add(remainder.split('/')[0])
        return items

    def _populate_level(self, level_index, prefix_path):
        if level_index >= len(self.dynamic_lists): return
        items = self._get_children(prefix_path)
                    
        lst = self.dynamic_lists[level_index]['list']
        
        lst.blockSignals(True)
        lst.clear()
        
        for name in sorted(list(items)):
            item = QListWidgetItem(name)
            
            test_folder = f"{prefix_path}{name}/"
            test_file = f"{prefix_path}{name}"
            
            if test_folder in self.tag_cache or any(p.startswith(test_folder) for p in self.tag_cache.keys()):
                item.setData(Qt.UserRole, test_folder)
                item.setIcon(self.style().standardIcon(QStyle.SP_DirIcon)) 
            else:
                item.setData(Qt.UserRole, test_file) 
                item.setIcon(self.style().standardIcon(QStyle.SP_FileIcon)) 
                
            lst.addItem(item)
            
        lst.blockSignals(False)

    def _populate_all_tags(self):
        all_tags = set(tag for tags in self.tag_cache.values() for tag in tags if tag)
        self.tag_list.clear()
        for t in sorted(list(all_tags)): self.tag_list.addItem(QListWidgetItem(t))

    def on_level_clicked(self, level_index, item):
        v_path = item.data(Qt.UserRole)
        
        while len(self.dynamic_lists) > level_index + 1:
            col_data = self.dynamic_lists.pop()
            col_data['widget'].setParent(None)
            col_data['widget'].deleteLater()
            
        if v_path.endswith('/'):
            children = self._get_children(v_path)
            if children:
                next_level = level_index + 1
                title = self.hierarchy_levels[next_level] if next_level < len(self.hierarchy_levels) else f"Level {next_level + 1}"
                self._add_column(next_level, title)
                self._populate_level(next_level, v_path)
                
                QTimer.singleShot(50, lambda: self.column_scroll_area.horizontalScrollBar().setValue(self.column_scroll_area.horizontalScrollBar().maximum()))
                
        for col in self.dynamic_lists:
            lst = col['list']
            for row in range(lst.count()): lst.item(row).setHidden(False)

    def on_tag_clicked(self, item):
        target_tag = item.text()
        valid_paths = [p for p, tags in self.tag_cache.items() if target_tag in tags]
        base_depth = len([p for p in self.base_v_path.split('/') if p])
        
        max_depth = 0
        for vp in valid_paths:
            parts = [p for p in vp.split('/') if p]
            depth = len(parts) - base_depth
            if depth > max_depth: max_depth = depth
            
        while len(self.dynamic_lists) > max_depth:
            col = self.dynamic_lists.pop()
            col['widget'].setParent(None)
            col['widget'].deleteLater()
            
        for i in range(len(self.dynamic_lists), max_depth):
            title = self.hierarchy_levels[i] if i < len(self.hierarchy_levels) else f"Level {i + 1}"
            self._add_column(i, title)
            
        for i in range(max_depth):
            lst = self.dynamic_lists[i]['list']
            lst.blockSignals(True) 
            lst.clear()
            
            level_nodes = {}
            for vp in valid_paths:
                parts = [p for p in vp.split('/') if p]
                if len(parts) > base_depth + i:
                    name = parts[base_depth + i]
                    is_folder = (len(parts) > base_depth + i + 1) or vp.endswith('/')
                    node_v_path = "/" + "/".join(parts[:base_depth + i + 1]) + ("/" if is_folder else "")
                    level_nodes[name] = (node_v_path, is_folder)
            
            for name in sorted(level_nodes.keys()):
                node_v_path, is_folder = level_nodes[name]
                l_item = QListWidgetItem(name)
                l_item.setData(Qt.UserRole, node_v_path)
                
                if is_folder:
                    l_item.setIcon(self.style().standardIcon(QStyle.SP_DirIcon))
                else:
                    l_item.setIcon(self.style().standardIcon(QStyle.SP_FileIcon))
                
                if node_v_path in self.tag_cache and target_tag in self.tag_cache[node_v_path]:
                    l_item.setForeground(QBrush(QColor("#58a6ff")))
                    font = l_item.font()
                    font.setBold(True)
                    l_item.setFont(font)
                    
                lst.addItem(l_item)
                
            lst.blockSignals(False)

    def run_global_search(self, text):
        query = text.lower()
        if not query: 
            return self.refresh_memory_cache()
            
        valid_paths = set()
        
        include_folders = self.radio_folders.isChecked()
        include_files = self.radio_files.isChecked()

        for p in self.tag_cache.keys():
            is_fldr = p.endswith('/')
            if (is_fldr and not include_folders) or (not is_fldr and not include_files):
                continue 
            if query in p.lower():
                valid_paths.add(p)
                
        if self.radio_files.isChecked():
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute("SELECT parent_path, name FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND name LIKE ?", (f"%{query}%",))
                for pp, name in cur.fetchall():
                    valid_paths.add(f"{pp}{name}")
                    
        valid_paths = list(valid_paths)
        base_depth = len([p for p in self.base_v_path.split('/') if p])
        
        max_depth = 0
        for vp in valid_paths:
            parts = [p for p in vp.split('/') if p]
            depth = len(parts) - base_depth
            if depth > max_depth: max_depth = depth
            
        while len(self.dynamic_lists) > max_depth:
            col = self.dynamic_lists.pop()
            col['widget'].setParent(None)
            col['widget'].deleteLater()
            
        for i in range(len(self.dynamic_lists), max_depth):
            title = self.hierarchy_levels[i] if hasattr(self, 'hierarchy_levels') and i < len(self.hierarchy_levels) else f"Level {i + 1}"
            self._add_column(i, title)
            
        for i in range(max_depth):
            lst = self.dynamic_lists[i]['list']
            lst.blockSignals(True)
            lst.clear()
            
            level_nodes = {}
            for vp in valid_paths:
                parts = [p for p in vp.split('/') if p]
                if len(parts) > base_depth + i:
                    name = parts[base_depth + i]
                    is_folder = (len(parts) > base_depth + i + 1) or vp.endswith('/')
                    node_v_path = "/" + "/".join(parts[:base_depth + i + 1]) + ("/" if is_folder else "")
                    level_nodes[name] = (node_v_path, is_folder)
            
            for name in sorted(level_nodes.keys()):
                node_v_path, is_folder = level_nodes[name]
                l_item = QListWidgetItem(name)
                l_item.setData(Qt.UserRole, node_v_path)
                
                if is_folder:
                    l_item.setIcon(self.style().standardIcon(QStyle.SP_DirIcon))
                else:
                    l_item.setIcon(self.style().standardIcon(QStyle.SP_FileIcon))
                
                if query in name.lower():
                    l_item.setForeground(QBrush(QColor("#2ea043")))
                    font = l_item.font()
                    font.setBold(True)
                    l_item.setFont(font)
                    
                lst.addItem(l_item)
                
            lst.blockSignals(False)

    def _filter_list(self, list_widget, text):
        query = text.lower()
        for i in range(list_widget.count()):
            item = list_widget.item(i)
            item.setHidden(query not in item.text().lower())


    def map_global_base_to_os(self):
        real_p = QFileDialog.getExistingDirectory(self, f"Select the real Root Folder for '{self.base_v_path}'")
        if not real_p: return
        real_p = real_p.replace('\\', '/')
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cur = conn.cursor()
                if self.base_v_path != "/":
                    parts = [p for p in self.base_v_path.split('/') if p]
                    name = parts[-1]
                    pp = "/" + "/".join(parts[:-1]) + "/" if len(parts) > 1 else "/"
                    cur.execute("UPDATE virtual_fs SET real_path=? WHERE parent_path=? AND name=?", (real_p, pp, name))

                cur.execute("SELECT id, parent_path, name FROM virtual_fs WHERE parent_path LIKE ?", (f"{self.base_v_path}%",))
                for db_id, pp, name in cur.fetchall():
                    full_v = f"{pp}{name}/".replace("//", "/")
                    rel_path = full_v[len(self.base_v_path):]
                    new_real = os.path.join(real_p, rel_path).replace('\\', '/').rstrip('/')
                    cur.execute("UPDATE virtual_fs SET real_path=? WHERE id=?", (new_real, db_id))
                conn.commit()
            QMessageBox.information(self, "Success", f"Globally mapped '{self.base_v_path}' and all subfolders to:\n{real_p}")
        except Exception as e: QMessageBox.critical(self, "Error", str(e))

    def show_context_menu(self, list_widget, pos):
        item = list_widget.itemAt(pos)
        if not item: return
        menu = QMenu(self)
        
        v_path = item.data(Qt.UserRole)
        is_folder = v_path.endswith('/')
        
        action_open = menu.addAction("🚀 Open in Native OS Explorer")
        if is_folder:
            action_link = menu.addAction("🔗 Map THIS Folder to Physical OS")
        else: 
            action_link = None
            
        menu.addSeparator()
        
        action_edit = menu.addAction("🏷️ Edit Tags")
        action_copy_v = menu.addAction("📋 Copy Virtual Path")
        action_copy_r = menu.addAction("📋 Copy Local OS Path")
        
        menu.addSeparator()
        
        action_props = menu.addAction("ℹ️ Properties")
        
        action = menu.exec(list_widget.viewport().mapToGlobal(pos))
        
        if action == action_edit: 
            self.edit_tags_for_item(item)
        elif action == action_link: 
            self.link_specific_path(item)
        elif action == action_open: 
            self.jump_to_virtual_or_real_path(item, force_real=True)
        elif action == action_copy_v: 
            QApplication.clipboard().setText(v_path)
            if self.main_app: self.main_app.status.showMessage("Virtual path copied to clipboard.", 3000)
        elif action == action_copy_r:
            real_p = self._get_real_path_for_item(item)
            if real_p: 
                QApplication.clipboard().setText(real_p)
                if self.main_app: self.main_app.status.showMessage("Physical OS path copied to clipboard.", 3000)
            else: 
                QMessageBox.warning(self, "Copy Failed", "This item has no mapped physical path on your hard drive.")
        elif action == action_props:
            self.show_item_properties(item)

    def _get_real_path_for_item(self, item):
        v_path = item.data(Qt.UserRole)
        is_folder = v_path.endswith('/')
        parts = [p for p in v_path.split('/') if p]
        if not parts: return None
        name = parts[-1]
        parent_path = "/" + "/".join(parts[:-1]) + "/" if len(parts) > 1 else "/"
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                res = conn.cursor().execute("SELECT real_path FROM virtual_fs WHERE parent_path=? AND name=? AND is_folder=?", (parent_path, name, 1 if is_folder else 0)).fetchone()
                return res[0] if res and res[0] else None
        except Exception:
            return None

    def show_item_properties(self, item):
        v_path = item.data(Qt.UserRole)
        is_folder = v_path.endswith('/')
        
        parts = [p for p in v_path.split('/') if p]
        name = parts[-1] if parts else ""
        parent_path = "/" + "/".join(parts[:-1]) + "/" if len(parts) > 1 else "/"
            
        dlg = QDialog(self)
        dlg.setWindowTitle(f"Properties: {name}")
        dlg.setMinimumWidth(500)
        
        if self.main_app and hasattr(self.main_app, 'theme_combo'):
            dlg.setStyleSheet(THEMES.get(self.main_app.theme_combo.currentText(), THEMES["Dark"]))
            
        layout = QFormLayout(dlg)
        
        layout.addRow("Virtual Path:", QLineEdit(v_path))
        
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cur = conn.cursor()
                res = cur.execute("SELECT id, size, extension, modified, real_path, custom_tags, color_tag, secondary_name FROM virtual_fs WHERE parent_path=? AND name=? AND is_folder=?", (parent_path, name, 1 if is_folder else 0)).fetchone()
                
                if res:
                    db_id, size, ext, mod, real_p, tags, color, sec_name = res
                    layout.addRow("Type:", QLabel("Directory (Virtual Folder)" if is_folder else "Virtual File"))
                    if not is_folder:
                        layout.addRow("Extension:", QLabel(str(ext)))
                        layout.addRow("Size:", QLabel(human_size(size or 0)))
                    layout.addRow("Modified Date:", QLabel(str(mod)))
                    
                    txt_real = QLineEdit(str(real_p) if real_p else "Disconnected / Virtual Only")
                    txt_real.setReadOnly(True)
                    layout.addRow("Local Target OS Path:", txt_real)
                    
                    layout.addRow("Custom Tags:", QLabel(str(tags) if tags else "None"))
                    layout.addRow("Color Label:", QLabel(str(color) if color else "None"))
                    layout.addRow("Secondary Name:", QLabel(str(sec_name) if sec_name else "None"))
                    layout.addRow("Database ID:", QLabel(str(db_id)))
                else:
                    layout.addRow("Status:", QLabel("Virtual Container (Not explicitly tracked in DB)"))
        except Exception as e:
            layout.addRow("Error:", QLabel(str(e)))
                
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(dlg.accept)
        layout.addRow("", btn_close)
        
        dlg.exec()

    def link_specific_path(self, item):
        v_path = item.data(Qt.UserRole)
        real_p = QFileDialog.getExistingDirectory(self, f"Select Real Folder mapped to {v_path}")
        if not real_p: return
        real_p = real_p.replace('\\', '/')
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cur = conn.cursor()
                cur.execute("SELECT id, parent_path, name FROM virtual_fs WHERE parent_path LIKE ? OR (parent_path=? AND name=?)", 
                            (f"{v_path}%", "/" + "/".join(v_path.strip('/').split('/')[:-1]) + "/", v_path.strip('/').split('/')[-1]))
                
                for db_id, pp, name in cur.fetchall():
                    full_v = f"{pp}{name}/".replace("//", "/")
                    rel = full_v[len(v_path):]
                    new_real = os.path.join(real_p, rel).replace('\\', '/').rstrip('/')
                    cur.execute("UPDATE virtual_fs SET real_path=? WHERE id=?", (new_real, db_id))
                conn.commit()
            QMessageBox.information(self, "Mapped", f"Successfully mapped '{v_path}'")
        except Exception as e: QMessageBox.critical(self, "Error", str(e))

    def jump_to_virtual_or_real_path(self, item, force_real=False):
        v_path = item.data(Qt.UserRole)
        if not v_path: return
        
        is_folder = v_path.endswith('/')
        parts = [p for p in v_path.strip('/').split('/')]
        name = parts[-1]
        parent_path = "/" + "/".join(parts[:-1]) + "/" if len(parts) > 1 else "/"
 
        if not force_real:
            if self.main_app:
                if is_folder: self.main_app.nav_to_path(v_path)
                else:
                    self.main_app.nav_to_path(parent_path)
                    self.main_app.local_filter.setText(name)
                
                self.main_app.raise_()
                self.main_app.activateWindow()
                
            self.hide() 
            return
            
        real_abs_path = None
        try:
            with sqlite3.connect(self.db_path) as conn:
                res = conn.cursor().execute("SELECT real_path FROM virtual_fs WHERE parent_path=? AND name=?", (parent_path, name)).fetchone()
                if res and res[0]: real_abs_path = res[0]
        except Exception: pass

        if real_abs_path and os.path.exists(real_abs_path):
            try:
                if sys.platform == "win32": os.startfile(real_abs_path)
                elif sys.platform == "darwin": subprocess.Popen(["open", real_abs_path])
                else: subprocess.Popen(["xdg-open", real_abs_path])
            except Exception as e: print(f"OS Open Error: {e}")
        else:
            QMessageBox.warning(self, "Not Mapped", "This item has not been linked to a physical location on your OS.")

    def edit_tags_for_item(self, item):
        v_path = item.data(Qt.UserRole)
        is_folder = v_path.endswith('/')
        parts = [p for p in v_path.split('/') if p]
        name = parts[-1]
        parent_path = "/" + "/".join(parts[:-1]) + "/" if len(parts) > 1 else "/"
        
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            cur = conn.cursor()
            res = cur.execute("SELECT id, custom_tags, real_path FROM virtual_fs WHERE parent_path=? AND name=? AND is_folder=?", (parent_path, name, 1 if is_folder else 0)).fetchone()
            existing_tags = res[1] if res and res[1] else ""
            real_path = res[2] if res and res[2] else None
            
            new_tags, ok = QInputDialog.getText(self, "Edit Tags", f"Tags for {name} (Comma separated):", QLineEdit.Normal, existing_tags)
            if ok:
                if res: cur.execute("UPDATE virtual_fs SET custom_tags=? WHERE id=?", (new_tags.strip(), res[0]))
                else: cur.execute("INSERT INTO virtual_fs (parent_path, name, is_folder, custom_tags, modified) VALUES (?, ?, ?, ?, '2023-01-01 12:00:00')", (parent_path, name, 1 if is_folder else 0, new_tags.strip()))
                conn.commit()

                if is_folder and real_path and os.path.exists(real_path):
                    try:
                        with open(os.path.join(real_path, "tag.txt"), "w", encoding='utf-8') as f: f.write(new_tags.strip())
                    except Exception: pass
                
        self.refresh_memory_cache()
        if self.main_app: self.main_app.refresh_all()

    def import_csv(self):
        path, _ = QFileDialog.getOpenFileName(self, "Import Tags", "", "CSV Files (*.csv)")
        if not path: return
        
        reply = QMessageBox.question(self, "Physical Sync", "Do you want to actually generate physical folders on your hard drive for this data structure?", QMessageBox.Yes | QMessageBox.No)
        create_real = (reply == QMessageBox.Yes)
        
        target_v_path = self.base_v_path
        if target_v_path == "/": 
            target_v_path = "/CSV_Library/"
            
        base_real_dir = None
        if create_real:
            dest = QFileDialog.getExistingDirectory(self, "Select destination to securely create the Root folder")
            if not dest: return
            base_real_dir = os.path.join(dest, target_v_path.strip('/')).replace('\\', '/')
            os.makedirs(base_real_dir, exist_ok=True)

        try:
            with sqlite3.connect(self.db_path, timeout=20) as conn:
                cur = conn.cursor()
                
                if target_v_path != "/":
                    parts = [p for p in target_v_path.split('/') if p]
                    cur.execute("INSERT OR IGNORE INTO virtual_fs (parent_path, name, is_folder) VALUES (?, ?, 1)", 
                                ("/" + "/".join(parts[:-1]) + "/" if len(parts)>1 else "/", parts[-1]))
                
                with open(path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if len(row) < 2 or row[0].lower() == "path": continue
                        
                        raw_path, tags = row[0], row[1]
                        parts = raw_path.replace('\\', '/').strip('/').split('/')
                        if not parts: continue
                        
                        curr = target_v_path 
                        for i, part in enumerate(parts):
                            is_last = (i == len(parts) - 1)
                            cur.execute("SELECT id FROM virtual_fs WHERE parent_path=? AND name=? AND is_folder=1", (curr, part))
                            existing = cur.fetchone()
                            
                            final_tags = tags if is_last else ''
                            db_id = None
                            
                            if existing:
                                db_id = existing[0]
                                if is_last and tags: cur.execute("UPDATE virtual_fs SET custom_tags=? WHERE id=?", (tags, db_id))
                            else:
                                cur.execute("INSERT INTO virtual_fs (parent_path, name, is_folder, custom_tags, modified) VALUES (?, ?, 1, ?, '2023-01-01 12:00:00')", (curr, part, final_tags))
                                db_id = cur.lastrowid
                            
                            curr += part + "/"
                            
                            if create_real and base_real_dir:
                                rel = curr[len(target_v_path):]
                                real_p = os.path.join(base_real_dir, rel).replace('\\', '/')
                                os.makedirs(real_p, exist_ok=True)
                                cur.execute("UPDATE virtual_fs SET real_path=? WHERE id=?", (real_p, db_id))
                                if final_tags:
                                    with open(os.path.join(real_p, "tag.txt"), "w", encoding='utf-8') as f_tag: f_tag.write(final_tags)
                conn.commit()
            
            self.base_v_path = target_v_path 
            self._populate_base_contexts()
            self.refresh_memory_cache()
            if self.main_app: self.main_app.refresh_all()
            QMessageBox.information(self, "Success", f"Database updated successfully inside {target_v_path}")
        except Exception as e: QMessageBox.critical(self, "Import Error", str(e))

    def export_csv(self):
        path, _ = QFileDialog.getSaveFileName(self, "Export Tags", "", "CSV Files (*.csv)")
        if not path: return
        try:
            with open(path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Path', 'Tags'])
                for v_path, tags in self.tag_cache.items():
                    if tags: writer.writerow([v_path.replace(self.base_v_path, ''), ', '.join(tags)])
            QMessageBox.information(self, "Success", "Tags exported successfully.")
        except Exception as e: QMessageBox.critical(self, "Error", str(e))


class TimelineDiaryDialog(QDialog):
    def __init__(self, db_path, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.setWindowTitle("Timeline Diary & Analytics")
        
        self.resize(1100, 670)
        self.setMinimumSize(950, 660)
        
        if parent and hasattr(parent, 'theme_combo'):
            self.setStyleSheet(THEMES.get(parent.theme_combo.currentText(), THEMES["Dark"]))
        else:
            self.setStyleSheet(THEMES["Dark"])

        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(10, 10, 10, 10)
        
        self.splitter = QSplitter(Qt.Horizontal)
        
        # ==========================================
        # LEFT PANEL: Calendar & Filters
        # ==========================================
        left_widget = QWidget()
        left_lay = QVBoxLayout(left_widget)
        left_lay.setContentsMargins(0, 0, 5, 0)
        left_lay.setSpacing(15)
        
        # 1. Calendar
        self.calendar = QCalendarWidget()

        self.date_mode_mod = QRadioButton("Modified")
        self.date_mode_mod.setChecked(True)
        self.date_mode_cre = QRadioButton("Created")
        
        self.date_mode_mod.toggled.connect(self.on_date_mode_changed)
        self.date_mode_cre.toggled.connect(self.on_date_mode_changed)
        
        rb_lay = QHBoxLayout()
        rb_lay.addWidget(self.date_mode_mod)
        rb_lay.addWidget(self.date_mode_cre)
        left_lay.addLayout(rb_lay)
        

        
        # --- FIXED: Wire up dynamic month highlighting and click routing ---
        self.calendar.currentPageChanged.connect(self.highlight_month)
        self.calendar.clicked.connect(self.on_calendar_clicked)
        # -------------------------------------------------------------------
        
        font = self.calendar.font(); font.setPointSize(10); self.calendar.setFont(font)
        self.calendar.setStyleSheet("""
            QCalendarWidget QWidget { alternate-background-color: #161b22; background-color: #0d1117; color: #c9d1d9; }
            QCalendarWidget QToolButton { color: #c9d1d9; font-weight: bold; background-color: transparent; padding: 5px; }
            QCalendarWidget QToolButton::hover { background-color: #30363d; border-radius: 4px; }
            QCalendarWidget QMenu { background-color: #161b22; color: white; }
            QCalendarWidget QSpinBox { background: #161b22; color: white; border: 1px solid #30363d; }
            QCalendarWidget QAbstractItemView:enabled { background-color: #0d1117; color: #c9d1d9; selection-background-color: #2ea043; selection-color: white; outline: none; }
            QCalendarWidget QAbstractItemView:disabled { color: #484f58; }
        """)
        self.calendar.setFixedHeight(280)
        left_lay.addWidget(self.calendar)
        
        # 2. Filters
        filter_grp = QGroupBox("Data Filters")
        filter_grp.setStyleSheet("QGroupBox { font-weight: bold; color: #8b949e; border: 1px solid #30363d; border-radius: 6px; margin-top: 10px; } QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 3px; }")
        f_lay = QFormLayout(filter_grp)
        f_lay.setContentsMargins(10, 15, 10, 10)
        f_lay.setVerticalSpacing(8)
        
        self.cb_year = QComboBox()
        self.cb_month = QComboBox()
        self.cb_category = QComboBox()
        self.cb_ext = QComboBox()
        self.cb_size = QComboBox()
        self.cb_tag = QComboBox()
        
        f_lay.addRow("Year:", self.cb_year)
        f_lay.addRow("Month:", self.cb_month)
        f_lay.addRow("Category:", self.cb_category)
        f_lay.addRow("Type/Ext:", self.cb_ext)
        f_lay.addRow("Size:", self.cb_size)
        f_lay.addRow("Tag:", self.cb_tag)
        
        self.btn_filter = QPushButton("🔍 Apply Filters")
        self.btn_filter.setStyleSheet("background-color: #2ea043; color: white; font-weight: bold; padding: 8px; border-radius: 4px; margin-top: 5px;")
        self.btn_filter.clicked.connect(self.load_by_filters)
        f_lay.addRow(self.btn_filter)
        
        left_lay.addWidget(filter_grp)
        left_lay.addStretch() 
        
        # ==========================================
        # RIGHT PANEL: 3-Tab Interface
        # ==========================================
        self.tabs = QTabWidget()
        self.tabs.setStyleSheet("""
            QTabBar::tab { background: #161b22; color: #8b949e; padding: 8px 20px; border: 1px solid #30363d; border-bottom: none; border-top-left-radius: 4px; border-top-right-radius: 4px; margin-right: 2px;}
            QTabBar::tab:selected { background: #0d1117; color: #58a6ff; font-weight: bold; border-top: 2px solid #58a6ff; }
            QTabWidget::pane { border: 1px solid #30363d; background: #0d1117; border-radius: 4px; border-top-left-radius: 0px; }
        """)
        
        # --- TAB 1: HTML Diary Reader ---
        tab_diary = QWidget()
        diary_lay = QVBoxLayout(tab_diary)
        diary_lay.setContentsMargins(0, 0, 0, 0)
        self.diary_browser = QTextBrowser()
        self.diary_browser.setStyleSheet("background-color: #0d1117; border: none; padding: 10px;")
        diary_lay.addWidget(self.diary_browser)
        self.tabs.addTab(tab_diary, "📖 Daily Diary")

        # --- TAB 2: Data Table ---
        tab_data = QWidget()
        data_lay = QVBoxLayout(tab_data)
        data_lay.setContentsMargins(0, 0, 0, 0)
        
        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(["Name", "Type", "Ext", "Size", "Virtual Location", "ID"])
        self.table.setSortingEnabled(True)
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)
        self.table.doubleClicked.connect(self.open_scanned_file)
        
        self.table.setColumnWidth(0, 220); self.table.setColumnWidth(1, 80); self.table.setColumnWidth(2, 60); self.table.setColumnWidth(3, 80)
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self.table.setColumnHidden(5, True)
        
        data_lay.addWidget(self.table)
        self.tabs.addTab(tab_data, "📋 Activity Log")
        
        # --- TAB 3: Visual Analytics ---
        tab_charts = QWidget()
        chart_lay = QVBoxLayout(tab_charts)
        
        top_chart_bar = QHBoxLayout()
        top_chart_bar.addWidget(QLabel("<b>Chart Metric:</b>"))
        self.cb_chart_metric = QComboBox()
        self.cb_chart_metric.addItems([
            "File Count by Extension",
            "Storage Size by Extension",
            "Storage Usage by Year",
            "File Age Distribution",
            "Top 10 Largest Files",
            "File Modification Timeline",
            "File Size Distribution",
            "Tag Utilization"
        ])
        self.cb_chart_metric.currentTextChanged.connect(self.force_chart_redraw)
        top_chart_bar.addWidget(self.cb_chart_metric, stretch=1)
        chart_lay.addLayout(top_chart_bar)
        
        self.fig = Figure(figsize=(8, 5), dpi=100)
        self.fig.patch.set_facecolor('#0d1117')
        self.canvas = FigureCanvasQTAgg(self.fig)
        chart_lay.addWidget(self.canvas, stretch=1)
        self.tabs.addTab(tab_charts, "📊 Visual Analytics")
        
        # Layout Assembly
        self.splitter.addWidget(left_widget)
        self.splitter.addWidget(self.tabs)
        self.splitter.setSizes([300, 800])
        main_layout.addWidget(self.splitter)
        
        self.latest_analytics = None
        self.populate_dropdowns()
        
        today = QDate.currentDate()
        self.calendar.setSelectedDate(today)
        self.highlight_month(today.year(), today.month())
        self.on_calendar_clicked(today)

    def on_date_mode_changed(self):
        # Instantly forces the calendar, HTML reader, and Table to reload with the newly chosen metric
        self.highlight_month(self.calendar.yearShown(), self.calendar.monthShown())
        self.on_calendar_clicked(self.calendar.selectedDate())

    def populate_dropdowns(self):
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            self.cb_year.addItem("All")
            cur.execute("SELECT DISTINCT year FROM virtual_fs WHERE year IS NOT NULL AND year != '' AND in_trash=0 ORDER BY year DESC")
            self.cb_year.addItems([str(r[0]) for r in cur.fetchall()])
            self.cb_month.addItem("All")
            cur.execute("SELECT DISTINCT month FROM virtual_fs WHERE month IS NOT NULL AND month != '' AND in_trash=0 ORDER BY month ASC")
            self.cb_month.addItems([str(r[0]) for r in cur.fetchall()])
            self.cb_category.addItem("All")
            cur.execute("SELECT DISTINCT category FROM virtual_fs WHERE category IS NOT NULL AND category != '' AND in_trash=0 ORDER BY category ASC")
            self.cb_category.addItems([str(r[0]) for r in cur.fetchall()])
            self.cb_ext.addItem("All")
            cur.execute("SELECT DISTINCT extension FROM virtual_fs WHERE extension IS NOT NULL AND extension != '' AND is_folder=0 AND in_trash=0 ORDER BY extension ASC")
            self.cb_ext.addItems([str(r[0]) for r in cur.fetchall()])
            self.cb_size.addItems(["All", "Tiny (< 1MB)", "Medium (1MB - 500MB)", "Huge (> 500MB)"])
            self.cb_tag.addItem("All")
            cur.execute("SELECT custom_tags FROM virtual_fs WHERE custom_tags IS NOT NULL AND custom_tags != '' AND in_trash=0")
            all_tags = set()
            for (tags_str,) in cur.fetchall():
                for t in tags_str.split(','):
                    if t.strip(): all_tags.add(t.strip())
            self.cb_tag.addItems(sorted(list(all_tags)))

    def highlight_month(self, year, month):
        self.calendar.setDateTextFormat(QDate(), QTextCharFormat())
        col = "creation_date" if self.date_mode_cre.isChecked() else "modified"
        with sqlite3.connect(self.db_path) as conn:
            days = [r[0] for r in conn.cursor().execute(f"SELECT DISTINCT SUBSTR({col}, 9, 2) FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND {col} LIKE ?", (f"{year}-{month:02d}-%",)).fetchall()]
        
        fmt = QTextCharFormat()
        fmt.setBackground(QColor("#2ea043")); fmt.setForeground(QColor("white")); fmt.setFontWeight(QFont.Bold)
        for d in days:
            try: self.calendar.setDateTextFormat(QDate(year, month, int(d)), fmt)
            except ValueError: pass

    def on_calendar_clicked(self, date):
        self.load_html_diary(date)
        date_str = date.toString("yyyy-MM-dd")
        col = "creation_date" if self.date_mode_cre.isChecked() else "modified"
        query = f"SELECT id, name, is_folder, extension, size, parent_path, {col}, custom_tags FROM virtual_fs WHERE {col} LIKE ? AND in_trash=0"
        
        self.execute_search(query, (f"{date_str}%",), update_highlights=False)
        self.tabs.setCurrentIndex(0)

    def load_by_date(self, date):
        date_str = date.toString("yyyy-MM-dd")
        col = "creation_date" if self.date_mode_cre.isChecked() else "modified"
        query = f"SELECT id, name, is_folder, extension, size, parent_path, {col}, custom_tags FROM virtual_fs WHERE {col} LIKE ? AND in_trash=0"
        
        self.execute_search(query, (f"{date_str}%",), update_highlights=False)

    def load_html_diary(self, date):
        dt_str = date.toString("yyyy-MM-dd")
        col = "creation_date" if self.date_mode_cre.isChecked() else "modified"
        with sqlite3.connect(self.db_path) as conn:
            entries = conn.cursor().execute(f"SELECT SUBSTR({col}, 12, 8), name, parent_path, size, category FROM virtual_fs WHERE {col} LIKE ? AND is_folder=0 AND in_trash=0 ORDER BY {col} ASC", (f"{dt_str}%",)).fetchall()
        
        html = f"<h1 style='color:#58a6ff; text-align:center;'>📖 System Timeline: {date.toString('dddd, MMMM d, yyyy')}</h1><hr>"
        if not entries: 
            html += "<h3 style='color:#8b949e; text-align:center;'><br><br>No system activity recorded on this day.</h3>"
        else:
            html += f"<p style='color:#c9d1d9; text-align:center;'><b>{len(entries)}</b> files were modified or logged.</p><br><ul style='list-style-type: none; padding-left: 0;'>"
            cat_colors = {"Images": "#a371f7", "Videos": "#f85149", "Audio": "#ff7b72", "Documents": "#d2a8ff", "Code": "#79c0ff", "Others": "#8b949e"}
            
            action_verb = "Created" if self.date_mode_cre.isChecked() else "Modified"
            
            for time_str, name, pp, size, cat in entries:
                c_color = cat_colors.get(cat, "#8b949e")
                try: safe_size = human_size(size)
                except NameError: safe_size = f"{size} bytes"
                
                html += f"<li style='margin-bottom: 15px; background-color: rgba(33, 38, 45, 0.6); padding: 12px; border-left: 5px solid {c_color}; border-radius: 6px;'><span style='color: #58a6ff; font-size: 15px;'><b>🕒 {time_str}</b></span><br><span style='font-size: 16px; color: white;'>{action_verb} <b style='color: {c_color};'>{name}</b></span> <span style='color: #8b949e; font-size: 13px;'>({safe_size})</span><br><span style='color: #8b949e; font-size: 13px;'>Path: {pp}</span></li>"
            html += "</ul>"
        self.diary_browser.setHtml(html)

    def load_by_filters(self):
        y, m, c, e = self.cb_year.currentText(), self.cb_month.currentText(), self.cb_category.currentText(), self.cb_ext.currentText()
        s, t = self.cb_size.currentText(), self.cb_tag.currentText()
        
        if y != "All":
            target_month = int(m) if m != "All" else 1
            self.calendar.setCurrentPage(int(y), target_month)
            self.highlight_month(int(y), target_month)
        
        col = "creation_date" if self.date_mode_cre.isChecked() else "modified"
        query = f"SELECT id, name, is_folder, extension, size, parent_path, {col}, custom_tags FROM virtual_fs WHERE in_trash=0"
        params = []
        
        if y != "All": query += " AND year=?"; params.append(y)
        if m != "All": query += " AND month=?"; params.append(m)
        if c != "All": query += " AND category=?"; params.append(c)
        if e != "All": query += " AND extension=?"; params.append(e)
        if s == "Tiny (< 1MB)": query += " AND size < 1048576"
        elif s == "Medium (1MB - 500MB)": query += " AND size >= 1048576 AND size <= 524288000"
        elif s == "Huge (> 500MB)": query += " AND size > 524288000"
        if t != "All": query += " AND custom_tags LIKE ?"; params.append(f"%{t}%")
            
        self.execute_search(query, tuple(params))
        
        self.tabs.setCurrentIndex(1)

    # ADDED the update_highlights flag here
    def execute_search(self, query, params, update_highlights=True):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        
        dates_found = set()
        analytics = {
            'ext_counts': {}, 'ext_sizes': {},
            'year_sizes': {}, 'age_days': [],
            'all_files': [], 
            'mod_timeline': {}, 
            'size_list': [], 
            'tags': {'Tagged': 0, 'Untagged': 0},
            'total': 0
        }
        
        now = datetime.now()

        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            results = cur.fetchall()
            
            for db_id, name, is_folder, ext, size, path, mod, tags in results:
                row = self.table.rowCount()
                self.table.insertRow(row)
                self.table.setItem(row, 0, QTableWidgetItem(name))
                self.table.setItem(row, 1, QTableWidgetItem("📁 Folder" if is_folder else "📄 File"))
                self.table.setItem(row, 2, QTableWidgetItem(ext if ext else ""))
                self.table.setItem(row, 3, SizeTableWidgetItem(size or 0)) 
                self.table.setItem(row, 4, QTableWidgetItem(path))
                self.table.setItem(row, 5, QTableWidgetItem(str(db_id)))
                
                analytics['total'] += 1
                
                if mod:
                    date_part = mod.split(" ")[0]
                    dates_found.add(date_part)
                    analytics['mod_timeline'][date_part] = analytics['mod_timeline'].get(date_part, 0) + 1
                    
                    try:
                        dt_mod = datetime.strptime(mod, "%Y-%m-%d %H:%M:%S")
                        analytics['year_sizes'][dt_mod.year] = analytics['year_sizes'].get(dt_mod.year, 0) + (size or 0)
                        analytics['age_days'].append((now - dt_mod).days)
                    except: pass

                if not is_folder:
                    safe_ext = ext.upper() if ext else "UNKNOWN"
                    safe_size = size or 0
                    analytics['ext_counts'][safe_ext] = analytics['ext_counts'].get(safe_ext, 0) + 1
                    analytics['ext_sizes'][safe_ext] = analytics['ext_sizes'].get(safe_ext, 0) + safe_size
                    analytics['size_list'].append(safe_size)
                    analytics['all_files'].append((name, safe_size))
                    
                    if tags and str(tags).strip(): analytics['tags']['Tagged'] += 1
                    else: analytics['tags']['Untagged'] += 1
                
        self.table.setSortingEnabled(True)
        
        # Only update the calendar highlights if the flag is True!
        if update_highlights:
            self.sync_calendar_highlights(dates_found)
            
        self.latest_analytics = analytics
        self.render_charts()

    def force_chart_redraw(self):
        if self.latest_analytics:
            self.render_charts()

    def render_charts(self):
        self.fig.clear()
        an = self.latest_analytics
        if not an or an['total'] == 0:
            ax = self.fig.add_subplot(111)
            ax.set_facecolor('#0d1117'); ax.text(0.5, 0.5, "No Data", color='#8b949e', ha='center', va='center'); ax.axis('off')
            self.canvas.draw(); return
            
        metric = self.cb_chart_metric.currentText()
        ax = self.fig.add_subplot(111)
        ax.set_facecolor('#0d1117')
        colors = ["#58a6ff", "#3fb950", "#e3b341", "#a371f7", "#f85149", "#8b949e"]

        if "Extension" in metric:
            data = an['ext_counts'] if "Count" in metric else an['ext_sizes']
            sorted_d = sorted(data.items(), key=lambda x: x[1], reverse=True)[:6]
            ax.bar([x[0] for x in sorted_d], [x[1] for x in sorted_d], color=colors)
            ax.set_title(metric, color="#c9d1d9")

        elif "Usage by Year" in metric:
            years = sorted(an['year_sizes'].keys())
            sizes = [an['year_sizes'][y] for y in years]
            ax.plot(years, sizes, marker='o', color="#58a6ff", linewidth=2)
            ax.fill_between(years, sizes, color="#58a6ff", alpha=0.2)
            ax.set_title("Storage Growth by Year", color="#c9d1d9")

        elif "Age Distribution" in metric:
            bins = [0, 7, 30, 180, 365, 1825]
            labels = ["<1wk", "<1mo", "<6mo", "<1yr", ">1yr"]
            counts = [0] * len(labels)
            for d in an['age_days']:
                if d <= 7: counts[0]+=1
                elif d <= 30: counts[1]+=1
                elif d <= 180: counts[2]+=1
                elif d <= 365: counts[3]+=1
                else: counts[4]+=1
            ax.bar(labels, counts, color="#a371f7")
            ax.set_title("File Age (Time since Modified)", color="#c9d1d9")

        elif "Top 10" in metric:
            top_10 = sorted(an['all_files'], key=lambda x: x[1], reverse=True)[:10]
            names = [x[0][:15] + "..." if len(x[0]) > 15 else x[0] for x in top_10]
            sizes = [x[1] for x in top_10]
            ax.barh(names, sizes, color="#f85149")
            ax.invert_yaxis()
            ax.set_title("Top 10 Largest Files", color="#c9d1d9")

        elif "Modification Timeline" in metric:
            sorted_dates = sorted(an['mod_timeline'].keys())
            counts = [an['mod_timeline'][d] for d in sorted_dates]
            # Show only the last 15 active days to prevent overcrowding
            disp_dates = sorted_dates[-15:]
            disp_counts = counts[-15:]
            ax.bar(disp_dates, disp_counts, color="#3fb950")
            ax.tick_params(axis='x', rotation=45)
            ax.set_title("Activity Spikes (Last 15 Active Days)", color="#c9d1d9")

        elif "Size Distribution" in metric:
            # Grouping files into logical size buckets
            buckets = {"<1MB": 0, "1-10MB": 0, "10-100MB": 0, "100MB-1GB": 0, ">1GB": 0}
            for s in an['size_list']:
                if s < 1048576: buckets["<1MB"] += 1
                elif s < 10485760: buckets["1-10MB"] += 1
                elif s < 104857600: buckets["10-100MB"] += 1
                elif s < 1073741824: buckets["100MB-1GB"] += 1
                else: buckets[">1GB"] += 1
            ax.bar(buckets.keys(), buckets.values(), color="#e3b341")
            ax.set_title("Count by Size Range", color="#c9d1d9")

        elif "Tag" in metric:
            vals = [an['tags']['Tagged'], an['tags']['Untagged']]
            ax.pie(vals, labels=["Tagged", "Untagged"], autopct='%1.1f%%', colors=["#3fb950", "#30363d"], textprops={'color':"white"})
            ax.set_title("Tag Coverage", color="#c9d1d9")

        # Global styling for Dark theme
        ax.tick_params(axis='x', colors='#c9d1d9', labelsize=9)
        ax.tick_params(axis='y', colors='#8b949e')
        for spine in ['top', 'right']: ax.spines[spine].set_visible(False)
        for spine in ['bottom', 'left']: ax.spines[spine].set_color('#30363d')
        
        self.fig.tight_layout()
        self.canvas.draw()

    def show_context_menu(self, pos):
        item = self.table.itemAt(pos)
        if not item: return
        row = item.row()
        db_id = int(self.table.item(row, 5).text())
        typ = "folder" if "Folder" in self.table.item(row, 1).text() else "file"
        v_path = self.table.item(row, 4).text()
        name = self.table.item(row, 0).text()
        
        full_v_path = f"{v_path}{name}/" if typ == "folder" else f"{v_path}{name}"
        
        menu = QMenu(self)
        act_open = menu.addAction("🚀 Open Native File")
        act_loc = menu.addAction("📂 Open OS Location")
        menu.addSeparator()
        act_copy_p = menu.addAction("📋 Copy Virtual Path")
        act_props = menu.addAction("📊 Show Properties")
        menu.addSeparator()
        act_trash = menu.addAction("🗑️ Move to Trash")
        act_perm = menu.addAction("🧨 Delete Permanently")
        
        action = menu.exec(self.table.viewport().mapToGlobal(pos))
        
        if not self.parent(): return
        if action == act_open: self.parent().open_local_file_system(db_id)
        elif action == act_loc: self.parent().open_file_location(db_id)
        elif action == act_copy_p: QApplication.clipboard().setText(full_v_path)
        elif action == act_props: self.parent().show_properties(typ, full_v_path, db_id)
        elif action == act_trash: self.trash_selected_items(permanent=False)
        elif action == act_perm:
            if QMessageBox.question(self, "Delete", "Permanently delete selected files?", QMessageBox.Yes|QMessageBox.No) == QMessageBox.Yes:
                self.trash_selected_items(permanent=True)

    def open_scanned_file(self, index):
        if self.parent(): self.parent().open_local_file_system(int(self.table.item(index.row(), 5).text()))

    def trash_selected_items(self, permanent=False):
        ids_to_del = []
        rows_to_remove = []
        for r in range(self.table.rowCount()):
            if self.table.item(r, 0).isSelected():
                ids_to_del.append(int(self.table.item(r, 5).text()))
                rows_to_remove.append(r)
                
        if not ids_to_del: return
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            if permanent:
                cur.executemany("DELETE FROM virtual_fs WHERE id=?", [(i,) for i in ids_to_del])
            else:
                cur.executemany("UPDATE virtual_fs SET in_trash=1 WHERE id=?", [(i,) for i in ids_to_del])
            conn.commit()
            
        for r in sorted(rows_to_remove, reverse=True): self.table.removeRow(r)
        if self.parent(): self.parent().clear_cache(); self.parent().refresh_all()
        
        # Reload the current active date/filters to reflect the deleted items
        self.load_html_diary(self.calendar.selectedDate())
        self.force_chart_redraw()


    def sync_calendar_highlights(self, date_strings):
        # Clear old highlights
        self.calendar.setDateTextFormat(QDate(), QTextCharFormat())
        
        # Create the visual style for active days
        fmt = QTextCharFormat()
        fmt.setBackground(QBrush(QColor("#2ea043"))) 
        fmt.setForeground(QBrush(QColor("white")))
        fmt.setFontWeight(QFont.Bold)
        
        # Apply the highlight to every active date found in the search
        for ds in date_strings:
            qdate = QDate.fromString(ds, "yyyy-MM-dd")
            if qdate.isValid():
                self.calendar.setDateTextFormat(qdate, fmt)

class SpaceAnalyzerDialog(QDialog):
    def __init__(self, db_path, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.scan_roots = ["/"] 
        self.setWindowTitle("Space & Integrity Analyzer")
        
        # --- Increased width to 1350 to perfectly fit all 9 columns ---
        self.resize(1350, 700) 
        self.setMinimumSize(1100, 500)
        # ----------------------------------------------------------------------
        
        layout = QVBoxLayout(self)

        folder_lay = QHBoxLayout()
        self.lbl_path = QLabel(f"<b>Scanning:</b> {', '.join(self.scan_roots)}")
        self.lbl_path.setWordWrap(True)
        btn_choose = QPushButton("📂 Set Scan Folders")
        btn_choose.clicked.connect(self.select_scan_folder)
        folder_lay.addWidget(self.lbl_path, stretch=1)
        folder_lay.addWidget(btn_choose)
        layout.addLayout(folder_lay)
        
        # Setup Table (Now 9 Columns)
        self.table = QTableWidget(0, 9)
        self.table.setHorizontalHeaderLabels(["Select", "Type", "Name", "Location", "Ext", "Size", "Modified Date", "SHA-256", "ID"])
        self.table.setSortingEnabled(True) 
        
        self.table.setColumnWidth(0, 50)   # Select
        self.table.setColumnWidth(1, 140)  # Type
        self.table.setColumnWidth(2, 220)  # Name
        self.table.setColumnWidth(3, 260)  # Location
        self.table.setColumnWidth(4, 70)   # Ext
        self.table.setColumnWidth(5, 90)   # Size
        self.table.setColumnWidth(6, 140)  # Modified
        self.table.setColumnWidth(7, 240)  # Hash
        self.table.setColumnHidden(8, True)# ID

        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)        
        
        # ---Double click to open files seamlessly ---
        self.table.doubleClicked.connect(self.open_scanned_file)
        
        layout.addWidget(self.table)

        
        sel_lay = QHBoxLayout()
        self.btn_select_all = QPushButton("☑ Check All")
        self.btn_select_all.clicked.connect(self.toggle_select_all)
        self.btn_check_hl = QPushButton("✓ Check Highlighted")
        self.btn_check_hl.clicked.connect(lambda: self.set_highlighted_state(Qt.Checked))
        self.btn_uncheck_hl = QPushButton("✗ Uncheck Highlighted")
        self.btn_uncheck_hl.clicked.connect(lambda: self.set_highlighted_state(Qt.Unchecked))
        
        self.btn_mark_safe = QPushButton("🛡️ Mark Safe (Ignore)")
        self.btn_mark_safe.setStyleSheet("color: #3fb950; font-weight: bold;")
        self.btn_mark_safe.clicked.connect(lambda: self.update_safety_status(True))
        
        self.btn_unmark_safe = QPushButton("❌ Unmark Safe")
        self.btn_unmark_safe.setStyleSheet("color: #e3b341;")
        self.btn_unmark_safe.clicked.connect(lambda: self.update_safety_status(False))

        sel_lay.addWidget(self.btn_select_all)
        sel_lay.addWidget(self.btn_check_hl)
        sel_lay.addWidget(self.btn_uncheck_hl)
        sel_lay.addStretch()
        sel_lay.addWidget(self.btn_mark_safe)
        sel_lay.addWidget(self.btn_unmark_safe)
        layout.addLayout(sel_lay)

        # ----- Scanning & Deletion Tools
        scan_lay = QHBoxLayout()
        self.btn_refresh = QPushButton("🔄 Scan Junk")
        self.btn_refresh.clicked.connect(self.scan)
        
        self.btn_scan_hash = QPushButton("🧬 Exact Duplicates")
        self.btn_scan_hash.clicked.connect(self.scan_hash_duplicates)
        
        self.btn_scan_versions = QPushButton("📝 Version Conflicts")
        self.btn_scan_versions.setStyleSheet("color: #58a6ff; font-weight: bold;")
        self.btn_scan_versions.clicked.connect(self.scan_version_conflicts)
        
        self.btn_scan_corrupt = QPushButton("⚠️ Data Anomalies")
        self.btn_scan_corrupt.setStyleSheet("color: #e3b341; font-weight: bold;")
        self.btn_scan_corrupt.clicked.connect(self.scan_corrupt_files)
        
        self.btn_view_safe = QPushButton("👁️ View Safe Files")
        self.btn_view_safe.clicked.connect(self.view_safe_files)
        
        self.btn_delete = QPushButton("🗑️ Delete Checked")
        self.btn_delete.setStyleSheet("background-color: #8b0000; font-weight: bold; color: white;")
        self.btn_delete.clicked.connect(self.delete_selected)
        
        scan_lay.addWidget(self.btn_refresh)
        scan_lay.addWidget(self.btn_scan_hash)
        scan_lay.addWidget(self.btn_scan_versions)
        scan_lay.addWidget(self.btn_scan_corrupt)
        scan_lay.addStretch()
        scan_lay.addWidget(self.btn_view_safe)
        scan_lay.addWidget(self.btn_delete)
        self.btn_apply_tag = QPushButton("🏷️ Assign Custom Tag")
        self.btn_apply_tag.setStyleSheet("background-color: #1f6feb; font-weight: bold; color: white;")
        self.btn_apply_tag.clicked.connect(self.assign_tags_selected)
        scan_lay.addWidget(self.btn_apply_tag)
        layout.addLayout(scan_lay)

    def assign_tags_selected(self):
        ids = [int(self.table.item(r, 8).text()) for r in range(self.table.rowCount()) if self.table.item(r, 0).checkState() == Qt.Checked]
        if not ids: return
        tags, ok = QInputDialog.getText(self, "Assign Tag", "Enter Custom Tag:")
        if ok and tags.strip():
            prog = QProgressDialog("Applying custom tags...", "Cancel", 0, len(ids), self)
            prog.setWindowModality(Qt.WindowModal); prog.show()
            with sqlite3.connect(self.db_path) as conn:
                for i, db_id in enumerate(ids):
                    if prog.wasCanceled(): break
                    old = conn.cursor().execute("SELECT custom_tags FROM virtual_fs WHERE id=?", (db_id,)).fetchone()[0]
                    new_val = f"{old}, {tags.strip()}".strip(", ") if old else tags.strip()
                    conn.cursor().execute("UPDATE virtual_fs SET custom_tags=? WHERE id=?", (new_val, db_id))
                    prog.setValue(i+1)
                conn.commit()
            if self.parent(): self.parent().refresh_all()
            QMessageBox.information(self, "Complete", "Tags assigned to selected files.")

    def show_context_menu(self, pos):
        item = self.table.itemAt(pos)
        if not item: return
        row = item.row()
        
        typ = self.table.item(row, 1).text()
        db_id = int(self.table.item(row, 8).text())
        
        menu = QMenu(self)
        
        # Ensure all conflict types trigger the Proof button!
        if any(keyword in typ for keyword in ["Duplicate", "Version", "Paradox", "0-Byte"]):
            act_proof = menu.addAction("⚖️ Compare / Show Proof")
            menu.addSeparator()
        else:
            act_proof = None
            
        act_open = menu.addAction("🚀 Open Native File")
        act_loc = menu.addAction("📂 Open File Location")
        
        action = menu.exec(self.table.viewport().mapToGlobal(pos))
        
        if action == act_proof:
            self.show_proof_dialog(row, typ, db_id)
        elif action == act_open:
            if self.parent(): self.parent().open_local_file_system(db_id)
        elif action == act_loc:
            if self.parent(): self.parent().open_file_location(db_id)

    def show_proof_dialog(self, row, typ, db_id):
        ext = self.table.item(row, 4).text()
        sha = self.table.item(row, 7).text()
        
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            res = cur.execute("SELECT name, size, modified FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
            if not res: return
            true_name, true_size, true_mod = res
            
        query = ""
        query_params = ()
        
        if "Exact" in typ and sha and sha not in ("Not Computed", ""):
            # 1. Exact Hash Duplicates
            query = "SELECT id, name, parent_path, size, modified, sha256, real_path FROM virtual_fs WHERE sha256=? AND is_folder=0 AND in_trash=0"
            query_params = (sha,)
            
        elif "Duplicate" in typ:
            # 2. Standard Junk Scan Duplicates (Matches by Size & Extension)
            query = "SELECT id, name, parent_path, size, modified, sha256, real_path FROM virtual_fs WHERE size=? AND extension=? AND is_folder=0 AND in_trash=0"
            query_params = (true_size, ext)
            
        elif "Version" in typ:
            # 3. Version Conflicts (Matches by Name)
            query = "SELECT id, name, parent_path, size, modified, sha256, real_path FROM virtual_fs WHERE name=? AND is_folder=0 AND in_trash=0 ORDER BY modified DESC"
            query_params = (true_name,)
            
        elif "Paradox" in typ:
            # 4. Hash Paradox (Matches by Name, Size, Modified Date)
            query = "SELECT id, name, parent_path, size, modified, sha256, real_path FROM virtual_fs WHERE name=? AND size=? AND modified=? AND is_folder=0 AND in_trash=0"
            query_params = (true_name, true_size, true_mod)
            
        elif "0-Byte" in typ:
            QMessageBox.information(self, "Proof", "This file is exactly 0 bytes, indicating it is empty or structurally broken. No side-by-side comparison is needed.")
            return
            
        else:
            QMessageBox.information(self, "Proof Unavailable", "Proof comparison is only available for duplicates and versions.")
            return
            
        dlg = DuplicateProofDialog(self.db_path, query, query_params, typ, self.parent(), self)
        dlg.exec()

    def open_scanned_file(self, index):
        row = index.row()
        db_id_item = self.table.item(row, 8)
        if not db_id_item: return
        
        db_id = int(db_id_item.text())
        
        with sqlite3.connect(self.db_path) as conn:
            res = conn.cursor().execute("SELECT real_path FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
            
            if res and res[0] and os.path.exists(res[0]):
                try:
                    if sys.platform == "win32": 
                        os.startfile(res[0])
                    elif sys.platform == "darwin": 
                        subprocess.Popen(["open", res[0]])
                    else: 
                        subprocess.Popen(["xdg-open", res[0]])
                except Exception as e:
                    QMessageBox.warning(self, "Open Error", str(e))
            else:
                QMessageBox.warning(self, "Not Found", "The physical file does not exist on your hard drive or is disconnected.")

    def select_scan_folder(self):
        current_str = ", ".join(self.scan_roots)
        path_str, ok = QInputDialog.getText(self, "Select Folders", "Enter Virtual Paths (comma separated):", QLineEdit.Normal, current_str)
        if ok and path_str.strip():
            raw_paths = [p.strip() for p in path_str.split(',')]
            clean_paths = []
            invalid_paths = []
            
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                for p in raw_paths:
                    if not p: continue
                    if not p.endswith('/'): p += '/'
                    if not p.startswith('/'): p = '/' + p
                    
                    if p == "/":
                        clean_paths.append(p)
                        continue
                        
                    # Check if folder actually exists in the database
                    cur.execute("SELECT id FROM virtual_fs WHERE parent_path LIKE ? LIMIT 1", (f"{p}%",))
                    if cur.fetchone():
                        clean_paths.append(p)
                    else:
                        invalid_paths.append(p)
            
            # Warn the user if they made a typo
            if invalid_paths:
                QMessageBox.warning(self, "Invalid Folders", f"The following folders do not exist in the virtual database and were skipped:\n\n{', '.join(invalid_paths)}")
                
            if clean_paths:
                self.scan_roots = clean_paths
                self.lbl_path.setText(f"<b>Scanning:</b> {', '.join(self.scan_roots)}")

    def toggle_select_all(self):
        self.select_all_state = not getattr(self, 'select_all_state', False)
        state = Qt.Checked if self.select_all_state else Qt.Unchecked
        for r in range(self.table.rowCount()):
            if self.table.item(r, 0): self.table.item(r, 0).setCheckState(state)

    def set_highlighted_state(self, state):
        selected_rows = set(idx.row() for idx in self.table.selectedIndexes())
        for r in selected_rows:
            item = self.table.item(r, 0)
            if item: item.setCheckState(state)

    def update_safety_status(self, is_safe):
        rows_to_remove = []
        ids_to_update = []
        for r in range(self.table.rowCount()):
            # ID is now at index 8
            if self.table.item(r, 0).checkState() == Qt.Checked:
                rows_to_remove.append(r)
                ids_to_update.append(int(self.table.item(r, 8).text()))
                
        if not ids_to_update: return
        
        val = 1 if is_safe else 0
        with sqlite3.connect(self.db_path) as conn:
            conn.cursor().executemany("UPDATE virtual_fs SET hash_verified=? WHERE id=?", [(val, i) for i in ids_to_update])
            conn.commit()
            
        for r in sorted(rows_to_remove, reverse=True):
            self.table.removeRow(r)
            
        action = "Marked Safe" if is_safe else "Unmarked Safe"
        QMessageBox.information(self, action, f"{len(ids_to_update)} items updated.")

    def get_path_conditions(self):
        cond = " OR ".join(["parent_path LIKE ?"] * len(self.scan_roots))
        params = tuple(f"{p}%" for p in self.scan_roots)
        return cond, params

    def view_safe_files(self):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        cond, params = self.get_path_conditions()
        
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT id, name, parent_path, extension, size, modified, sha256 FROM virtual_fs WHERE hash_verified=1 AND ({cond})", params)
            files = cur.fetchall()
            for f in files:
                self._add_row_with_state("🛡️ Verified Safe", f[1], f[2], f[3] or "", f[4] or 0, f[5], f[6] or "", f[0], Qt.Unchecked)
        
        self.table.setSortingEnabled(True)
        if self.table.rowCount() == 0: QMessageBox.information(self, "Result", "No files are currently marked as safe in these locations.")

    def scan(self):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        self.btn_refresh.setEnabled(False)
        
        self.scanner = SpaceScannerThread(self.db_path, self.scan_roots, self)
        self.scanner.found.connect(self._add_row)
        
        self.prog_dlg = QProgressDialog(f"Scanning directories...", "Cancel", 0, 100, self)
        self.prog_dlg.setWindowModality(Qt.WindowModal)
        self.scanner.progress.connect(lambda v, t, txt: (self.prog_dlg.setValue(v), self.prog_dlg.setLabelText(txt)))
        self.prog_dlg.canceled.connect(self.scanner.cancel)
        self.scanner.finished_scan.connect(self.on_scan_finished)
        self.scanner.start()
        self.prog_dlg.show()

    def on_scan_finished(self):
        self.prog_dlg.close()
        self.btn_refresh.setEnabled(True)
        self.table.setSortingEnabled(True)

    def scan_hash_duplicates(self):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        cond, params = self.get_path_conditions()
        
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT sha256, COUNT(*) as c FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND ({cond}) AND sha256 IS NOT NULL AND sha256 != '' GROUP BY sha256 HAVING c > 1", params)
            duplicates = cur.fetchall()
            for sha, count in duplicates:
                cur.execute(f"SELECT id, name, parent_path, extension, size, modified FROM virtual_fs WHERE sha256=? AND ({cond}) AND is_folder=0 AND in_trash=0", (sha,) + params)
                files = cur.fetchall()
                for idx, f in enumerate(files):
                    chk_state = Qt.Checked if idx > 0 else Qt.Unchecked
                    self._add_row_with_state("Exact Duplicate", f[1], f[2], f[3] or "", f[4] or 0, f[5], sha, f[0], chk_state)
        self.table.setSortingEnabled(True)
        if self.table.rowCount() == 0: QMessageBox.information(self, "Result", "No exact SHA-256 duplicates found.")

    def scan_version_conflicts(self):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        cond, params = self.get_path_conditions()
        
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            # Find files with the exact same name but different hashes (Modified versions)
            cur.execute(f"""
                SELECT name FROM virtual_fs 
                WHERE is_folder=0 AND in_trash=0 AND ({cond}) AND sha256 IS NOT NULL AND sha256 != '' AND hash_verified=0
                GROUP BY name HAVING COUNT(DISTINCT sha256) > 1
            """, params)
            names = cur.fetchall()
            for (name,) in names:
                cur.execute(f"SELECT id, name, parent_path, extension, size, modified, sha256 FROM virtual_fs WHERE name=? AND ({cond}) AND is_folder=0 AND in_trash=0 AND sha256 IS NOT NULL AND hash_verified=0 ORDER BY modified DESC", (name,) + params)
                files = cur.fetchall()
                for idx, f in enumerate(files):
                    # Leave the newest file (index 0) unchecked, check the older versions for deletion
                    chk_state = Qt.Unchecked if idx == 0 else Qt.Checked
                    tag = "📝 Latest Version" if idx == 0 else "🕰️ Older Version"
                    self._add_row_with_state(tag, f[1], f[2], f[3] or "", f[4] or 0, f[5], f[6] or "", f[0], chk_state)
                    
        self.table.setSortingEnabled(True)
        if self.table.rowCount() == 0: QMessageBox.information(self, "Result", "No version conflicts found.")

    def scan_corrupt_files(self):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        cond, params = self.get_path_conditions()
        
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            
            # Type A: The Metadata Paradox (Same Name, Size, and Date... but different Hash)
            cur.execute(f"""
                SELECT name, size, modified FROM virtual_fs 
                WHERE is_folder=0 AND in_trash=0 AND ({cond}) AND sha256 IS NOT NULL AND sha256 != '' AND hash_verified=0
                GROUP BY name, size, modified HAVING COUNT(DISTINCT sha256) > 1
            """, params)
            paradoxes = cur.fetchall()
            for name, size, modified in paradoxes:
                cur.execute(f"SELECT id, name, parent_path, extension, size, modified, sha256 FROM virtual_fs WHERE name=? AND size=? AND modified=? AND ({cond}) AND is_folder=0 AND in_trash=0 AND hash_verified=0", (name, size, modified) + params)
                for f in cur.fetchall():
                    self._add_row_with_state("⚠️ Hash Paradox", f[1], f[2], f[3] or "", f[4], f[5], f[6] or "", f[0], Qt.Unchecked)

            # Type B: 0-Byte Dead Files
            cur.execute(f"SELECT id, name, parent_path, extension, size, modified, sha256 FROM virtual_fs WHERE size=0 AND is_folder=0 AND in_trash=0 AND ({cond}) AND hash_verified=0", params)
            for f in cur.fetchall():
                self._add_row_with_state("💀 0-Byte File", f[1], f[2], f[3] or "", 0, f[5], f[6] or "None", f[0], Qt.Checked)

        self.table.setSortingEnabled(True)
        if self.table.rowCount() == 0: QMessageBox.information(self, "Result", "No corrupted or anomalous files found.")

    def _add_row(self, typ, name, location, ext, size, modified, sha256, db_id):
        self._add_row_with_state(typ, name, location, ext, size, modified, sha256, db_id, Qt.Unchecked)

    def _add_row_with_state(self, typ, name, location, ext, size, modified, sha256, db_id, chk_state):
        row = self.table.rowCount()
        self.table.insertRow(row)
        
        chk = QTableWidgetItem()
        chk.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
        chk.setCheckState(chk_state)
        
        type_item = QTableWidgetItem(typ)
        name_item = QTableWidgetItem(name)
        loc_item = QTableWidgetItem(location)
        ext_item = QTableWidgetItem(ext)
        
        # Leverage the custom sorting class here!
        size_item = SizeTableWidgetItem(size or 0)
        
        date_item = QTableWidgetItem(str(modified))
        hash_item = QTableWidgetItem(str(sha256))
        id_item = QTableWidgetItem(str(db_id))

        self.table.setItem(row, 0, chk)
        self.table.setItem(row, 1, type_item)
        self.table.setItem(row, 2, name_item)
        self.table.setItem(row, 3, loc_item)
        self.table.setItem(row, 4, ext_item)
        self.table.setItem(row, 5, size_item)
        self.table.setItem(row, 6, date_item)
        self.table.setItem(row, 7, hash_item)
        self.table.setItem(row, 8, id_item)

    def delete_selected(self):
        # ID is at index 8
        ids = [int(self.table.item(r, 8).text()) for r in range(self.table.rowCount()) if self.table.item(r, 0).checkState() == Qt.Checked]
        if not ids: return
        if QMessageBox.question(self, "Confirm", f"Permanently delete {len(ids)} flagged items?", QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            with sqlite3.connect(self.db_path) as conn:
                conn.cursor().executemany("DELETE FROM virtual_fs WHERE id=?", [(i,) for i in ids])
                conn.commit()
            if self.parent(): 
                self.parent().clear_cache()
                self.parent().refresh_all()
            
            rows_to_remove = [r for r in range(self.table.rowCount()) if self.table.item(r, 0).checkState() == Qt.Checked]
            for r in sorted(rows_to_remove, reverse=True):
                self.table.removeRow(r)
                
            QMessageBox.information(self, "Success", "Items deleted.")


class BulkOperationEngine(QDialog):
    def __init__(self, db_path, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.setWindowTitle("Bulk Operations Engine")
        self.resize(700, 600)
        
        if parent and hasattr(parent, 'theme_combo'):
            self.setStyleSheet(THEMES.get(parent.theme_combo.currentText(), THEMES["Dark"]))
        else:
            self.setStyleSheet(THEMES["Dark"])

        layout = QVBoxLayout(self)
        
        # --- 1. Target Folder ---
        grp_target = QFrame()
        grp_target.setStyleSheet("border: 1px solid #30363d; border-radius: 6px; padding: 5px;")
        t_lay = QHBoxLayout(grp_target)
        self.lbl_target = QLabel("<b>Target Folder:</b> /")
        btn_target = QPushButton("📂 Change Target")
        btn_target.clicked.connect(self.select_target_folder)
        t_lay.addWidget(self.lbl_target, stretch=1)
        t_lay.addWidget(btn_target)
        self.target_path = "/"
        layout.addWidget(grp_target)

        # --- 2. Filter Condition ---
        form_cond = QFormLayout()
        self.combo_cond = QComboBox()
        self.combo_cond.addItems([
            "Extension equals (e.g., .tmp)",
            "Name contains (e.g., copy)",
            "Name starts with",
            "Name ends with",
            "Size greater than (MB)",
            "Older than (Days)",
            "Has Custom Tag"
        ])
        self.txt_cond_val = QLineEdit()
        self.txt_cond_val.setPlaceholderText("Enter filter value...")
        form_cond.addRow("Match Condition:", self.combo_cond)
        form_cond.addRow("Condition Value:", self.txt_cond_val)
        layout.addLayout(form_cond)

        # --- 3. Action to Perform ---
        form_action = QFormLayout()
        self.combo_action = QComboBox()
        self.combo_action.addItems([
            "Send to Virtual Trash",
            "Delete Permanently",
            "Move to Folder",
            "Add Custom Tag",
            "Set Color Tag"
        ])
        self.combo_action.currentTextChanged.connect(self.on_action_changed)
        
        self.stack_action_val = QStackedWidget()
        self.txt_action_val = QLineEdit()
        self.txt_action_val.setPlaceholderText("Enter destination path or tag...")
        self.combo_action_color = QComboBox()
        self.combo_action_color.addItems(["None", "Red", "Green", "Blue", "Gold"])
        
        self.stack_action_val.addWidget(self.txt_action_val)
        self.stack_action_val.addWidget(self.combo_action_color)
        
        form_action.addRow("Action to Apply:", self.combo_action)
        self.lbl_action_param = QLabel("Action Param:")
        form_action.addRow(self.lbl_action_param, self.stack_action_val)
        layout.addLayout(form_action)
        self.on_action_changed(self.combo_action.currentText()) # init state

        # --- 4. Preview & Log ---
        self.log_box = QTextBrowser()
        self.log_box.setPlaceholderText("Click 'Preview Matches' to see which files will be affected before committing changes...")
        layout.addWidget(self.log_box, stretch=1)

        # --- 5. Buttons ---
        btn_lay = QHBoxLayout()
        self.btn_preview = QPushButton("🔍 Preview Matches")
        self.btn_preview.clicked.connect(self.preview_matches)
        
        self.btn_exec = QPushButton("⚡ Execute Bulk Action")
        self.btn_exec.setStyleSheet("background-color: #8b0000; font-weight:bold; color: white;")
        self.btn_exec.clicked.connect(self.execute_action)
        self.btn_exec.setEnabled(False) # Require preview first
        
        btn_lay.addWidget(self.btn_preview)
        btn_lay.addStretch()
        btn_lay.addWidget(self.btn_exec)
        layout.addLayout(btn_lay)
        
        self.matched_ids = []

    def select_target_folder(self):
        path, ok = QInputDialog.getText(self, "Target Folder", "Enter Virtual Path (e.g., /Documents/):", QLineEdit.Normal, self.target_path)
        if ok and path.strip():
            p = path.strip()
            if not p.endswith('/'): p += '/'
            if not p.startswith('/'): p = '/' + p
            self.target_path = p
            self.lbl_target.setText(f"<b>Target Folder:</b> {self.target_path}")
            self.btn_exec.setEnabled(False)

    def on_action_changed(self, action_text):
        if "Color" in action_text:
            self.stack_action_val.setCurrentIndex(1)
            self.lbl_action_param.setText("Select Color:")
            self.lbl_action_param.show()
            self.stack_action_val.show()
        elif "Trash" in action_text or "Delete" in action_text:
            self.lbl_action_param.hide()
            self.stack_action_val.hide()
        else:
            self.stack_action_val.setCurrentIndex(0)
            self.lbl_action_param.show()
            self.stack_action_val.show()
            if "Move" in action_text:
                self.lbl_action_param.setText("Dest Path:")
                self.txt_action_val.setPlaceholderText("e.g., /Archive/")
            else:
                self.lbl_action_param.setText("Tag Name:")
                self.txt_action_val.setPlaceholderText("e.g., urgent")

    def build_query(self):
        cond_type = self.combo_cond.currentText()
        val = self.txt_cond_val.text().strip()
        
        if not val:
            return None, None, "Error: Condition value cannot be empty."
            
        query = "SELECT id, name, parent_path, size FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND parent_path LIKE ?"
        params = [f"{self.target_path}%"]
        
        if "Extension" in cond_type:
            if not val.startswith('.'): val = '.' + val
            query += " AND extension = ?"
            params.append(val.lower())
        elif "contains" in cond_type:
            query += " AND name LIKE ?"
            params.append(f"%{val}%")
        elif "starts with" in cond_type:
            query += " AND name LIKE ?"
            params.append(f"{val}%")
        elif "ends with" in cond_type:
            query += " AND name LIKE ?"
            params.append(f"%{val}")
        elif "Size" in cond_type:
            try:
                mb_val = float(val)
                query += " AND size > ?"
                params.append(mb_val * 1024 * 1024)
            except ValueError:
                return None, None, "Error: Size must be a valid number."
        elif "Older" in cond_type:
            try:
                days = int(val)
                cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
                query += " AND modified < ?"
                params.append(cutoff_date)
            except ValueError:
                return None, None, "Error: Days must be a valid whole number."
        elif "Tag" in cond_type:
            query += " AND custom_tags LIKE ?"
            params.append(f"%{val}%")
            
        return query, tuple(params), None

    def preview_matches(self):
        query, params, error = self.build_query()
        if error:
            QMessageBox.warning(self, "Input Error", error)
            return
            
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            matches = cur.fetchall()
            
        self.matched_ids = [m[0] for m in matches]
        
        if not matches:
            self.log_box.setHtml("<h3 style='color:#e3b341;'>0 files matched your criteria.</h3>")
            self.btn_exec.setEnabled(False)
            return
            
        html = f"<h3 style='color:#58a6ff;'>Found {len(matches)} matching files:</h3><ul>"
        for m in matches[:50]: # Show up to 50 in preview
            html += f"<li>[{m[2]}] <b>{m[1]}</b></li>"
        if len(matches) > 50:
            html += f"<li><i>...and {len(matches) - 50} more.</i></li>"
        html += "</ul><br><b style='color:#f85149;'>Review the list above. If correct, configure your action and click Execute.</b>"
        
        self.log_box.setHtml(html)
        self.btn_exec.setEnabled(True)

    def execute_action(self):
        if not self.matched_ids: return
        action_type = self.combo_action.currentText()
        param_val = self.txt_action_val.text().strip() if self.stack_action_val.currentIndex() == 0 else self.combo_action_color.currentText()
        
        if "Move" in action_type or "Tag" in action_type:
            if not param_val:
                return QMessageBox.warning(self, "Input Error", "Please provide a destination or tag value.")
                
        # --- Pre-process Move action outside of the loop ---
        if "Move" in action_type:
            if not param_val.endswith('/'): param_val += '/'
            if not param_val.startswith('/'): param_val = '/' + param_val
            
            parts = [p for p in param_val.split('/') if p]
            if not parts: return QMessageBox.warning(self, "Input Error", "Invalid destination path.")
            
            folder_name = parts[-1]
            parent_p = "/" + "/".join(parts[:-1]) + "/" if len(parts) > 1 else "/"
            
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                # Check if the exact folder already exists
                cur.execute("SELECT id FROM virtual_fs WHERE parent_path=? AND name=? AND is_folder=1", (parent_p, folder_name))
                existing_folder = cur.fetchone()
                
                if existing_folder:
                    msg = f"The folder '{param_val}' already exists.\nDo you want to merge these {len(self.matched_ids)} files into it?"
                    if QMessageBox.question(self, "Folder Exists", msg, QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes:
                        return
                else:
                    # Create the folder exactly ONCE before starting the move loop
                    cur.execute("INSERT INTO virtual_fs (parent_path, name, is_folder, modified) VALUES (?, ?, 1, ?)", (parent_p, folder_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    conn.commit()
        # ---------------------------------------------------------

        msg = f"Are you sure you want to apply '{action_type}' to {len(self.matched_ids)} files?"
        if QMessageBox.question(self, "Confirm Bulk Action", msg, QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes:
            return
            
        prog = QProgressDialog(f"Applying {action_type}...", "Cancel", 0, len(self.matched_ids), self)
        prog.setWindowModality(Qt.WindowModal); prog.show()
        
        success_count = 0
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            for i, db_id in enumerate(self.matched_ids):
                if prog.wasCanceled(): break
                
                if "Trash" in action_type:
                    cur.execute("UPDATE virtual_fs SET in_trash=1 WHERE id=?", (db_id,))
                elif "Delete Permanently" in action_type:
                    cur.execute("DELETE FROM virtual_fs WHERE id=?", (db_id,))
                elif "Move" in action_type:
                    # Now we ONLY move the files, because the folder was handled safely above
                    cur.execute("UPDATE virtual_fs SET parent_path=? WHERE id=?", (param_val, db_id))
                elif "Color" in action_type:
                    color = "" if param_val == "None" else param_val
                    cur.execute("UPDATE virtual_fs SET color_tag=? WHERE id=?", (color, db_id))
                elif "Tag" in action_type:
                    existing = cur.execute("SELECT custom_tags FROM virtual_fs WHERE id=?", (db_id,)).fetchone()[0]
                    new_tags = f"{existing}, {param_val}".strip(", ") if existing else param_val
                    cur.execute("UPDATE virtual_fs SET custom_tags=? WHERE id=?", (new_tags, db_id))
                
                success_count += 1
                prog.setValue(i+1)
            conn.commit()
            
        self.btn_exec.setEnabled(False)
        self.matched_ids = []
        self.log_box.setHtml(f"<h3 style='color:#3fb950;'>Success! Action applied to {success_count} files.</h3>")
        
        if self.parent():
            self.parent().clear_cache()
            self.parent().refresh_all()

class AdvancedImageViewer(QGraphicsView):
    def __init__(self, parent=None):
        super().__init__(parent); self.scene = QGraphicsScene(self); self.setScene(self.scene)
        self.setRenderHints(QPainter.Antialiasing | QPainter.SmoothPixmapTransform); self.setDragMode(QGraphicsView.ScrollHandDrag)
        self.setStyleSheet("background: transparent; border: none;"); self._pixmap_item = None; self.zoom_factor = 1.15
    def set_image(self, pixmap):
        self.scene.clear(); self._pixmap_item = self.scene.addPixmap(pixmap)
        self.setSceneRect(self._pixmap_item.boundingRect()); self.fitInView(self.sceneRect(), Qt.KeepAspectRatio)
    def wheelEvent(self, event):
        if event.modifiers() == Qt.ControlModifier:
            if event.angleDelta().y() > 0: self.scale(self.zoom_factor, self.zoom_factor)
            else: self.scale(1 / self.zoom_factor, 1 / self.zoom_factor)
        else: super().wheelEvent(event)
        
    def keyPressEvent(self, event):
        # Ignore these keys so the main Viewer dialog catches them for navigation/media controls!
        if event.key() in (Qt.Key_Left, Qt.Key_Right, Qt.Key_Up, Qt.Key_Down, Qt.Key_Space):
            event.ignore()
        else:
            super().keyPressEvent(event)    
        

class StaticWaveform(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedHeight(100)
        self.bars = [10] * 30
        
    def generate_from_string(self, text):
        h = hashlib.md5(text.encode()).digest()
        self.bars = [10 + (b % 80) for b in h[:30]]
        while len(self.bars) < 30: self.bars.append(10)
        self.update()
        
    def paintEvent(self, e):
        painter = QPainter(self); painter.setBrush(QBrush(QColor("#58a6ff"))); painter.setPen(Qt.NoPen)
        w = self.width() / 30
        for i, h in enumerate(self.bars): painter.drawRect(int(i*w), self.height() - h, int(w-2), h)

class AdvancedVideoViewer(QGraphicsView):
    def __init__(self, scene, parent=None):
        super().__init__(scene, parent)
        self.setRenderHints(QPainter.Antialiasing | QPainter.SmoothPixmapTransform)
        self.setDragMode(QGraphicsView.ScrollHandDrag)
        self.setStyleSheet("background: transparent; border: none;")
        self.zoom_factor = 1.15
    def wheelEvent(self, event):
        if event.modifiers() == Qt.ControlModifier:
            if event.angleDelta().y() > 0: self.scale(self.zoom_factor, self.zoom_factor)
            else: self.scale(1 / self.zoom_factor, 1 / self.zoom_factor)
        else: super().wheelEvent(event)
    def keyPressEvent(self, event):
        # Ignore these keys so the main Viewer dialog catches them for playback/navigation
        if event.key() in (Qt.Key_Left, Qt.Key_Right, Qt.Key_Up, Qt.Key_Down, Qt.Key_Space): event.ignore()
        else: super().keyPressEvent(event)

class vmanViewer(QDialog):
    def __init__(self, playlist, start_index, parent=None):
        super().__init__(parent)
        self.playlist, self.current_index = playlist, start_index
        self.setWindowTitle("VMan Media Engine"); self.resize(1100, 800)
        self.setStyleSheet(THEMES.get(parent.theme_combo.currentText() if parent else "Dark", THEMES["Dark"]))
        
        # Absolute layout for floating toolbar
        self.main_layout = QVBoxLayout(self)
        
        # Top Filename Header (Separate from bottom tools)
        self.lbl_title = QLabel()
        self.lbl_title.setFont(QFont("Segoe UI", 14, QFont.Bold))
        self.lbl_title.setAlignment(Qt.AlignCenter)
        self.lbl_title.setStyleSheet("background-color: #161b22; padding: 10px; border-bottom: 2px solid #58a6ff;")
        self.main_layout.addWidget(self.lbl_title)

        self.stack = QStackedWidget()
        self.main_layout.addWidget(self.stack)

        self.txt_view = QPlainTextEdit(); self.txt_view.setReadOnly(True)
        self.img_view = AdvancedImageViewer()
        self.stack.addWidget(self.txt_view); self.stack.addWidget(self.img_view)

        self.media_container = QWidget()
        m_lay = QVBoxLayout(self.media_container)
        
        self.wave_vis = StaticWaveform(); self.wave_vis.hide()
        m_lay.addWidget(self.wave_vis)
        
        if HAS_MULTIMEDIA:
            # Mount video inside a GraphicsView so it can be flipped, rotated, AND Zoomed/Dragged!
            self.video_scene = QGraphicsScene()
            self.video_view = AdvancedVideoViewer(self.video_scene)
            self.video_item = QGraphicsVideoItem()
            self.video_scene.addItem(self.video_item)
            
            self.video_item.nativeSizeChanged.connect(lambda size: (self.video_item.setSize(size), self.video_view.fitInView(self.video_item.boundingRect(), Qt.KeepAspectRatio)))
            m_lay.addWidget(self.video_view, 1)
            
            self.player = QMediaPlayer(); self.audio = QAudioOutput()
            self.player.setAudioOutput(self.audio); self.player.setVideoOutput(self.video_item)
            
            # A-B Loop mechanism
            self.loop_a, self.loop_b = -1, -1
            self.player.positionChanged.connect(self._check_loop)
            
        self.stack.addWidget(self.media_container)

        # Bottom Toolbar
        self.toolbar = QFrame()
        self.toolbar.setStyleSheet("background-color: rgba(22, 27, 34, 0.9); border-radius: 8px;")
        t_lay = QHBoxLayout(self.toolbar)
        
        btn_prev = QPushButton("◀"); btn_prev.clicked.connect(self._prev_item)
        btn_next = QPushButton("▶"); btn_next.clicked.connect(self._next_item)
        self.btn_play = QPushButton("⏯ Play"); self.btn_play.clicked.connect(self._toggle_playback)
        self.btn_flip = QPushButton("Flip (F)"); self.btn_flip.clicked.connect(self._flip_img)
        self.btn_rot = QPushButton("Rotate (R)"); self.btn_rot.clicked.connect(self._rot_img)
        self.btn_slide = QPushButton("Slideshow"); self.btn_slide.setCheckable(True); self.btn_slide.clicked.connect(self._toggle_slide)
        self.btn_ab = QPushButton("A-B Loop"); self.btn_ab.clicked.connect(self._set_ab_loop)
        
        self.slider = QSlider(Qt.Horizontal)
        if HAS_MULTIMEDIA:
            self.player.positionChanged.connect(self.slider.setValue)
            self.player.durationChanged.connect(self.slider.setMaximum)
            self.slider.sliderMoved.connect(self.player.setPosition)

        t_lay.addWidget(btn_prev); t_lay.addWidget(self.btn_play); t_lay.addWidget(self.slider)
        t_lay.addWidget(self.btn_ab); t_lay.addWidget(self.btn_flip); t_lay.addWidget(self.btn_rot)
        t_lay.addWidget(self.btn_slide); t_lay.addWidget(btn_next)
        self.main_layout.addWidget(self.toolbar)
        
        self.slide_timer = QTimer(self); self.slide_timer.timeout.connect(self._next_item)
        self.rot_angle = 0; self.flip_h = False

        # # Shortcuts
        QShortcut(QKeySequence("F"), self, self._flip_img)
        QShortcut(QKeySequence("R"), self, self._rot_img)
        QShortcut(QKeySequence("H"), self, self._toggle_ui_visibility)
        QShortcut(QKeySequence("F11"), self, self._toggle_fullscreen)
        
        # Make Hide/Mute GLOBAL so they work perfectly while the window is hidden in the background
        parent_win = self.parent() if self.parent() else self
        self.sc_hide = QShortcut(QKeySequence("B"), parent_win)
        self.sc_hide.setContext(Qt.ApplicationShortcut)
        self.sc_hide.activated.connect(self._toggle_hide)
        
        self.sc_mute = QShortcut(QKeySequence("M"), parent_win)
        self.sc_mute.setContext(Qt.ApplicationShortcut)
        self.sc_mute.activated.connect(self._toggle_mute)
        
        QShortcut(QKeySequence(Qt.Key_Right), self, self._next_item)
        QShortcut(QKeySequence(Qt.Key_Left), self, self._prev_item)
        QShortcut(QKeySequence(Qt.Key_Space), self, self._toggle_playback)
        QShortcut(QKeySequence(Qt.Key_Up), self, self._vol_up)
        QShortcut(QKeySequence(Qt.Key_Down), self, self._vol_down)

        self._load_current_item()


    def _toggle_hide(self):
        if self.isVisible(): 
            self.hide()
        else: 
            self.show()
            self.raise_()
            self.activateWindow()

    def _flip_img(self): 
        if self.stack.currentIndex() == 1: 
            self.flip_h = not self.flip_h; self._apply_img_transform()
        elif self.stack.currentIndex() == 2 and HAS_MULTIMEDIA:
            self.flip_v = not getattr(self, 'flip_v', False); self._apply_video_transform()

    def _rot_img(self): 
        if self.stack.currentIndex() == 1: 
            self.rot_angle = (self.rot_angle + 90) % 360; self._apply_img_transform()
        elif self.stack.currentIndex() == 2 and HAS_MULTIMEDIA:
            self.rot_v_angle = (getattr(self, 'rot_v_angle', 0) + 90) % 360; self._apply_video_transform()

    def _apply_video_transform(self):
        if not HAS_MULTIMEDIA: return
        trans = QTransform()
        center = self.video_item.boundingRect().center()
        trans.translate(center.x(), center.y())
        trans.rotate(getattr(self, 'rot_v_angle', 0))
        if getattr(self, 'flip_v', False): trans.scale(-1, 1)
        trans.translate(-center.x(), -center.y())
        self.video_item.setTransform(trans)
        self.video_view.fitInView(self.video_item.boundingRect(), Qt.KeepAspectRatio)

    def _load_current_item(self):
        if not self.playlist: return
        item = self.playlist[self.current_index]
        self.lbl_title.setText(f"{item['name']} ({self.current_index + 1}/{len(self.playlist)})")
        if hasattr(self, 'player'): self.player.stop(); self.loop_a = -1; self.loop_b = -1; self.btn_ab.setText("A-B Loop")
        
        self.btn_flip.hide(); self.btn_rot.hide(); self.btn_slide.hide(); self.wave_vis.hide(); self.btn_ab.hide()
        self.flip_v = False; self.rot_v_angle = 0
        if HAS_MULTIMEDIA: self._apply_video_transform()
        
        if hasattr(self, 'video_view'): self.video_view.hide()
        
        if item['ext'] in ['.png', '.jpg', '.jpeg', '.bmp', '.gif', '.webp']: 
            self.stack.setCurrentIndex(1)
            self.orig_pm = QPixmap(item['path'])
            self.flip_h, self.rot_angle = False, 0
            self._apply_img_transform()
            self.btn_flip.show(); self.btn_rot.show(); self.btn_slide.show()
        elif item['ext'] in ['.txt', '.csv', '.json', '.xml', '.py', '.md', '.log', '.ini', '.sh', '.cpp', '.c', '.h']:
            self.stack.setCurrentIndex(0)
            try:
                with open(item['path'], 'r', encoding='utf-8', errors='replace') as f: self.txt_view.setPlainText(f.read())
            except Exception as e: self.txt_view.setPlainText(str(e))
        elif item['ext'] in ['.mp3', '.wav', '.ogg', '.mp4', '.avi', '.mkv', '.mov'] and HAS_MULTIMEDIA: 
            self.stack.setCurrentIndex(2)
            if item['ext'] in ['.mp3', '.wav', '.ogg']: 
                self.wave_vis.generate_from_string(item['name'] + str(os.path.getsize(item['path'])))
                self.wave_vis.show()
            else:
                self.video_view.show()
                self.btn_flip.show(); self.btn_rot.show()
            self.btn_ab.show()
            self.player.setSource(QUrl.fromLocalFile(item['path'])); self._toggle_playback()
        else:
            self.stack.setCurrentIndex(0)
            self.txt_view.setPlainText("Format unsupported natively.")

    def _toggle_mute(self):
        if HAS_MULTIMEDIA and hasattr(self, 'audio'): 
            self.audio.setMuted(not self.audio.isMuted())

    def _vol_up(self):
        if HAS_MULTIMEDIA and hasattr(self, 'audio'): 
            self.audio.setVolume(min(1.0, self.audio.volume() + 0.1))

    def _vol_down(self):
        if HAS_MULTIMEDIA and hasattr(self, 'audio'): 
            self.audio.setVolume(max(0.0, self.audio.volume() - 0.1))

    def _toggle_ui_visibility(self):
        is_visible = not self.toolbar.isVisible()
        self.toolbar.setVisible(is_visible)
        self.lbl_title.setVisible(is_visible)

    def _check_loop(self, pos):
        if self.loop_b > 0 and pos >= self.loop_b: self.player.setPosition(max(0, self.loop_a))

    def _set_ab_loop(self):
        if self.loop_a == -1: self.loop_a = self.player.position(); self.btn_ab.setText("Set B")
        elif self.loop_b == -1: self.loop_b = self.player.position(); self.btn_ab.setText("Clear A-B")
        else: self.loop_a = -1; self.loop_b = -1; self.btn_ab.setText("A-B Loop")

    def _toggle_slide(self):
        if self.btn_slide.isChecked(): self.slide_timer.start(3000)
        else: self.slide_timer.stop()

    def _apply_img_transform(self):
        if hasattr(self, 'orig_pm'):
            img = self.orig_pm.toImage()
            if self.flip_h: img = img.mirrored(True, False)
            pm = QPixmap.fromImage(img).transformed(QTransform().rotate(self.rot_angle))
            self.img_view.set_image(pm)

    def _toggle_fullscreen(self): self.showNormal() if self.isFullScreen() else self.showFullScreen()
    def _toggle_playback(self):
        if hasattr(self, 'player'):
            if self.player.playbackState() == QMediaPlayer.PlayingState: self.player.pause()
            else: self.player.play()

    def _next_item(self):
        if self.current_index < len(self.playlist) - 1: self.current_index += 1; self._load_current_item()
    def _prev_item(self):
        if self.current_index > 0: self.current_index -= 1; self._load_current_item()
    
    def closeEvent(self, ev):
        # 1. Stop background slide timers if active
        if hasattr(self, 'slide_timer'): 
            self.slide_timer.stop()

        # 2. Aggressively clean up multimedia handles to prevent OS deadlocks
        if HAS_MULTIMEDIA and hasattr(self, 'player') and self.player is not None:
            self.player.stop()
            self.player.setSource(QUrl())  # Release physical file lock instantly
            self.player.setVideoOutput(None)
            self.player.setAudioOutput(None)
            self.player.deleteLater()
            self.player = None
            
        if HAS_MULTIMEDIA and hasattr(self, 'audio') and self.audio is not None:
            self.audio.deleteLater()
            self.audio = None

        # 3. Destroy global shortcuts so they don't block the next window
        if hasattr(self, 'sc_hide'):
            self.sc_hide.setEnabled(False)
            self.sc_hide.deleteLater()
            del self.sc_hide
            
        if hasattr(self, 'sc_mute'):
            self.sc_mute.setEnabled(False)
            self.sc_mute.deleteLater()
            del self.sc_mute
            
        super().closeEvent(ev)

# ---------------- Main Application Window ----------------
class vmanVirtualManager(QMainWindow):
    def __init__(self):
        super().__init__()
        self.show_hidden = False
        self.show_secondary_names = False
        self.current_prefix = "/"
        self.active_db_path = str(DB_FILE)
        
        # --- Persistent Storage Settings ---
        self.settings = QSettings("vmanOS", "VirtualManager")
        self.max_storage_gb = float(self.settings.value("max_storage_gb", 100.0))
        self.max_virtual_storage = self.max_storage_gb * 1024 * 1024 * 1024
        # ----------------------------------------

        
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
        self.db = vmanDB(DB_FILE)
        self.setWindowTitle(APP_TITLE)
        self.resize(1600, 950)
        self.setFont(QFont("Segoe UI", 10))
        self.setWindowIcon(QIcon("icons/vman.png"))
        
        self._build_ui()
        self._setup_shortcuts()
        self.apply_theme("Dark") 
        self.refresh_all()

    def sys_log(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_console.appendPlainText(f"[{timestamp}] {message}")
        self.log_console.moveCursor(QTextCursor.End)

    def clear_cache(self): self.view_cache.clear()

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
        
        act_analyzer = QAction("🧹 Analyzer", self)
        act_analyzer.triggered.connect(lambda: SpaceAnalyzerDialog(self.active_db_path, self).exec())
        
        act_bulk_del = QAction("🗑 Bulk Operations", self)
        act_bulk_del.triggered.connect(lambda: BulkOperationEngine(self.active_db_path, self).exec())
        
        act_load_ext = QAction("📂 Load DB...", self)
        act_load_ext.triggered.connect(self.load_external_db)
        
        act_csv_lib = QAction("📚 Tag Library", self)
        act_csv_lib.triggered.connect(self.open_tag_library)

        act_set_storage = QAction("💾 Set Storage", self)
        act_set_storage.triggered.connect(self.set_storage_capacity)

        
        self.act_view_mode = QAction("🖼 Grid View", self)
        self.act_view_mode.triggered.connect(self.toggle_view_mode)
        
        self.act_sec_name = QAction("🏷", self)
        self.act_sec_name.setCheckable(True)
        self.act_sec_name.triggered.connect(self.toggle_secondary_names)
        tb.addAction(self.act_sec_name)
        
        self.act_toggle_sidebar = QAction("📊 Inspector", self)
        self.act_toggle_sidebar.triggered.connect(self.toggle_sidebar)
        
        act_toggle_log = QAction("📝 Console", self)
        act_toggle_log.triggered.connect(lambda: self.log_dock.setVisible(not self.log_dock.isVisible()))
        
        self.theme_combo = QComboBox()
        self.theme_combo.addItems(list(THEMES.keys()))
        self.theme_combo.currentTextChanged.connect(self.apply_theme)
        
        act_help = QAction("❓", self)
        act_help.triggered.connect(self.show_help)
        
        tb.addActions([self.act_back, self.act_fwd, act_up])
        tb.addSeparator()
        tb.addActions([act_new_folder, act_new_file])
        tb.addSeparator()
        
 
        tb.addActions([act_timeline, act_analyzer, act_bulk_del, act_load_ext, act_csv_lib, act_set_storage])
        tb.addSeparator()

        # --- ADDING THE GRID VIEW BUTTON TO THE UI ---
        tb.addActions([self.act_view_mode, self.act_toggle_sidebar, act_toggle_log])       
        
        empty = QWidget()
        empty.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        tb.addWidget(empty)
        tb.addWidget(self.theme_combo)
        tb.addAction(act_help)
        
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

        self.file_grid = SandboxListView()
        self.file_grid.filesDroppedOS.connect(self.on_files_dropped)
        self.file_grid.internalDrop.connect(self.execute_internal_drop)
        self.file_grid.setContextMenuPolicy(Qt.CustomContextMenu)
        self.file_grid.customContextMenuRequested.connect(lambda pos: self.context_menu(pos, is_grid=True))
        self.file_grid.doubleClicked.connect(self.open_selected)
        self.file_grid.clicked.connect(self.on_grid_click)
        self.file_grid.openRequest.connect(self.open_selected)
        self.view_stack.addWidget(self.file_grid)
        
        vbox.addWidget(self.view_stack)

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
        self.folder_tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.folder_tree.customContextMenuRequested.connect(self.on_tree_context_menu)      
        self.tree_dock.setWidget(self.folder_tree)
        self.addDockWidget(Qt.LeftDockWidgetArea, self.tree_dock)

        self.right_dock = QDockWidget("Inspector", self)
        self.right_dock.setAllowedAreas(Qt.RightDockWidgetArea)
        self.right_tabs = QTabWidget()
        
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

        props_container = QWidget()
        ed_layout = QFormLayout(props_container)
        self.ed_name = QLineEdit()
        
        self.ed_v_path = QLineEdit()
        self.ed_v_path.setReadOnly(True)
        
        self.ed_target = QLineEdit()
        self.ed_target.setReadOnly(True)
        self.ed_custom_tags = QLineEdit()
        self.ed_custom_tags.setPlaceholderText("tag1, tag2")
        self.ed_secondary = QLineEdit()
        
        self.ed_sha = QLineEdit()
        self.ed_sha.setReadOnly(True)
        self.ed_sha.setPlaceholderText("Not Computed")
        
        self.ed_tag = QComboBox()
        self.ed_tag.addItems(["None", "Red", "Green", "Blue", "Gold"])
        self.btn_save_ed = QPushButton("Apply Properties")
        self.btn_save_ed.clicked.connect(self.save_properties_editor)
        self.btn_calc_hash = QPushButton("Compute & Store SHA-256")
        self.btn_calc_hash.clicked.connect(self.compute_checksum)
        
        ed_layout.addRow("Name:", self.ed_name)
        ed_layout.addRow("Sec Name:", self.ed_secondary)
        ed_layout.addRow("Virtual Path:", self.ed_v_path)
        ed_layout.addRow("Target:", self.ed_target)
        ed_layout.addRow("Labels:", self.ed_custom_tags)
        ed_layout.addRow("Color:", self.ed_tag)
        ed_layout.addRow("SHA-256:", self.ed_sha)
        ed_layout.addRow("", self.btn_save_ed)
        ed_layout.addRow("", self.btn_calc_hash)
        self.right_tabs.addTab(props_container, "Properties")

        stats_container = QWidget()
        st_layout = QVBoxLayout(stats_container)
        self.lbl_stats_txt = QTextBrowser()
        st_layout.addWidget(self.lbl_stats_txt)
        self.right_tabs.addTab(stats_container, "Analytics")

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

    def cmd_map_folder(self, item):
        typ, v_path, db_id = item
        if typ != "folder": return
        real_p = QFileDialog.getExistingDirectory(self, f"Select Real Folder mapped to {v_path}")
        if not real_p: return
        real_p = real_p.replace('\\', '/')
        try:
            with sqlite3.connect(self.db.path, timeout=10) as conn:
                cur = conn.cursor()
                # 1. Update the folder itself
                cur.execute("UPDATE virtual_fs SET real_path=? WHERE id=?", (real_p, db_id))
                # 2. Automatically sync all contents inside the folder
                cur.execute("SELECT id, parent_path, name FROM virtual_fs WHERE parent_path LIKE ?", (f"{v_path}%",))
                for c_id, pp, name in cur.fetchall():
                    full_v = f"{pp}{name}/".replace("//", "/")
                    rel = full_v[len(v_path):]
                    new_real = os.path.join(real_p, rel).replace('\\', '/').rstrip('/')
                    cur.execute("UPDATE virtual_fs SET real_path=? WHERE id=?", (new_real, c_id))
                conn.commit()
            QMessageBox.information(self, "Mapped", f"Successfully mapped '{v_path}' and all its contents to:\n{real_p}")
            self.clear_cache(); self.refresh_all()
        except Exception as e: QMessageBox.critical(self, "Error", str(e))

    def toggle_secondary_names(self):
        self.show_secondary_names = self.act_sec_name.isChecked()
        self.clear_cache()
        self.load_directory(self.current_prefix)

    def show_multi_properties(self, items):
        total_size, folders, files = 0, 0, 0
        with sqlite3.connect(self.db.path) as conn:
            cur = conn.cursor()
            for typ, path, db_id in items:
                if db_id == -1: continue
                if typ == "file":
                    files += 1
                    sz = cur.execute("SELECT size FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
                    total_size += sz[0] if sz and sz[0] else 0
                else:
                    folders += 1
                    cnt, sz = cur.execute("SELECT COUNT(id), SUM(size) FROM virtual_fs WHERE parent_path LIKE ? AND is_folder=0", (f"{path}%",)).fetchone()
                    files += cnt or 0
                    total_size += sz or 0
        QMessageBox.information(self, "Multi-Selection Properties", f"<b>Selected Items:</b> {len(items)}<br><br><b>Total Folders:</b> {folders}<br><b>Total Files:</b> {files}<br><b>Combined Size:</b> {human_size(total_size)}")

    def cmd_delete_physical(self, items):
        safe_files_exist = False
        with sqlite3.connect(self.db.path) as conn:
            cur = conn.cursor()
            for typ, path, db_id in items:
                if db_id != -1:
                    res = cur.execute("SELECT hash_verified FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
                    if res and res[0] == 1:
                        safe_files_exist = True
                        break
                        
        if safe_files_exist:
            QMessageBox.warning(self, "Protected Files", "One or more selected items are marked as 'Safe' and cannot be deleted physically.\n\nPlease unmark them in the Space Analyzer first.")
            return

        if QMessageBox.question(self, "Physical Deletion", f"WARNING: This will delete {len(items)} items from your REAL hard drive permanently.\n\nAre you sure?", QMessageBox.Yes|QMessageBox.No) != QMessageBox.Yes: return
        deleted = 0
        with sqlite3.connect(self.db.path) as conn:
            cur = conn.cursor()
            for typ, path, db_id in items:
                if db_id == -1: continue
                rp = cur.execute("SELECT real_path FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
                if rp and rp[0] and os.path.exists(rp[0]):
                    try:
                        shutil.rmtree(rp[0]) if os.path.isdir(rp[0]) else os.remove(rp[0])
                        deleted += 1
                    except Exception as e: QMessageBox.warning(self, "Error", f"Failed to delete {rp[0]}: {e}")
                if typ == "file": cur.execute("DELETE FROM virtual_fs WHERE id=?", (db_id,))
                else: cur.execute("DELETE FROM virtual_fs WHERE parent_path LIKE ? OR id=?", (f"{path}%", db_id))
            conn.commit()
        self.clear_cache(); self.refresh_all(); self.status.showMessage(f"Physically deleted {deleted} items from disk.")

    def cmd_map_parent_drive(self, item):
        db_id = item[2]
        if db_id == -1: return
        with sqlite3.connect(self.db.path) as conn:
            rp = conn.cursor().execute("SELECT real_path FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
            if not rp or not rp[0]: return QMessageBox.warning(self, "Error", "No physical path associated with this item.")
            
            # Auto-detect Windows drive letter (C:/) or Linux root mount (/mnt/usb/)
            old_root = rp[0][:3] if rp[0][1] == ':' else "/" + rp[0].strip("/").split("/")[0] + "/"
            new_root, ok = QInputDialog.getText(self, "Map Drive/Mount", f"Current root detected: {old_root}\nEnter new Drive Letter or Mount Point:", QLineEdit.Normal, old_root)
            if ok and new_root.strip():
                new_root = new_root.strip().replace('\\', '/')
                if not new_root.endswith('/'): new_root += '/'
                conn.cursor().execute("UPDATE virtual_fs SET real_path = REPLACE(real_path, ?, ?) WHERE real_path LIKE ?", (old_root, new_root, f"{old_root}%"))
                conn.commit()
                QMessageBox.information(self, "Remapped", f"Successfully updated paths mapped under {old_root} to {new_root}.")
                self.clear_cache(); self.refresh_all()

    def _setup_shortcuts(self):
        QShortcut(QKeySequence(Qt.Key_F11), self, self.toggle_fullscreen)
        QShortcut(QKeySequence("Shift+Delete"), self, self.cmd_delete_permanent)
        QShortcut(QKeySequence("Ctrl+H"), self, self.toggle_hidden_files)
        QShortcut(QKeySequence("Ctrl+C"), self, self.cmd_copy); QShortcut(QKeySequence("Ctrl+X"), self, self.cmd_cut)
        QShortcut(QKeySequence("Ctrl+V"), self, self.cmd_paste); QShortcut(QKeySequence("Delete"), self, self.cmd_delete)
        QShortcut(QKeySequence("F2"), self, self.cmd_rename); QShortcut(QKeySequence("Ctrl+Shift+N"), self, self.create_folder)
        QShortcut(QKeySequence("Ctrl+N"), self, self.create_virtual_file); QShortcut(QKeySequence("Ctrl+F"), self, self.search_box.setFocus)
        QShortcut(QKeySequence("Ctrl+O"), self, self.open_selected_vman)
        QShortcut(QKeySequence("Backspace"), self, self.nav_back); QShortcut(QKeySequence("Shift+Backspace"), self, self.nav_forward); QShortcut(QKeySequence("Alt+Up"), self, self.nav_up)

    def show_help(self):
        dlg = QDialog(self); dlg.setWindowTitle("Instructions"); dlg.setStyleSheet(THEMES[self.theme_combo.currentText()]); dlg.resize(600, 400); lay = QVBoxLayout(dlg); txt = QPlainTextEdit()
        txt.setPlainText("""VMan - KEYBOARD & FEATURE GUIDE\n\n[Keyboard Navigation]\nBackspace   : Go Back\nShift+Back  : Go Forward\nAlt+Up      : Go Up One Directory\nCtrl+F      : Focus Global Search\nEnter       : Open selected folder or OS File\n\n[Viewer Controls (Ctrl+O)]\nSpacebar    : Play/Pause Media\nRight/Left  : Next/Previous Item\nUp/Down     : Volume Control\nZoom In/Out : Dedicated buttons for Images and Text\nEscape      : Close Viewer\n\n[Operations]\nMultiselect : Use Ctrl/Shift + Click to highlight multiple items.\nExport/OS   : Right click to "Materialize" ANY number of files back to Physical Windows/Mac OS.\nF2          : Rename (Select multiple files to Bulk Rename sequentially!)\nDelete      : Send to Trash / Permanently Delete\nCtrl+C/V/X  : Copy, Paste, Cut (Works on multiple items!)""")
        txt.setReadOnly(True); lay.addWidget(txt); dlg.exec()

    def filter_current_view(self, text):
        term = text.lower()
        if self.view_stack.currentIndex() == 0:
            for row in range(self.file_table.model().rowCount()):
                name = self.file_table.model().data(self.file_table.model().index(row, 0), Qt.DisplayRole)
                self.file_table.setRowHidden(row, term not in str(name).lower())
        else:
            for row in range(self.file_grid.model().rowCount()):
                name = self.file_grid.model().data(self.file_grid.model().index(row, 0), Qt.DisplayRole)
                self.file_grid.setRowHidden(row, term not in str(name).lower())

    def apply_theme(self, theme_name=None):
        if theme_name is None: theme_name = self.theme_combo.currentText()
        self.setStyleSheet(THEMES.get(theme_name, THEMES["Dark"]))
        self.is_dark_mode = "Light" not in theme_name
        self.update_statistics() 
        
    def toggle_view_mode(self):
        self.view_stack.setCurrentIndex(1 if self.view_stack.currentIndex() == 0 else 0)
        self.act_view_mode.setText("📄 List View" if self.view_stack.currentIndex() == 1 else "🖼 Grid View")
        
    def toggle_sidebar(self): self.right_dock.setVisible(not self.right_dock.isVisible())

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
        path, _ = QFileDialog.getOpenFileName(self, "Load vman DB", "", "SQLite DB (*.db)")
        if path:
            dest = VIEWS_DIR / Path(path).name
            if str(Path(path).resolve()) != str(dest.resolve()): shutil.copy2(path, dest)
            self.refresh_all()
            self.status.showMessage(f"Imported DB to Compiled Views: {Path(path).name}")
            self.sys_log(f"Imported External Database: {Path(path).name}")

    def refresh_tree(self):
        self.folder_tree.clear()
        db_label = Path(self.active_db_path).name if self.active_db_path != str(DB_FILE) else "Main System DB"
        sys_root = QTreeWidgetItem(self.folder_tree, [f"💽 {db_label}"]); sys_root.setData(0, Qt.UserRole, "/"); sys_root.setIcon(0, self.style().standardIcon(QStyle.SP_DirHomeIcon)); sys_root.setExpanded(True)
        QTreeWidgetItem(sys_root, ["⭐ Favorites"]).setData(0, Qt.UserRole, "fav://")
        QTreeWidgetItem(sys_root, ["🗑 Trash Bin"]).setData(0, Qt.UserRole, "trash://")
        smart = QTreeWidgetItem(sys_root, ["💡 Dynamic Smart Views"]); smart.setIcon(0, self.style().standardIcon(QStyle.SP_FileDialogDetailedView)); smart.setExpanded(True)
        QTreeWidgetItem(smart, ["🏷️ By Custom Tags"]).setData(0, Qt.UserRole, "tags://")
        QTreeWidgetItem(smart, ["🗂 Stat: Year ➔ Month ➔ Folder"]).setData(0, Qt.UserRole, "y_m_f://")
        for proto, cols in SMART_PROTOCOLS.items():
            if proto == "tags://": continue
            QTreeWidgetItem(smart, [f"🗂 {' ➔ '.join([c.capitalize() for c in cols])}"]).setData(0, Qt.UserRole, proto)
        compiled_root = QTreeWidgetItem(self.folder_tree, ["📦 Switch Database..."]); compiled_root.setIcon(0, self.style().standardIcon(QStyle.SP_DriveHDIcon)); compiled_root.setExpanded(True)
        if self.active_db_path != str(DB_FILE):
            node = QTreeWidgetItem(compiled_root, ["⬅ Return to Main DB"]); node.setData(0, Qt.UserRole, "db://main"); node.setIcon(0, self.style().standardIcon(QStyle.SP_ArrowBack))
        for view_db in VIEWS_DIR.glob("*.db"):
            node = QTreeWidgetItem(compiled_root, [view_db.stem]); node.setData(0, Qt.UserRole, f"db://{view_db.name}"); node.setIcon(0, self.style().standardIcon(QStyle.SP_FileIcon))

    def refresh_all(self):
        self.refresh_tree()
        self.load_directory(self.current_prefix)

    def on_tree_click(self, item: QTreeWidgetItem, col: int):
        path = item.data(0, Qt.UserRole)
        if path and path.startswith("db://"):
            db_name = path.replace("db://", "")
            if db_name == "main":
                self.active_db_path = str(DB_FILE); self.status.showMessage("Reconnected to Main System DB"); self.sys_log("Reconnected to Main System DB.")
            else:
                self.active_db_path = str(VIEWS_DIR / db_name); self.status.showMessage(f"Connected to isolated DB: {db_name}"); self.sys_log(f"Switched context to Isolated DB: {db_name}")
            self.db.close(); self.db = vmanDB(Path(self.active_db_path)); self.clear_cache(); self.refresh_tree(); self.nav_to_path("/")
        elif path: self.nav_to_path(path)

    def on_folder_expand(self, item: QTreeWidgetItem):
        if item.data(0, Qt.UserRole + 1) or item.data(0, Qt.UserRole) in ["fav://", "trash://"] or self._is_smart_path(str(item.data(0, Qt.UserRole))) or str(item.data(0, Qt.UserRole)).startswith("db://"): return
        for sf in [str(r[0]) for r in self.db.conn.cursor().execute(f"SELECT name FROM virtual_fs WHERE parent_path = ? AND is_folder = 1 AND in_trash = 0 {'AND is_hidden=0' if not self.show_hidden else ''} ORDER BY name", (item.data(0, Qt.UserRole),)).fetchall() if r[0]]:
            child = QTreeWidgetItem(item, [sf]); child.setData(0, Qt.UserRole, f"{item.data(0, Qt.UserRole)}{sf}/"); child.setIcon(0, self.style().standardIcon(QStyle.SP_DirIcon)); child.setChildIndicatorPolicy(QTreeWidgetItem.ShowIndicator)
        item.setData(0, Qt.UserRole + 1, True)

    def nav_to_path(self, path: str, record_history=True):
        if record_history and self.current_prefix != path: 
            self.history_back.append(self.current_prefix); self.history_forward.clear()
        self.load_directory(path)

    def nav_back(self):
        if self.history_back: 
            self.history_forward.append(self.current_prefix); self.nav_to_path(self.history_back.pop(), False)
            self.act_back.setEnabled(len(self.history_back) > 0); self.act_fwd.setEnabled(len(self.history_forward) > 0)
            
    def nav_forward(self):
        if self.history_forward: 
            self.history_back.append(self.current_prefix); self.nav_to_path(self.history_forward.pop(), False)
            self.act_back.setEnabled(len(self.history_back) > 0); self.act_fwd.setEnabled(len(self.history_forward) > 0)
            
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
        
        self.search_box.clear(); self.local_filter.clear()
        self.act_back.setEnabled(len(self.history_back) > 0); self.act_fwd.setEnabled(len(self.history_forward) > 0)
        
        if target_path in self.view_cache: 
            self._start_ui_render(*self.view_cache[target_path], cached=True); return
            
        if self.loader_thread and self.loader_thread.isRunning():
            self.loader_thread.cancel(); self.loader_thread.quit(); self.loader_thread.wait()

        self.status.showMessage("Computing View...")
        self.loader_thread = DataLoaderThread(self.active_db_path, target_path, self.show_hidden, self)
        self.loader_thread.data_ready.connect(lambda f, fl: self._start_ui_render(f, fl, cached=False))
        self.loader_thread.start()

    def _start_ui_render(self, folders, files, cached=False, is_search=False):
        # Prevent search results from overwriting the folder cache!
        if not cached and not is_search: self.view_cache[self.current_prefix] = (folders, files) 
        self.render_queue = folders + files; total = len(self.render_queue); self.table_rows_buffer = []
        
        if total > CHUNK_SIZE:
            self.render_progress = QProgressDialog(f"{'Loading from RAM Cache' if cached else 'Rendering'} {total} items...", "Cancel", 0, total, self)
            self.render_progress.setWindowModality(Qt.WindowModal); self.render_progress.show()
        else: self.render_progress = None

        self.file_table.setUpdatesEnabled(False); self.file_grid.setUpdatesEnabled(False)
        self.render_timer.start(1) 

    def run_global_search(self):
        term = self.search_box.text().strip()
        if not term: 
            return self.load_directory(self.current_prefix)
        cur = self.db.conn.cursor()
        cur.execute(f"SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE (name LIKE ? OR secondary_name LIKE ? OR custom_tags LIKE ?) AND is_folder=0 AND in_trash=0", (f"%{term}%", f"%{term}%", f"%{term}%"))
        # Pass is_search=True to protect the cache
        self._start_ui_render([], cur.fetchall(), cached=False, is_search=True) 
        self.sys_log(f"Global Search executed for: {term}")

    def _render_chunk(self):
        if not self.render_queue:
            self.render_timer.stop()
            # UPDATED: Expanded to 7 highly detailed columns
            shared_model = vmanTableModel(["Name", "Ext", "Size", "Modified", "Type", "Location", "Tag"], self.table_rows_buffer)
            self.file_table.setModel(shared_model); self.file_grid.setModel(shared_model)
            
            # Adjusted column widths for the new data
            self.file_table.setColumnWidth(0, 260) # Name
            self.file_table.setColumnWidth(1, 60)  # Ext
            self.file_table.setColumnWidth(2, 90)  # Size
            self.file_table.setColumnWidth(3, 140) # Modified
            self.file_table.setColumnWidth(4, 120) # Type
            self.file_table.setColumnWidth(5, 350) # Location
            
            self.file_table.setUpdatesEnabled(True); self.file_grid.setUpdatesEnabled(True)
            if self.render_progress: self.render_progress.close()
            self.status.showMessage("Rendering complete.", 3000); self.update_statistics()
            return

        if self.render_progress and self.render_progress.wasCanceled(): self.render_queue.clear(); return

        chunk = self.render_queue[:CHUNK_SIZE]; self.render_queue = self.render_queue[CHUNK_SIZE:]
        dir_icon = self.style().standardIcon(QStyle.SP_DirIcon)

        for item in chunk:
            if len(item) == 8:
                db_id, pp, f_name, c_tag, sec_n, is_h, count, size = item
                v_path = f"{pp}{f_name}/" if not f_name.endswith("/") else f"{pp}{f_name}"
                disp_name = sec_n if (self.show_secondary_names and sec_n) else (f"{f_name}\n({sec_n})" if sec_n else f"{f_name}")
                
                self.table_rows_buffer.append({"display": [disp_name, "", human_size(size), "N/A", f"Virtual Folder ({count})", pp, c_tag], "sort_keys": [(0, f_name.lower()), (0, ""), (0, size), (0, ""), (0, count), (0, pp.lower()), (0, c_tag)], "user_data": ("folder", v_path, db_id), "color_tag": c_tag, "is_hidden": is_h, "icon": dir_icon})
            else:
                db_id, n, s, ext, rp, mod, c_tag, sec_n, is_h = item[:9]
                icon = self._get_native_icon(rp, False, ext); s_val = s if s else 0; ext_str = str(ext) if ext else ""
                
                # FIXED: Uses 'n' instead of 'f_name' for virtual files!
                disp_name = sec_n if (self.show_secondary_names and sec_n) else (f"{n}\n({sec_n})" if sec_n else f"{n}")
                
                self.table_rows_buffer.append({"display": [disp_name, ext_str, human_size(s_val), str(mod), "Virtual File", str(rp) if rp else "", c_tag], "sort_keys": [(1, str(n).lower()), (1, ext_str.lower()), (1, s_val), (1, str(mod)), (1, "Virtual File"), (1, str(rp).lower() if rp else ""), (1, c_tag)], "user_data": ("file", str(rp), db_id), "color_tag": c_tag, "is_hidden": is_h, "icon": icon})

        if self.render_progress: self.render_progress.setValue(self.render_progress.maximum() - len(self.render_queue))

    def _trigger_preview(self, typ, path, db_id, name):
        if db_id == -1: return 
        self.btn_save_ed.setProperty("db_id", db_id); self.btn_save_ed.setProperty("is_folder", typ == "folder")
        if HAS_MULTIMEDIA and hasattr(self, 'player'): self.player.stop()
        self.preview_image.clear(); self.preview_text.clear()
        
        row = self.db.conn.cursor().execute("SELECT real_path, size, extension, modified, secondary_name, custom_tags, sha256 FROM virtual_fs WHERE id = ?", (db_id,)).fetchone()
        if not row: return
        real_path, size, ext, mod, sec_n, custom_tags, sha256_val = row
        
        self.ed_v_path.setText(path) # -> FIXED: Virtual path is now populated!
        self.ed_secondary.setText(str(sec_n) if sec_n else "")
        self.ed_custom_tags.setText(str(custom_tags) if custom_tags else "")
        self.ed_sha.setText(str(sha256_val) if sha256_val else "")
        
        if typ == "folder": 
            self.preview_stack.setCurrentIndex(1)
            self.ed_target.setText(f"Virtual Container: {path}")
            self.preview_text.setPlainText(f"Directory Data:\n{path}")
            self.btn_calc_hash.setEnabled(False)
            return
            
        self.btn_calc_hash.setEnabled(True)
        self.ed_target.setText(real_path); ext = str(ext).lower()
        
        if real_path and os.path.exists(real_path):
            if ext in [".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp"]:
                self.preview_stack.setCurrentIndex(0)
                loader = ImageLoader(real_path, parent=self); loader.finished.connect(self.on_image_loaded); self._register_worker(loader); loader.start()
            elif ext in [".txt", ".csv", ".json", ".xml", ".py", ".md", ".log", ".ini", ".sh", ".cpp", ".c", ".h"]:
                self.preview_stack.setCurrentIndex(1)
                try:
                    with open(real_path, 'r', encoding='utf-8', errors='replace') as f: self.preview_text.setPlainText(f.read(5000))
                except Exception as e: self.preview_text.setPlainText(f"Error reading file: {e}")
            elif ext in [".mp3", ".wav", ".ogg"] and HAS_MULTIMEDIA:
                self.preview_stack.setCurrentIndex(2); self.lbl_media_title.setText(name); self.player.setSource(QUrl.fromLocalFile(real_path))
            else: 
                self.preview_stack.setCurrentIndex(1); self.preview_text.setPlainText(f"File: {name}\nTarget: {real_path}\nSize: {human_size(size)}\nModified: {mod}")
        else: 
            self.preview_stack.setCurrentIndex(1); self.preview_text.setPlainText(f"Virtual File: {name}\nDisconnected or missing local file.")

    def on_image_loaded(self, path, image):
        if image and not image.isNull() and image.width() > 0: self.preview_image.setPixmap(QPixmap.fromImage(image))
        else: self.preview_image.clear(); self.preview_text.setPlainText("Image preview failed to load."); self.preview_stack.setCurrentIndex(1)

    def on_file_click(self, index: QModelIndex):
        if self.file_table.model() and self.file_table.model().data(self.file_table.model().index(index.row(), 0), Qt.UserRole):
            data = self.file_table.model().data(self.file_table.model().index(index.row(), 0), Qt.UserRole)
            if data[2] == -1: return 
            name = self.file_table.model().data(self.file_table.model().index(index.row(), 0), Qt.DisplayRole)
            self.ed_name.setText(str(name).split('\n')[0]); self.ed_tag.setCurrentIndex(max(0, self.ed_tag.findText(self.file_table.model().data(self.file_table.model().index(index.row(), 0), Qt.UserRole + 1))))
            self._trigger_preview(data[0], data[1], data[2], name)

    def on_grid_click(self, index: QModelIndex):
        if self.file_grid.model() and self.file_grid.model().data(index, Qt.UserRole):
            data = self.file_grid.model().data(index, Qt.UserRole)
            if data[2] == -1: return
            name = self.file_grid.model().data(index, Qt.DisplayRole)
            self.ed_name.setText(str(name).split('\n')[0]); self.ed_tag.setCurrentIndex(max(0, self.ed_tag.findText(self.file_grid.model().data(index, Qt.UserRole + 1))))
            self._trigger_preview(data[0], data[1], data[2], name)

    def save_properties_editor(self):
        db_id = self.btn_save_ed.property("db_id")
        if not db_id or db_id == -1 or not self.ed_name.text().strip(): return
        cur = self.db.conn.cursor()
        new_name, new_sec, new_tag, new_custom_tags = self.ed_name.text().strip(), self.ed_secondary.text().strip(), "" if self.ed_tag.currentText() == "None" else self.ed_tag.currentText(), self.ed_custom_tags.text().strip()
        
        if self.btn_save_ed.property("is_folder"):
            pp, old_name = cur.execute("SELECT parent_path, name FROM virtual_fs WHERE id = ?", (db_id,)).fetchone()
            cur.execute("UPDATE virtual_fs SET name=?, secondary_name=?, color_tag=?, custom_tags=? WHERE id=?", (new_name, new_sec, new_tag, new_custom_tags, db_id))
            cur.execute("UPDATE virtual_fs SET parent_path = ? || SUBSTR(parent_path, LENGTH(?) + 1) WHERE parent_path LIKE ?", (f"{pp}{new_name}/", f"{pp}{old_name}/", f"{pp}{old_name}/%"))
        else: cur.execute("UPDATE virtual_fs SET name=?, secondary_name=?, color_tag=?, custom_tags=? WHERE id=?", (new_name, new_sec, new_tag, new_custom_tags, db_id))
            
        self.db.conn.commit(); self.clear_cache(); self.load_directory(self.current_prefix)
        self.status.showMessage("Virtual Properties Saved.", 3000); self.sys_log(f"Properties updated for DB ID: {db_id}")

    def compute_checksum(self):
        rp = self.ed_target.text()
        db_id = self.btn_save_ed.property("db_id")
        if not rp or not os.path.exists(rp) or os.path.isdir(rp): return QMessageBox.warning(self, "Error", "Invalid or missing physical file.")
        self.status.showMessage("Computing SHA-256 Hash...")
        
        calc = HashCalculator(rp, self)
        calc.finished.connect(lambda h: self._on_hash_computed(h, db_id, rp))
        self._register_worker(calc); calc.start()

    def _on_hash_computed(self, h, db_id, rp):
        if db_id and db_id != -1:
            with sqlite3.connect(self.db.path) as conn:
                conn.cursor().execute("UPDATE virtual_fs SET sha256=? WHERE id=?", (h, db_id))
                conn.commit()
            self.ed_sha.setText(h)
            self.clear_cache() # Clear cache so properties menus reflect the new hash
            
        self.sys_log(f"Calculated and saved Hash for {os.path.basename(rp)}: {h}")
        QMessageBox.information(self, "SHA-256 Checksum", f"File: {os.path.basename(rp)}\n\nHash:\n{h}\n\n(Saved to Database)")

    # ---------- Operations & Integrations ----------
    def open_selected(self):
        sel = self._get_selected_items()
        if not sel: return
        data = sel[0]
        if data[0] == "folder" or data[2] == -1: self.nav_to_path(data[1])
        elif data[1] and os.path.exists(data[1]):
            try: os.startfile(data[1]) if sys.platform=="win32" else os.system(f"open '{data[1]}'" if sys.platform=="darwin" else f"xdg-open '{data[1]}'")
            except Exception as e: QMessageBox.warning(self, "Open", str(e))
        else: QMessageBox.warning(self, "Not Found", "Target file is missing locally.")

    def open_selected_vman(self):
        sel = self._get_selected_items()
        if not sel or sel[0][2] == -1: return
        target_typ, target_rp, target_db_id = sel[0]
        if target_typ != "file" or not target_rp or not os.path.exists(target_rp): return QMessageBox.warning(self, "vman Viewer", "Cannot open virtual folder or missing local file.")
            
        playlist, start_index = [], 0
        model = self.file_table.model()
        for r in range(model.rowCount()):
            data = model.data(model.index(r, 0), Qt.UserRole); name = model.data(model.index(r, 0), Qt.DisplayRole)
            if data and data[0] == "file" and data[1] and os.path.exists(data[1]):
                playlist.append({'path': data[1], 'name': name.split('\n')[0], 'ext': os.path.splitext(data[1])[1].lower()})
                if data[2] == target_db_id: start_index = len(playlist) - 1

        # CLEANUP OLD VIEWER TO PREVENT BACKGROUND CONFLICTS
        if hasattr(self, 'viewer') and self.viewer is not None:
            self.viewer.close()
            self.viewer.deleteLater()
            self.viewer = None

        self.viewer = vmanViewer(playlist, start_index, self); self.viewer.show()

    def open_local_file_system(self, db_id):
        row = self.db.conn.cursor().execute("SELECT real_path FROM virtual_fs WHERE id = ?", (db_id,)).fetchone()
        if row and row[0] and os.path.exists(row[0]):
            try: os.startfile(row[0]) if sys.platform=="win32" else os.system(f"open '{row[0]}'" if sys.platform=="darwin" else f"xdg-open '{row[0]}'")
            except Exception as e: QMessageBox.warning(self, "Open", str(e))
        else: QMessageBox.information(self, "Open", "No accessible path for this file on the local machine.")

    def open_file_location(self, db_id):
        row = self.db.conn.cursor().execute("SELECT real_path FROM virtual_fs WHERE id = ?", (db_id,)).fetchone()
        if row and row[0] and os.path.exists(row[0]):
            try:
                norm_path = os.path.normpath(row[0])
                if sys.platform == "win32": subprocess.Popen(['explorer', '/select,', norm_path])
                elif sys.platform == "darwin": subprocess.Popen(["open", "-R", norm_path])
                else: os.system(f"xdg-open '{os.path.dirname(norm_path)}'")
            except Exception as e: QMessageBox.warning(self, "Open Location", str(e))
        else: QMessageBox.warning(self, "Not Found", "Item does not exist locally.")

    def show_properties(self, typ, path, db_id):
        cur = self.db.conn.cursor()
        dlg = QDialog(self); dlg.setWindowTitle("vman Entity Properties"); dlg.setMinimumWidth(450); dlg.setStyleSheet(THEMES.get(self.theme_combo.currentText(), THEMES["Dark"]))
        layout = QFormLayout(dlg)
        
        if typ == "folder":
            r = cur.execute("SELECT name, secondary_name, custom_tags, color_tag, real_path FROM virtual_fs WHERE id = ?", (db_id,)).fetchone()
            cnt, sz = cur.execute(f"SELECT COUNT(id), SUM(size) FROM virtual_fs WHERE parent_path LIKE ? AND is_folder=0", (f"{path}%",)).fetchone()
            
            if r:
                n, sec_n, tags, color, real_p = r
                layout.addRow("Folder Name:", QLabel(n))
                layout.addRow("Secondary Name:", QLabel(sec_n if sec_n else "None"))
                layout.addRow("Virtual Path:", QLineEdit(path))
                
                txt_real = QLineEdit(real_p if real_p else "Disconnected / Virtual Only")
                txt_real.setReadOnly(True)
                layout.addRow("Local Target:", txt_real)
                
                layout.addRow("Total Items:", QLabel(str(cnt or 0)))
                layout.addRow("Total Size:", QLabel(human_size(sz or 0)))
                layout.addRow("Custom Tags:", QLabel(tags if tags else "None"))
                layout.addRow("Color Tag:", QLabel(color if color else "None"))
            else:
                layout.addRow("Location:", QLabel(path)); layout.addRow("Total Items:", QLabel(str(cnt or 0))); layout.addRow("Total Size:", QLabel(human_size(sz or 0)))
        else:
            r = cur.execute("SELECT name, size, extension, modified, real_path, sha256, custom_tags, secondary_name FROM virtual_fs WHERE id = ?", (db_id,)).fetchone()
            if not r: return
            n, s, e, m, fp, sha, tags, sec_n = r
            
            layout.addRow("File Name:", QLabel(n))
            layout.addRow("Secondary Name:", QLabel(sec_n if sec_n else "None"))
            layout.addRow("Virtual Path:", QLineEdit(path))
            
            txt_real = QLineEdit(fp if fp else "Disconnected")
            txt_real.setReadOnly(True)
            layout.addRow("Local Target:", txt_real)
            
            layout.addRow("Size:", QLabel(human_size(s)))
            layout.addRow("Extension:", QLabel(e))
            layout.addRow("Modified:", QLabel(m))
            layout.addRow("Custom Tags:", QLabel(tags if tags else "None"))
            layout.addRow("SHA-256:", QLineEdit(sha if sha else "Not Computed"))
            
        dlg.exec()

    def export_virtual_to_os(self):
        items_to_export = self._get_selected_items()
        if not items_to_export:
            model = self.file_table.model()
            for r in range(model.rowCount()): items_to_export.append(model.data(model.index(r, 0), Qt.UserRole))

        if not items_to_export: return QMessageBox.warning(self, "Export", "The current view is empty.")
            
        dest_dir = QFileDialog.getExistingDirectory(self, "Select OS Destination to Materialize")
        if not dest_dir: return
        
        self.export_dlg = QProgressDialog(f"Materializing {len(items_to_export)} selections to physical OS...", "Cancel", 0, len(items_to_export), self)
        self.export_dlg.setWindowModality(Qt.WindowModal); self.export_dlg.show()
        
        self.mat_thread = MaterializeThread(str(self.db.path), items_to_export, dest_dir, self)
        self.mat_thread.progress.connect(lambda c,t,m: (self.export_dlg.setValue(int((c/max(1,t))*100)), self.export_dlg.setLabelText(m)))
        self.export_dlg.canceled.connect(self.mat_thread.cancel)
        self.mat_thread.finished.connect(lambda p: (self.export_dlg.close(), QMessageBox.information(self, "Success", f"Structure materialized at:\n{p}"), self.sys_log("Materialized Virtual Structure to OS.")))
        self.mat_thread.error.connect(lambda e: (self.export_dlg.close(), QMessageBox.critical(self, "Error", f"Failed:\n{e}")))
        self._register_worker(self.mat_thread); self.mat_thread.start()

    def export_csv(self, sel_items):
        csv_path, _ = QFileDialog.getSaveFileName(self, "Export Rich CSV Manifest", "", "CSV Files (*.csv)")
        if not csv_path: return
        try:
            with sqlite3.connect(self.db.path) as conn:
                cur = conn.cursor()
                with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    # NEW: Comprehensive Audit Headers
                    writer.writerow(["Type", "File/Folder Name", "Virtual Path", "Physical OS Path", "Size (Bytes)", "Extension", "Modified Date", "SHA-256 Hash", "Custom Tags", "Color Tag", "Secondary Name"])
                    
                    for typ, path_val, db_id in sel_items:
                        if db_id == -1: continue
                        
                        if typ == "file":
                            cur.execute("SELECT name, parent_path, real_path, size, extension, modified, sha256, custom_tags, color_tag, secondary_name FROM virtual_fs WHERE id=?", (db_id,))
                            r = cur.fetchone()
                            if r: writer.writerow(["File", r[0], r[1] + r[0], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]])
                        else:
                            # Log the folder itself
                            cur.execute("SELECT name, parent_path, modified, custom_tags, color_tag, secondary_name FROM virtual_fs WHERE id=?", (db_id,))
                            r = cur.fetchone()
                            if r: writer.writerow(["Folder", r[0], r[1] + r[0] + "/", "N/A (Virtual)", 0, "", r[2], "", r[3], r[4], r[5]])
                            
                            # Expand and log ALL contents of the selected folder for a full audit
                            cur.execute("SELECT name, parent_path, real_path, size, extension, modified, sha256, custom_tags, color_tag, secondary_name FROM virtual_fs WHERE parent_path LIKE ? AND is_folder=0", (f"{path_val}%",))
                            for r in cur.fetchall():
                                writer.writerow(["File", r[0], r[1] + r[0], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]])
                                
            QMessageBox.information(self, "Success", "Rich Database Manifest exported successfully.")
            self.sys_log(f"Exported detailed CSV manifest to: {csv_path}")
        except Exception as e: QMessageBox.warning(self, "Error", str(e))

    def _get_selected_items(self):
        if self.view_stack.currentIndex() == 0:
            return [self.file_table.model().data(self.file_table.model().index(idx.row(), 0), Qt.UserRole) for idx in self.file_table.selectionModel().selectedRows() if self.file_table.model().data(self.file_table.model().index(idx.row(), 0), Qt.UserRole)]
        return [self.file_grid.model().data(idx, Qt.UserRole) for idx in self.file_grid.selectionModel().selectedIndexes() if self.file_grid.model().data(idx, Qt.UserRole)]

    def context_menu(self, pos, is_grid=False):
        menu = QMenu(self)

        # 1. Selection & General
        menu.addAction("☑ Select All (Ctrl+A)", self.cmd_select_all)
        
        # Determine state variables
        sel_items = [i for i in self._get_selected_items() if i[2] != -1]
        is_trash = self.current_prefix.startswith("trash://")
        
        if is_trash:
            menu.addSeparator()
            menu.addAction("🔥 Empty Trash", self.empty_trash)

        # 2. File/Folder Creation & Import (Only if not smart/trash)
        if not self._is_smart_path(self.current_prefix) and self.active_db_path == str(DB_FILE) and not is_trash:
            menu.addSeparator()
            create_menu = menu.addMenu("✨ Create / Import")
            create_menu.addAction("📂 Create Virtual Folder (Ctrl+Shift+N)", self.create_folder)
            create_menu.addAction("📥 Import Real Files", self.import_real_files)
            create_menu.addAction("📁 Import Real Folder", self.import_real_folder)

        # 3. Operations on Selected Items
        if sel_items:
            menu.addSeparator()
            
            # --- OPEN / VIEW ---
            if len(sel_items) == 1:
                open_menu = menu.addMenu("🚀 Open / Navigate")
                if sel_items[0][0] == "file":
                    open_menu.addAction("🚀 Open Native System App", lambda: self.open_local_file_system(sel_items[0][2]))
                    open_menu.addAction("📂 Show in OS Explorer", lambda: self.open_file_location(sel_items[0][2]))
                    open_menu.addAction("🎞 Open in vman Viewer (Ctrl+O)", self.open_selected_vman)
                open_menu.addAction("📋 Copy Virtual Path", lambda: QApplication.clipboard().setText(sel_items[0][1]))
            
            # --- CLIPBOARD ---
            menu.addAction("Copy (Ctrl+C)", self.cmd_copy)
            menu.addAction("Cut (Ctrl+X)", self.cmd_cut)
            
            # --- EDIT / MODIFY ---
            edit_menu = menu.addMenu("✏️ Edit & Modify")
            edit_menu.addAction("Rename (F2)", self.cmd_rename)
            edit_menu.addAction("🧹 Clean '[copy]' Prefix", lambda: self.cmd_remove_copy_prefix(sel_items))
            
            if len(sel_items) == 1:
                edit_menu.addAction("🏷 Set Secondary Name", self.cmd_set_secondary_name)
                res = self.db.conn.cursor().execute("SELECT is_hidden, is_favorite FROM virtual_fs WHERE id=?", (sel_items[0][2],)).fetchone()
                if res:
                    edit_menu.addAction("👁️ Unhide" if res[0] else "🙈 Hide", lambda: self.toggle_item_hidden(sel_items[0][2], not res[0]))
                    edit_menu.addAction("💔 Remove Favorite" if res[1] else "⭐ Add Favorite", lambda: self.toggle_item_fav(sel_items[0][2], not res[1]))
            
            # --- TAGS ---
            tag_menu = menu.addMenu("🏷 Tags & Labels")
            color_menu = tag_menu.addMenu("🎨 Set Color Tag")
            for color in ["None", "Red", "Green", "Blue", "Gold"]: 
                color_menu.addAction(color, lambda checked=False, c=color: self.bulk_tag_items(c, sel_items))
            tag_menu.addAction("📝 Bulk Add Custom Tags...", lambda: self.bulk_add_custom_tags(sel_items))

            # --- SYSTEM & MAPPING ---
            sys_menu = menu.addMenu("⚙️ System & Mapping")
            if len(sel_items) == 1:
                if sel_items[0][0] == "folder":
                    sys_menu.addAction("🧬 Compute SHA-256 for all Contents", lambda: self.bulk_compute_hash(sel_items[0][1]))
                    sys_menu.addAction("🔗 Map THIS Folder to Physical OS", lambda: self.cmd_map_folder(sel_items[0]))
                sys_menu.addAction("🗺️ Map Parent Drive/Mount (Auto-Detect)", lambda: self.cmd_map_parent_drive(sel_items[0]))

            # --- EXPORT ---
            export_menu = menu.addMenu("📤 Export & Extract")
            export_menu.addAction(f"💾 Materialize {len(sel_items)} Items to OS", lambda: self.materialize_to_os(sel_items))
            export_menu.addAction(f"📤 Export {len(sel_items)} Items to OS Location...", self.export_virtual_to_os)
            export_menu.addAction("📦 Export Selected to ZIP...", lambda: self.export_to_zip(sel_items))
            export_menu.addAction("📊 Export View to CSV", lambda: self.export_csv(sel_items))
            
            # --- PROPERTIES ---
            if len(sel_items) == 1:
                menu.addAction("ℹ️ Properties", lambda: self.show_properties(sel_items[0][0], sel_items[0][1], sel_items[0][2]))
            else:
                menu.addAction("ℹ️ Multi-Item Properties", lambda: self.show_multi_properties(sel_items))

            menu.addSeparator()

            # --- DELETE & TRASH ---
            del_menu = menu.addMenu("🗑️ Delete Options")
            if is_trash:
                del_menu.addAction("♻️ Restore from Trash", self.restore_from_trash)
                del_menu.addAction("🧨 Permanent Delete (Shift+Del)", self.cmd_delete_permanent)
            else:
                del_menu.addAction("🗑️ Move to Trash (Delete)", self.cmd_delete)
                del_menu.addAction("🧨 Permanent Delete (Shift+Del)", self.cmd_delete_permanent)
            
            del_menu.addSeparator()
            del_menu.addAction("💀 Delete PHYSICAL OS Items", lambda: self.cmd_delete_physical(sel_items))

        # 4. View / Global Actions (If no selection)
        menu.addSeparator()
        if not sel_items: 
            menu.addAction("📤 Materialize Entire View to OS", self.export_virtual_to_os)
        
        menu.addAction("⚙️ Compile View to Isolated DB", self.compile_current_view)

        # 5. Paste Action (Always available if conditions met)
        act_paste = QAction("📋 Paste (Ctrl+V)", self)
        act_paste.triggered.connect(self.cmd_paste)
        act_paste.setEnabled(bool(self.v_clipboard["items"]) and not self._is_smart_path(self.current_prefix))
        menu.addAction(act_paste)

        menu.exec(self.file_grid.viewport().mapToGlobal(pos) if is_grid else self.file_table.viewport().mapToGlobal(pos))

    def toggle_item_hidden(self, db_id, hide: bool): 
        with sqlite3.connect(self.db.path) as conn:
            conn.cursor().execute("UPDATE virtual_fs SET is_hidden = ? WHERE id = ?", (1 if hide else 0, db_id))
            conn.commit()
        self.clear_cache(); self.load_directory(self.current_prefix); self.sys_log(f"Item visibility toggled for DB_ID {db_id}")
    def toggle_item_fav(self, db_id, fav: bool): 
        with sqlite3.connect(self.db.path) as conn:
            conn.cursor().execute("UPDATE virtual_fs SET is_favorite = ? WHERE id = ?", (1 if fav else 0, db_id))
            conn.commit()
        self.clear_cache(); self.load_directory(self.current_prefix)

    def bulk_tag_items(self, color, sel_items):
        with sqlite3.connect(self.db.path) as conn:
            for typ, path, db_id in sel_items: 
                conn.cursor().execute("UPDATE virtual_fs SET color_tag = ? WHERE id = ?", ("" if color=="None" else color, db_id))
            conn.commit()
        self.clear_cache(); self.load_directory(self.current_prefix); self.sys_log(f"Applied Color Tag: {color} to {len(sel_items)} items.")

    def bulk_add_custom_tags(self, sel_items):
        tags, ok = QInputDialog.getText(self, "Bulk Apply Tags", "Enter tags separated by comma (e.g. urgent, work, vacation):")
        if not ok or not tags.strip(): return
        with sqlite3.connect(self.db.path) as conn:
            cur = conn.cursor()
            for typ, path, db_id in sel_items:
                old_tags = cur.execute("SELECT custom_tags FROM virtual_fs WHERE id=?", (db_id,)).fetchone()[0]
                new_val = f"{old_tags}, {tags.strip()}".strip(", ") if old_tags else tags.strip()
                cur.execute("UPDATE virtual_fs SET custom_tags = ? WHERE id = ?", (new_val, db_id))
            conn.commit()
        self.clear_cache(); self.load_directory(self.current_prefix); self.sys_log(f"Bulk applied custom tags '{tags}' to {len(sel_items)} items.")

    def cmd_copy(self):
        items = self._get_selected_items()
        if items: self.v_clipboard = {"action": "copy", "items": items}; self.status.showMessage(f"Copied {len(items)} items virtually.", 3000)
    def cmd_cut(self):
        items = self._get_selected_items()
        if items: self.v_clipboard = {"action": "cut", "items": items}; self.status.showMessage(f"Cut {len(items)} items virtually.", 3000)
    def cmd_paste(self):
        if not self.v_clipboard["items"]: return
        if self._is_smart_path(self.current_prefix): return QMessageBox.warning(self, "Error", "Cannot paste into dynamic Smart Views.")
        self._current_drag_items = self.v_clipboard["items"]
        is_copy = (self.v_clipboard["action"] == "copy"); self.execute_internal_drop(self.current_prefix, is_copy)
        if not is_copy: self.v_clipboard = {"action": None, "items": []} 

    def cmd_delete_permanent(self):
        self.cmd_delete(force_permanent=True)

    def cmd_delete(self, force_permanent=False):
        items = self._get_selected_items()
        if not items: return
        clean_items = [i for i in items if i[2] != -1] 
        if not clean_items: return
        
        # Check for marked safe files
        safe_files_exist = False
        with sqlite3.connect(self.db.path) as conn:
            cur = conn.cursor()
            for typ, path, db_id in clean_items:
                res = cur.execute("SELECT hash_verified FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
                if res and res[0] == 1:
                    safe_files_exist = True
                    break
                    
        if safe_files_exist:
            QMessageBox.warning(self, "Protected Files", "One or more selected items are marked as 'Safe' and cannot be deleted.\n\nPlease unmark them in the Space Analyzer first.")
            return

        # Now respects Shift+Delete bypassing the Trash
        is_permanent = force_permanent or self.current_prefix.startswith("trash://") or self.active_db_path != str(DB_FILE)
        
        msg = "Permanently delete from VMan? (This cannot be undone!)" if is_permanent else "Move selected items to Virtual Trash?"
        if QMessageBox.question(self, "Delete", msg, QMessageBox.Yes|QMessageBox.No) != QMessageBox.Yes: return
        
        with sqlite3.connect(self.db.path) as conn:
            cur = conn.cursor()
            prog = QProgressDialog(f"Deleting {len(clean_items)} items...", "Cancel", 0, len(clean_items), self); prog.setWindowModality(Qt.WindowModal); prog.show()
            for i, (typ, path, db_id) in enumerate(clean_items):
                if prog.wasCanceled(): break
                if is_permanent:
                    if typ == "file": cur.execute("DELETE FROM virtual_fs WHERE id = ?", (db_id,))
                    else: cur.execute("DELETE FROM virtual_fs WHERE parent_path LIKE ? OR id = ?", (f"{path}%", db_id))
                else:
                    if typ == "file": cur.execute("UPDATE virtual_fs SET in_trash = 1 WHERE id = ?", (db_id,))
                    else: cur.execute("UPDATE virtual_fs SET in_trash = 1 WHERE parent_path LIKE ? OR id = ?", (f"{path}%", db_id))
                prog.setValue(i+1)
            conn.commit()
            
        self.clear_cache(); self.refresh_all(); self.sys_log(f"{'Permanently deleted' if is_permanent else 'Trashed'} {len(clean_items)} items.")

    def empty_trash(self):
        if QMessageBox.question(self, "Empty Trash", "Are you sure you want to permanently delete ALL items in the Virtual Trash?\n\nThis cannot be undone.", QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            with sqlite3.connect(self.db.path) as conn:
                conn.cursor().execute("DELETE FROM virtual_fs WHERE in_trash=1")
                conn.commit()
            self.clear_cache()
            self.refresh_all()
            self.sys_log("Virtual Trash emptied.")
            QMessageBox.information(self, "Trash Emptied", "All items in the Virtual Trash have been permanently deleted.")

    def cmd_rename(self):
        items = self._get_selected_items()
        if not items: return
        
        with sqlite3.connect(self.db.path) as conn:
            cur = conn.cursor()
            if len(items) == 1 and items[0][2] != -1: 
                typ, path, db_id = items[0]
                old_name = cur.execute("SELECT name FROM virtual_fs WHERE id = ?", (db_id,)).fetchone()[0]
                if typ == "file":
                    base, ext = os.path.splitext(old_name)
                    new_base, ok = QInputDialog.getText(self, "Rename File", "New Name:", QLineEdit.Normal, base)
                    if not ok or not new_base.strip() or new_base.strip() == base: return
                    new_name = new_base.strip() + ext
                    cur.execute("UPDATE virtual_fs SET name = ? WHERE id = ?", (new_name, db_id))
                else:
                    new_name, ok = QInputDialog.getText(self, "Rename Folder", "New Name:", QLineEdit.Normal, old_name)
                    if not ok or not new_name.strip() or new_name.strip() == old_name: return
                    cur.execute("UPDATE virtual_fs SET name = ? WHERE id = ?", (new_name.strip(), db_id))
                    cur.execute("UPDATE virtual_fs SET parent_path = ? || SUBSTR(parent_path, LENGTH(?) + 1) WHERE parent_path LIKE ?", (f"{self.current_prefix}{new_name.strip()}/", path, f"{path}%"))
                conn.commit(); self.clear_cache(); self.refresh_tree(); self.load_directory(self.current_prefix); self.sys_log(f"Renamed item to '{new_name}'")
            elif len(items) > 1:
                base_name, ok = QInputDialog.getText(self, "Bulk Rename", f"Enter base name to serialize {len(items)} items:")
                if ok and base_name.strip():
                    prog = QProgressDialog(f"Bulk Renaming {len(items)} items...", "Cancel", 0, len(items), self); prog.setWindowModality(Qt.WindowModal); prog.show()
                    for i, (typ, path, db_id) in enumerate(items):
                        if prog.wasCanceled(): break
                        if db_id == -1: continue
                        if typ == "file":
                            ext = cur.execute("SELECT extension FROM virtual_fs WHERE id=?", (db_id,)).fetchone()[0]
                            cur.execute("UPDATE virtual_fs SET name=? WHERE id=?", (f"{base_name.strip()} ({i+1}){ext}", db_id))
                        else:
                            new_name = f"{base_name.strip()} ({i+1})"
                            cur.execute("UPDATE virtual_fs SET name=? WHERE id=?", (new_name, db_id))
                            cur.execute("UPDATE virtual_fs SET parent_path = ? || SUBSTR(parent_path, LENGTH(?) + 1) WHERE parent_path LIKE ?", (f"{self.current_prefix}{new_name}/", path, f"{path}%"))
                        prog.setValue(i+1)
                    conn.commit(); self.clear_cache(); self.refresh_tree(); self.load_directory(self.current_prefix); self.sys_log(f"Bulk Renamed {len(items)} items to base '{base_name.strip()}'")

    def cmd_remove_copy_prefix(self, items):
        if not items: return
        renamed_count = 0
        
        with sqlite3.connect(self.db.path) as conn:
            cur = conn.cursor()
            for typ, path, db_id in items:
                if db_id == -1: continue
                
                old_name = cur.execute("SELECT name FROM virtual_fs WHERE id = ?", (db_id,)).fetchone()[0]
                new_name = old_name
                
                # Strip all [copy] prefixes 
                while new_name.startswith("[copy]"):
                    new_name = new_name[6:]
                    
                # Ensure the new name isn't completely empty, then apply
                if new_name != old_name and new_name.strip():
                    cur.execute("UPDATE virtual_fs SET name = ? WHERE id = ?", (new_name, db_id))
                    
                    # If it's a folder, we must update all sub-paths so children don't break
                    if typ == "folder":
                        cur.execute("UPDATE virtual_fs SET parent_path = ? || SUBSTR(parent_path, LENGTH(?) + 1) WHERE parent_path LIKE ?", (f"{self.current_prefix}{new_name}/", path, f"{path}%"))
                        
                    renamed_count += 1
            conn.commit()
            
        if renamed_count > 0:
            self.clear_cache()
            self.refresh_tree()
            self.load_directory(self.current_prefix)
            self.sys_log(f"Removed '[copy]' prefix from {renamed_count} items.")
            self.status.showMessage(f"Cleaned names of {renamed_count} items.", 3000)
        else:
            self.status.showMessage("No '[copy]' prefixes found on selected items.", 3000)

    def cmd_set_secondary_name(self):
        items = self._get_selected_items()
        if len(items) == 1 and items[0][2] != -1:
            db_id = items[0][2]
            with sqlite3.connect(self.db.path) as conn:
                old_sec = conn.cursor().execute("SELECT secondary_name FROM virtual_fs WHERE id = ?", (db_id,)).fetchone()[0]
                new_sec, ok = QInputDialog.getText(self, "Secondary Name", "Enter Secondary Name / Description:", QLineEdit.Normal, str(old_sec))
                if ok: 
                    conn.cursor().execute("UPDATE virtual_fs SET secondary_name = ? WHERE id = ?", (new_sec.strip(), db_id)); conn.commit(); self.clear_cache(); self.load_directory(self.current_prefix)

    def execute_internal_drop(self, dest_path, is_copy):
        if not self._current_drag_items or dest_path.startswith("trash://") or self._is_smart_path(dest_path) or self.active_db_path != str(DB_FILE): return
        
        # HELPER TO MANAGE [copy] PREFIX
        def get_copy_name(name):
            clean_name = name
            while clean_name.startswith("[copy]"):
                clean_name = clean_name[6:]
            return f"[copy]{clean_name}"

        with sqlite3.connect(self.db.path) as conn:
            cur = conn.cursor()
            for typ, path, db_id in self._current_drag_items:
                if db_id == -1: continue 
                if typ == "file":
                    if is_copy:
                        row = cur.execute("SELECT name, is_folder, real_path, size, extension, modified, color_tag, is_hidden, category, year, month, custom_tags FROM virtual_fs WHERE id = ?", (db_id,)).fetchone()
                        if row: 
                            new_name = get_copy_name(row[0])
                            cur.execute("INSERT INTO virtual_fs (parent_path, name, is_folder, real_path, size, extension, modified, color_tag, is_hidden, category, year, month, custom_tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (dest_path, new_name, *row[1:]))
                    else: cur.execute("UPDATE virtual_fs SET parent_path = ? WHERE id = ?", (dest_path, db_id))
                else:
                    row = cur.execute("SELECT name FROM virtual_fs WHERE id = ?", (db_id,)).fetchone()
                    if not row: continue
                    if is_copy:
                        new_base_name = get_copy_name(row[0])
                        cur.execute("INSERT INTO virtual_fs (parent_path, name, is_folder) VALUES (?, ?, 1)", (dest_path, new_base_name))
                        for r in cur.execute("SELECT name, is_folder, real_path, size, extension, modified, color_tag, is_hidden, parent_path, category, year, month, custom_tags FROM virtual_fs WHERE parent_path LIKE ?", (f"{path}%",)).fetchall(): 
                            cur.execute("INSERT INTO virtual_fs (parent_path, name, is_folder, real_path, size, extension, modified, color_tag, is_hidden, category, year, month, custom_tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (f"{dest_path}{new_base_name}/" + r[8][len(path):], *r[:8], r[9], r[10], r[11], r[12]))
                    else:
                        cur.execute("UPDATE virtual_fs SET parent_path = ? WHERE id = ?", (dest_path, db_id))
                        cur.execute("UPDATE virtual_fs SET parent_path = ? || SUBSTR(parent_path, LENGTH(?) + 1) WHERE parent_path LIKE ?", (f"{dest_path}{row[0]}/", path, f"{path}%"))
            conn.commit()
        self._current_drag_items = []; self.clear_cache(); self.refresh_all(); self.sys_log(f"Internal Drag/Drop executed to '{dest_path}'")

    def create_folder(self):
        if self.current_prefix.startswith("trash://") or self.current_prefix.startswith("fav://") or self._is_smart_path(self.current_prefix): return
        name, ok = QInputDialog.getText(self, "New Virtual Folder", "Folder Name:")
        if ok and name.strip():
            with sqlite3.connect(self.db.path) as conn:
                if not conn.cursor().execute("SELECT id FROM virtual_fs WHERE parent_path=? AND name=? AND is_folder=1", (self.current_prefix, name.strip())).fetchone():
                    conn.cursor().execute("INSERT INTO virtual_fs (parent_path, name, is_folder, modified) VALUES (?, ?, 1, ?)", (self.current_prefix, name.strip(), now_ts())); conn.commit(); self.clear_cache(); self.refresh_tree(); self.load_directory(self.current_prefix)

    def create_virtual_file(self):
        if self.current_prefix.startswith("trash://") or self.current_prefix.startswith("fav://") or self._is_smart_path(self.current_prefix): return
        name, ok = QInputDialog.getText(self, "New Virtual File", "File Name:")
        if ok and name.strip():
            with sqlite3.connect(self.db.path) as conn:
                conn.cursor().execute("INSERT INTO virtual_fs (parent_path, name, is_folder, real_path, size, extension, modified, creation_date) VALUES (?, ?, 0, '', 0, ?, ?, ?)", (self.current_prefix, name.strip(), os.path.splitext(name.strip())[1].lower(), now_ts(), now_ts())); conn.commit(); self.clear_cache(); self.load_directory(self.current_prefix)
       

    def import_real_files(self):
        if not self.current_prefix.startswith("trash://") and not self.current_prefix.startswith("fav://") and not self._is_smart_path(self.current_prefix):
            files, _ = QFileDialog.getOpenFileNames(self, "Import Real Files")
            if files: self.on_files_dropped(files)

    def import_real_folder(self):
        if not self.current_prefix.startswith("trash://") and not self.current_prefix.startswith("fav://") and not self._is_smart_path(self.current_prefix):
            folder = QFileDialog.getExistingDirectory(self, "Import Real Folder")
            if folder: self.on_files_dropped([folder])

    def on_files_dropped(self, paths):
        if self._is_smart_path(self.current_prefix) or self.current_prefix.startswith("trash://"): return QMessageBox.warning(self, "Error", "Cannot import directly into Smart Views.")
        self.import_dlg = QProgressDialog("Importing files into Virtual Sandbox...", "Cancel", 0, 100, self); self.import_dlg.setWindowModality(Qt.WindowModal); self.import_dlg.show()
        self.import_thread = ImportFilesThread(str(self.db.path), self.current_prefix, paths, self)
        self.import_thread.progress.connect(lambda c,t,m: (self.import_dlg.setMaximum(t), self.import_dlg.setValue(c), self.import_dlg.setLabelText(m)))
        self.import_dlg.canceled.connect(self.import_thread.cancel)
        def on_import_finished(f_cnt, d_cnt):
            self.import_dlg.close(); self.clear_cache(); self.refresh_all(); self.status.showMessage(f"Imported {f_cnt} files and {d_cnt} folders.", 5000); self.sys_log(f"Successfully Sandbox Imported {f_cnt} files.")
        self.import_thread.finished_import.connect(on_import_finished)
        self.import_thread.error.connect(lambda e: (self.import_dlg.close(), QMessageBox.critical(self, "Import Error", e)))
        self._register_worker(self.import_thread); self.import_thread.start()

    def compile_current_view(self):
        name, ok = QInputDialog.getText(self, "Compile DB View", "Enter name for new separate database (e.g., 'Project_Backup'):")
        if not ok or not name.strip(): return
        target_path = VIEWS_DIR / f"{name.strip().replace(' ', '_')}.db"
        query = ""; params = ()
        matched_proto = next((p for p in SMART_PROTOCOLS if self.current_prefix.startswith(p)), None)
        
        if self.current_prefix.startswith("y_m_f://"):
            parts = [p for p in self.current_prefix.replace("y_m_f://", "").split("/") if p]
            if len(parts) >= 3:
                with sqlite3.connect(self.db.path) as conn:
                    cur = conn.cursor()
                    folder_age, temp_tracker = {}, {}
                    for pp, y, m, c in cur.execute("SELECT parent_path, year, month, COUNT(id) FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND year!='' AND month!='' GROUP BY parent_path, year, month").fetchall():
                        if pp not in temp_tracker or c > temp_tracker[pp]: temp_tracker[pp], folder_age[pp] = c, (y, m)
                    matched_pp = next((pp for pp, age in folder_age.items() if age == (parts[0], parts[1]) and (pp.strip("/").split("/")[-1] if pp.strip("/") else "Root_Files") == parts[2]), None)
                    if matched_pp: query = "SELECT * FROM virtual_fs WHERE parent_path LIKE ? AND is_folder=0 AND in_trash=0"; params = (f"{matched_pp}%",)
                    else: return QMessageBox.warning(self, "Compile Error", "Folder matching failed.")
            else: return QMessageBox.warning(self, "Compile Error", "You must navigate deeply into a folder to compile it from the Year/Month view.")
        elif matched_proto:
            cols = SMART_PROTOCOLS[matched_proto]
            parts = [p for p in self.current_prefix.replace(matched_proto, "").split("/") if p]
            where = ["is_folder=0", "in_trash=0"] + [f"{cols[i]}=?" for i in range(len(parts))]
            query = f"SELECT * FROM virtual_fs WHERE {' AND '.join(where)}"; params = tuple(parts)
        else:
            query = "SELECT * FROM virtual_fs WHERE parent_path LIKE ? AND in_trash=0"; params = (f"{self.current_prefix}%",)

        self.compile_dlg = QProgressDialog("Compiling standalone database...", "Cancel", 0, 100, self); self.compile_dlg.setWindowModality(Qt.WindowModal); self.compile_dlg.show()
        self.compiler = CompilerThread(str(self.db.path), str(target_path), self.current_prefix, query, params, self)
        self.compiler.progress.connect(lambda c, t, msg: (self.compile_dlg.setValue(c), self.compile_dlg.setLabelText(msg)))
        self.compile_dlg.canceled.connect(self.compiler.cancel)
        def on_compile_finished(db_res):
            self.compile_dlg.close(); QMessageBox.information(self, "Success", f"DB compiled to:\n{db_res}"); self.refresh_tree(); self.sys_log(f"Compiled Isolated DB: {db_res}")
        self.compiler.finished.connect(on_compile_finished)
        self.compiler.error.connect(lambda e: (self.compile_dlg.close(), QMessageBox.critical(self, "Compile Error", e)))
        self._register_worker(self.compiler); self.compiler.start()

    def update_statistics(self):        
        stats = self.db.get_stats(self.current_prefix if not self._is_smart_path(self.current_prefix) else "")
        usage_pct = (stats['used_bytes'] / self.max_virtual_storage) * 100 if self.max_virtual_storage else 0
        html = f"<h3 style='color:#58a6ff;'>System Analytics ({Path(self.active_db_path).name})</h3><hr><b>Total Virtual Files:</b> {stats['files']}<br><b>Total Virtual Folders:</b> {stats['folders']}<br><b>Simulated Storage Used:</b> {human_size(stats['used_bytes'])}<br><b>Average File Size:</b> {human_size(stats['avg_bytes'])}<br><b>System Allocation:</b> {usage_pct:.4f}%<br><hr><b>Oldest Mod Date:</b> {stats['oldest']}<br><b>Newest Mod Date:</b> {stats['newest']}<br><hr><h4 style='color:#58a6ff;'>Top Largest Managed Files:</h4><ul style='list-style-type: square; margin-left: -20px;'>"
        for f in stats['top_files'][:5]: html += f"<li>{f[0]} <span style='color:#8b949e;'>({human_size(f[1])})</span></li>"
        html += "</ul>"
        self.lbl_stats_txt.setHtml(html)
        
        if not self.figure: return
        self.figure.clear(); mode = self.stat_combo.currentText(); ax = self.figure.add_subplot(111)
        bg_c, txt_c = ('#0d1117', 'white') if self.is_dark_mode else ('#ffffff', 'black')
        self.figure.patch.set_facecolor(bg_c); ax.set_facecolor(bg_c); ax.tick_params(colors=txt_c)
        
        if "Line Chart" in mode or "Over Time" in mode:
            data = stats["time_series"]
            if not data: ax.text(0.5, 0.5, "No temporal data available", color=txt_c, ha='center')
            else:
                dates = [datetime.strptime(d[0], "%Y-%m") for d in data if len(d[0]) == 7]
                if "Count" in mode: vals = [d[1] for d in data if len(d[0]) == 7]; ylabel = "Total Files Modified"; color = '#00e5ff'
                else: vals = [d[2] / (1024*1024) for d in data if len(d[0]) == 7]; ylabel = "Storage Size (MB)"; color = '#d7ba7d'
                if dates and vals:
                    ax.plot(dates, vals, marker='o', linestyle='-', color=color, linewidth=2); ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y')); self.figure.autofmt_xdate(); ax.set_ylabel(ylabel, color=txt_c); ax.set_title(mode, color=txt_c); ax.grid(True, color='#30363d', linestyle='--')
        elif "Distribution by Extension" in mode:
            data = stats["distribution"]
            if not data: ax.text(0.5, 0.5, "No distribution data available", color=txt_c, ha='center')
            else:
                clean_data = [(d[0] if d[0] else 'none', d[1], d[2]) for d in data]
                if "Size" in mode: 
                    sorted_d = sorted(clean_data, key=lambda x: x[2], reverse=True)[:15]
                    ax.bar([x[0] for x in sorted_d], [x[2] / (1024*1024) for x in sorted_d], color='#1e7145'); ax.set_ylabel("Storage Size (MB)", color=txt_c)
                else: 
                    sorted_d = sorted(clean_data, key=lambda x: x[1], reverse=True)[:15]
                    ax.bar([x[0] for x in sorted_d], [x[1] for x in sorted_d], color='#58a6ff'); ax.set_ylabel("Total File Count", color=txt_c)
                ax.set_title(mode, color=txt_c); ax.tick_params(axis='x', rotation=45); ax.grid(axis='y', color='#30363d', linestyle='--')
        elif "Largest Files" in mode:
            data = stats["top_files"]
            if not data: ax.text(0.5, 0.5, "No files available", color=txt_c, ha='center')
            else:
                names = [d[0][:15] + ".." if len(d[0])>15 else d[0] for d in data]; sizes = [d[1] / (1024*1024) for d in data]
                ax.barh(names, sizes, color='#a371f7'); ax.set_xlabel("Size (MB)", color=txt_c); ax.set_title(mode, color=txt_c); ax.invert_yaxis(); ax.grid(axis='x', color='#30363d', linestyle='--')
        
        elif "Ratio" in mode:
            labels = ['Used Storage', 'Free Space']; sizes = [stats['used_bytes'], max(0, self.max_virtual_storage - stats['used_bytes'])]; colors = ['#d7ba7d', '#2ea043']         
            ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, colors=colors, textprops={'color': txt_c}); ax.set_title(mode, color=txt_c)

        self.figure.tight_layout(); self.canvas.draw()

    def export_to_zip(self, items_to_export):
        zip_path, _ = QFileDialog.getSaveFileName(self, "Compile to ZIP", "", "ZIP Files (*.zip)")
        if zip_path:
            self.export_dlg = QProgressDialog("Compiling ZIP...", "Cancel", 0, 100, self); self.export_dlg.setWindowModality(Qt.WindowModal); self.export_dlg.show()
            self.zip_thread = ExportZipThread(str(self.db.path), items_to_export, zip_path, self)
            self.zip_thread.progress.connect(lambda c,t,m: self.export_dlg.setValue(int((c/max(1,t))*100)) if self.export_dlg else None)
            self.export_dlg.canceled.connect(self.zip_thread.cancel)
            self.zip_thread.finished.connect(lambda p: (self.export_dlg.close() if self.export_dlg else None, QMessageBox.information(self, "Success", f"ZIP created:\n{p}"), self.sys_log(f"Exported View to Zip: {Path(p).name}")))
            self.zip_thread.error.connect(lambda e: (self.export_dlg.close() if self.export_dlg else None, QMessageBox.critical(self, "Error", f"Failed:\n{e}")))
            self._register_worker(self.zip_thread); self.zip_thread.start()

    def _register_worker(self, worker: QThread): 
        self._workers.append(worker); worker.finished.connect(lambda: self._cleanup_worker(worker))
        
    def _cleanup_worker(self, worker: QThread):
        try: self._workers.remove(worker); worker.deleteLater()
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
            self.loader_thread.cancel(); self.loader_thread.quit(); self.loader_thread.wait()
        self.db.close()
        super().closeEvent(ev)
        
    def open_tag_library(self):
        if not hasattr(self, 'tag_library_instance') or self.tag_library_instance is None:
            self.tag_library_instance = vmanTagLibraryDialog(self.active_db_path, self)
        else:
            if self.tag_library_instance.db_path != self.active_db_path:
                self.tag_library_instance.db_path = self.active_db_path
                self.tag_library_instance.refresh_memory_cache()
                
        self.tag_library_instance.show()
        self.tag_library_instance.raise_()
        self.tag_library_instance.activateWindow()        

    def on_tree_context_menu(self, pos):
        item = self.folder_tree.itemAt(pos)
        if not item: return
        path = item.data(0, Qt.UserRole)
        
        if path and path.startswith("db://") and path != "db://main":
            db_name = path.replace("db://", "")
            db_file = VIEWS_DIR / db_name
            
            menu = QMenu(self)
            act_rename = menu.addAction("✏️ Rename Database")
            act_delete = menu.addAction("🗑️ Delete Database")
            action = menu.exec(self.folder_tree.viewport().mapToGlobal(pos))
            
            if action == act_rename:
                new_name, ok = QInputDialog.getText(self, "Rename Database", "New name (without .db):", QLineEdit.Normal, db_file.stem)
                if ok and new_name.strip():
                    new_file = VIEWS_DIR / f"{new_name.strip().replace(' ', '_')}.db"
                    if new_file.exists():
                        QMessageBox.warning(self, "Error", "A database with this name already exists.")
                        return
                    
                    try:
                        if self.active_db_path == str(db_file):
                            self.db.close()
                            db_file.rename(new_file)
                            self.active_db_path = str(new_file)
                            self.db = vmanDB(Path(self.active_db_path))
                            self.status.showMessage(f"Renamed active database to {new_file.name}")
                        else:
                            db_file.rename(new_file)
                            
                        self.refresh_tree()
                        self.sys_log(f"Renamed database '{db_name}' to '{new_file.name}'")
                    except Exception as e:
                        QMessageBox.critical(self, "Error", f"Failed to rename: {e}")
                        
            elif action == act_delete:
                if QMessageBox.question(self, "Delete Database", f"Are you sure you want to permanently delete '{db_name}'?\nThis cannot be undone.", QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
                    try:
                        if self.active_db_path == str(db_file):
                            self.db.close()
                            self.active_db_path = str(DB_FILE)
                            self.db = vmanDB(Path(self.active_db_path))
                            self.status.showMessage("Reconnected to Main System DB. Active isolated DB was deleted.")
                            self.nav_to_path("/")
                            
                        if db_file.exists():
                            os.remove(db_file)
                            
                        self.refresh_tree()
                        self.sys_log(f"Deleted database '{db_name}'")
                    except Exception as e:
                        QMessageBox.critical(self, "Error", f"Failed to delete: {e}")

    def toggle_hidden_files(self):
        self.show_hidden = not self.show_hidden
        self.status.showMessage(f"Hidden items are now {'VISIBLE' if self.show_hidden else 'HIDDEN'}.", 3000)
        self.clear_cache()
        self.load_directory(self.current_prefix)
        
    def bulk_compute_hash(self, folder_v_path):
        self.hash_dlg = QProgressDialog(f"Scanning & Hashing files in {folder_v_path}...", "Cancel", 0, 100, self)
        self.hash_dlg.setWindowModality(Qt.WindowModal); self.hash_dlg.show()
        
        self.bulk_hasher = BulkHashCalculator(self.active_db_path, folder_v_path, self)
        self.bulk_hasher.progress.connect(lambda c,t,m: (self.hash_dlg.setMaximum(t), self.hash_dlg.setValue(c), self.hash_dlg.setLabelText(m)))
        self.hash_dlg.canceled.connect(self.bulk_hasher.cancel)
        self.bulk_hasher.finished.connect(lambda count: (self.hash_dlg.close(), QMessageBox.information(self, "Complete", f"Successfully computed and stored SHA-256 hashes for {count} files.")))
        self._register_worker(self.bulk_hasher); self.bulk_hasher.start()        

    def set_storage_capacity(self):
        val, ok = QInputDialog.getDouble(self, "Virtual Capacity", "Enter maximum simulated storage in GB:", self.max_storage_gb, 1.0, 100000.0, 1)
        if ok:
            self.max_storage_gb = val
            self.max_virtual_storage = val * 1024 * 1024 * 1024
            self.settings.setValue("max_storage_gb", val)
            self.update_statistics() # Refresh charts instantly
            self.status.showMessage(f"Simulated storage limit permanently updated to {val} GB.", 4000)

    def restore_from_trash(self):
        items = self._get_selected_items()
        if not items: return
        clean_items = [i for i in items if i[2] != -1] 
        if not clean_items: return
        
        with sqlite3.connect(self.db.path) as conn:
            cur = conn.cursor()
            for typ, path, db_id in clean_items:
                if typ == "file": 
                    cur.execute("UPDATE virtual_fs SET in_trash = 0 WHERE id = ?", (db_id,))
                else: 
                    cur.execute("UPDATE virtual_fs SET in_trash = 0 WHERE parent_path LIKE ? OR id = ?", (f"{path}%", db_id))
            conn.commit()
            
        self.clear_cache()
        self.refresh_all()
        self.sys_log(f"Restored {len(clean_items)} items from Trash.")
        self.status.showMessage(f"Restored {len(clean_items)} items to their original locations.", 4000)


    def cmd_select_all(self):
        if self.view_stack.currentIndex() == 0:
            self.file_table.selectAll()
        else:
            self.file_grid.selectAll()
        self.status.showMessage("All items selected.", 2000)        

    def toggle_fullscreen(self):
            if self.isFullScreen():
                self.showNormal()
                self.status.showMessage("Exited Fullscreen", 2000)
            else:
                self.showFullScreen()
                self.status.showMessage("Entered Fullscreen (Press F11 to exit)", 3000)
                
    def materialize_to_os(self, items):
        if not items: return
        dest_dir = QFileDialog.getExistingDirectory(self, "Select Physical OS Destination")
        if not dest_dir: return

        self.mat_dlg = QProgressDialog("Analyzing structure for export...", "Cancel", 0, 100, self)
        self.mat_dlg.setWindowModality(Qt.WindowModal); self.mat_dlg.show()

        self.mat_thread = MaterializeThread(self.active_db_path, items, dest_dir, self)
        self.mat_thread.progress.connect(lambda v, t, m: (self.mat_dlg.setMaximum(t), self.mat_dlg.setValue(v), self.mat_dlg.setLabelText(m)))
        self.mat_dlg.canceled.connect(self.mat_thread.cancel)
        self.mat_thread.finished.connect(lambda count: (self.mat_dlg.close(), QMessageBox.information(self, "Materialize Complete", f"Successfully exported {count} files to the OS.")))
        self.mat_thread.error.connect(lambda err: (self.mat_dlg.close(), QMessageBox.warning(self, "Export Error", err)))
        self._register_worker(self.mat_thread); self.mat_thread.start()                
                
        
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName(APP_TITLE)
    win = vmanVirtualManager()
    win.show()
    sys.exit(app.exec())
