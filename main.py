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
    QScrollArea, QDialog, QGraphicsView, QGraphicsScene, QTextBrowser, QTabBar,
    QTableWidget, QTableWidgetItem, QCheckBox, QCalendarWidget, QSpinBox, QDoubleSpinBox, 
    QGridLayout, QFrame, QSplitter, QListWidget, QListWidgetItem, QGroupBox, QFormLayout, QProgressBar
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

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', str(s))]

import sys
import matplotlib
matplotlib.use('QtAgg')
# Safely prioritize fonts based on OS to prevent missing font errors in Linux terminal
if sys.platform == "win32":
    matplotlib.rcParams['font.family'] = ['Segoe UI', 'Nirmala UI', 'sans-serif']
elif sys.platform == "darwin":
    matplotlib.rcParams['font.family'] = ['San Francisco', 'Helvetica Neue', 'sans-serif']
else:
    matplotlib.rcParams['font.family'] = ['DejaVu Sans', 'Liberation Sans', 'sans-serif']
matplotlib.rcParams['axes.unicode_minus'] = False

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg
from matplotlib.figure import Figure


from PySide6.QtWidgets import QSplitter, QRadioButton, QColorDialog, QButtonGroup
from PySide6.QtCore import QTimer, Qt
import colorsys #colorsys and QColorDialog for HeatmapColorConfigDialog
import datetime as dt_lib # for render_heatmap
# QButtonGroup for class RowLimitDialog

import json

#--------my modules
from themes import THEMES
from help_dialog import VManHelpDialog
from media_viewer import vmanViewer, ImageLoader
from database import vmanDB, SMART_PROTOCOLS
from splash import PremiumSplash  
from console import VManConsole
from jobs import ExtraFeaturesDialog

# ---------------- Constants & Themes ----------------
APP_TITLE = "VMan"
DATA_DIR = Path("vman_data")
VIEWS_DIR = DATA_DIR / "compiled_views"
DB_FILE = DATA_DIR / "vman_vfs.db"
ICONS_DIR = Path("icons")                    # Where you put jpg.png, png.png, txt.png
THUMBS_DIR = DATA_DIR / "thumbnails"         # Where manual grid thumbnails get saved
MAX_VIRTUAL_STORAGE = 100 * 1024 * 1024 * 1024  
CHUNK_SIZE = 150


FILE_CATEGORIES = {
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

# ZERO-LAG O(1) LOOKUP ENGINE
EXT_TO_CAT_MAIN = {}
for cat, exts in FILE_CATEGORIES.items():
    for ext in exts:
        EXT_TO_CAT_MAIN[ext] = cat
        
GLOBAL_CAT_COLORS = {
    "Images": "#a371f7", "Videos": "#f85149", "Audio": "#ff7b72", "Design": "#ff9ade",
    "Documents": "#d2a8ff", "Data": "#58a6ff", "Code": "#79c0ff", "Configs": "#7ee787",
    "Python": "#e3b341", "Apps": "#ff9429", "System": "#8b949e", "Archives": "#e3b341",
    "Forensics": "#ff7b72", "Security": "#f0883e", "VMs": "#8b949e", "3D": "#a371f7",
    "GIS": "#3fb950", "Fonts": "#c9d1d9", "Chats": "#2ea043", "Temp": "#484f58", "Others": "#8b949e"
}        

def get_category_for_ext(ext):
    return EXT_TO_CAT_MAIN.get(str(ext).lower(), "Others")


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

def resolve_smart_folder(cur, path_val, sanitize_fn):
    """
    Recursively resolves Smart Views into actual physical files and their virtual destinations.
    Returns relative paths that perfectly mirror the visual UI structure.
    """
    results = []
    
    if path_val.startswith("y_m_f://"):
        parts = [p for p in path_val.replace("y_m_f://", "").split("/") if p]
        
        # 1. Identify which original physical/virtual folders match the criteria
        if len(parts) == 0:
            cur.execute("SELECT DISTINCT year, month, parent_path FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND year!='' AND month!=''")
            targets = cur.fetchall()
        elif len(parts) == 1:
            cur.execute("SELECT DISTINCT year, month, parent_path FROM virtual_fs WHERE year=? AND is_folder=0 AND in_trash=0 AND month!=''", (parts[0],))
            targets = cur.fetchall()
        elif len(parts) == 2:
            cur.execute("SELECT DISTINCT year, month, parent_path FROM virtual_fs WHERE year=? AND month=? AND is_folder=0 AND in_trash=0", (parts[0], parts[1]))
            targets = cur.fetchall()
        else:
            cur.execute("SELECT DISTINCT year, month, parent_path FROM virtual_fs WHERE year=? AND month=? AND is_folder=0 AND in_trash=0", (parts[0], parts[1]))
            target_folder = parts[2]
            # Filter targets to only include the specific folder the user clicked on
            targets = [r for r in cur.fetchall() if (r[2].strip('/').split('/')[-1] if r[2].strip('/') else "Root_Files") == target_folder]
            
        # 2. Grab ALL files inside those folders to perfectly mirror the UI!
        for y, m, pp in targets:
            folder = pp.strip("/").split("/")[-1] if pp.strip("/") else "Root_Files"
            # We fetch everything in the parent_path, guaranteeing we don't miss any files!
            cur.execute("SELECT name, id, real_path, size FROM virtual_fs WHERE parent_path=? AND is_folder=0 AND in_trash=0", (pp,))
            for n, f_id, rp, sz in cur.fetchall():
                dest = f"{sanitize_fn(y)}/{sanitize_fn(m)}/{sanitize_fn(folder)}/{sanitize_fn(n)}"
                results.append((dest, f_id, rp, sz))
                
    elif path_val.startswith("tags://"):
        parts = [p for p in path_val.replace("tags://", "").split("/") if p]
        if len(parts) == 0:
            cur.execute("SELECT custom_tags, name, id, real_path, size FROM virtual_fs WHERE custom_tags IS NOT NULL AND custom_tags != '' AND is_folder=0 AND in_trash=0")
            for tags, n, f_id, rp, sz in cur.fetchall():
                for t in [x.strip() for x in tags.split(',') if x.strip()]:
                    results.append((f"{sanitize_fn(t)}/{sanitize_fn(n)}", f_id, rp, sz))
        else:
            tag = parts[0]
            cur.execute("SELECT name, id, real_path, size FROM virtual_fs WHERE custom_tags LIKE ? AND is_folder=0 AND in_trash=0", (f"%{tag}%",))
            for n, f_id, rp, sz in cur.fetchall():
                results.append((f"{sanitize_fn(n)}", f_id, rp, sz))
                
    else:
        # Handles all other Smart Views (y_c_m_e://, etc.) dynamically
        matched_proto = next((p for p in SMART_PROTOCOLS if path_val.startswith(p)), None)
        if matched_proto:
            cols = SMART_PROTOCOLS[matched_proto]
            parts = [p for p in path_val.replace(matched_proto, "").split("/") if p]
            where = ["is_folder=0", "in_trash=0"]
            params = []
            
            for col in cols: where.append(f"{col}!='' AND {col} IS NOT NULL")
            for i, p in enumerate(parts):
                if i < len(cols):
                    where.append(f"{cols[i]}=?")
                    params.append(p)
            
            select_sql = ", ".join(cols[len(parts):]) + (", name, id, real_path, size" if len(parts) < len(cols) else "name, id, real_path, size")
            if len(parts) >= len(cols): select_sql = "name, id, real_path, size"
            
            cur.execute(f"SELECT {select_sql} FROM virtual_fs WHERE {' AND '.join(where)}", tuple(params))
            for row in cur.fetchall():
                f_id, rp, sz = row[-3], row[-2], row[-1]
                n = row[-4]
                if len(parts) < len(cols):
                    path_parts = [sanitize_fn(str(c)) for c in row[:-4]]
                    path_parts.append(sanitize_fn(n))
                    dest = "/".join(path_parts)
                else:
                    dest = sanitize_fn(n)
                results.append((dest, f_id, rp, sz))
                
    return results

# ---------------- Background Threads ----------------

class MaterializeThread(QThread):
    progress = Signal(int, int, str)
    finished = Signal(int)
    error = Signal(str)
    def __init__(self, db_path, items, dest_dir, parent=None):
        super().__init__(parent); self.db_path, self.items, self.dest_dir = db_path, items, dest_dir; self.is_cancelled = False
    def cancel(self): self.is_cancelled = True
    def sanitize_filename(self, name): return re.sub(r'[\\/*?:"<>|]', '_', str(name))

    def run(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor(); all_exports = []
                for typ, path_val, db_id in self.items:
                    if self.is_cancelled: return
                    if typ == "file" and db_id != -1:
                        res = cur.execute("SELECT name, real_path FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
                        if res: all_exports.append((self.sanitize_filename(res[0]), res[1]))
                    elif typ == "folder":
                        if "://" in path_val and not path_val.startswith("trash://") and not path_val.startswith("fav://"):
                            base_f = self.sanitize_filename(path_val.strip('/').split('/')[-1]) if path_val.strip('/').replace('://', '') else ""
                            smart_files = resolve_smart_folder(cur, path_val, self.sanitize_filename)
                            for rel_dest, f_id, rp, sz in smart_files:
                                dest = os.path.join(base_f, rel_dest) if base_f else rel_dest
                                all_exports.append((dest, rp))
                        elif path_val.startswith("/"):
                            base_f = self.sanitize_filename(path_val.strip('/').split('/')[-1]) if path_val.strip('/') else ""
                            for pp, n, rp in cur.execute("SELECT parent_path, name, real_path FROM virtual_fs WHERE parent_path LIKE ? AND is_folder=0 AND in_trash=0", (f"{path_val}%",)).fetchall():
                                rel_p = pp[len(path_val):].lstrip('/') 
                                safe_parts = [self.sanitize_filename(p) for p in rel_p.split('/') if p]
                                if base_f: safe_parts.insert(0, base_f)
                                dest = os.path.join(os.path.join(*safe_parts) if safe_parts else "", self.sanitize_filename(n))
                                all_exports.append((dest, rp))

            total = len(all_exports)
            if total == 0: return self.error.emit("View is empty. Compilation cancelled.")

            count = 0
            for i, (rel_dest, source_rp) in enumerate(all_exports):
                if self.is_cancelled: return
                safe_rel_dest = rel_dest.replace('\\', '/').strip('/')
                final_dest = os.path.join(self.dest_dir, os.path.normpath(safe_rel_dest))
                os.makedirs(os.path.dirname(final_dest), exist_ok=True)
                self.progress.emit(i+1, total, f"Materializing:\n{safe_rel_dest}")
                try:
                    if source_rp and str(source_rp).strip() not in ("None", "") and os.path.exists(str(source_rp)):
                        shutil.copy2(str(source_rp), final_dest); count += 1
                except Exception: pass
            self.finished.emit(count)
        except Exception as e: self.error.emit(str(e))

class DummyReplicaThread(QThread):
    progress = Signal(int, int, str)
    finished = Signal(int)
    error = Signal(str)
    def __init__(self, db_path, items, dest_dir, zero_byte_mode=False, parent=None):
        super().__init__(parent); self.db_path, self.items, self.dest_dir, self.zero_byte_mode = db_path, items, dest_dir, zero_byte_mode; self.is_cancelled = False
    def cancel(self): self.is_cancelled = True
    def sanitize_filename(self, name): return re.sub(r'[\\/*?:"<>|]', '_', str(name))
    def _parse_size(self, size_val):
        if not size_val: return 0
        if isinstance(size_val, (int, float)): return int(size_val)
        s = str(size_val).strip().upper().replace(',', '')
        try: return int(float(s))
        except ValueError: pass
        m = re.search(r'([\d\.]+)\s*([A-Z]+)', s)
        if m:
            val, unit = float(m.group(1)), m.group(2)
            if unit in ['B', 'BYTE', 'BYTES']: return int(val)
            elif unit in ['KB', 'K', 'KIB']: return int(val * 1024)
            elif unit in ['MB', 'M', 'MIB']: return int(val * 1024**2)
            elif unit in ['GB', 'G', 'GIB']: return int(val * 1024**3)
            return int(val)
        return 0

    def run(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor(); all_exports = []
                for typ, path_val, db_id in self.items:
                    if self.is_cancelled: return
                    if typ == "file" and db_id != -1:
                        res = cur.execute("SELECT name, size FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
                        if res: all_exports.append((self.sanitize_filename(res[0]), res[1]))
                    elif typ == "folder":
                        if "://" in path_val and not path_val.startswith("trash://") and not path_val.startswith("fav://"):
                            base_f = self.sanitize_filename(path_val.strip('/').split('/')[-1]) if path_val.strip('/').replace('://', '') else ""
                            smart_files = resolve_smart_folder(cur, path_val, self.sanitize_filename)
                            for rel_dest, f_id, rp, sz in smart_files:
                                dest = os.path.join(base_f, rel_dest) if base_f else rel_dest
                                all_exports.append((dest, sz))
                        elif path_val.startswith("/"):
                            base_f = self.sanitize_filename(path_val.strip('/').split('/')[-1]) if path_val.strip('/') else ""
                            for pp, n, sz in cur.execute("SELECT parent_path, name, size FROM virtual_fs WHERE parent_path LIKE ? AND is_folder=0 AND in_trash=0", (f"{path_val}%",)).fetchall():
                                rel_p = pp[len(path_val):].lstrip('/') 
                                safe_parts = [self.sanitize_filename(p) for p in rel_p.split('/') if p]
                                if base_f: safe_parts.insert(0, base_f)
                                dest = os.path.join(os.path.join(*safe_parts) if safe_parts else "", self.sanitize_filename(n))
                                all_exports.append((dest, sz))

            total = len(all_exports)
            if total == 0: return self.error.emit("No files found to replicate.")

            count = 0
            for i, (rel_dest, f_size) in enumerate(all_exports):
                if self.is_cancelled: return
                safe_rel_dest = rel_dest.replace('\\', '/').strip('/')
                final_dest = os.path.join(self.dest_dir, os.path.normpath(safe_rel_dest))
                os.makedirs(os.path.dirname(final_dest), exist_ok=True)
                self.progress.emit(i+1, total, f"Creating Replica:\n{safe_rel_dest}")
                try:
                    sz = self._parse_size(f_size)
                    with open(final_dest, "wb") as f: pass
                    if not self.zero_byte_mode and sz > 0:
                        if sys.platform == "win32": subprocess.run(["fsutil", "sparse", "setflag", final_dest], creationflags=subprocess.CREATE_NO_WINDOW)
                        with open(final_dest, "r+b") as f: f.truncate(sz)
                    count += 1
                except Exception: pass
            self.finished.emit(count)
        except Exception as e: self.error.emit(str(e))

class CompilerThread(QThread):
    progress = Signal(int, int, str)
    finished = Signal(str)
    error = Signal(str)
    def __init__(self, source_db, target_db, items, append_mode=False, parent=None):
        super().__init__(parent); self.source_db = source_db; self.target_db = target_db; self.items = items; self.append_mode = append_mode; self.is_cancelled = False
    def cancel(self): self.is_cancelled = True
    def sanitize_filename(self, name): return re.sub(r'[\\/*?:"<>|]', '_', str(name))

    def run(self):
        try:
            if self.is_cancelled: return
            self.progress.emit(0, 100, "Initializing transfer...")
            
            if not self.append_mode and os.path.exists(self.target_db): 
                os.remove(self.target_db)
            
            src_conn = sqlite3.connect(self.source_db); src_cur = src_conn.cursor()
            src_cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='virtual_fs'")
            schema = src_cur.fetchone()[0]
            
            tgt_conn = sqlite3.connect(self.target_db); tgt_cur = tgt_conn.cursor()
            if not self.append_mode or not os.path.exists(self.target_db) or os.path.getsize(self.target_db) == 0:
                tgt_cur.execute(schema)
            tgt_conn.commit()
            
            src_cur.execute("PRAGMA table_info(virtual_fs)")
            cols = [row[1] for row in src_cur.fetchall()]
            pp_idx = cols.index("parent_path")
            id_idx = cols.index("id") 
            
            self.progress.emit(10, 100, "Resolving folder structures...")
            
            all_exports = []
            for typ, path_val, db_id in self.items:
                if self.is_cancelled: return
                if typ == "file" and db_id != -1: all_exports.append(("/", db_id))
                elif typ == "folder":
                    if "://" in path_val and not path_val.startswith("trash://") and not path_val.startswith("fav://"):
                        base_f = self.sanitize_filename(path_val.strip('/').split('/')[-1]) if path_val.strip('/').replace('://', '') else ""
                        smart_files = resolve_smart_folder(src_cur, path_val, self.sanitize_filename)
                        for rel_dest, f_id, rp, sz in smart_files:
                            dest = "/" + (f"{base_f}/{rel_dest}" if base_f else rel_dest)
                            
                            # --- CRITICAL FIX: Clean Double Slashes ---
                            dest_folder = "/".join(dest.split("/")[:-1]) + "/"
                            while "//" in dest_folder: dest_folder = dest_folder.replace("//", "/")
                            if not dest_folder.startswith("/"): dest_folder = "/" + dest_folder
                            
                            all_exports.append((dest_folder, f_id))
                    elif path_val.startswith("/"):
                        base_f = self.sanitize_filename(path_val.strip('/').split('/')[-1]) if path_val.strip('/') else ""
                        for pp, f_id in src_cur.execute("SELECT parent_path, id FROM virtual_fs WHERE parent_path LIKE ? AND is_folder=0 AND in_trash=0", (f"{path_val}%",)).fetchall():
                            rel_p = pp[len(path_val):].lstrip('/') 
                            safe_parts = [self.sanitize_filename(p) for p in rel_p.split('/') if p]
                            if base_f: safe_parts.insert(0, base_f)
                            dest = "/" + "/".join(safe_parts) + "/" if safe_parts else "/"
                            while "//" in dest: dest = dest.replace("//", "/")
                            all_exports.append((dest, f_id))

            total = len(all_exports)
            if total == 0: 
                self.error.emit("View is empty. Compilation cancelled.")
                src_conn.close(); tgt_conn.close(); return
                
            created_folders = set(); modified_rows = []
            
            for i, (new_pp, f_id) in enumerate(all_exports):
                if self.is_cancelled: return
                
                row = src_cur.execute("SELECT * FROM virtual_fs WHERE id=?", (f_id,)).fetchone()
                if not row: continue
                r_list = list(row)
                r_list[pp_idx] = new_pp
                r_list[id_idx] = None 
                modified_rows.append(tuple(r_list))
                
                path_build = "/"
                for part in [p for p in new_pp.split('/') if p]:
                    if path_build + part + "/" not in created_folders:
                        if not tgt_cur.execute("SELECT id FROM virtual_fs WHERE parent_path=? AND name=? AND is_folder=1", (path_build, part)).fetchone():
                            tgt_cur.execute(f"INSERT OR IGNORE INTO virtual_fs (parent_path, name, is_folder) VALUES (?, ?, 1)", (path_build, part))
                        created_folders.add(path_build + part + "/")
                    path_build += part + "/"
                    
                if len(modified_rows) >= 1000:
                    tgt_cur.executemany(f"INSERT OR IGNORE INTO virtual_fs VALUES ({','.join(['?']*len(cols))})", modified_rows)
                    tgt_conn.commit(); modified_rows.clear()
                    self.progress.emit(int(30 + (i/total)*70), 100, f"Compiled {i}/{total} items...")
                    
            if modified_rows:
                tgt_cur.executemany(f"INSERT OR IGNORE INTO virtual_fs VALUES ({','.join(['?']*len(cols))})", modified_rows)
                tgt_conn.commit()
                
            src_conn.close(); tgt_conn.close(); self.finished.emit(self.target_db)
        except Exception as e: self.error.emit(str(e))

class ExportZipThread(QThread):
    progress = Signal(int, int, str)
    finished = Signal(str)
    error = Signal(str)
    def __init__(self, db_path, items, zip_filepath, parent=None):
        super().__init__(parent); self.db_path, self.items, self.zip_filepath = db_path, items, zip_filepath; self.is_cancelled = False
    def cancel(self): self.is_cancelled = True
    def sanitize_filename(self, name): return re.sub(r'[\\/*?:"<>|]', '_', str(name))

    def run(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor(); all_exports = []
                for typ, path_val, db_id in self.items:
                    if self.is_cancelled: return
                    if typ == "file" and db_id != -1:
                        res = cur.execute("SELECT real_path, name FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
                        if res and res[0] and os.path.exists(res[0]): all_exports.append((res[0], self.sanitize_filename(res[1])))
                    elif typ == "folder":
                        if "://" in path_val and not path_val.startswith("trash://") and not path_val.startswith("fav://"):
                            base_f = self.sanitize_filename(path_val.strip('/').split('/')[-1]) if path_val.strip('/').replace('://', '') else ""
                            smart_files = resolve_smart_folder(cur, path_val, self.sanitize_filename)
                            for rel_dest, f_id, rp, sz in smart_files:
                                if rp and os.path.exists(rp): 
                                    dest = os.path.join(base_f, rel_dest) if base_f else rel_dest
                                    all_exports.append((rp, dest))
                        elif path_val.startswith("/"):
                            base_f = self.sanitize_filename(path_val.strip('/').split('/')[-1]) if path_val.strip('/') else ""
                            for pp, n, rp in cur.execute("SELECT parent_path, name, real_path FROM virtual_fs WHERE parent_path LIKE ? AND is_folder=0 AND in_trash=0", (f"{path_val}%",)).fetchall():
                                if not rp or not os.path.exists(rp): continue
                                rel_p = pp[len(path_val):].lstrip('/') 
                                safe_parts = [self.sanitize_filename(p) for p in rel_p.split('/') if p]
                                if base_f: safe_parts.insert(0, base_f)
                                dest = os.path.join(os.path.join(*safe_parts) if safe_parts else "", self.sanitize_filename(n))
                                all_exports.append((rp, dest))

            total = len(all_exports)
            if total == 0: return self.error.emit("No physical files found to zip.")
                
            with zipfile.ZipFile(self.zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zf:
                for i, (real_os_path, zip_virtual_path) in enumerate(all_exports):
                    if self.is_cancelled: return
                    self.progress.emit(i+1, total, f"Compressing:\n{zip_virtual_path}")
                    zf.write(real_os_path, arcname=zip_virtual_path)
                    
            self.finished.emit(self.zip_filepath)
        except Exception as e: self.error.emit(str(e))

class BulkHashCalculator(QThread):
    progress = Signal(int, int, str)
    finished = Signal(int)
    def __init__(self, db_path, target_v_path=None, target_ids=None, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.target_v_path = target_v_path
        self.target_ids = target_ids
        self.is_cancelled = False
    def cancel(self): self.is_cancelled = True
    def run(self):
        try:
            with sqlite3.connect(self.db_path, timeout=30) as conn:
                cur = conn.cursor()
                if self.target_ids is not None:
                    # Fetch paths directly using provided IDs
                    files = []
                    for i in range(0, len(self.target_ids), 900):
                        chunk = self.target_ids[i:i+900]
                        cur.execute(f"SELECT id, real_path, name FROM virtual_fs WHERE id IN ({','.join(['?']*len(chunk))}) AND is_folder=0", chunk)
                        files.extend(cur.fetchall())
                else:
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

class TagLibraryLoaderThread(QThread):
    finished_loading = Signal(dict)
    
    def __init__(self, db_path, base_v_path, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.base_v_path = base_v_path

    def run(self):
        tag_cache = {}
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                # --- LINUX OPTIMIZATION: Boosts read speed by up to 10x ---
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                conn.execute("PRAGMA cache_size=-64000;")
                
                cur = conn.cursor()
                cur.execute("SELECT parent_path, name, custom_tags, is_folder FROM virtual_fs WHERE parent_path LIKE ?", (f"{self.base_v_path}%",))
                for pp, name, tags, is_folder in cur.fetchall():
                    full_path = f"{pp}{name}/" if is_folder else f"{pp}{name}"
                    full_path = full_path.replace("//", "/")
                    
                    # Store ALL files and folders so the columns explore completely
                    tag_cache[full_path] = [t.strip() for t in str(tags).split(',')] if tags else []
        except Exception as e: 
            print(f"DB Load Error: {e}")
            
        self.finished_loading.emit(tag_cache)

class DataLoaderThread(QThread):
    data_ready = Signal(list, list) 
    def __init__(self, db_path, target_path, show_hidden, fast_mode=False, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.target_path = target_path
        self.show_hidden = show_hidden
        self.fast_mode = fast_mode
        self.is_cancelled = False
    def cancel(self): self.is_cancelled = True
    def run(self):
        conn = sqlite3.connect(str(self.db_path))
        # --- LINUX OPTIMIZATION ---
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA cache_size=-64000;")
        
        cur = conn.cursor()
        h_q = "AND is_hidden = 0" if not self.show_hidden else ""
        folders, files = [], []
        try:
            if self.is_cancelled: return
            matched_proto = next((p for p in SMART_PROTOCOLS if self.target_path.startswith(p)), None)
            
            if self.target_path.startswith("tags://"):
                parts = [p for p in self.target_path.replace("tags://", "").split("/") if p]
                if len(parts) == 0:
                    if getattr(self, 'fast_mode', False):
                        cur.execute("SELECT custom_tags, 0 FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND custom_tags IS NOT NULL AND custom_tags != ''")
                    else:
                        cur.execute("SELECT custom_tags, size FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND custom_tags IS NOT NULL AND custom_tags != ''")
                        
                    tag_stats = defaultdict(lambda: [0, 0])
                    for tags_str, sz in cur.fetchall():
                        for t in [x.strip() for x in tags_str.split(",") if x.strip()]:
                            tag_stats[t][0] += 1
                            tag_stats[t][1] += sz or 0
                    folders = [(-1, "tags://", t, "", "", 0, stats[0], stats[1]) for t, stats in tag_stats.items()]
                elif len(parts) >= 1:
                    target_tag = parts[0]
                    cur.execute(f"SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden, custom_tags FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND custom_tags LIKE ? {h_q} LIMIT 500", (f"%{target_tag}%",))
                    for row in cur.fetchall():
                        if target_tag in [x.strip() for x in str(row[9]).split(",") if x.strip()]: files.append(row[:9])

            elif self.target_path.startswith("y_m_f://"):
                parts = [p for p in self.target_path.replace("y_m_f://", "").split("/") if p]
                
                if getattr(self, 'fast_mode', False):
                    cur.execute("SELECT DISTINCT parent_path, year, month FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND year IS NOT NULL AND year != '' AND month IS NOT NULL AND month != ''")
                    folder_age = {pp: (y, m) for pp, y, m in cur.fetchall()}
                    totals = {}
                else:
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
                            cur.execute(f"SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE parent_path=? AND is_folder=0 AND in_trash=0 {h_q} LIMIT 500", (pp,))
                            files = cur.fetchall()
                            break

            elif matched_proto:
                cols = SMART_PROTOCOLS[matched_proto]
                parts = [p for p in self.target_path.replace(matched_proto, "").split("/") if p]
                depth = len(parts)
                if depth < len(cols):
                    target_col = cols[depth]
                    where_clauses = ["is_folder=0", "in_trash=0", f"{target_col} != ''"] + [f"{cols[i]}=?" for i in range(depth)]
                    
                    if getattr(self, 'fast_mode', False):
                        cur.execute(f"SELECT DISTINCT {target_col} FROM virtual_fs WHERE {' AND '.join(where_clauses)}", tuple(parts))
                        base_path = matched_proto + "/".join(parts) + "/" if parts else matched_proto
                        folders = [(-1, base_path, r[0], "", "", 0, 0, 0) for r in cur.fetchall() if r[0]]
                    else:
                        cur.execute(f"SELECT {target_col}, COUNT(id), SUM(size) FROM virtual_fs WHERE {' AND '.join(where_clauses)} GROUP BY {target_col}", tuple(parts))
                        base_path = matched_proto + "/".join(parts) + "/" if parts else matched_proto
                        folders = [(-1, base_path, r[0], "", "", 0, r[1], r[2] or 0) for r in cur.fetchall() if r[0]]
                else:
                    where_clauses = ["is_folder=0", "in_trash=0"] + [f"{cols[i]}=?" for i in range(len(cols))]
                    if not self.show_hidden: where_clauses.append("is_hidden=0")
                    cur.execute(f"SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE {' AND '.join(where_clauses)} LIMIT 500", tuple(parts))
                    files = cur.fetchall()

            elif self.target_path == "trash://":
                cur.execute("SELECT id, parent_path, name, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE is_folder=1 AND in_trash=1 LIMIT 500")
                folders = [(r[0], r[1], r[2], r[3], r[4], r[5], 0, 0) for r in cur.fetchall()]
                cur.execute("SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE is_folder=0 AND in_trash=1 LIMIT 500")
                files = cur.fetchall()
            elif self.target_path == "fav://":
                cur.execute(f"SELECT id, parent_path, name, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE is_folder=1 AND is_favorite=1 AND in_trash=0 {h_q} LIMIT 500")
                folders = [(r[0], r[1], r[2], r[3], r[4], r[5], 0, 0) for r in cur.fetchall()]
                cur.execute(f"SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE is_folder=0 AND is_favorite=1 AND in_trash=0 {h_q} LIMIT 500")
                files = cur.fetchall()
            else:
                if getattr(self, 'fast_mode', False):
                    # Fast Mode: Bypass recursive COUNT() and SUM() for instant loading
                    cur.execute(f"SELECT id, name, color_tag, secondary_name, is_hidden, 0, 0 FROM virtual_fs WHERE parent_path=? AND is_folder=1 AND in_trash=0 {h_q} LIMIT 500", (self.target_path,))
                else:
                    # Normal Mode: Perform deep calculation of folder sizes and items
                    cur.execute(f"SELECT id, name, color_tag, secondary_name, is_hidden, (SELECT COUNT(id) FROM virtual_fs f2 WHERE f2.parent_path LIKE virtual_fs.parent_path || virtual_fs.name || '/%' AND f2.is_folder=0 AND f2.in_trash=0), (SELECT SUM(size) FROM virtual_fs f2 WHERE f2.parent_path LIKE virtual_fs.parent_path || virtual_fs.name || '/%' AND f2.is_folder=0 AND f2.in_trash=0) FROM virtual_fs WHERE parent_path=? AND is_folder=1 AND in_trash=0 {h_q} LIMIT 500", (self.target_path,))
                
                folders = [(r[0], self.target_path, r[1], r[2], r[3], r[4], r[5] or 0, r[6] or 0) for r in cur.fetchall()] 
                cur.execute(f"SELECT id, name, size, extension, real_path, modified, color_tag, secondary_name, is_hidden FROM virtual_fs WHERE parent_path=? AND is_folder=0 AND in_trash=0 {h_q} LIMIT 500", (self.target_path,))
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
        
class CustomSortWidgetItem(QTableWidgetItem):
    def __init__(self, text, sort_val):
        super().__init__(text)
        self.sort_val = sort_val

    def __lt__(self, other):
        if hasattr(other, 'sort_val'):
            return self.sort_val < other.sort_val
        return super().__lt__(other)        

class NumericTableItem(QTableWidgetItem):
    def __lt__(self, other):
        return self.data(Qt.UserRole) < other.data(Qt.UserRole)

class SpaceScannerThread(QThread):
    progress = Signal(int, int, str)
    found = Signal(str, str, str, str, object, str, str, str, str, object) 
    finished_scan = Signal()
    
    def __init__(self, db_path, scan_roots=["/"], huge_threshold=524288000, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.scan_roots = scan_roots
        self.huge_threshold = huge_threshold
        self.is_cancelled = False
        
    def cancel(self): self.is_cancelled = True
    
    def run(self):
        try:
            with sqlite3.connect(self.db_path, timeout=30) as conn:
                cur = conn.cursor()
                path_cond = " OR ".join(["parent_path LIKE ?"] * len(self.scan_roots))
                path_params = tuple(f"{p}%" for p in self.scan_roots)
            
                self.progress.emit(10, 100, "Scanning for Junk...")
                cur.execute(f"SELECT id, name, parent_path, extension, size, modified, custom_tags, color_tag, sha256 FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND hash_verified=0 AND ({path_cond}) AND (extension IN ('.tmp', '.bak', '.log', '.cache') OR name LIKE '%cache%')", path_params)
                for r in cur.fetchall():
                    if self.is_cancelled: return
                    self.found.emit("Junk File", r[1], r[2], r[3] or "", r[4] or 0, r[5] or "Unknown", r[6] or "", r[7] or "", r[8] or "", r[0])
                
                self.progress.emit(40, 100, "Scanning for Huge Files...")
                cur.execute(f"SELECT id, name, parent_path, extension, size, modified, custom_tags, color_tag, sha256 FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND hash_verified=0 AND ({path_cond}) AND size > ? ORDER BY size DESC", path_params + (self.huge_threshold,))
                for r in cur.fetchall():
                    if self.is_cancelled: return
                    self.found.emit(f"Huge File (>{self.huge_threshold//1024//1024}MB)", r[1], r[2], r[3] or "", r[4] or 0, r[5] or "Unknown", r[6] or "", r[7] or "", r[8] or "", r[0])
                
                self.progress.emit(70, 100, "Scanning for Duplicates...")
                cur.execute(f"SELECT size, extension, COUNT(*) as c FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND hash_verified=0 AND ({path_cond}) AND size > 0 GROUP BY size, extension HAVING c > 1", path_params)
                for size, ext, count in cur.fetchall():
                    if self.is_cancelled: return
                    cur.execute(f"SELECT id, name, parent_path, extension, modified, custom_tags, color_tag, sha256 FROM virtual_fs WHERE size=? AND extension=? AND is_folder=0 AND in_trash=0 AND hash_verified=0 AND ({path_cond})", (size, ext) + path_params)
                    files = cur.fetchall()
                    for f in files[1:]: 
                        self.found.emit("Duplicate File", f[1], f[2], f[3] or "", size or 0, f[4] or "Unknown", f[5] or "", f[6] or "", f[7] or "", f[0])
                
                self.progress.emit(100, 100, "Scan Complete.")
        except Exception as e: print(f"Space Scanner Error: {e}")
        finally: self.finished_scan.emit()

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
            # Added a timeout to prevent SQLite database locks from freezing the import
            conn = sqlite3.connect(str(self.db_path), timeout=30)
            cur = conn.cursor()
            added_f, added_d = 0, 0
            all_files_to_process = []
            
            self.progress.emit(0, 100, "Calculating files to import...")
            
            for p in self.paths:
                if os.path.isdir(p):
                    for root, _, files in os.walk(p):
                        for f in files: all_files_to_process.append(os.path.join(root, f))
                else: all_files_to_process.append(p)
                
            total = len(all_files_to_process)
            if total == 0:
                self.finished_import.emit(0, 0)
                conn.close()
                return
            
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
                            if self.is_cancelled: break
                            fp = os.path.join(root, f)
                            
                            # CRITICAL FIX: Wrapped in try-except so permission errors on a single file don't crash the entire batch
                            try:
                                sz = os.path.getsize(fp)
                                ext = os.path.splitext(f)[1].lower()
                                mod = datetime.fromtimestamp(os.path.getmtime(fp)).strftime("%Y-%m-%d %H:%M:%S")
                                cre = datetime.fromtimestamp(os.path.getctime(fp)).strftime("%Y-%m-%d %H:%M:%S")
                                records.append((curr_parent, f, 0, fp, sz, ext, mod, get_category_for_ext(ext), mod[0:4], mod[5:7], cre))
                            except Exception:
                                pass # Skip unreadable files (like system files or broken symlinks)
                                
                            added_f += 1
                            
                            # Update progress smoothly during iteration
                            if added_f % 20 == 0:
                                self.progress.emit(added_f, total, f"Imported {added_f}/{total} files...")
                                
                        if records:
                            cur.executemany("INSERT INTO virtual_fs (parent_path, name, is_folder, real_path, size, extension, modified, category, year, month, creation_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", records)
                            conn.commit() # Commit chunks to clear RAM
                else:
                    try:
                        sz = os.path.getsize(p)
                        ext = os.path.splitext(p)[1].lower()
                        mod = datetime.fromtimestamp(os.path.getmtime(p)).strftime("%Y-%m-%d %H:%M:%S")
                        cre = datetime.fromtimestamp(os.path.getctime(p)).strftime("%Y-%m-%d %H:%M:%S")
                        cur.execute("INSERT INTO virtual_fs (parent_path, name, is_folder, real_path, size, extension, modified, category, year, month, creation_date) VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?)", (self.target_prefix, os.path.basename(p), p, sz, ext, mod, get_category_for_ext(ext), mod[0:4], mod[5:7], cre))
                    except Exception:
                        pass
                        
                    added_f += 1
                    if added_f % 10 == 0:
                        self.progress.emit(added_f, total, f"Imported {added_f}/{total} files...")
                        
            conn.commit()
            conn.close()
            self.progress.emit(total, total, "Import Complete!")
            self.finished_import.emit(added_f, added_d)
        except Exception as e: 
            self.error.emit(str(e))


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
        elif actual_path.startswith("y_m_f://"):
            add_btn("💡 STAT: YEAR ➔ MONTH ➔ FOLDER", "y_m_f://")
            p_str = actual_path.replace("y_m_f://", "").strip("/")
            if p_str:
                curr = "y_m_f://"
                for part in p_str.split("/"): 
                    curr += part + "/"
                    add_btn(part, curr)
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
        self.colors = {
            "Red": QColor("#5c2121"), "Orange": QColor("#663c14"),
            "Gold": QColor("#5c4c21"), "Green": QColor("#215c2b"), 
            "Cyan": QColor("#1b5e5e"), "Blue": QColor("#213c5c"), 
            "Purple": QColor("#43215c"), "Pink": QColor("#5c2144")
        }
        
    def rowCount(self, parent=QModelIndex()): return min(len(self.all_rows), self.display_limit)
    def columnCount(self, parent=QModelIndex()): return len(self.headers)
    def data(self, index: QModelIndex, role=Qt.DisplayRole):
        if not index.isValid(): return None
        row = self.all_rows[index.row()]
        col = index.column()
        if role == Qt.DisplayRole: return row["display"][col]
        elif role == Qt.UserRole: return row.get("user_data")
        elif role == Qt.UserRole + 1: return row.get("color_tag")
        
        # ---> FIXED: Icons now dynamically follow the "Name" column! <---
        elif role == Qt.DecorationRole and self.headers[col] == "Name": return row.get("icon") 
        
        elif role == Qt.TextAlignmentRole: return int(Qt.AlignRight | Qt.AlignVCenter) if self.headers[col] == "Size" else int(Qt.AlignLeft | Qt.AlignVCenter)
        elif role == Qt.ForegroundRole: 
            if row.get("is_hidden"): return QBrush(QColor("#888888"))
            if row.get("color_tag") in self.colors: return QBrush(QColor("#ffffff"))
            return None
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
        self.setStyleSheet("background: #161b22; border-radius: 8px; color: #8b949e;")
        self.setText("Waiting for drive...") # Visual feedback
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
class DriveComparatorDialog(QDialog):
    def __init__(self, main_db, target_db=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Advanced Drive Comparator & Bitrot Auditing")
        self.resize(1150, 750)
        self.setWindowFlags(self.windowFlags() | Qt.WindowMaximizeButtonHint | Qt.WindowMinimizeButtonHint)
        self.main_db = main_db
        self.target_db = target_db
        
        if parent and hasattr(parent, 'styleSheet'):
            self.setStyleSheet(parent.styleSheet())
            
        layout = QVBoxLayout(self)
        
        # Config UI
        config_grp = QGroupBox("Scan Configuration")
        config_lay = QGridLayout(config_grp)
        
        self.lbl_target = QLabel(f"<b>Target DB:</b> {Path(target_db).name if target_db else 'None Selected'}")
        btn_select = QPushButton("📂 Select Target DB")
        btn_select.clicked.connect(self.select_target)
        
        self.combo_match = QComboBox()
        self.combo_match.addItems(["Match Engine: Real OS Path", "Match Engine: Virtual VMan Path"])
        
        self.chk_bitrot = QCheckBox("Detect Bitrot (Hash Mismatch)")
        self.chk_bitrot.setChecked(True)
        self.chk_missing = QCheckBox("Detect Missing (In Target, Not in Main)")
        self.chk_missing.setChecked(True)
        self.chk_moved = QCheckBox("Detect Moved/Renamed (Same Hash, Diff Path)")
        self.chk_moved.setChecked(False) 
        
        btn_scan = QPushButton("🔍 Run Advanced Scan")
        btn_scan.setStyleSheet("background-color: #1f6feb; color: white; font-weight: bold; padding: 6px;")
        btn_scan.clicked.connect(self.run_scan)
        
        config_lay.addWidget(self.lbl_target, 0, 0, 1, 2)
        config_lay.addWidget(btn_select, 0, 2)
        config_lay.addWidget(self.combo_match, 1, 0)
        config_lay.addWidget(self.chk_bitrot, 1, 1)
        config_lay.addWidget(self.chk_missing, 1, 2)
        config_lay.addWidget(self.chk_moved, 1, 3)
        config_lay.addWidget(btn_scan, 0, 3, 1, 1)
        layout.addWidget(config_grp)
        
        # Interactive Table
        self.table = QTableWidget(0, 7)
        self.table.setHorizontalHeaderLabels(["Select", "Detection Type", "File Name", "Main DB Info", "Target DB Info", "Main ID", "Target ID"])
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setColumnWidth(0, 50); self.table.setColumnWidth(1, 150)
        self.table.setColumnWidth(2, 200); self.table.setColumnWidth(3, 300); self.table.setColumnWidth(4, 300)
        self.table.setColumnHidden(5, True); self.table.setColumnHidden(6, True)
        
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection) # Enables Shift+Click
        self.table.setSortingEnabled(True)
        
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)
        layout.addWidget(self.table)
        
        # Action Tools
        action_lay = QHBoxLayout()
        btn_sel_all = QPushButton("☑ Toggle All")
        btn_sel_all.clicked.connect(self.toggle_all)
        
        self.btn_export = QPushButton("📤 Export CSV Report")
        self.btn_export.clicked.connect(self.export_report)
        
        self.combo_resolve = QComboBox()
        self.combo_resolve.addItems([
            "Overwrite Main Hash with Target Hash",
            "Overwrite Target Hash with Main Hash",
            "Merge Target Data into Main DB (Updates Only)",
            "Merge Unique Target Items to NEW 3rd Database"
        ])
        
        self.btn_resolve = QPushButton("⚡ Resolve Selected")
        self.btn_resolve.setStyleSheet("background-color: #2ea043; color: white; font-weight: bold; padding: 6px 15px;")
        self.btn_resolve.clicked.connect(self.resolve_selected)
        
        action_lay.addWidget(btn_sel_all)
        action_lay.addWidget(self.btn_export)
        action_lay.addStretch()
        action_lay.addWidget(self.combo_resolve)
        action_lay.addWidget(self.btn_resolve)
        layout.addLayout(action_lay)
        
        if self.target_db: QTimer.singleShot(100, self.run_scan)

    def select_target(self):
        path, _ = QFileDialog.getOpenFileName(self, "Select Secondary Database", "", "Database (*.db)")
        if path:
            self.target_db = path
            self.lbl_target.setText(f"<b>Target DB:</b> {Path(path).name}")

    def show_context_menu(self, pos):
        menu = QMenu(self)
        act_check = menu.addAction("☑ Check Highlighted")
        act_uncheck = menu.addAction("☐ Uncheck Highlighted")
        action = menu.exec(self.table.viewport().mapToGlobal(pos))
        
        if action in [act_check, act_uncheck]:
            state = Qt.Checked if action == act_check else Qt.Unchecked
            for idx in self.table.selectionModel().selectedRows():
                self.table.item(idx.row(), 0).setCheckState(state)

    def add_row(self, type_str, name, m_info, t_info, m_id, t_id):
        row = self.table.rowCount()
        self.table.insertRow(row)
        chk = QTableWidgetItem()
        chk.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
        chk.setCheckState(Qt.Unchecked)
        self.table.setItem(row, 0, chk)
        self.table.setItem(row, 1, QTableWidgetItem(type_str))
        self.table.setItem(row, 2, QTableWidgetItem(name))
        self.table.setItem(row, 3, QTableWidgetItem(m_info))
        self.table.setItem(row, 4, QTableWidgetItem(t_info))
        
        m_id_item = QTableWidgetItem()
        m_id_item.setData(Qt.DisplayRole, m_id)
        self.table.setItem(row, 5, m_id_item)
        
        t_id_item = QTableWidgetItem()
        t_id_item.setData(Qt.DisplayRole, t_id)
        self.table.setItem(row, 6, t_id_item)

    def toggle_all(self):
        state = Qt.Checked if getattr(self, 'all_checked', False) else Qt.Unchecked
        self.all_checked = not getattr(self, 'all_checked', False)
        for r in range(self.table.rowCount()):
            if self.table.item(r, 0): self.table.item(r, 0).setCheckState(state)

    def run_scan(self):
        if not self.target_db: return QMessageBox.warning(self, "Missing Target", "Please select a Target Database first.")
        
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        match_virtual = (self.combo_match.currentIndex() == 1)
        
        prog = QProgressDialog("Cross-referencing databases...", "Cancel", 0, 0, self)
        prog.setWindowModality(Qt.WindowModal); prog.setMinimumDuration(0); prog.show()
        QApplication.processEvents()
        
        try:
            # Force the main app to release any pending background locks
            if self.parent() and hasattr(self.parent(), 'db'):
                self.parent().db.conn.commit()

            conn = sqlite3.connect(self.main_db, timeout=30)
            try:
                cur = conn.cursor()
                cur.execute(f"ATTACH DATABASE '{self.target_db}' AS target")
                
                join_cond = "m.parent_path = t.parent_path AND m.name = t.name" if match_virtual else "m.real_path = t.real_path"
                
                if self.chk_bitrot.isChecked():
                    cur.execute(f"SELECT m.id, t.id, m.name, m.sha256, t.sha256, m.real_path FROM main.virtual_fs m JOIN target.virtual_fs t ON {join_cond} WHERE m.sha256 != t.sha256 AND m.sha256 != '' AND t.sha256 != '' AND m.is_folder=0")
                    for mid, tid, name, m_hash, t_hash, rp in cur.fetchall():
                        self.add_row("⚠️ Bitrot Conflict", name, f"Hash: {m_hash}", f"Hash: {t_hash}", mid, tid)
                        
                if self.chk_missing.isChecked():
                    where_missing = "t.parent_path || t.name NOT IN (SELECT parent_path || name FROM main.virtual_fs)" if match_virtual else "t.real_path NOT IN (SELECT real_path FROM main.virtual_fs WHERE real_path != '') AND t.real_path != ''"
                    cur.execute(f"SELECT t.id, t.name, t.real_path, t.size FROM target.virtual_fs t WHERE {where_missing} AND t.is_folder=0")
                    for tid, name, rp, size in cur.fetchall():
                        self.add_row("➕ Missing in Main", name, "File Not Found", f"Size: {size} | OS: {rp}", -1, tid)

                if self.chk_moved.isChecked():
                    field = "parent_path || name" if match_virtual else "real_path"
                    cur.execute(f"SELECT m.id, t.id, m.name, m.{field.split('||')[0].strip()}, t.{field.split('||')[0].strip()} FROM main.virtual_fs m JOIN target.virtual_fs t ON m.sha256 = t.sha256 WHERE m.sha256 != '' AND m.{field} != t.{field} AND m.is_folder=0")
                    for mid, tid, name, m_path, t_path in cur.fetchall():
                        self.add_row("🔄 Moved/Renamed", name, f"Main: {m_path}", f"Target: {t_path}", mid, tid)
                        
                cur.execute("DETACH DATABASE target")
            finally:
                conn.close() # <--- CRITICAL FIX: Explicitly release the database lock!
                
            self.table.setSortingEnabled(True)
            prog.close()
            QMessageBox.information(self, "Scan Complete", f"Deep scan finished. Found {self.table.rowCount()} anomalies.")
        except Exception as e:
            prog.close(); QMessageBox.critical(self, "Scan Error", str(e))

    def resolve_selected(self):
        selected = [r for r in range(self.table.rowCount()) if self.table.item(r, 0).checkState() == Qt.Checked]
        if not selected: return QMessageBox.information(self, "No Selection", "Please check items to resolve.")
        
        if QMessageBox.question(self, "Resolve", f"Apply resolutions to {len(selected)} selected items?", QMessageBox.Yes|QMessageBox.No) != QMessageBox.Yes: return

        action_idx = self.combo_resolve.currentIndex() 
        
        prog = QProgressDialog(f"Resolving {len(selected)} items...", "Cancel", 0, len(selected), self)
        prog.setWindowModality(Qt.WindowModal); prog.setMinimumDuration(0); prog.setValue(0); prog.show()
        QApplication.processEvents()

        try:
            if self.parent() and hasattr(self.parent(), 'db'):
                self.parent().db.conn.commit()

            if action_idx == 3: # Create 3rd DB
                save_path, _ = QFileDialog.getSaveFileName(self, "Create 3rd Merged Database", "", "Database (*.db)")
                if not save_path: return prog.close()
                import shutil
                shutil.copy2(self.main_db, save_path)
                conn_m = sqlite3.connect(save_path, timeout=30)
            else:
                conn_m = sqlite3.connect(self.main_db, timeout=30)
                
            try:
                cur_m = conn_m.cursor()
                cur_m.execute(f"ATTACH DATABASE '{self.target_db}' AS target")
                
                # Execute in a bulk transaction block to prevent intra-loop locking
                cur_m.execute("BEGIN IMMEDIATE")
                
                for i, r in enumerate(selected):
                    if prog.wasCanceled(): break
                    issue_type = self.table.item(r, 1).text()
                    m_id = int(self.table.item(r, 5).text())
                    t_id = int(self.table.item(r, 6).text())
                    
                    if "Missing" in issue_type and action_idx in [2, 3]:
                        cur_m.execute("INSERT INTO main.virtual_fs (parent_path, name, is_folder, real_path, size, extension, modified, sha256, custom_tags, color_tag, secondary_name, hash_verified, creation_date) SELECT parent_path, name, is_folder, real_path, size, extension, modified, sha256, custom_tags, color_tag, secondary_name, hash_verified, creation_date FROM target.virtual_fs WHERE id=?", (t_id,))
                    elif "Bitrot" in issue_type:
                        if action_idx in [0, 2, 3]: cur_m.execute("UPDATE main.virtual_fs SET sha256 = (SELECT sha256 FROM target.virtual_fs WHERE id=?) WHERE id=?", (t_id, m_id))
                        elif action_idx == 1: cur_m.execute("UPDATE target.virtual_fs SET sha256 = (SELECT sha256 FROM main.virtual_fs WHERE id=?) WHERE id=?", (m_id, t_id))
                    elif "Moved" in issue_type and action_idx in [0, 2, 3]:
                        cur_m.execute("UPDATE main.virtual_fs SET real_path = (SELECT real_path FROM target.virtual_fs WHERE id=?), parent_path = (SELECT parent_path FROM target.virtual_fs WHERE id=?), name = (SELECT name FROM target.virtual_fs WHERE id=?) WHERE id=?", (t_id, t_id, t_id, m_id))

                    prog.setValue(i+1)
                    if i % 10 == 0: QApplication.processEvents()
                    
                conn_m.commit()
                cur_m.execute("DETACH DATABASE target")
            finally:
                conn_m.close() # <--- CRITICAL FIX: Explicitly release the database lock!
                
            prog.close()
            QMessageBox.information(self, "Success", f"Successfully resolved {len(selected)} anomalies.")
            
            if self.parent() and action_idx != 3:
                self.parent().clear_cache()
                self.parent().refresh_all()
            self.run_scan()
                
        except Exception as e:
            prog.close(); QMessageBox.critical(self, "Resolve Error", str(e))

    def export_report(self):
        if self.table.rowCount() == 0: return
        path, _ = QFileDialog.getSaveFileName(self, "Export Audit Report", "", "CSV Files (*.csv)")
        if not path: return
        try:
            import csv
            with open(path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Issue Type", "File Name", "Main DB Details", "Target DB Details"])
                for r in range(self.table.rowCount()):
                    writer.writerow([self.table.item(r, 1).text(), self.table.item(r, 2).text(), self.table.item(r, 3).text(), self.table.item(r, 4).text()])
            QMessageBox.information(self, "Exported", "Audit report saved successfully.")
        except Exception as e:
            QMessageBox.warning(self, "Export Error", str(e))

class PhysicalDeleteWarningDialog(QDialog):
    def __init__(self, target_str, parent=None):
        super().__init__(parent)
        self.setWindowTitle("CRITICAL WARNING: Physical Deletion")
        self.setFixedSize(550, 300)
        self.setModal(True)
        
        self.layout = QVBoxLayout(self)
        self.layout.setAlignment(Qt.AlignCenter)
        
        # Uses the smart string (e.g., "3 FILES AND 1 FOLDER")
        self.lbl_warning = QLabel(f"⚠️ YOU ARE ABOUT TO PERMANENTLY DELETE {target_str.upper()} FROM YOUR OS HARD DRIVE! ⚠️")
        self.lbl_warning.setWordWrap(True)
        self.lbl_warning.setAlignment(Qt.AlignCenter)
        font = QFont("Segoe UI", 16, QFont.Bold)
        self.lbl_warning.setFont(font)
        self.layout.addWidget(self.lbl_warning)
        
        self.lbl_sub = QLabel("This action CANNOT be undone. Items will NOT go to the Recycle Bin.")
        self.lbl_sub.setAlignment(Qt.AlignCenter)
        self.layout.addWidget(self.lbl_sub)
        
        self.btn_confirm = QPushButton("I Understand, Delete Everything")
        self.btn_confirm.setFixedHeight(50)
        self.btn_confirm.setFont(QFont("Segoe UI", 12, QFont.Bold))
        self.btn_confirm.setEnabled(False) # Disabled initially
        self.btn_confirm.clicked.connect(self.accept)
        
        self.btn_cancel = QPushButton("Cancel & Keep Files Safe")
        self.btn_cancel.setFixedHeight(50)
        self.btn_cancel.clicked.connect(self.reject)
        
        self.layout.addSpacing(20)
        self.layout.addWidget(self.btn_confirm)
        self.layout.addWidget(self.btn_cancel)
        
        # Flashing Animation State
        self.is_red = False
        self.flash_timer = QTimer(self)
        self.flash_timer.timeout.connect(self.flash_bg)
        self.flash_timer.start(500)
        
        # 4 Second Countdown to enable Delete button
        self.countdown = 4
        self.enable_timer = QTimer(self)
        self.enable_timer.timeout.connect(self.tick_enable)
        self.enable_timer.start(1000)
        self.tick_enable()

    def flash_bg(self):
        self.is_red = not self.is_red
        if self.is_red:
            self.setStyleSheet("QDialog { background-color: #8b0000; color: white; } QLabel { color: white; } QPushButton { background-color: #21262d; color: white; }")
        else:
            self.setStyleSheet("QDialog { background-color: #5c2121; color: white; } QLabel { color: #ffcccc; } QPushButton { background-color: #30363d; color: white; }")

    def tick_enable(self):
        if self.countdown > 0:
            self.btn_confirm.setText(f"Wait {self.countdown} seconds...")
            self.countdown -= 1
        else:
            self.btn_confirm.setText("I Understand, Delete Everything")
            self.btn_confirm.setStyleSheet("background-color: black; color: red; border: 2px solid red;")
            self.btn_confirm.setEnabled(True)
            self.enable_timer.stop()

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

        # Escapes the $ symbol so Matplotlib doesn't crash trying to render Math equations
        keys = [str(k).replace('$', '\\$')[:30] + '..' if len(str(k)) > 30 else str(k).replace('$', '\\$') for k, v in page_data]
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

    def contextMenuEvent(self, event):
        items = self.selectedItems()
        if not items: return
        menu = QMenu(self)
        
        if len(items) == 1:
            act_edit = menu.addAction("✏️ Edit Tag Globally")
        else: act_edit = None
            
        act_del = menu.addAction(f"🗑️ Delete {len(items)} Tag(s) Globally")
        action = menu.exec(self.mapToGlobal(event.pos()))
        
        if act_edit and action == act_edit: self.parent_dialog.edit_global_tag(items[0].text())
        elif action == act_del:
            tags_to_delete = [i.text() for i in items]
            self.parent_dialog.delete_multiple_global_tags(tags_to_delete)

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
        
        # Strictly enforce exactly these 5 levels, ignoring past history
        self.hierarchy_levels = ["Parent Folder", "Level 1", "Level 2", "Level 3", "Level 4"]
            
        self.setWindowTitle("Universal Tag Engine & Analytics")
        self.resize(1300, 800)
        self.setWindowFlags(self.windowFlags() | Qt.WindowMaximizeButtonHint | Qt.WindowMinimizeButtonHint)
        
        # --- FIX: Disconnect from OS Theme & Force Dark Mode Deeply ---
        self.setStyleSheet(THEMES["Dark"])
        self.setAutoFillBackground(True)
        p = self.palette(); p.setColor(self.backgroundRole(), QColor("#0d1117")); self.setPalette(p)

        self._build_toolbar()
        self.main_layout = QVBoxLayout(self)
        self.main_layout.addLayout(self.top_toolbar)
        
        # --- Master Tab Widget with Smooth Drag Scrolling ---
        self.tabs = QTabWidget()
        self.tabs.setTabBar(DragScrollTabBar()) # Inject custom drag-scroll bar
        self.tabs.setStyleSheet("""
            QTabBar::tab { background: #161b22; color: #8b949e; padding: 10px 15px; border: 1px solid #30363d; border-top-left-radius: 4px; border-top-right-radius: 4px; margin-right: 2px;}
            QTabBar::tab:selected { background: #0d1117; color: #58a6ff; font-weight: bold; border-bottom: 2px solid #58a6ff; }
            QTabWidget::pane { border: 1px solid #30363d; top: -1px; background: #0d1117; }
        """)
        self.main_layout.addWidget(self.tabs, stretch=1)

        # Tab 1: Original Browser UI
        self.browser_tab = QWidget()
        self.browser_tab.setStyleSheet("background-color: #0d1117; color: #c9d1d9;") # Enforce Dark Mode on children
        self.browser_layout = QVBoxLayout(self.browser_tab)
        self.browser_layout.setContentsMargins(0,0,0,0)

        self.main_splitter = QSplitter(Qt.Horizontal)
        self.column_scroll_area = QScrollArea()
        self.column_scroll_area.setWidgetResizable(True)
        self.column_scroll_area.setFrameShape(QScrollArea.NoFrame)
        self.column_scroll_area.setStyleSheet("background-color: transparent;")
        
        self.column_scroll_widget = QWidget()
        self.column_scroll_widget.setStyleSheet("background-color: #0d1117;")
        self.column_container_layout = QHBoxLayout(self.column_scroll_widget)
        self.column_container_layout.setAlignment(Qt.AlignLeft)
        self.column_scroll_area.setWidget(self.column_scroll_widget)

        self.tag_search_box = QLineEdit(); self.tag_search_box.setPlaceholderText("Search tags...")        
        self.tag_list = TagListWidget(self)   
        
        # Enable Shift-Click Multi Select for Tags
        self.tag_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
             
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
        
        self.tabs.addTab(self.browser_tab, "📂 Library")

        # --- FIX: Dashboard Tab with ScrollBar ---
        self.dashboard_tab = QWidget()
        self.dashboard_tab.setStyleSheet("background-color: #0d1117;")
        self.dashboard_layout = QGridLayout(self.dashboard_tab)
        
        self.dash_scroll = QScrollArea()
        self.dash_scroll.setWidgetResizable(True)
        self.dash_scroll.setFrameShape(QScrollArea.NoFrame)
        self.dash_scroll.setWidget(self.dashboard_tab)
        self.tabs.addTab(self.dash_scroll, "🏠 Dashboard")
        
        # Tabs 2+: Built-in Analytics 
        self.tag_chart_tab = PaginatingChartWidget("Tag Frequency", self)
        self.tabs.addTab(self.tag_chart_tab, "🏷 Tags")
        
        # Dynamically generate clean named tabs
        for i, level_name in enumerate(self.hierarchy_levels):
            chart_tab = PaginatingChartWidget(f"Volume by {level_name}", self)
            setattr(self, f"l{i}_chart_tab", chart_tab)
            self.tabs.addTab(chart_tab, f"🗂 {level_name}")

        # --- NEW: Dynamic Tab Creator Button (+) ---
        self.btn_add_tab = QPushButton("+")
        self.btn_add_tab.setCursor(Qt.PointingHandCursor)
        self.btn_add_tab.setToolTip("Add Next Hierarchy Level Analytics")
        self.btn_add_tab.setStyleSheet("background-color: transparent; border: none; font-size: 20px; font-weight: bold; color: #3fb950; padding: 0 10px;")
        self.btn_add_tab.clicked.connect(self.add_dynamic_level_tab)
        self.tabs.setCornerWidget(self.btn_add_tab, Qt.TopRightCorner)

        self.dynamic_lists = [] 
        self._populate_base_contexts()
        self.refresh_memory_cache()
        self._setup_shortcuts()

    def add_dynamic_level_tab(self):
        new_level_idx = len(self.hierarchy_levels)
        new_name = f"Level {new_level_idx}"
        self.hierarchy_levels.append(new_name)
        
        new_chart_tab = PaginatingChartWidget(f"Volume by {new_name}", self)
        setattr(self, f"l{new_level_idx}_chart_tab", new_chart_tab)
        self.tabs.addTab(new_chart_tab, f"📑 {new_name}")
        
        self.update_analytics_data() # Force UI Re-calculation

    def edit_global_tag(self, old_tag):
        new_tag, ok = QInputDialog.getText(self, "Edit Tag", f"Rename '{old_tag}' to:")
        if not ok or not new_tag.strip() or new_tag.strip() == old_tag: return
        new_tag = new_tag.strip()
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, custom_tags FROM virtual_fs WHERE custom_tags LIKE ?", (f"%{old_tag}%",))
            for db_id, tags in cur.fetchall():
                tag_list = [t.strip() for t in tags.split(',')]
                if old_tag in tag_list:
                    tag_list = [new_tag if t == old_tag else t for t in tag_list]
                    cur.execute("UPDATE virtual_fs SET custom_tags=? WHERE id=?", (", ".join(tag_list), db_id))
            conn.commit()
        self.refresh_memory_cache()

    def delete_multiple_global_tags(self, target_tags):
        if QMessageBox.question(self, "Delete Tags", f"Remove {len(target_tags)} tags from ALL files?", QMessageBox.Yes|QMessageBox.No) != QMessageBox.Yes: return
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            for target_tag in target_tags:
                cur.execute("SELECT id, custom_tags FROM virtual_fs WHERE custom_tags LIKE ?", (f"%{target_tag}%",))
                for db_id, tags in cur.fetchall():
                    tag_list = [t.strip() for t in tags.split(',')]
                    if target_tag in tag_list:
                        tag_list.remove(target_tag)
                        cur.execute("UPDATE virtual_fs SET custom_tags=? WHERE id=?", (", ".join(tag_list), db_id))
            conn.commit()
        self.refresh_memory_cache()

    def delete_global_tag(self, target_tag):
        self.delete_multiple_global_tags([target_tag])

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
        
        self.btn_map_global = QPushButton("🔗 Map to OS")
        self.btn_map_global.setStyleSheet("font-weight: bold; color: #58a6ff;")
        self.btn_map_global.clicked.connect(self.map_global_base_to_os)
        self.top_toolbar.addWidget(self.btn_map_global)
        
        self.top_toolbar.addStretch() 
        
        self.radio_folders = QRadioButton("Dirs")
        self.radio_files = QRadioButton("Files")
        self.radio_folders.setChecked(True)
        
        self.global_search_box = QLineEdit()
        self.global_search_box.setPlaceholderText("Global Search ...")
        self.global_search_box.setFixedWidth(250)
        
        self.btn_search = QPushButton("🔍")
        self.btn_search.clicked.connect(lambda: self.run_global_search(self.global_search_box.text()))
        
        self.top_toolbar.addWidget(self.radio_folders)
        self.top_toolbar.addWidget(self.radio_files)
        self.top_toolbar.addWidget(self.global_search_box)
        self.top_toolbar.addWidget(self.btn_search)
        
        self.global_search_box.returnPressed.connect(lambda: self.run_global_search(self.global_search_box.text()))
        self.radio_folders.toggled.connect(lambda: self.run_global_search(self.global_search_box.text()))
        
        btn_refresh = QPushButton("🔄 Refresh")
        btn_refresh.clicked.connect(self.full_refresh)
        
        self.btn_export = QPushButton("📤 Export CSV")
        self.btn_export.clicked.connect(self.export_csv)

        self.top_toolbar.addWidget(btn_refresh)
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
        QMessageBox.information(self, "Refreshed", "Tag Library data has been fully synced.")

    def refresh_memory_cache(self):
        while self.dynamic_lists:
            col_data = self.dynamic_lists.pop()
            col_data['widget'].setParent(None)
            col_data['widget'].deleteLater()

        self.tag_list.clear()
        self.tag_cache.clear()

        self.tabs.setEnabled(False)
        self.setWindowTitle(f"Universal Tag Engine & Analytics - [LOADING {self.base_v_path} ...]")

        self.loader_thread = TagLibraryLoaderThread(self.db_path, self.base_v_path, self)
        self.loader_thread.finished_loading.connect(self._on_cache_loaded)
        self.loader_thread.start()
        
    def _on_cache_loaded(self, loaded_cache):
        self.tag_cache = loaded_cache
        self.setWindowTitle("Universal Tag Engine & Analytics")
        self.tabs.setEnabled(True)

        self.update_analytics_data()
        self._add_column(0, self.hierarchy_levels[0] if len(self.hierarchy_levels) > 0 else "Root")
        self._populate_level(0, self.base_v_path)
        self._populate_all_tags()       
        
    def update_analytics_data(self):
        tags_data = {}
        # Dynamically build dictionaries for however many levels exist
        level_dicts = [{} for _ in range(len(self.hierarchy_levels))]
        
        total_items, total_folders, total_files = 0, 0, 0
        multi_tagged_items, total_tags_applied = 0, 0
        base_depth = len([p for p in self.base_v_path.split('/') if p])
        
        # High-Speed Data extraction Loop
        for path, tags in self.tag_cache.items():
            total_items += 1
            if path.endswith('/'): total_folders += 1
            else: total_files += 1
                
            valid_tags = [t for t in tags if t]
            total_tags_applied += len(valid_tags)
            if len(valid_tags) > 1: multi_tagged_items += 1
                
            parts = [p for p in path.split('/') if p]
            relative_parts = parts[base_depth:]
            
            for i in range(min(len(relative_parts), len(self.hierarchy_levels))):
                level_dicts[i][relative_parts[i]] = level_dicts[i].get(relative_parts[i], 0) + 1
            
            for t in valid_tags:
                tags_data[t] = tags_data.get(t, 0) + 1

        self.tag_chart_tab.update_data(tags_data)
        
        # Update all level charts dynamically
        for i, level_name in enumerate(self.hierarchy_levels):
            if hasattr(self, f"l{i}_chart_tab"):
                getattr(self, f"l{i}_chart_tab").update_data(level_dicts[i])
        
        for i in reversed(range(self.dashboard_layout.count())): 
            item = self.dashboard_layout.itemAt(i)
            if item.widget(): item.widget().setParent(None)

        avg_tags = round(total_tags_applied / total_items, 2) if total_items > 0 else 0

        # Base 11 Cards
        metrics = [
            ("TOTAL VIRTUAL ITEMS", total_items),
            ("VIRTUAL FOLDERS", total_folders),
            ("VIRTUAL FILES", total_files),
            ("UNIQUE TAGS", len(tags_data)),
            ("MULTI-TAGGED ITEMS", multi_tagged_items),
            ("AVG TAGS PER ITEM", avg_tags),
            (f"TOTAL {self.hierarchy_levels[0].upper()}S", len(level_dicts[0]) if len(level_dicts) > 0 else 0),
            (f"TOTAL {self.hierarchy_levels[1].upper()}S", len(level_dicts[1]) if len(level_dicts) > 1 else 0),
            (f"TOTAL {self.hierarchy_levels[2].upper()}S", len(level_dicts[2]) if len(level_dicts) > 2 else 0),
            (f"TOTAL {self.hierarchy_levels[3].upper()}S", len(level_dicts[3]) if len(level_dicts) > 3 else 0),
            (f"TOTAL {self.hierarchy_levels[4].upper()}S", len(level_dicts[4]) if len(level_dicts) > 4 else 0)
        ]
        
        # --- THE 12TH INTERACTIVE CARD ---
        metrics.append(("TOTAL HIERARCHY LEVELS", "Want to know how many level?"))

        colors = ["#58a6ff", "#3fb950", "#e3b341", "#a371f7", "#f85149", "#d2a8ff", 
                  "#79c0ff", "#2ea043", "#ff7b72", "#bc8cff", "#f2cc60"]
  
        row, col = 0, 0
        for idx, (title, val) in enumerate(metrics):
            accent_color = colors[idx % len(colors)]
            
            card = InteractiveDashboardCard() if title == "TOTAL HIERARCHY LEVELS" else QFrame()
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
            lbl_val.setAlignment(Qt.AlignCenter)
            
            # Specific styling and click logic for the 12th card
            if title == "TOTAL HIERARCHY LEVELS":
                lbl_val.setStyleSheet(f"color: {accent_color}; font-size: 16px; font-style: italic; border: none;")
                card.setCursor(Qt.PointingHandCursor)
                
                # Dynamic calculation engine for true max depth
                def reveal_levels(l=lbl_val, a=accent_color):
                    max_depth = 0
                    for p in self.tag_cache.keys():
                        depth = len([part for part in p.split('/') if part])
                        if depth > max_depth: max_depth = depth
                    base_depth = len([p for p in self.base_v_path.split('/') if p])
                    actual_levels = max(0, max_depth - base_depth)
                    
                    l.setText(str(actual_levels))
                    l.setStyleSheet(f"color: {a}; font-size: 34px; font-weight: bold; border: none;")
                card.clicked.connect(reveal_levels)
            else:
                lbl_val.setStyleSheet(f"color: {accent_color}; font-size: 34px; font-weight: bold; border: none;")
            
            c_lay.addWidget(lbl_title, alignment=Qt.AlignTop | Qt.AlignLeft)
            c_lay.addStretch()
            c_lay.addWidget(lbl_val)
            c_lay.addStretch()
            
            self.dashboard_layout.addWidget(card, row, col)
            
            col += 1
            if col > 3: 
                col = 0
                row += 1
                
        for i in range(4): self.dashboard_layout.setColumnStretch(i, 1)

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
        widget.setStyleSheet("background-color: #0d1117;") # Force child dark mode
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

    def _get_icon_for_node(self, name, is_folder):
        if self.main_app and hasattr(self.main_app, '_get_native_icon'):
            ext = os.path.splitext(name)[1].lower() if not is_folder else ""
            return self.main_app._get_native_icon(name, is_folder, ext, -1)
        return self.style().standardIcon(QStyle.SP_DirIcon if is_folder else QStyle.SP_FileIcon)

    def _populate_level(self, level_index, prefix_path):
        if level_index >= len(self.dynamic_lists): return
        items = self._get_children(prefix_path)
                    
        lst = self.dynamic_lists[level_index]['list']
        lst.blockSignals(True); lst.clear()
        
        folders, files = [], []
        cache_keys = self.tag_cache.keys()
        
        for name in items:
            test_folder = f"{prefix_path}{name}/"
            is_fldr = test_folder in self.tag_cache or any(p.startswith(test_folder) for p in cache_keys)
            
            if is_fldr: folders.append((name, True, test_folder))
            else: files.append((name, False, f"{prefix_path}{name}"))
                
        folders.sort(key=lambda x: natural_sort_key(x[0]))
        files.sort(key=lambda x: natural_sort_key(x[0]))
        
        for name, is_fldr, node_v_path in (folders + files):
            item = QListWidgetItem(name)
            item.setData(Qt.UserRole, node_v_path)
            item.setIcon(self._get_icon_for_node(name, is_fldr))
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
        prog = QProgressDialog(f"Loading tag '{target_tag}'...", "Cancel", 0, 100, self)
        prog.setWindowModality(Qt.WindowModal); prog.setMinimumDuration(0); prog.show()

        valid_paths = []
        keys = list(self.tag_cache.items())
        total = max(1, len(keys))
        
        for i, (p, tags) in enumerate(keys):
            if i % 2000 == 0:
                prog.setValue(int((i / total) * 30)); QApplication.processEvents()
            if prog.wasCanceled(): return
            if target_tag in tags: valid_paths.append(p)
        
        base_depth = len([p for p in self.base_v_path.split('/') if p])
        max_depth = 0
        for vp in valid_paths:
            parts = [p for p in vp.split('/') if p]
            depth = len(parts) - base_depth
            if depth > max_depth: max_depth = depth
            
        while len(self.dynamic_lists) > max_depth:
            col = self.dynamic_lists.pop()
            col['widget'].setParent(None); col['widget'].deleteLater()
            
        for i in range(len(self.dynamic_lists), max_depth):
            title = self.hierarchy_levels[i] if hasattr(self, 'hierarchy_levels') and i < len(self.hierarchy_levels) else f"Level {i + 1}"
            self._add_column(i, title)
            
        total_vp = max(1, len(valid_paths))
        
        for i in range(max_depth):
            if prog.wasCanceled(): break
            prog.setLabelText(f"Building Structure for Column {i+1} of {max_depth}...")
            
            lst = self.dynamic_lists[i]['list']
            lst.blockSignals(True); lst.clear()
            
            level_nodes = {}
            for j, vp in enumerate(valid_paths):
                if j % 2000 == 0:
                    col_base_pct = 30 + (i / max(1, max_depth)) * 70
                    col_progress = (j / total_vp) * (70 / max(1, max_depth))
                    prog.setValue(int(col_base_pct + col_progress)); QApplication.processEvents()
                    
                parts = [p for p in vp.split('/') if p]
                if len(parts) > base_depth + i:
                    name = parts[base_depth + i]
                    is_folder = (len(parts) > base_depth + i + 1) or vp.endswith('/')
                    node_v_path = "/" + "/".join(parts[:base_depth + i + 1]) + ("/" if is_folder else "")
                    level_nodes[name] = (node_v_path, is_folder)
            
            f_nodes = [k for k, v in level_nodes.items() if v[1]]
            file_nodes = [k for k, v in level_nodes.items() if not v[1]]
            sorted_names = sorted(f_nodes, key=natural_sort_key) + sorted(file_nodes, key=natural_sort_key)
            display_names = sorted_names[:1000]
            
            for j, name in enumerate(display_names):
                if j % 100 == 0: QApplication.processEvents()
                node_v_path, is_folder = level_nodes[name]
                l_item = QListWidgetItem(name)
                l_item.setData(Qt.UserRole, node_v_path)
                l_item.setIcon(self._get_icon_for_node(name, is_folder))
                
                if node_v_path in self.tag_cache and target_tag in self.tag_cache[node_v_path]:
                    l_item.setForeground(QBrush(QColor("#58a6ff")))
                    font = l_item.font(); font.setBold(True); l_item.setFont(font)
                    
                lst.addItem(l_item)
                
            if len(sorted_names) > 1000:
                warning_item = QListWidgetItem(f"... and {len(sorted_names) - 1000} more items (Refine search)")
                warning_item.setForeground(QBrush(QColor("#e3b341")))
                font = warning_item.font(); font.setItalic(True); warning_item.setFont(font)
                lst.addItem(warning_item)
                
            lst.blockSignals(False)

        prog.setValue(100); prog.close()

    def run_global_search(self, text):
        query = text.lower()
        if not query: return self.refresh_memory_cache()
            
        valid_paths = set()
        include_folders = self.radio_folders.isChecked()
        include_files = self.radio_files.isChecked()

        total_items = max(1, len(self.tag_cache))

        prog = QProgressDialog("Searching memory cache...", "Cancel", 0, 100, self)
        prog.setWindowModality(Qt.WindowModal); prog.setMinimumDuration(0); prog.show()

        keys = list(self.tag_cache.keys())
        for i, p in enumerate(keys):
            if i % 2000 == 0:
                prog.setValue(int((i / total_items) * 50)); QApplication.processEvents()
            if prog.wasCanceled(): return

            is_fldr = p.endswith('/')
            if (is_fldr and not include_folders) or (not is_fldr and not include_files): continue 
            
            parts = [part for part in p.split('/') if part]
            target_name = parts[-1] if parts else ""
            
            if query in target_name.lower(): valid_paths.add(p)
                
        if not valid_paths:
            prog.close()
            QMessageBox.information(self, "Search Results", f"No items found matching '{text}' in this folder.")
            return

        valid_paths = list(valid_paths)
        base_depth = len([p for p in self.base_v_path.split('/') if p])
        
        max_depth = 0
        for vp in valid_paths:
            parts = [p for p in vp.split('/') if p]
            depth = len(parts) - base_depth
            if depth > max_depth: max_depth = depth
            
        while len(self.dynamic_lists) > max_depth:
            col = self.dynamic_lists.pop()
            col['widget'].setParent(None); col['widget'].deleteLater()
            
        for i in range(len(self.dynamic_lists), max_depth):
            title = self.hierarchy_levels[i] if hasattr(self, 'hierarchy_levels') and i < len(self.hierarchy_levels) else f"Level {i + 1}"
            self._add_column(i, title)
            
        total_vp = max(1, len(valid_paths))
        
        for i in range(max_depth):
            if prog.wasCanceled(): break
            prog.setLabelText(f"Building Structure for Column {i+1} of {max_depth}...")
            
            lst = self.dynamic_lists[i]['list']
            lst.blockSignals(True); lst.clear()
            
            level_nodes = {}
            for j, vp in enumerate(valid_paths):
                if j % 2000 == 0:
                    col_base_pct = 50 + (i / max(1, max_depth)) * 50
                    col_progress = (j / total_vp) * (50 / max(1, max_depth))
                    prog.setValue(int(col_base_pct + col_progress)); QApplication.processEvents()
                    
                parts = [p for p in vp.split('/') if p]
                if len(parts) > base_depth + i:
                    name = parts[base_depth + i]
                    is_folder = (len(parts) > base_depth + i + 1) or vp.endswith('/')
                    node_v_path = "/" + "/".join(parts[:base_depth + i + 1]) + ("/" if is_folder else "")
                    level_nodes[name] = (node_v_path, is_folder)
            
            f_nodes = [k for k, v in level_nodes.items() if v[1]]
            file_nodes = [k for k, v in level_nodes.items() if not v[1]]
            sorted_names = sorted(f_nodes, key=natural_sort_key) + sorted(file_nodes, key=natural_sort_key)
            display_names = sorted_names[:1000]
            
            for j, name in enumerate(display_names):
                if j % 100 == 0: QApplication.processEvents() 
                    
                node_v_path, is_folder = level_nodes[name]
                l_item = QListWidgetItem(name)
                l_item.setData(Qt.UserRole, node_v_path)
                l_item.setIcon(self._get_icon_for_node(name, is_folder))
                
                if query in name.lower():
                    l_item.setForeground(QBrush(QColor("#2ea043")))
                    font = l_item.font(); font.setBold(True); l_item.setFont(font)
                    
                lst.addItem(l_item)
                
            if len(sorted_names) > 1000:
                warning_item = QListWidgetItem(f"... and {len(sorted_names) - 1000} more items (Refine search)")
                warning_item.setForeground(QBrush(QColor("#e3b341")))
                font = warning_item.font(); font.setItalic(True); warning_item.setFont(font)
                lst.addItem(warning_item)
                
            lst.blockSignals(False)

        prog.setValue(100); prog.close()

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
        action_vman = menu.addAction("🎞 Open in vman Viewer")
        if is_folder: action_link = menu.addAction("🔗 Map THIS Folder to Physical OS")
        else: action_link = None
            
        menu.addSeparator()
        action_edit = menu.addAction("🏷️ Edit Tags")
        action_copy_v = menu.addAction("📋 Copy Virtual Path")
        action_copy_r = menu.addAction("📋 Copy Local OS Path")
        menu.addSeparator()
        action_props = menu.addAction("ℹ️ Properties")
        
        action = menu.exec(list_widget.viewport().mapToGlobal(pos))
        
        if action == action_edit: self.edit_tags_for_item(item)
        elif action == action_link: self.link_specific_path(item)
        elif action == action_vman: 
            if is_folder: QMessageBox.warning(self, "Viewer", "Please select a file to view, not a folder.")
            else:
                real_p = self._get_real_path_for_item(item)
                if not real_p or not os.path.exists(real_p): QMessageBox.warning(self, "Viewer", "Physical file not found.")
                else:
                    ext = os.path.splitext(real_p)[1].lower()
                    playlist = [{'path': real_p, 'name': item.text(), 'ext': ext}]
                    new_viewer = vmanViewer(playlist, 0, self.main_app if self.main_app else self)
                    
                    parent_ref = self.main_app if self.main_app else self
                    if not hasattr(parent_ref, 'active_viewers'): parent_ref.active_viewers = []
                    parent_ref.active_viewers.append(new_viewer)
                    new_viewer.show() 
        elif action == action_open: self.jump_to_virtual_or_real_path(item, force_real=True)
        elif action == action_copy_v: 
            QApplication.clipboard().setText(v_path)
            if self.main_app: self.main_app.status.showMessage("Virtual path copied to clipboard.", 3000)
        elif action == action_copy_r:
            real_p = self._get_real_path_for_item(item)
            if real_p: 
                QApplication.clipboard().setText(real_p)
                if self.main_app: self.main_app.status.showMessage("Physical OS path copied to clipboard.", 3000)
            else: QMessageBox.warning(self, "Copy Failed", "This item has no mapped physical path on your hard drive.")
        elif action == action_props: self.show_item_properties(item)

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
        except Exception: return None

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
                res = conn.cursor().execute("SELECT id, size, extension, modified, real_path, custom_tags, color_tag, secondary_name FROM virtual_fs WHERE parent_path=? AND name=? AND is_folder=?", (parent_path, name, 1 if is_folder else 0)).fetchone()
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
                else: layout.addRow("Status:", QLabel("Virtual Container (Not explicitly tracked in DB)"))
        except Exception as e: layout.addRow("Error:", QLabel(str(e)))
                
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
        else: QMessageBox.warning(self, "Not Mapped", "This item has not been linked to a physical location on your OS.")

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

class HeatmapColorConfigDialog(QDialog):
    def __init__(self, db_path, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.setWindowTitle("🎨 Color Legend & Customization")
        self.resize(500, 600)
        if parent and hasattr(parent, 'styleSheet'): self.setStyleSheet(parent.styleSheet())
        
        self.settings = QSettings("vmanOS", "HeatmapColors")
        self.color_map = self.settings.value("custom_colors", {})
        if not isinstance(self.color_map, dict): self.color_map = {}
        
        # Default Category Colors (High Contrast)
        defaults = {f"Category_{k}": v for k, v in GLOBAL_CAT_COLORS.items()}
        for k, v in defaults.items():
            if k not in self.color_map: self.color_map[k] = v

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("<b>Double-click a color square to change it. Colors are saved permanently.</b>"))
        
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Type", "Name", "Color"])
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table.setColumnWidth(0, 100); self.table.setColumnWidth(2, 80)
        self.table.verticalHeader().setVisible(False)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.cellDoubleClicked.connect(self.edit_color)
        layout.addWidget(self.table)
        
        btn_reset = QPushButton("Reset to Defaults")
        btn_reset.clicked.connect(self.reset_colors)
        layout.addWidget(btn_reset)
        
        self.populate_table()

    def get_distinct_color(self, index):
        hue = (index * 137.508) % 360  # Golden angle distributes colors evenly
        r, g, b = colorsys.hls_to_rgb(hue/360.0, 0.65, 0.85)
        return f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}"

    def populate_table(self):
        self.table.setRowCount(0)
        # Dynamically fetch top extensions from DB to ensure they have colors
        with sqlite3.connect(self.db_path) as conn:
            exts = conn.cursor().execute("SELECT extension, COUNT(id) FROM virtual_fs WHERE is_folder=0 GROUP BY extension ORDER BY COUNT(id) DESC LIMIT 50").fetchall()
            for i, (ext, _) in enumerate(exts):
                ext_clean = str(ext).lower() if ext else "none"
                key = f"Extension_{ext_clean}"
                if key not in self.color_map:
                    self.color_map[key] = self.get_distinct_color(i)
        
        self.settings.setValue("custom_colors", self.color_map)
        
        for k, hex_val in sorted(self.color_map.items()):
            row = self.table.rowCount()
            self.table.insertRow(row)
            parts = k.split("_", 1)
            typ = parts[0]
            name = parts[1] if len(parts) > 1 else ""
            
            self.table.setItem(row, 0, QTableWidgetItem(typ))
            self.table.setItem(row, 1, QTableWidgetItem(name.upper() if typ == "Extension" else name))
            
            color_item = QTableWidgetItem()
            color_item.setBackground(QBrush(QColor(hex_val)))
            color_item.setData(Qt.UserRole, k) # Store key
            self.table.setItem(row, 2, color_item)

    def edit_color(self, row, col):
        if col != 2: return
        item = self.table.item(row, 2)
        key = item.data(Qt.UserRole)
        current_color = QColor(self.color_map.get(key, "#ffffff"))
        
        new_color = QColorDialog.getColor(current_color, self, f"Pick Color for {key}")
        if new_color.isValid():
            hex_color = new_color.name()
            self.color_map[key] = hex_color
            self.settings.setValue("custom_colors", self.color_map)
            item.setBackground(QBrush(new_color))
            if self.parent():
                if hasattr(self.parent(), 'render_heatmap'): self.parent().render_heatmap()
                # Force Calendar to instantly update with the new color
                if hasattr(self.parent(), 'highlight_month') and hasattr(self.parent(), 'calendar'):
                    self.parent().highlight_month(self.parent().calendar.yearShown(), self.parent().calendar.monthShown())

    def reset_colors(self):
        self.settings.remove("custom_colors")
        self.color_map = {}
        defaults = {f"Category_{k}": v for k, v in GLOBAL_CAT_COLORS.items()}
        for k, v in defaults.items(): self.color_map[k] = v
        self.populate_table()
        if self.parent():
            if hasattr(self.parent(), 'render_heatmap'): self.parent().render_heatmap()
            if hasattr(self.parent(), 'highlight_month') and hasattr(self.parent(), 'calendar'):
                self.parent().highlight_month(self.parent().calendar.yearShown(), self.parent().calendar.monthShown())

class RowLimitDialog(QDialog):
    def __init__(self, total_rows, parent=None):
        super().__init__(parent)
        self.total_rows = total_rows
        self.setWindowTitle("Data View Limit")
        self.resize(400, 250)
        if parent and hasattr(parent, 'styleSheet'): self.setStyleSheet(parent.styleSheet())
        
        layout = QVBoxLayout(self)
        lbl = QLabel(f"<b>Found {total_rows:,} matching records.</b><br>Loading millions of rows into the UI grid will reduce performance. How many rows would you like to render in the Activity Log?<br><i>(Note: Visual Charts will still process ALL data instantly).</i>")
        lbl.setWordWrap(True)
        layout.addWidget(lbl)
        
        self.radio_group = QButtonGroup(self)
        self.rb_rec = QRadioButton(f"Recommended (First {min(500, total_rows)} rows)")
        self.rb_rec.setChecked(True)
        self.rb_first_half = QRadioButton(f"First Half ({total_rows // 2} rows)")
        self.rb_last_half = QRadioButton(f"Last Half ({total_rows - (total_rows // 2)} rows)")
        self.rb_all = QRadioButton(f"All Rows ({total_rows}) - ⚠️ May cause UI stutter")
        self.rb_custom = QRadioButton("Custom Range:")
        
        for i, rb in enumerate([self.rb_rec, self.rb_first_half, self.rb_last_half, self.rb_all, self.rb_custom]):
            self.radio_group.addButton(rb, i)
            layout.addWidget(rb)
            
        self.custom_lay = QHBoxLayout()
        self.spin_limit = QSpinBox()
        self.spin_limit.setRange(1, total_rows)
        self.spin_limit.setValue(min(500, total_rows))
        self.spin_limit.setPrefix("Show: ")
        
        self.spin_offset = QSpinBox()
        self.spin_offset.setRange(0, total_rows - 1)
        self.spin_offset.setValue(0)
        self.spin_offset.setPrefix("Skip first: ")
        
        self.custom_lay.addWidget(self.spin_limit)
        self.custom_lay.addWidget(self.spin_offset)
        layout.addLayout(self.custom_lay)
        
        self.rb_custom.toggled.connect(lambda checked: (self.spin_limit.setEnabled(checked), self.spin_offset.setEnabled(checked)))
        self.spin_limit.setEnabled(False); self.spin_offset.setEnabled(False)
        
        btn_box = QHBoxLayout()
        btn_ok = QPushButton("Render Data")
        btn_ok.setStyleSheet("background-color: #2ea043; color: white; font-weight: bold;")
        btn_ok.clicked.connect(self.accept)
        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(self.reject)
        btn_box.addWidget(btn_ok); btn_box.addWidget(btn_cancel)
        layout.addLayout(btn_box)

    def get_values(self):
        idx = self.radio_group.checkedId()
        if idx == 0: return min(500, self.total_rows), 0
        elif idx == 1: return self.total_rows // 2, 0
        elif idx == 2: return self.total_rows - (self.total_rows // 2), self.total_rows // 2
        elif idx == 3: return self.total_rows, 0
        else: return self.spin_limit.value(), self.spin_offset.value()

class HeatmapFilterDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Heatmap Advanced Filters")
        self.resize(450, 400)
        if parent and hasattr(parent, 'styleSheet'): self.setStyleSheet(parent.styleSheet())
        
        self.settings = QSettings("vmanOS", "HeatmapFilters")
        layout = QFormLayout(self)
        
        # --- NEW: Advanced Search & Category Filters ---
        self.txt_search = QLineEdit(str(self.settings.value("search_text", "")))
        self.txt_search.setPlaceholderText("Search string...")
        
        self.combo_match = QComboBox()
        self.combo_match.addItems(["Contains", "Exact Match", "Starts With", "Ends With"])
        self.combo_match.setCurrentText(str(self.settings.value("match_mode", "Contains")))
        
        self.combo_type = QComboBox()
        self.combo_type.addItems(["Files Only", "Folders Only", "Files & Folders"])
        self.combo_type.setCurrentText(str(self.settings.value("data_type", "Files Only")))
        
        self.combo_cat = QComboBox()
        self.combo_cat.addItems(["All"] + list(FILE_CATEGORIES.keys()) + ["Others"])
        self.combo_cat.setCurrentText(str(self.settings.value("category", "All")))
        
        self.txt_skip_names = QLineEdit(str(self.settings.value("skip_names", "")))
        self.txt_skip_names.setPlaceholderText("e.g. thumb.jpg, backup.tar")
        # -----------------------------------------------
        
        self.txt_inc = QLineEdit(str(self.settings.value("include_exts", "")))
        self.txt_inc.setPlaceholderText("e.g. jpg, png, mp4")
        self.txt_exc = QLineEdit(str(self.settings.value("exclude_exts", "")))
        self.txt_exc.setPlaceholderText("e.g. tmp, bak")
        
        self.spin_min = QDoubleSpinBox()
        self.spin_min.setRange(0, 999999)
        self.spin_min.setValue(float(self.settings.value("min_size_mb", 0.0)))
        self.spin_min.setSuffix(" MB")
        
        self.spin_max = QDoubleSpinBox()
        self.spin_max.setRange(0, 999999)
        self.spin_max.setValue(float(self.settings.value("max_size_mb", 999999.0)))
        self.spin_max.setSuffix(" MB")
        
        layout.addRow("Search Name:", self.txt_search)
        layout.addRow("Match Mode:", self.combo_match)
        layout.addRow("Data Type:", self.combo_type)
        layout.addRow("Category:", self.combo_cat)
        layout.addRow("Skip Names:", self.txt_skip_names)
        layout.addRow("Include Exts:", self.txt_inc)
        layout.addRow("Exclude Exts:", self.txt_exc)
        layout.addRow("Min Size:", self.spin_min)
        layout.addRow("Max Size:", self.spin_max)
        
        btn_box = QHBoxLayout()
        btn_clear = QPushButton("Clear Filters")
        btn_clear.clicked.connect(self.clear_filters)
        btn_apply = QPushButton("Apply & Render")
        btn_apply.setStyleSheet("background-color: #2ea043; color: white; font-weight: bold;")
        btn_apply.clicked.connect(self.accept)
        
        btn_box.addWidget(btn_clear)
        btn_box.addWidget(btn_apply)
        layout.addRow(btn_box)

    def clear_filters(self):
        self.txt_search.clear()
        self.combo_match.setCurrentIndex(0)
        self.combo_type.setCurrentIndex(0)
        self.combo_cat.setCurrentIndex(0)
        self.txt_skip_names.clear()
        self.txt_inc.clear()
        self.txt_exc.clear()
        self.spin_min.setValue(0)
        self.spin_max.setValue(999999)

    def accept(self):
        self.settings.setValue("search_text", self.txt_search.text().strip())
        self.settings.setValue("match_mode", self.combo_match.currentText())
        self.settings.setValue("data_type", self.combo_type.currentText())
        self.settings.setValue("category", self.combo_cat.currentText())
        self.settings.setValue("skip_names", self.txt_skip_names.text().strip())
        self.settings.setValue("include_exts", self.txt_inc.text().strip())
        self.settings.setValue("exclude_exts", self.txt_exc.text().strip())
        self.settings.setValue("min_size_mb", self.spin_min.value())
        self.settings.setValue("max_size_mb", self.spin_max.value())
        super().accept()

class TimelineDiaryDialog(QDialog):
    def __init__(self, db_path, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.setWindowTitle("Timeline Diary & Analytics")
        
        # --- PRIVACY ENFORCEMENT: Wipe Heatmap traces on launch, keep only exclusions ---
        heat_settings = QSettings("vmanOS", "HeatmapFilters")
        saved_exc = heat_settings.value("exclude_exts", "")
        heat_settings.clear()
        heat_settings.setValue("exclude_exts", saved_exc)
        # -------------------------------------------------------------------------------
        
        # --Add Minimize, Maximize, and Restore buttons ---
        self.setWindowFlags(self.windowFlags() | Qt.WindowMaximizeButtonHint | Qt.WindowMinimizeButtonHint)

        if sys.platform == "win32":
            self.resize(1100, 670)
            self.setMinimumSize(950, 660)
        elif sys.platform == "darwin":
            self.resize(1105, 670)
            self.setMinimumSize(950, 660)   
        
        
        # FORCE DARK MODE ALWAYS
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

        # --- NEW: Context Menu for Calendar to toggle auto-switch ---
        self.auto_switch_diary = True
        self.calendar.setContextMenuPolicy(Qt.CustomContextMenu)
        self.calendar.customContextMenuRequested.connect(self.show_calendar_context_menu)

        self.date_mode_mod = QRadioButton("Modified")
        self.date_mode_mod.setChecked(True)
        self.date_mode_cre = QRadioButton("Created")
        self.date_mode_cm = QRadioButton("C+M")
        self.date_mode_both = QRadioButton("Both")
        
        self.date_mode_mod.toggled.connect(self.on_date_mode_changed)
        self.date_mode_cre.toggled.connect(self.on_date_mode_changed)
        self.date_mode_cm.toggled.connect(self.on_date_mode_changed)
        self.date_mode_both.toggled.connect(self.on_date_mode_changed)
        
        rb_lay = QHBoxLayout()
        rb_lay.addWidget(self.date_mode_mod)
        rb_lay.addWidget(self.date_mode_cre)
        rb_lay.addWidget(self.date_mode_cm)
        rb_lay.addWidget(self.date_mode_both)
        left_lay.addLayout(rb_lay)
        

        
        # --- FIXED: Wire up dynamic month highlighting and click routing ---
        self.calendar.currentPageChanged.connect(self.highlight_month)
        self.calendar.clicked.connect(self.on_calendar_clicked)
        # -------------------------------------------------------------------
        
        # --- DYNAMIC CALENDAR STYLING ---
        self.update_calendar_style()
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
        
        # Ensure setting property exists
        self.settings = QSettings("vmanOS", "TimelineSettings")
        self.calendar_view_mode = self.settings.value("calendar_view_mode", "Default (Green Highlight)")

        # --- TAB 1: HTML Diary Reader ---
        tab_diary = QWidget()
        diary_lay = QVBoxLayout(tab_diary)
        diary_lay.setContentsMargins(0, 0, 0, 0)
        self.diary_browser = QTextBrowser()
        
        # Custom Thin Scrollbar & Sleek Dark Mode Styling
        self.diary_browser.setStyleSheet("""
            QTextBrowser { background-color: #0d1117; border: none; padding: 10px; color: #c9d1d9; }
            QScrollBar:vertical { border: none; background: #0d1117; width: 8px; margin: 0px; }
            QScrollBar::handle:vertical { background: #30363d; min-height: 30px; border-radius: 4px; }
            QScrollBar::handle:vertical:hover { background: #58a6ff; }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { border: none; background: none; }
        """)
        diary_lay.addWidget(self.diary_browser)
        self.tabs.addTab(tab_diary, "📖 Daily Diary")

        # --- TAB 2: Data Table ---
        tab_data = QWidget()
        data_lay = QVBoxLayout(tab_data)
        data_lay.setContentsMargins(0, 0, 0, 0)
        
        # NEW: Local Activity Filter Bar
        local_filter_lay = QHBoxLayout()
        local_filter_lay.setContentsMargins(10, 10, 10, 5)
        self.log_search_box = QLineEdit()
        self.log_search_box.setPlaceholderText("🔍 Filter Name or Virtual Location...")
        self.log_search_box.textChanged.connect(self.filter_activity_log)
        
        self.log_cat_box = QComboBox()
        self.log_cat_box.addItems(["All Types"] + list(FILE_CATEGORIES.keys()) + ["Others"])
        self.log_cat_box.setEditable(True)
        self.log_cat_box.lineEdit().setPlaceholderText("Or type ext (e.g. .jpg)")
        self.log_cat_box.currentTextChanged.connect(self.filter_activity_log)
        
        local_filter_lay.addWidget(self.log_search_box, stretch=1)
        local_filter_lay.addWidget(self.log_cat_box)
        data_lay.addLayout(local_filter_lay)
        
        self.table = QTableWidget(0, 9)
        self.table.setIconSize(QSize(20, 20))
        self.table.setHorizontalHeaderLabels(["S.No.", "Name", "Type", "Ext", "Size", "Modified Date", "Virtual Location", "Tags", "ID"])
        self.table.setSortingEnabled(True)
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)
        self.table.doubleClicked.connect(self.open_scanned_file)
        
        # --- HEADER CONTEXT MENU ---
        self.table.horizontalHeader().setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.horizontalHeader().customContextMenuRequested.connect(self.show_header_menu)
        
        self.table.setColumnWidth(0, 50); self.table.setColumnWidth(1, 220); self.table.setColumnWidth(2, 80)
        # Establish default widths for all 9 columns
        self.table.setColumnWidth(0, 50)
        self.table.setColumnWidth(1, 220)
        self.table.setColumnWidth(2, 80)
        self.table.setColumnWidth(3, 60)
        self.table.setColumnWidth(4, 80)
        self.table.setColumnWidth(5, 140)
        self.table.setColumnWidth(6, 300) # Virtual Location (~3 inches)
        self.table.setColumnWidth(7, 150) # Tags
        self.table.setColumnWidth(8, 120) # ID
        
        # Completely unlock column boundaries to allow infinite rightward resizing
        self.table.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.table.horizontalHeader().setStretchLastSection(False)
        for c in range(9):
            self.table.horizontalHeader().setSectionResizeMode(c, QHeaderView.Interactive)
            
        self.table.setColumnHidden(0, True) # Hide S.No by default
        self.table.setColumnHidden(8, True) # Hide ID by default
        
        self.settings = QSettings("vmanOS", "TimelineSettings")
        saved_state = self.settings.value("table_state")
        if saved_state: self.table.horizontalHeader().restoreState(saved_state)
        
        # --- PERMANENT RESIZE FIX: Override any corrupted saved states ---
        self.table.horizontalHeader().setStretchLastSection(False)
        self.table.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        for c in range(9):
            self.table.horizontalHeader().setSectionResizeMode(c, QHeaderView.Interactive)
            # Failsafe: if a column got squished to 0 in memory, reset it to make the drag handle appear
            if self.table.columnWidth(c) < 30: 
                self.table.setColumnWidth(c, 150)
        # -----------------------------------------------------------------

        data_lay.addWidget(self.table)
        self.tabs.addTab(tab_data, "📋 Activity Log")
        
        # --- TAB 3: Visual Analytics ---
        tab_charts = QWidget()
        chart_lay = QVBoxLayout(tab_charts)
        
        top_chart_bar = QHBoxLayout()
        top_chart_bar.addWidget(QLabel("<b>Chart Metric:</b>"))
        self.cb_chart_metric = QComboBox()
        self.cb_chart_metric.addItems([
            "File Count by Extension", "Storage Size by Extension", "Storage Usage by Year",
            "File Age Distribution", "Top 10 Largest Files", "File Modification Timeline",
            "File Size Distribution", "Tag Utilization"
        ])
        self.cb_chart_metric.currentTextChanged.connect(self.force_chart_redraw)
        top_chart_bar.addWidget(self.cb_chart_metric, stretch=1)
        chart_lay.addLayout(top_chart_bar)
        
        self.fig = Figure(figsize=(8, 5), dpi=100)
        self.fig.patch.set_facecolor('#0d1117')
        self.canvas = FigureCanvasQTAgg(self.fig)
        chart_lay.addWidget(self.canvas, stretch=1)
        self.tabs.addTab(tab_charts, "📊 Visual Analytics")

        # --- TAB 4: Monthly Chart (NEW) ---
        tab_monthly = QWidget()
        monthly_lay = QVBoxLayout(tab_monthly)
        
        top_monthly_bar = QHBoxLayout()
        top_monthly_bar.addWidget(QLabel("<b>Chart Metric:</b>"))
        self.cb_monthly_metric = QComboBox()
        self.cb_monthly_metric.addItems([
            "Daily Activity (Count)", "Daily Activity (Size)", 
            "By Category (Count)", "By Category (Size)", 
            "By Extension (Count)", "By Extension (Size)", 
            "Tag Utilization", "Top 10 Largest Files"
        ])
        self.cb_monthly_metric.currentTextChanged.connect(self.render_monthly_chart)
        top_monthly_bar.addWidget(self.cb_monthly_metric, stretch=1)
        monthly_lay.addLayout(top_monthly_bar)

        self.fig_monthly = Figure(figsize=(8, 5), dpi=100)
        self.fig_monthly.patch.set_facecolor('#0d1117')
        self.canvas_monthly = FigureCanvasQTAgg(self.fig_monthly)
        
        monthly_scroll = QScrollArea()
        monthly_scroll.setWidgetResizable(True)
        monthly_scroll.setWidget(self.canvas_monthly)
        monthly_lay.addWidget(monthly_scroll)
        self.tabs.addTab(tab_monthly, "📅 Monthly Chart")

        # --- TAB 5: Yearly Heatmap (GitHub Style) ---
        tab_heatmap = QWidget()
        heat_lay = QVBoxLayout(tab_heatmap)
        
        heat_bar = QHBoxLayout()
        self.cb_heat_year = QComboBox()
        self.cb_heat_mode = QComboBox()
        
        # Clean fluid options (No repeated colors!)
        self.cb_heat_mode.addItems([
            "Activity Volume by Count", 
            "Data Volume by Size", 
            "Dominant Category by Count",  
            "Dominant Category by Size",  
            "Dominant Extension by Count",
            "Dominant Extension by Size",
            "Forensic: Tagging Activity",
            "Forensic: Average File Size",
            "Forensic: Max Single File Size"
        ])
        
        self.btn_heat_render = QPushButton("Generate Heatmap")
        self.btn_heat_render.setStyleSheet("QPushButton { background-color: rgba(88, 166, 255, 0.1); color: #58a6ff; border: 1px solid rgba(88, 166, 255, 0.4); border-radius: 5px; padding: 5px 15px; font-weight: bold; } QPushButton:hover { background-color: rgba(88, 166, 255, 0.25); border: 1px solid #58a6ff; color: #ffffff; }")
        self.btn_heat_render.clicked.connect(self.render_heatmap)
        
        self.btn_heat_colors = QPushButton("Colors")
        self.btn_heat_colors.setStyleSheet("QPushButton { background-color: rgba(227, 179, 65, 0.1); color: #e3b341; border: 1px solid rgba(227, 179, 65, 0.4); border-radius: 5px; padding: 5px 15px; font-weight: bold; } QPushButton:hover { background-color: rgba(227, 179, 65, 0.25); border: 1px solid #e3b341; color: #ffffff; }")
        self.btn_heat_colors.clicked.connect(self.open_color_manager)
        
        self.btn_heat_grid = QPushButton("Settings")
        self.btn_heat_grid.setStyleSheet("QPushButton { background-color: rgba(139, 148, 158, 0.1); color: #8b949e; border: 1px solid rgba(139, 148, 158, 0.4); border-radius: 5px; padding: 5px 15px; font-weight: bold; } QPushButton:hover { background-color: rgba(139, 148, 158, 0.25); border: 1px solid #c9d1d9; color: #ffffff; }")
        self.btn_heat_grid.clicked.connect(self.show_grid_settings_menu)
        
        heat_bar.addWidget(QLabel("<b>Year:</b>"))
        heat_bar.addWidget(self.cb_heat_year)
        heat_bar.addWidget(QLabel("<b>Mode:</b>"))
        heat_bar.addWidget(self.cb_heat_mode)
        heat_bar.addWidget(self.btn_heat_render)
        heat_bar.addWidget(self.btn_heat_colors)
        heat_bar.addWidget(self.btn_heat_grid)
        heat_bar.addStretch()
        heat_lay.addLayout(heat_bar)
        
        # Heatmap advanced filters
        self.fig_heat = Figure(figsize=(10, 3), dpi=100)
        self.fig_heat.patch.set_facecolor('#0d1117')
        self.canvas_heat = FigureCanvasQTAgg(self.fig_heat)
        self.canvas_heat.mpl_connect('button_press_event', self.on_heatmap_click)
        
        heat_scroll_area = QScrollArea()
        heat_scroll_area.setWidgetResizable(True)
        heat_scroll_area.setWidget(self.canvas_heat)
        heat_lay.addWidget(heat_scroll_area)
        
        self.tabs.addTab(tab_heatmap, "📅 Yearly Activity")

        # --- TAB 6: Deep Report (NEW) ---
        tab_report = QWidget()
        rep_lay = QVBoxLayout(tab_report)
        rep_lay.setContentsMargins(0, 0, 0, 0)

        # --- Report Filter Bar (SLIM & SMALL TEXT) ---
        rep_filter_bg = QWidget()
        rep_filter_bg.setStyleSheet("background-color: #161b22; border-bottom: 1px solid #30363d;")
        rep_filter_lay = QHBoxLayout(rep_filter_bg)
        rep_filter_lay.setContentsMargins(5, 4, 5, 4) # Slim margins
        
        self.cb_rep_from_year = QComboBox(); self.cb_rep_from_year.setMinimumWidth(65)
        self.cb_rep_to_year = QComboBox(); self.cb_rep_to_year.setMinimumWidth(65)
        self.cb_rep_month = QComboBox(); self.cb_rep_month.setMinimumWidth(110) # <-- Increased width for month names
        self.cb_rep_month.addItems(["All", "01 - January", "02 - February", "03 - March", "04 - April", "05 - May", "06 - June", "07 - July", "08 - August", "09 - September", "10 - October", "11 - November", "12 - December"])
        self.cb_rep_dow = QComboBox()
        self.cb_rep_dow.addItems(["All", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
        self.cb_rep_groupby = QComboBox()
        self.cb_rep_groupby.addItems(["Day", "Week", "Month", "Year", "Day of Week"])
        
        # Apply smaller font AND slim dropdown scrollbars
        slim_combo_style = """
            QComboBox { font-size: 11px; padding: 2px; }
            QComboBox QAbstractItemView {
                background: #161b22;
                border: 1px solid #30363d;
            }
            QComboBox QAbstractItemView QScrollBar:vertical {
                border: none; background: #0d1117; width: 6px; margin: 0px;
            }
            QComboBox QAbstractItemView QScrollBar::handle:vertical {
                background: #30363d; border-radius: 3px; min-height: 20px;
            }
            QComboBox QAbstractItemView QScrollBar::handle:vertical:hover {
                background: #58a6ff;
            }
            QComboBox QAbstractItemView QScrollBar::add-line:vertical, 
            QComboBox QAbstractItemView QScrollBar::sub-line:vertical {
                border: none; background: none; height: 0px;
            }
        """
        for cb in [self.cb_rep_from_year, self.cb_rep_to_year, self.cb_rep_month, self.cb_rep_dow, self.cb_rep_groupby]:
            cb.setStyleSheet(slim_combo_style)

        self.btn_rep_generate = QPushButton("📈 Generate")
        self.btn_rep_generate.setStyleSheet("QPushButton { background-color: #243447; color: #dbe7f3; font-weight: bold; padding: 4px 12px; border-radius: 4px; font-size: 11px; } QPushButton:hover { background-color: #2f4358; }")
        self.btn_rep_generate.clicked.connect(self.generate_deep_report)

        rep_filter_lay.addWidget(QLabel("<span style='font-size: 11px;'><b>From:</b></span>")); rep_filter_lay.addWidget(self.cb_rep_from_year)
        rep_filter_lay.addWidget(QLabel("<span style='font-size: 11px;'><b>To:</b></span>")); rep_filter_lay.addWidget(self.cb_rep_to_year)
        rep_filter_lay.addWidget(QLabel("<span style='font-size: 11px;'><b>Month:</b></span>")); rep_filter_lay.addWidget(self.cb_rep_month)
        rep_filter_lay.addWidget(QLabel("<span style='font-size: 11px;'><b>Day:</b></span>")); rep_filter_lay.addWidget(self.cb_rep_dow)
        rep_filter_lay.addWidget(QLabel("<span style='font-size: 11px;'><b>Group By:</b></span>")); rep_filter_lay.addWidget(self.cb_rep_groupby)
        rep_filter_lay.addSpacing(5)
        rep_filter_lay.addWidget(self.btn_rep_generate)
        rep_filter_lay.addStretch()

        rep_lay.addWidget(rep_filter_bg)

        self.rep_scroll = QScrollArea()
        self.rep_scroll.setWidgetResizable(True)
        self.rep_scroll.setFrameShape(QScrollArea.NoFrame)
        self.rep_scroll.setStyleSheet("QScrollArea { background-color: #0d1117; }")
        
        self.rep_container = QWidget()
        self.rep_vbox = QVBoxLayout(self.rep_container)
        self.rep_vbox.setSpacing(25) 
        self.rep_vbox.setContentsMargins(20, 15, 20, 30)

        # Overview Stats (SMALL TEXT)
        self.rep_lbl_overview = QLabel("<b>Overview:</b> Select filters and click Generate Report.")
        self.rep_lbl_overview.setStyleSheet("font-size: 12px; color: #c9d1d9; background: #161b22; padding: 12px; border-radius: 6px; border: 1px solid #30363d; line-height: 1.4;")
        self.rep_lbl_overview.setWordWrap(True)
        self.rep_vbox.addWidget(self.rep_lbl_overview)

        # Charts Canvas
        self.fig_rep = Figure(figsize=(12, 9), dpi=100) 
        self.fig_rep.patch.set_facecolor('#0d1117')
        self.canvas_rep = FigureCanvasQTAgg(self.fig_rep)
        self.canvas_rep.setMinimumHeight(700) 
        self.rep_vbox.addWidget(self.canvas_rep)
        
        # --- CTRL+SCROLL TO ZOOM FEATURE ---
        def on_rep_scroll(event):
            from PySide6.QtGui import QGuiApplication
            from PySide6.QtCore import Qt
            
            # Use PySide6 hardware detection to perfectly catch Ctrl key on Linux/Windows
            modifiers = QGuiApplication.keyboardModifiers()
            if event.inaxes and (modifiers == Qt.ControlModifier):
                ax = event.inaxes
                # Scroll up (step > 0) = Zoom In, Scroll down = Zoom Out
                scale_factor = 0.85 if event.step > 0 else 1.15
                
                xdata, ydata = event.xdata, event.ydata
                if xdata is None or ydata is None: return
                
                xlim, ylim = ax.get_xlim(), ax.get_ylim()
                
                # Math to zoom directly into the mouse cursor position
                new_xlim = [xdata + (x - xdata) * scale_factor for x in xlim]
                new_ylim = [ydata + (y - ydata) * scale_factor for y in ylim]
                
                ax.set_xlim(new_xlim)
                ax.set_ylim(new_ylim)
                self.canvas_rep.draw_idle()
                
        self.canvas_rep.mpl_connect('scroll_event', on_rep_scroll)

        # Helper method to create styled, SORTABLE tables
        def create_rep_table(headers):
            tbl = QTableWidget(0, len(headers))
            tbl.setHorizontalHeaderLabels(headers)
            tbl.verticalHeader().setVisible(False)
            tbl.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
            tbl.horizontalHeader().setStretchLastSection(True)
            tbl.setEditTriggers(QAbstractItemView.NoEditTriggers)
            tbl.setSelectionBehavior(QAbstractItemView.SelectRows)
            tbl.setCursor(Qt.PointingHandCursor)
            tbl.setSortingEnabled(True) # ENABLE SORTING
            tbl.setMinimumHeight(350) 
            tbl.setAlternatingRowColors(True)
            tbl.setStyleSheet("""
                QTableWidget { background-color: #0d1117; alternate-background-color: #161b22; border: 1px solid #30363d; border-radius: 6px; }
                QHeaderView::section { background-color: #21262d; color: #c9d1d9; font-weight: bold; padding: 8px; border: 1px solid #30363d; }
            """)
            tbl.cellDoubleClicked.connect(self.handle_report_drilldown)
            return tbl

        # Initialize Tables
        self.tbl_rep_group = create_rep_table(["Group", "Files", "Size"])
        self.tbl_rep_cat = create_rep_table(["Category", "Files", "Size"])
        self.tbl_rep_ext = create_rep_table(["Extension", "Files", "Size"])
        self.tbl_rep_tag = create_rep_table(["Tag", "Files", "Size", "Avg Size"])
        self.tbl_rep_largest = create_rep_table(["Largest Files", "Size", "Type", "ID"])
        self.tbl_rep_smallest = create_rep_table(["Smallest Files", "Size", "Type", "ID"])
        
        self.tbl_rep_ext_by_cat = create_rep_table(["Category", "Extension", "File Count", "Total Size"])
        self.tbl_rep_common_sizes = create_rep_table(["Exact File Size", "Number of Files", "Total Volume"])

        # Hide internal ID columns
        self.tbl_rep_largest.setColumnHidden(3, True)
        self.tbl_rep_smallest.setColumnHidden(3, True)
        
        # FIX: Allow interactive resizing while still stretching to fill the right side!
        self.tbl_rep_largest.horizontalHeader().setSectionResizeMode(0, QHeaderView.Interactive)
        self.tbl_rep_smallest.horizontalHeader().setSectionResizeMode(0, QHeaderView.Interactive)

        # Helper to add full-width sections cleanly
        def add_section(title, subtitle, widget):
            header = QLabel(f"<span style='font-size: 20px; color: #58a6ff; font-weight: bold;'>{title}</span><br><span style='color: #8b949e; font-size: 13px;'>{subtitle}</span>")
            self.rep_vbox.addWidget(header)
            self.rep_vbox.addWidget(widget)

        # Add Full-Width Stacked Layout
        add_section("📅 Temporal Grouping", "Double-click any row to filter the Activity Log to that specific time period.", self.tbl_rep_group)
        add_section("📁 Data by Category", "Distribution of storage and file counts across major file categories.", self.tbl_rep_cat)
        
        add_section("🗂️ Extensions by Category", "Detailed breakdown of file formats within each category.", self.tbl_rep_ext_by_cat)
        add_section("📊 Most Common File Sizes", "Groups files by their exact byte size to find massively duplicated or identical files.", self.tbl_rep_common_sizes)
        
        add_section("📄 Top 50 Extensions", "Most dominant file formats matching your current filters.", self.tbl_rep_ext)
        add_section("🏷️ Custom Tag Utilization", "File metrics grouped by your user-defined tags.", self.tbl_rep_tag)
        add_section("🐘 Top 20 Largest Files", "Heaviest outliers in the current dataset (Double click to locate).", self.tbl_rep_largest)
        add_section("🦠 Top 20 Smallest Files", "Smallest tracked items (Double click to locate).", self.tbl_rep_smallest)
        
        self.rep_vbox.addStretch()
        self.rep_scroll.setWidget(self.rep_container)
        rep_lay.addWidget(self.rep_scroll)

        self.tabs.addTab(tab_report, "📑 Report")


        # Layout Assembly
        self.splitter.addWidget(left_widget)
        self.splitter.addWidget(self.tabs)
        self.splitter.setSizes([300, 800])
        main_layout.addWidget(self.splitter)
        
        self.latest_analytics = None
        self.is_populating = False
        
        # Set default tab switch behavior (0 = Diary, 1 = Activity Log, etc)
        self.auto_switch_target = 0
        
        self.cb_year.currentTextChanged.connect(lambda: self.update_smart_filters('year'))
        self.cb_category.currentTextChanged.connect(lambda: self.update_smart_filters('category'))
        
        self.populate_dropdowns()
        
        today = QDate.currentDate()
        self.calendar.setSelectedDate(today)
        self.highlight_month(today.year(), today.month())

    def on_heatmap_click(self, event):
        if event.inaxes != self.fig_heat.axes[0] or event.xdata is None or event.ydata is None: return
        year_str = self.cb_heat_year.currentText()
        if not year_str or year_str == "All": return
        try: start_date = dt_lib.date(int(year_str), 1, 1)
        except ValueError: return
        
        grid_settings = QSettings("vmanOS", "HeatmapGridSettings")
        block_spacing = grid_settings.value("block_spacing", "Normal")
        gap_x, gap_y = 0.2, 0.2
        if block_spacing == "Touching (No Gap)": gap_x, gap_y = 0.0, 0.0
        elif block_spacing == "Wide": gap_x, gap_y = 0.5, 0.5
        elif block_spacing == "Custom":
            gap_x = grid_settings.value("custom_gap_x", 0.1, type=float)
            gap_y = grid_settings.value("custom_gap_y", 0.1, type=float)
            
        layout_style = grid_settings.value("layout_style", "Standard")
        if layout_style == "Pure GitHub": gap_x, gap_y = 0.2, 0.2
        elif layout_style == "Weeks View (Spreadsheet)": gap_x, gap_y = 0.0, 0.0
        
        col = int(event.xdata / (1.0 + gap_x))
        row = int(event.ydata / (1.0 + gap_y))
        
        start_weekday = start_date.weekday()
        day_offset = (col * 7) + row - start_weekday
        
        if 0 <= day_offset <= (dt_lib.date(int(year_str), 12, 31) - start_date).days:
            clicked_date = start_date + dt_lib.timedelta(days=day_offset)
            from PySide6.QtCore import QDate
            qdate = QDate(clicked_date.year, clicked_date.month, clicked_date.day)
            self.calendar.setSelectedDate(qdate)
            
            # --- START FILTER INJECTION LOGIC ---
            date_str = qdate.toString("yyyy-MM-dd")
            db_col = "creation_date" if getattr(self, 'date_mode_cre', None) and self.date_mode_cre.isChecked() else "modified"
            
            filter_settings = QSettings("vmanOS", "HeatmapFilters")
            search_text = str(filter_settings.value("search_text", "")).strip()
            match_mode = str(filter_settings.value("match_mode", "Contains"))
            data_type = str(filter_settings.value("data_type", "Files Only"))
            category = str(filter_settings.value("category", "All"))
            skip_names = [x.strip().lower() for x in str(filter_settings.value("skip_names", "")).split(',') if x.strip()]
            inc_exts = [x.strip().lower() for x in str(filter_settings.value("include_exts", "")).split(',') if x.strip()]
            exc_exts = [x.strip().lower() for x in str(filter_settings.value("exclude_exts", "")).split(',') if x.strip()]
            min_bytes = int(float(filter_settings.value("min_size_mb", 0.0)) * 1024 * 1024)
            max_bytes = int(float(filter_settings.value("max_size_mb", 999999.0)) * 1024 * 1024)
            
            where_sql = f"{db_col} LIKE ? AND in_trash=0"
            params = [f"{date_str}%"]
            
            if data_type == "Files Only": where_sql += " AND is_folder=0"
            elif data_type == "Folders Only": where_sql += " AND is_folder=1"
            
            if category != "All":
                where_sql += " AND category=?"
                params.append(category)
                
            if search_text:
                where_sql += " AND LOWER(name) LIKE ?"
                if match_mode == "Exact Match": params.append(search_text.lower())
                elif match_mode == "Starts With": params.append(f"{search_text.lower()}%")
                elif match_mode == "Ends With": params.append(f"%{search_text.lower()}")
                else: params.append(f"%{search_text.lower()}%")
                
            if skip_names:
                for skip_val in skip_names:
                    where_sql += " AND LOWER(name) NOT LIKE ?"
                    params.append(f"%{skip_val}%")
            
            if inc_exts:
                ext_placeholders = ",".join(["?"] * len(inc_exts))
                inc_exts = [ext if ext.startswith('.') else f".{ext}" for ext in inc_exts]
                where_sql += f" AND LOWER(extension) IN ({ext_placeholders})"
                params.extend(inc_exts)
                
            if exc_exts:
                ext_placeholders = ",".join(["?"] * len(exc_exts))
                exc_exts = [ext if ext.startswith('.') else f".{ext}" for ext in exc_exts]
                where_sql += f" AND LOWER(extension) NOT IN ({ext_placeholders})"
                params.extend(exc_exts)
                
            if min_bytes > 0:
                where_sql += " AND size >= ?"
                params.append(min_bytes)
            if max_bytes < int(999999.0 * 1024 * 1024):
                where_sql += " AND size <= ?"
                params.append(max_bytes)
            
            query = f"SELECT id, name, is_folder, extension, size, parent_path, {db_col}, custom_tags FROM virtual_fs WHERE {where_sql}"
            self.execute_search(query, tuple(params), update_highlights=False)
            
            # Update HTML Diary with exactly the same constraints
            with sqlite3.connect(self.db_path) as conn:
                diary_query = f"SELECT SUBSTR({db_col}, 12, 8), name, parent_path, size, category FROM virtual_fs WHERE {where_sql} ORDER BY {db_col} ASC LIMIT 150"
                entries = conn.cursor().execute(diary_query, tuple(params)).fetchall()
                
            html = f"<h1 style='color:#58a6ff; text-align:center;'>📖 System Timeline: {qdate.toString('dddd, MMMM d, yyyy')}</h1><hr>"
            if not entries: 
                html += "<h3 style='color:#8b949e; text-align:center;'><br><br>No system activity recorded matching these filters.</h3>"
            else:
                if len(entries) == 150:
                    html += f"<p style='color:#e3b341; text-align:center;'><b>Showing first 150 activities. Check Activity Log tab for full list.</b></p><br>"
                else:
                    html += f"<p style='color:#c9d1d9; text-align:center;'><b>{len(entries)}</b> files were logged.</p><br>"
                
                html += "<ul style='list-style-type: none; padding-left: 0;'>"
                cat_colors = {"Images": "#a371f7", "Videos": "#f85149", "Audio": "#ff7b72", "Documents": "#d2a8ff", "Code": "#79c0ff", "Others": "#8b949e"}
                action_verb = "Created" if getattr(self, 'date_mode_cre', None) and self.date_mode_cre.isChecked() else "Modified"
                
                for time_str, name, pp, size, cat in entries:
                    c_color = cat_colors.get(cat, "#8b949e")
                    try: safe_size = human_size(size)
                    except: safe_size = f"{size} bytes"
                    
                    html += f"<li style='margin-bottom: 15px; background-color: rgba(33, 38, 45, 0.6); padding: 12px; border-left: 5px solid {c_color}; border-radius: 6px;'><span style='color: #58a6ff; font-size: 15px;'><b>🕒 {time_str}</b></span><br><span style='font-size: 16px; color: white;'>{action_verb} <b style='color: {c_color};'>{name}</b></span> <span style='color: #8b949e; font-size: 13px;'>({safe_size})</span><br><span style='color: #8b949e; font-size: 13px;'>Path: {pp}</span></li>"
                html += "</ul>"
            self.diary_browser.setHtml(html)
            # --- END FILTER INJECTION LOGIC ---

            self.tabs.setCurrentIndex(1) # Instantly switch to Activity Log
            
    def generate_deep_report(self):
        try:
            # --- AUTO-PATCH: Fix Stale or Missing Categories in Database ---
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("UPDATE virtual_fs SET category = 'Archives' WHERE LOWER(extension) IN ('.zip', '.rar', '.7z', '.tar', '.gz', 'zip', 'rar', '7z', 'tar', 'gz')")
                conn.execute("UPDATE virtual_fs SET category = 'Archives' WHERE category = 'Archive'")
                conn.commit()

            # --- SETUP FILTERS ---
            f_year = self.cb_rep_from_year.currentText()
            t_year = self.cb_rep_to_year.currentText()
            month = self.cb_rep_month.currentText()
            dow = self.cb_rep_dow.currentText()
            groupby = self.cb_rep_groupby.currentText()

            col = "creation_date" if getattr(self, 'date_mode_cre', None) and self.date_mode_cre.isChecked() else "modified"

            where_clauses = ["is_folder=0", "in_trash=0", f"{col} IS NOT NULL", f"{col} != ''"]
            params = []

            if f_year and f_year != "All":
                where_clauses.append(f"SUBSTR({col}, 1, 4) >= ?"); params.append(f_year)
            if t_year and t_year != "All":
                where_clauses.append(f"SUBSTR({col}, 1, 4) <= ?"); params.append(t_year)
            if month != "All":
                where_clauses.append(f"SUBSTR({col}, 6, 2) = ?"); params.append(month[:2])

            dow_map = {"Sunday": "0", "Monday": "1", "Tuesday": "2", "Wednesday": "3", "Thursday": "4", "Friday": "5", "Saturday": "6"}
            if dow != "All":
                where_clauses.append(f"strftime('%w', {col}) = ?"); params.append(dow_map[dow])

            base_where = " AND ".join(where_clauses)
            self.rep_base_where = base_where
            self.rep_base_params = tuple(params)
            self.rep_date_col = col

            prog = QProgressDialog("Generating Deep Report Analytics...", "Cancel", 0, 100, self)
            prog.setWindowModality(Qt.WindowModal)
            prog.show(); QApplication.processEvents()

            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()

                # 1. Overview
                prog.setLabelText("Calculating Overview..."); prog.setValue(10); QApplication.processEvents()
                cur.execute(f"SELECT COUNT(id), SUM(size), AVG(size), MAX(size), MIN(size) FROM virtual_fs WHERE {base_where}", params)
                tot_count, tot_size, avg_size, max_size, min_size = cur.fetchone()

                if not tot_count:
                    self.rep_lbl_overview.setText("<b>Overview:</b> No data found for the selected filters.")
                    self.render_report_charts([], []) # Clear charts
                    prog.close()
                    return

                self.rep_lbl_overview.setText(f"<b>Overview:</b> <b>Total Files:</b> {tot_count:,} &nbsp;|&nbsp; <b>Total Size:</b> {human_size(tot_size)} &nbsp;|&nbsp; <b>Avg Size:</b> {human_size(avg_size)} &nbsp;|&nbsp; <b>Max Size:</b> {human_size(max_size)} &nbsp;|&nbsp; <b>Min Size:</b> {human_size(min_size)}")

                # 2. Group By Engine
                prog.setLabelText("Grouping Data..."); prog.setValue(20); QApplication.processEvents()
                if groupby == "Day": gb_sql = f"SUBSTR({col}, 1, 10)"
                elif groupby == "Month": gb_sql = f"SUBSTR({col}, 1, 7)"
                elif groupby == "Year": gb_sql = f"SUBSTR({col}, 1, 4)"
                elif groupby == "Week": gb_sql = f"strftime('%W', {col})"
                elif groupby == "Day of Week": gb_sql = f"strftime('%w', {col})"

                cur.execute(f"SELECT {gb_sql}, COUNT(id), SUM(size) FROM virtual_fs WHERE {base_where} GROUP BY {gb_sql} ORDER BY {gb_sql}", params)
                grp_data = cur.fetchall()
                self._fill_report_table(self.tbl_rep_group, grp_data, format_size=True, map_dow=(groupby=="Day of Week"))

                # 3. Categories (Dynamic to new massive list)
                prog.setLabelText("Analyzing Categories..."); prog.setValue(40); QApplication.processEvents()
                cur.execute(f"SELECT category, COUNT(id), SUM(size) FROM virtual_fs WHERE {base_where} GROUP BY category", params)
                cat_data_raw = cur.fetchall()
                
                db_cat_dict = {r[0] if r[0] else "Others": (r[1], r[2]) for r in cat_data_raw}
                cat_order = list(FILE_CATEGORIES.keys()) + ["Others"]
                cat_data = []
                for c in cat_order:
                    count, size = db_cat_dict.get(c, (0, 0))
                    cat_data.append((c, count, size))
                    
                self._fill_report_table(self.tbl_rep_cat, cat_data, format_size=True, custom_order_list=cat_order)
                
                # 4. Extensions
                prog.setLabelText("Analyzing Extensions..."); prog.setValue(60); QApplication.processEvents()
                cur.execute(f"SELECT extension, COUNT(id), SUM(size) FROM virtual_fs WHERE {base_where} GROUP BY extension ORDER BY SUM(size) DESC LIMIT 50", params)
                self._fill_report_table(self.tbl_rep_ext, cur.fetchall(), format_size=True)

                # 5. Tags
                prog.setLabelText("Analyzing Tags..."); prog.setValue(70); QApplication.processEvents()
                cur.execute(f"SELECT custom_tags, COUNT(id), SUM(size), AVG(size) FROM virtual_fs WHERE {base_where} AND custom_tags IS NOT NULL AND custom_tags != '' GROUP BY custom_tags ORDER BY SUM(size) DESC LIMIT 50", params)
                tag_data = cur.fetchall()
                self.tbl_rep_tag.setSortingEnabled(False)
                self.tbl_rep_tag.setRowCount(0)
                for r, row_data in enumerate(tag_data):
                    self.tbl_rep_tag.insertRow(r)
                    self.tbl_rep_tag.setItem(r, 0, QTableWidgetItem(str(row_data[0])))
                    self.tbl_rep_tag.setItem(r, 1, SizeTableWidgetItem(row_data[1] or 0)) 
                    self.tbl_rep_tag.setItem(r, 2, SizeTableWidgetItem(row_data[2] or 0))
                    self.tbl_rep_tag.setItem(r, 3, SizeTableWidgetItem(row_data[3] or 0))
                self.tbl_rep_tag.setSortingEnabled(True)

                # 6. Extremes
                prog.setLabelText("Finding Extremes..."); prog.setValue(80); QApplication.processEvents()
                cur.execute(f"SELECT name, size, extension, id FROM virtual_fs WHERE {base_where} ORDER BY size DESC LIMIT 20", params)
                self._fill_report_table(self.tbl_rep_largest, cur.fetchall(), is_files=True)

                cur.execute(f"SELECT name, size, extension, id FROM virtual_fs WHERE {base_where} AND size > 0 ORDER BY size ASC LIMIT 20", params)
                self._fill_report_table(self.tbl_rep_smallest, cur.fetchall(), is_files=True)
                
                # 7. Extensions By Category
                prog.setLabelText("Analyzing Extensions by Category..."); prog.setValue(84); QApplication.processEvents()
                cur.execute(f"SELECT category, extension, COUNT(id), SUM(size) FROM virtual_fs WHERE {base_where} AND is_folder=0 GROUP BY category, extension ORDER BY category ASC, COUNT(id) DESC", params)
                ext_cat_data = cur.fetchall()
                
                self.tbl_rep_ext_by_cat.setSortingEnabled(False)
                self.tbl_rep_ext_by_cat.setRowCount(0)
                for r, row_data in enumerate(ext_cat_data):
                    self.tbl_rep_ext_by_cat.insertRow(r)
                    cat_val = str(row_data[0]) if row_data[0] else "Others"
                    ext_val = str(row_data[1]) if row_data[1] else "None"
                    
                    self.tbl_rep_ext_by_cat.setItem(r, 0, QTableWidgetItem(cat_val))
                    self.tbl_rep_ext_by_cat.setItem(r, 1, QTableWidgetItem(ext_val))
                    
                    cnt_item = NumericTableItem(f"{row_data[2]:,}"); cnt_item.setData(Qt.UserRole, row_data[2])
                    self.tbl_rep_ext_by_cat.setItem(r, 2, cnt_item)
                    
                    self.tbl_rep_ext_by_cat.setItem(r, 3, SizeTableWidgetItem(row_data[3] or 0))
                self.tbl_rep_ext_by_cat.setSortingEnabled(True)

                # 8. Most Common File Sizes
                prog.setLabelText("Analyzing Common File Sizes..."); prog.setValue(88); QApplication.processEvents()
                cur.execute(f"SELECT size, COUNT(id), SUM(size) FROM virtual_fs WHERE {base_where} AND is_folder=0 AND size > 0 GROUP BY size ORDER BY COUNT(id) DESC LIMIT 50", params)
                common_sizes_data = cur.fetchall()
                
                self.tbl_rep_common_sizes.setSortingEnabled(False)
                self.tbl_rep_common_sizes.setRowCount(0)
                for r, row_data in enumerate(common_sizes_data):
                    self.tbl_rep_common_sizes.insertRow(r)
                    self.tbl_rep_common_sizes.setItem(r, 0, SizeTableWidgetItem(row_data[0] or 0))
                    
                    cnt_item = NumericTableItem(f"{row_data[1]:,}"); cnt_item.setData(Qt.UserRole, row_data[1])
                    self.tbl_rep_common_sizes.setItem(r, 1, cnt_item)
                    
                    self.tbl_rep_common_sizes.setItem(r, 2, SizeTableWidgetItem(row_data[2] or 0))
                self.tbl_rep_common_sizes.setSortingEnabled(True)
                
            prog.setLabelText("Rendering Charts..."); prog.setValue(95); QApplication.processEvents()
            self.render_report_charts(grp_data, cat_data)

            prog.setValue(100); prog.close()
            
        except Exception as e:
            if 'prog' in locals(): prog.close()
            QMessageBox.critical(self, "Deep Report Crash", f"Failed to generate report:\n{e}")

    def render_report_charts(self, grp_data, cat_data):
        try:
            self.fig_rep.clear()
            
            ax_pie = self.fig_rep.add_subplot(211)
            ax_pie.set_facecolor('#0d1117')
            
            ax_line = self.fig_rep.add_subplot(212)
            ax_line.set_facecolor('#0d1117')

            txt_color = '#c9d1d9'
            
            # --- 1. NEAT PIE CHART (Slices < 1% are hidden) ---
            if cat_data:
                total_sz = sum((r[2] or 0) for r in cat_data)
                threshold = total_sz * 0.01  # 1% threshold
                
                # Filter out empty slices and tiny clutter
                filtered_data = [r for r in cat_data if (r[2] or 0) >= threshold and (r[2] or 0) > 0]
                
                if filtered_data:
                    labels = [r[0] for r in filtered_data]
                    sizes = [r[2] for r in filtered_data]
                    
                    # Safely map to the global color palette
                    colors = [GLOBAL_CAT_COLORS.get(lbl, "#30363d") for lbl in labels]
                    
                    wedges, texts, autotexts = ax_pie.pie(
                        sizes, labels=labels, autopct='%1.1f%%', 
                        colors=colors, startangle=140, pctdistance=0.85,
                        wedgeprops={'edgecolor': '#0d1117', 'linewidth': 2.0}
                    )
                    
                    for text in texts:
                        text.set_color(txt_color)
                        text.set_fontsize(10)
                    for autotext in autotexts:
                        autotext.set_color('white')
                        autotext.set_fontsize(9)
                        autotext.set_fontweight('bold')
                        
                    ax_pie.set_title("Category Distribution by Total Size (Slices <1% Hidden)", color=txt_color, fontsize=13, fontweight='bold', pad=20)
                else:
                    ax_pie.text(0.5, 0.5, "Data volumes are too small to render a pie chart.", color='#8b949e', ha='center', va='center')
                    ax_pie.axis('off')
            else:
                ax_pie.text(0.5, 0.5, "No Category Data Available", color='#8b949e', ha='center', va='center')
                ax_pie.axis('off')

            # --- 2. LINE CHART (Smart Size/Count Fallback) ---
            if grp_data:
                x_vals = [str(r[0]) for r in grp_data][-30:] 
                
                total_grp_sz = sum((r[2] or 0) for r in grp_data)
                
                # If sizes are 0, dynamically switch to charting File Count instead so the chart isn't empty!
                if total_grp_sz > 0:
                    y_vals = [(r[2] or 0) / (1024 * 1024) for r in grp_data][-30:]
                    y_label = "Data Volume (MB)"
                else:
                    y_vals = [(r[1] or 0) for r in grp_data][-30:]
                    y_label = "File Count"

                ax_line.plot(x_vals, y_vals, marker='o', color='#58a6ff', linewidth=2)
                ax_line.fill_between(x_vals, y_vals, color='#58a6ff', alpha=0.2)
                ax_line.set_title(f"{y_label} Grouping (Ctrl+Scroll to Zoom)", color=txt_color, fontsize=12, fontweight='bold', pad=12)
                ax_line.tick_params(axis='x', rotation=45, colors=txt_color, labelsize=8)
                ax_line.tick_params(axis='y', colors='#8b949e', labelsize=8)
                ax_line.grid(True, linestyle='--', alpha=0.2, color='#ffffff')
                
                for spine in ['top', 'right']: ax_line.spines[spine].set_visible(False)
                for spine in ['bottom', 'left']: ax_line.spines[spine].set_color('#30363d')
            else:
                ax_line.text(0.5, 0.5, "No Grouping Data Available", color='#8b949e', ha='center', va='center')
                ax_line.axis('off')

            self.fig_rep.tight_layout(pad=3.0)
            
            # Force UI to instantly paint the canvas
            self.canvas_rep.draw()
            self.canvas_rep.update()
            
        except Exception as e:
            print(f"Error rendering report charts: {e}")
            self.fig_rep.clear()
            ax = self.fig_rep.add_subplot(111)
            ax.set_facecolor('#0d1117')
            ax.text(0.5, 0.5, f"Chart Rendering Error:\n{e}", color='#f85149', ha='center', va='center')
            ax.axis('off')
            self.canvas_rep.draw()

    def _fill_report_table(self, table, data, format_size=False, is_files=False, map_dow=False, custom_order_list=None):
        table.setSortingEnabled(False) # Suspend sorting during insert
        table.setRowCount(0)
        
        # Logical mappings for Day of Week (Monday = 1 ... Sunday = 7)
        dow_names = {"1": "Monday", "2": "Tuesday", "3": "Wednesday", "4": "Thursday", "5": "Friday", "6": "Saturday", "0": "Sunday"}
        dow_sort = {"Monday": 1, "Tuesday": 2, "Wednesday": 3, "Thursday": 4, "Friday": 5, "Saturday": 6, "Sunday": 7}
        
        for r, row_data in enumerate(data):
            table.insertRow(r)
            col0_val = str(row_data[0]) if row_data[0] else ("None" if not is_files else "Unknown")
            
            sort_val = r
            if map_dow:
                col0_val = dow_names.get(col0_val, col0_val)
                sort_val = dow_sort.get(col0_val, 99)
            elif custom_order_list:
                try: sort_val = custom_order_list.index(col0_val)
                except ValueError: sort_val = 99
                
            # Apply Custom Sorter if it's a DOW or Category table
            item0 = CustomSortWidgetItem(col0_val, sort_val) if (map_dow or custom_order_list) else QTableWidgetItem(col0_val)
            table.setItem(r, 0, item0)
            
            if is_files:
                table.setItem(r, 1, SizeTableWidgetItem(row_data[1] or 0))
                table.setItem(r, 2, QTableWidgetItem(str(row_data[2])))
                table.setItem(r, 3, QTableWidgetItem(str(row_data[3])))
            else:
                table.setItem(r, 1, SizeTableWidgetItem(row_data[1] or 0)) 
                table.setItem(r, 2, SizeTableWidgetItem(row_data[2] or 0)) 

        table.setSortingEnabled(True) # Re-enable sorting (Will now sort logically!)

    def handle_report_drilldown(self, row, col):
        sender = self.sender()
        if not hasattr(self, 'rep_base_where'): return

        drill_query = f"SELECT id, name, is_folder, extension, size, parent_path, {self.rep_date_col}, custom_tags FROM virtual_fs WHERE {self.rep_base_where}"
        drill_params = list(self.rep_base_params)

        try:
            if sender == self.tbl_rep_group:
                val = sender.item(row, 0).text()
                groupby = self.cb_rep_groupby.currentText()
                if groupby == "Day": drill_query += f" AND SUBSTR({self.rep_date_col}, 1, 10) = ?"; drill_params.append(val)
                elif groupby == "Month": drill_query += f" AND SUBSTR({self.rep_date_col}, 1, 7) = ?"; drill_params.append(val)
                elif groupby == "Year": drill_query += f" AND SUBSTR({self.rep_date_col}, 1, 4) = ?"; drill_params.append(val)
                elif groupby == "Week": drill_query += f" AND strftime('%W', {self.rep_date_col}) = ?"; drill_params.append(val)
                elif groupby == "Day of Week":
                    rev_dow = {"Sunday": "0", "Monday": "1", "Tuesday": "2", "Wednesday": "3", "Thursday": "4", "Friday": "5", "Saturday": "6"}
                    drill_query += f" AND strftime('%w', {self.rep_date_col}) = ?"; drill_params.append(rev_dow.get(val, val))
            
            elif sender == self.tbl_rep_cat:
                val = sender.item(row, 0).text()
                if val == "None": drill_query += " AND (category IS NULL OR category = '')"
                else: drill_query += " AND category = ?"; drill_params.append(val)
            
            elif sender == self.tbl_rep_ext:
                val = sender.item(row, 0).text()
                if val == "None": drill_query += " AND (extension IS NULL OR extension = '')"
                else: drill_query += " AND extension = ?"; drill_params.append(val)
            
            elif sender == self.tbl_rep_tag:
                val = sender.item(row, 0).text()
                drill_query += " AND custom_tags = ?"; drill_params.append(val)
            
            elif sender in (self.tbl_rep_largest, self.tbl_rep_smallest):
                db_id = sender.item(row, 3).text()
                drill_query += " AND id = ?"; drill_params.append(db_id)

            # --- NEW: Drilldown logic for the 2 new tables ---
            elif sender == self.tbl_rep_ext_by_cat:
                cat_val = sender.item(row, 0).text()
                ext_val = sender.item(row, 1).text()
                
                if cat_val == "Others": drill_query += " AND (category IS NULL OR category = 'Others' OR category = '')"
                else: drill_query += " AND category = ?"; drill_params.append(cat_val)
                
                if ext_val == "None": drill_query += " AND (extension IS NULL OR extension = '')"
                else: drill_query += " AND extension = ?"; drill_params.append(ext_val)

            elif sender == self.tbl_rep_common_sizes:
                size_item = sender.item(row, 0)
                if hasattr(size_item, 'size_bytes'):
                    exact_size = size_item.size_bytes
                    drill_query += " AND size = ?"; drill_params.append(exact_size)

            # Route to the main search execution which will populate the Activity Log!
            self.execute_search(drill_query, tuple(drill_params), update_highlights=False)
            
            # Switch view to Activity Log (Tab index 1)
            self.tabs.setCurrentIndex(1) 
            
        except Exception as e:
            QMessageBox.warning(self, "Drilldown Error", str(e))
 
    def update_calendar_style(self):
        font = self.calendar.font()
        font.setPointSize(10)
        self.calendar.setFont(font)
        
        # Check user setting for the yellow border
        settings = QSettings("vmanOS", "TimelineSettings")
        show_border = settings.value("calendar_show_border", True, type=bool)
        
        # Apply the transparent yellow border OR completely invisible selection background
        if show_border:
            sel_style = "border: 2px solid #e3b341; background-color: rgba(0, 0, 0, 160); color: white;"
        else:
            # Using rgba(0,0,0,0) ensures Qt does NOT erase the highlighted background color you painted.
            # A faint, semi-transparent white border is added so you still know which date is active.
            sel_style = "border: 1px solid rgba(255, 255, 255, 50); background-color: rgba(0, 0, 0, 0); color: white;"
            
        self.calendar.setStyleSheet(f"""
            QCalendarWidget QWidget {{ alternate-background-color: #161b22; background-color: #0d1117; color: #c9d1d9; }}
            QCalendarWidget QToolButton {{ color: #c9d1d9; font-weight: bold; background-color: transparent; padding: 5px; }}
            QCalendarWidget QToolButton::hover {{ background-color: #30363d; border-radius: 4px; }}
            QCalendarWidget QMenu {{ background-color: #161b22; color: white; }}
            QCalendarWidget QSpinBox {{ background: #161b22; color: white; border: 1px solid #30363d; }}
            QCalendarWidget QAbstractItemView:enabled {{ 
                background-color: #0d1117; 
                color: #c9d1d9; 
                selection-background-color: rgba(0, 0, 0, 0); 
                selection-color: white; 
                outline: none; 
            }}
            QCalendarWidget QAbstractItemView:disabled {{ color: #484f58; }}
            QCalendarWidget QTableView::item:selected {{ {sel_style} }}
        """)
        
    def toggle_calendar_border(self, checked):
        settings = QSettings("vmanOS", "TimelineSettings")
        settings.setValue("calendar_show_border", checked)
        self.update_calendar_style()
    
    def prompt_custom_spacing(self):
        settings = QSettings("vmanOS", "HeatmapGridSettings")
        curr_x = settings.value("custom_gap_x", 0.1, type=float)
        curr_y = settings.value("custom_gap_y", 0.1, type=float)
        
        # Ask for X (Horizontal distance between weeks)
        x_val, ok1 = QInputDialog.getDouble(self, "Custom Spacing", "Horizontal gap between weeks (e.g. 0.0 to 2.0):", curr_x, 0.0, 10.0, 2)
        if not ok1: return
        
        # Ask for Y (Vertical distance between days)
        y_val, ok2 = QInputDialog.getDouble(self, "Custom Spacing", "Vertical gap between days (e.g. 0.0 to 2.0):", curr_y, 0.0, 10.0, 2)
        if not ok2: return
        
        settings.setValue("block_spacing", "Custom")
        settings.setValue("custom_gap_x", x_val)
        settings.setValue("custom_gap_y", y_val)
        self.render_heatmap()

    def show_grid_settings_menu(self):
        menu = QMenu(self)
        settings = QSettings("vmanOS", "HeatmapGridSettings")
        
        # --- LAYOUT VIEWS ---
        layout_menu = menu.addMenu("🖥️ Layout Style")
        current_layout = settings.value("layout_style", "Standard")
        for label in ["Standard", "Pure GitHub", "Weeks View (Spreadsheet)"]:
            act = layout_menu.addAction(label); act.setCheckable(True); act.setChecked(current_layout == label)
            act.triggered.connect(lambda checked, v=label: (settings.setValue("layout_style", v), self.render_heatmap()))
            
        # --- BLOCK SPACING ---
        spacing_menu = menu.addMenu("📏 Block Spacing (Gap)")
        current_spacing = settings.value("block_spacing", "Normal")
        for label in ["Touching (No Gap)", "Normal", "Wide"]:
            act = spacing_menu.addAction(label); act.setCheckable(True); act.setChecked(current_spacing == label)
            act.triggered.connect(lambda checked, v=label: (settings.setValue("block_spacing", v), self.render_heatmap()))
        
        spacing_menu.addSeparator()
        act_custom = spacing_menu.addAction("✏️ Custom Input..."); act_custom.setCheckable(True); act_custom.setChecked(current_spacing == "Custom")
        act_custom.triggered.connect(self.prompt_custom_spacing)
            
        menu.addSeparator()
        
        # --- NEW ADVANCED FILTERS MENU HOOK ---
        act_filter = menu.addAction("🔍 Advanced Data Filters...")
        act_filter.triggered.connect(lambda: self.render_heatmap() if HeatmapFilterDialog(self).exec() == QDialog.Accepted else None)
        menu.addSeparator()

        color_int_menu = menu.addMenu("Gradient Color (Intensity Mode)")
        current_int = settings.value("intensity_color", "Fire")
        for label in ["Fire", "Green", "Blue"]:
            act = color_int_menu.addAction(label); act.setCheckable(True); act.setChecked(current_int == label)
            act.triggered.connect(lambda checked, v=label: (settings.setValue("intensity_color", v), self.render_heatmap()))
            
        menu.addSeparator()
            
        act_legend = menu.addAction("Show Category/Extension Legend")
        act_legend.setCheckable(True); act_legend.setChecked(settings.value("show_legend", False, type=bool))
        act_legend.toggled.connect(lambda v: (settings.setValue("show_legend", v), self.render_heatmap()))

        act_show = menu.addAction("Show Month Boundaries")
        act_show.setCheckable(True); act_show.setChecked(settings.value("show_grid", True, type=bool))
        act_show.toggled.connect(lambda v: (settings.setValue("show_grid", v), self.render_heatmap()))
        
        style_menu = menu.addMenu("Boundary Style")
        current_style = settings.value("line_style", "-")
        for label, val in [("Solid", "-"), ("Dashed", "--"), ("Dotted", ":")]:
            act = style_menu.addAction(label); act.setCheckable(True); act.setChecked(current_style == val)
            act.triggered.connect(lambda checked, v=val: (settings.setValue("line_style", v), self.render_heatmap()))
            
        color_menu = menu.addMenu("Boundary Color")
        current_color = settings.value("line_color", "#58a6ff")
        for label, val in [("Blue", "#58a6ff"), ("Green", "#3fb950"), ("Red", "#f85149"), ("Yellow", "#e3b341"), ("Gray", "#8b949e")]:
            act = color_menu.addAction(label); act.setCheckable(True); act.setChecked(current_color == val)
            act.triggered.connect(lambda checked, v=val: (settings.setValue("line_color", v), self.render_heatmap()))

        from PySide6.QtCore import QPoint
        menu.exec(self.btn_heat_grid.mapToGlobal(QPoint(0, self.btn_heat_grid.height())))
 
    def render_heatmap(self):
        self.fig_heat.clear()
        year_str = self.cb_heat_year.currentText()
        if not year_str or year_str == "All": return
        
        mode = self.cb_heat_mode.currentText()
        ax = self.fig_heat.add_subplot(111)
        ax.set_facecolor('#0d1117')
        
        data_dict = {} 
        is_intensity_mode = any(k in mode for k in ["Volume", "Forensic"])

        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            
            # Fetch securely from settings
            filter_settings = QSettings("vmanOS", "HeatmapFilters")
            
            search_text = str(filter_settings.value("search_text", "")).strip()
            match_mode = str(filter_settings.value("match_mode", "Contains"))
            data_type = str(filter_settings.value("data_type", "Files Only"))
            category = str(filter_settings.value("category", "All"))
            skip_names = [x.strip().lower() for x in str(filter_settings.value("skip_names", "")).split(',') if x.strip()]
            
            inc_exts = [x.strip().lower() for x in str(filter_settings.value("include_exts", "")).split(',') if x.strip()]
            exc_exts = [x.strip().lower() for x in str(filter_settings.value("exclude_exts", "")).split(',') if x.strip()]
            min_bytes = int(float(filter_settings.value("min_size_mb", 0.0)) * 1024 * 1024)
            max_bytes = int(float(filter_settings.value("max_size_mb", 999999.0)) * 1024 * 1024)
            
            # --- Base Architecture ---
            where_sql = "year=? AND in_trash=0"
            params = [year_str]
            
            # 1. Data Type Filter
            if data_type == "Files Only": where_sql += " AND is_folder=0"
            elif data_type == "Folders Only": where_sql += " AND is_folder=1"
            
            # 2. Category Filter
            if category != "All":
                where_sql += " AND category=?"
                params.append(category)
                
            # 3. Search Filter
            if search_text:
                where_sql += " AND LOWER(name) LIKE ?"
                if match_mode == "Exact Match": params.append(search_text.lower())
                elif match_mode == "Starts With": params.append(f"{search_text.lower()}%")
                elif match_mode == "Ends With": params.append(f"%{search_text.lower()}")
                else: params.append(f"%{search_text.lower()}%") # Contains
                
            # 4. Skip Names Filter (Partial Match Exclusions)
            if skip_names:
                for skip_val in skip_names:
                    where_sql += " AND LOWER(name) NOT LIKE ?"
                    params.append(f"%{skip_val}%")
            
            # 5. Extension & Size Filters
            if inc_exts:
                ext_placeholders = ",".join(["?"] * len(inc_exts))
                inc_exts = [ext if ext.startswith('.') else f".{ext}" for ext in inc_exts]
                where_sql += f" AND LOWER(extension) IN ({ext_placeholders})"
                params.extend(inc_exts)
                
            if exc_exts:
                ext_placeholders = ",".join(["?"] * len(exc_exts))
                exc_exts = [ext if ext.startswith('.') else f".{ext}" for ext in exc_exts]
                where_sql += f" AND LOWER(extension) NOT IN ({ext_placeholders})"
                params.extend(exc_exts)
                
            if min_bytes > 0:
                where_sql += " AND size >= ?"
                params.append(min_bytes)
            if max_bytes < int(999999.0 * 1024 * 1024):
                where_sql += " AND size <= ?"
                params.append(max_bytes)

            p_tuple = tuple(params)

            if "Volume by Count" in mode:
                cur.execute(f"SELECT SUBSTR(modified, 1, 10), COUNT(id) FROM virtual_fs WHERE {where_sql} GROUP BY SUBSTR(modified, 1, 10)", p_tuple)
                for dt, val in cur.fetchall(): data_dict[dt] = val
            elif "Volume by Size" in mode:
                cur.execute(f"SELECT SUBSTR(modified, 1, 10), SUM(size) FROM virtual_fs WHERE {where_sql} GROUP BY SUBSTR(modified, 1, 10)", p_tuple)
                for dt, val in cur.fetchall(): data_dict[dt] = val
            elif "Forensic: Tagging" in mode:
                cur.execute(f"SELECT SUBSTR(modified, 1, 10), COUNT(id) FROM virtual_fs WHERE {where_sql} AND custom_tags IS NOT NULL AND custom_tags != '' GROUP BY SUBSTR(modified, 1, 10)", p_tuple)
                for dt, val in cur.fetchall(): data_dict[dt] = val
            elif "Forensic: Average" in mode:
                cur.execute(f"SELECT SUBSTR(modified, 1, 10), AVG(size) FROM virtual_fs WHERE {where_sql} GROUP BY SUBSTR(modified, 1, 10)", p_tuple)
                for dt, val in cur.fetchall(): data_dict[dt] = val
            elif "Forensic: Max" in mode:
                cur.execute(f"SELECT SUBSTR(modified, 1, 10), MAX(size) FROM virtual_fs WHERE {where_sql} GROUP BY SUBSTR(modified, 1, 10)", p_tuple)
                for dt, val in cur.fetchall(): data_dict[dt] = val
            elif "Dominant Category" in mode:
                metric = "SUM(size)" if "Size" in mode else "COUNT(id)"
                cur.execute(f"SELECT SUBSTR(modified, 1, 10), category, {metric} FROM virtual_fs WHERE {where_sql} GROUP BY SUBSTR(modified, 1, 10), category", p_tuple)
                temp_dict = {}
                for dt, cat, val in cur.fetchall():
                    if dt not in temp_dict or (val or 0) > temp_dict[dt][1]: temp_dict[dt] = (cat, val or 0)
                for dt, (cat, _) in temp_dict.items(): data_dict[dt] = cat
            elif "Dominant Extension" in mode:
                metric = "SUM(size)" if "Size" in mode else "COUNT(id)"
                cur.execute(f"SELECT SUBSTR(modified, 1, 10), extension, {metric} FROM virtual_fs WHERE {where_sql} GROUP BY SUBSTR(modified, 1, 10), extension", p_tuple)
                temp_dict = {}
                for dt, ext, val in cur.fetchall():
                    if dt not in temp_dict or (val or 0) > temp_dict[dt][1]: temp_dict[dt] = (ext, val or 0)
                for dt, (ext, _) in temp_dict.items(): data_dict[dt] = ext
                
        import datetime as dt_lib
        try:
            start_date = dt_lib.date(int(year_str), 1, 1)
            end_date = dt_lib.date(int(year_str), 12, 31)
        except ValueError: return
        
        start_weekday = start_date.weekday()
        delta = end_date - start_date
        
        saved_colors = QSettings("vmanOS", "HeatmapColors").value("custom_colors", {})
        if not isinstance(saved_colors, dict): saved_colors = {}
        cat_colors = GLOBAL_CAT_COLORS
        
        grid_settings = QSettings("vmanOS", "HeatmapGridSettings")
        layout_style = grid_settings.value("layout_style", "Standard")
        block_spacing = grid_settings.value("block_spacing", "Normal")
        show_grid = grid_settings.value("show_grid", True, type=bool)
        show_legend = grid_settings.value("show_legend", False, type=bool)
        intensity_color = grid_settings.value("intensity_color", "Fire")
        line_style = grid_settings.value("line_style", "-")
        line_color = grid_settings.value("line_color", "#58a6ff")

        # --- INDEPENDENT GAP GEOMETRY ---
        gap_x, gap_y = 0.2, 0.2 # Normal
        if block_spacing == "Touching (No Gap)": gap_x, gap_y = 0.0, 0.0
        elif block_spacing == "Wide": gap_x, gap_y = 0.5, 0.5
        elif block_spacing == "Custom":
            gap_x = grid_settings.value("custom_gap_x", 0.1, type=float)
            gap_y = grid_settings.value("custom_gap_y", 0.1, type=float)
            
        box_w, box_h = 1.0, 1.0
        edge_c = "none"
        edge_lw = 0.0
        
        # --- VIEW PRESETS OVERRIDE ---
        if layout_style == "Pure GitHub":
            gap_x, gap_y = 0.2, 0.2
            show_grid = False
            ax.set_aspect('equal') # Forces perfect squares like GitHub
        elif layout_style == "Weeks View (Spreadsheet)":
            gap_x, gap_y = 0.0, 0.0
            edge_c = "#30363d"
            edge_lw = 1.0

        max_vol = max(data_dict.values()) if (is_intensity_mode and data_dict) else 1
        
        month_dividers = set() 
        month_labels_x = []

        import hashlib
        import matplotlib.patches as mpatches
        
        for i in range(delta.days + 1):
            current_d = start_date + dt_lib.timedelta(days=i)
            d_str = current_d.strftime("%Y-%m-%d")
            
            # Absolute Day-of-Year math for unbreakable alignment
            col = (i + start_weekday) // 7
            row = current_d.weekday() 
            
            x_pos = col * (box_w + gap_x)
            y_pos = row * (box_h + gap_y)
            
            if current_d.day == 1: 
                month_dividers.add(x_pos)
                month_labels_x.append(x_pos + (box_w / 2))
            
            val = data_dict.get(d_str, None)
            if val is None:
                c_hex = "#161b22" if layout_style != "Weeks View (Spreadsheet)" else "#0d1117"
            else:
                if is_intensity_mode:
                    intensity = max(0.2, min(1.0, float(val) / float(max_vol)))
                    if "Green" in intensity_color: r, g, b = int(22 + (57 - 22) * intensity), int(27 + (211 - 27) * intensity), int(34 + (83 - 34) * intensity)
                    elif "Blue" in intensity_color: r, g, b = int(22 + (88 - 22) * intensity), int(27 + (166 - 27) * intensity), int(34 + (255 - 34) * intensity)
                    else: r, g, b = int(22 + (248 - 22) * intensity), int(27 + (81 - 27) * intensity), int(34 + (73 - 34) * intensity) 
                    c_hex = f"#{r:02x}{g:02x}{b:02x}"
                elif "Category" in mode:
                    cat_clean = val if val else "Others"
                    c_hex = saved_colors.get(f"Category_{cat_clean}", cat_colors.get(cat_clean, "#8b949e"))
                elif "Extension" in mode:
                    ext_clean = str(val).lower() if val else "none"
                    key = f"Extension_{ext_clean}"
                    if key in saved_colors: c_hex = saved_colors[key]
                    else:
                        h = int(hashlib.md5(ext_clean.encode()).hexdigest(), 16)
                        c_hex = f"#{min(255, max(100, h & 0xFF)):02x}{min(255, max(100, (h >> 8) & 0xFF)):02x}{min(255, max(100, (h >> 16) & 0xFF)):02x}"
                        
            # Draw mathematically precise box
            rect = mpatches.Rectangle((x_pos, y_pos), box_w, box_h, facecolor=c_hex, edgecolor=edge_c, linewidth=edge_lw)
            ax.add_patch(rect)
        
        # Determine strict drawing boundaries
        max_x = (53 * (box_w + gap_x))
        max_y = (7 * (box_h + gap_y))
        
        ax.set_xlim(-gap_x, max_x)
        ax.set_ylim(max_y, -gap_y) # Inverted Y so Monday is at the top
        
        # Setup specific Y-Axis labels depending on layout mode
        y_ticks = [(r * (box_h + gap_y)) + (box_h / 2) for r in range(7)]
        ax.set_yticks(y_ticks)
        
        if layout_style == "Pure GitHub":
            ax.set_yticklabels(['', 'Mon', '', 'Wed', '', 'Fri', ''], color="#8b949e", fontsize=8)
        else:
            ax.set_yticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'], color="#8b949e", fontsize=8)

        # Apply Month Lines
        if show_grid and layout_style != "Pure GitHub":
            for x_line in month_dividers:
                ax.plot([x_line - (gap_x / 2), x_line - (gap_x / 2)], [-gap_y, max_y], color=line_color, linestyle=line_style, linewidth=1.5, zorder=5)

        ax.set_xticks(month_labels_x)
        ax.set_xticklabels(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'], color="#c9d1d9", fontsize=9, fontweight="bold")
        
        for spine in ['top', 'right', 'bottom', 'left']: ax.spines[spine].set_visible(False)
        ax.tick_params(axis='both', which='major', length=0)
        
        # Build Legendary Layout
        if not is_intensity_mode and show_legend:
            legend_patches = []
            if "Category" in mode:
                for cat_name in set(data_dict.values()): 
                    if cat_name: legend_patches.append(mpatches.Patch(color=saved_colors.get(f"Category_{cat_name}", cat_colors.get(cat_name, "#8b949e")), label=cat_name))
            elif "Extension" in mode:
                for ext_name in set(data_dict.values()): 
                    if ext_name:
                        ext_clean = str(ext_name).lower()
                        key = f"Extension_{ext_clean}"
                        if key in saved_colors: c_hex = saved_colors[key]
                        else:
                            h = int(hashlib.md5(ext_clean.encode()).hexdigest(), 16)
                            c_hex = f"#{min(255, max(100, h & 0xFF)):02x}{min(255, max(100, (h >> 8) & 0xFF)):02x}{min(255, max(100, (h >> 16) & 0xFF)):02x}"
                        legend_patches.append(mpatches.Patch(color=c_hex, label=str(ext_name).upper()))
            
            ax.legend(handles=legend_patches, loc='center left', bbox_to_anchor=(1, 0.5), frameon=False, labelcolor='#c9d1d9')
            self.fig_heat.tight_layout(rect=[0, 0, 0.9, 1]) 
        else:
            self.fig_heat.tight_layout()
            
        self.canvas_heat.draw()
 
    def show_calendar_context_menu(self, pos):
        menu = QMenu(self)
        
        act_refresh = menu.addAction("🔄 Refresh Timeline Data")
        act_refresh.triggered.connect(lambda: (self.populate_dropdowns(), self.highlight_month(self.calendar.yearShown(), self.calendar.monthShown()), self.render_heatmap()))
        menu.addSeparator()
        
        # --- NEW: Optional Yellow Selection Border Toggle ---
        settings = QSettings("vmanOS", "TimelineSettings")
        act_border = menu.addAction("Show Yellow Selection Border")
        act_border.setCheckable(True)
        act_border.setChecked(settings.value("calendar_show_border", True, type=bool))
        act_border.toggled.connect(self.toggle_calendar_border)
        menu.addSeparator()
        
        switch_menu = menu.addMenu("On Date Click, Switch to Tab...")
        tabs = [("📖 Daily Diary", 0), ("📋 Activity Log", 1), ("📊 Visual Analytics", 2), ("📅 Monthly Chart", 3), ("🔥 Yearly Activity", 4), ("Stay on Current Tab", -1)]
        
        for name, idx in tabs:
            act = switch_menu.addAction(name)
            act.setCheckable(True)
            act.setChecked(getattr(self, 'auto_switch_target', 0) == idx)
            act.triggered.connect(lambda checked=False, val=idx: self.set_auto_switch(val))
            
        menu.addSeparator()
        
        view_menu = menu.addMenu("Calendar Highlighting Mode")
        modes = ["Default (Green Highlight)", "Activity Volume (Green Gradient)", "Dominant Category (Count)", "Dominant Category (Size)", "Dominant Extension (Count)", "Dominant Extension (Size)"]
        for mode in modes:
            act = view_menu.addAction(mode)
            act.setCheckable(True)
            act.setChecked(self.calendar_view_mode == mode)
            act.triggered.connect(lambda checked=False, m=mode: self.set_calendar_view_mode(m))
            
        menu.exec(self.calendar.mapToGlobal(pos))
        
    def set_calendar_view_mode(self, mode):
        self.calendar_view_mode = mode
        self.settings.setValue("calendar_view_mode", mode)
        self.highlight_month(self.calendar.yearShown(), self.calendar.monthShown())
        
    def set_auto_switch(self, val):
        self.auto_switch_target = val
        

    def on_calendar_clicked(self, date):
        self.load_html_diary(date)
        date_str = date.toString("yyyy-MM-dd")
        col = "creation_date" if getattr(self, 'date_mode_cre', None) and self.date_mode_cre.isChecked() else "modified"
        
        query = f"SELECT id, name, is_folder, extension, size, parent_path, {col}, custom_tags FROM virtual_fs WHERE {col} LIKE ? AND is_folder=0 AND in_trash=0"
        
        self.execute_search(query, (f"{date_str}%",), update_highlights=False)
        
        if self.auto_switch_target != -1:
            self.tabs.setCurrentIndex(self.auto_switch_target)

    def open_color_manager(self):
        HeatmapColorConfigDialog(self.db_path, self).exec()
        

    def load_html_diary(self, date):
        dt_str = date.toString("yyyy-MM-dd")
        col = "creation_date" if getattr(self, 'date_mode_cre', None) and self.date_mode_cre.isChecked() else "modified"
        
        with sqlite3.connect(self.db_path) as conn:
            # FAST FETCH: Limits HTML text generation to 150 items to prevent hanging
            entries = conn.cursor().execute(f"SELECT SUBSTR({col}, 12, 8), name, parent_path, size, category FROM virtual_fs WHERE {col} LIKE ? AND is_folder=0 AND in_trash=0 ORDER BY {col} ASC LIMIT 150", (f"{dt_str}%",)).fetchall()
        
        html = f"<h1 style='color:#58a6ff; text-align:center;'>📖 System Timeline: {date.toString('dddd, MMMM d, yyyy')}</h1><hr>"
        if not entries: 
            html += "<h3 style='color:#8b949e; text-align:center;'><br><br>No system activity recorded on this day.</h3>"
        else:
            if len(entries) == 150:
                html += f"<p style='color:#e3b341; text-align:center;'><b>Showing first 150 activities for performance. View full list in Activity Log.</b></p><br>"
            else:
                html += f"<p style='color:#c9d1d9; text-align:center;'><b>{len(entries)}</b> files were logged.</p><br>"
            
            html += "<ul style='list-style-type: none; padding-left: 0;'>"
            cat_colors = {"Images": "#a371f7", "Videos": "#f85149", "Audio": "#ff7b72", "Documents": "#d2a8ff", "Code": "#79c0ff", "Others": "#8b949e"}
            action_verb = "Created" if getattr(self, 'date_mode_cre', None) and self.date_mode_cre.isChecked() else "Modified"
            
            for time_str, name, pp, size, cat in entries:
                c_color = cat_colors.get(cat, "#8b949e")
                try: safe_size = human_size(size)
                except: safe_size = f"{size} bytes"
                
                html += f"<li style='margin-bottom: 15px; background-color: rgba(33, 38, 45, 0.6); padding: 12px; border-left: 5px solid {c_color}; border-radius: 6px;'><span style='color: #58a6ff; font-size: 15px;'><b>🕒 {time_str}</b></span><br><span style='font-size: 16px; color: white;'>{action_verb} <b style='color: {c_color};'>{name}</b></span> <span style='color: #8b949e; font-size: 13px;'>({safe_size})</span><br><span style='color: #8b949e; font-size: 13px;'>Path: {pp}</span></li>"
            html += "</ul>"
        self.diary_browser.setHtml(html)
            
    def filter_activity_log(self):
        term = self.log_search_box.text().lower()
        cat_ext = self.log_cat_box.currentText().lower()
        
        is_cat = False
        target_exts = []
        
        if cat_ext == "all types":
            is_cat = True
        else:
            for c_name, c_exts in FILE_CATEGORIES.items():
                if cat_ext == c_name.lower():
                    is_cat = True
                    target_exts = c_exts
                    break

        self.table.setUpdatesEnabled(False)
        for r in range(self.table.rowCount()):
            name = self.table.item(r, 1).text().lower() if self.table.item(r, 1) else ""
            path = self.table.item(r, 6).text().lower() if self.table.item(r, 6) else ""
            ext = self.table.item(r, 3).text().lower() if self.table.item(r, 3) else ""
            
            match_text = (term in name or term in path) if term else True
            match_ext = True
            
            if not is_cat and cat_ext:
                match_ext = (cat_ext in ext)
            elif is_cat and cat_ext != "all types":
                match_ext = any(e in ext for e in target_exts)
            
            self.table.setRowHidden(r, not (match_text and match_ext))
        self.table.setUpdatesEnabled(True)

    def update_smart_filters(self, trigger_source):
        if self.is_populating: return
        self.is_populating = True
        
        y = self.cb_year.currentText()
        c = self.cb_category.currentText()
        
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            
            if trigger_source == 'year' and y != "All":
                cur.execute("SELECT DISTINCT month FROM virtual_fs WHERE year=? AND month IS NOT NULL AND month != '' AND in_trash=0 ORDER BY month ASC", (y,))
                months = ["All"] + [str(r[0]) for r in cur.fetchall()]
                self.cb_month.clear(); self.cb_month.addItems(months)
                
            if trigger_source in ['year', 'category']:
                query = "SELECT DISTINCT extension FROM virtual_fs WHERE extension IS NOT NULL AND extension != '' AND is_folder=0 AND in_trash=0"
                params = []
                if y != "All": query += " AND year=?"; params.append(y)
                if c != "All": query += " AND category=?"; params.append(c)
                query += " ORDER BY extension ASC"
                
                cur.execute(query, tuple(params))
                exts = ["All"] + [str(r[0]) for r in cur.fetchall()]
                self.cb_ext.clear(); self.cb_ext.addItems(exts)
                
        self.is_populating = False
    
    def closeEvent(self, event):
        self.settings.setValue("table_state", self.table.horizontalHeader().saveState())
        super().closeEvent(event)
        
    def show_header_menu(self, pos):
        menu = QMenu(self)
        for col in range(self.table.columnCount()):
            name = self.table.horizontalHeaderItem(col).text()
            action = menu.addAction(f"Show {name}")
            action.setCheckable(True)
            action.setChecked(not self.table.isColumnHidden(col))
            action.toggled.connect(lambda checked, c=col: self.table.setColumnHidden(c, not checked))
        menu.exec(self.table.horizontalHeader().mapToGlobal(pos))
        
    def on_date_mode_changed(self):
        # Instantly forces the calendar, HTML reader, and Table to reload with the newly chosen metric
        self.highlight_month(self.calendar.yearShown(), self.calendar.monthShown())
        self.on_calendar_clicked(self.calendar.selectedDate())

    def populate_dropdowns(self):
        try:
            self.cb_year.clear(); self.cb_month.clear(); self.cb_category.clear()
            self.cb_ext.clear(); self.cb_size.clear(); self.cb_tag.clear()
            
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                
                # Check existing columns to prevent SQLite crashes on older schemas
                cur.execute("PRAGMA table_info(virtual_fs)")
                cols = [row[1] for row in cur.fetchall()]
                
                self.cb_year.addItem("All")
                if 'year' in cols:
                    cur.execute("SELECT DISTINCT year FROM virtual_fs WHERE year IS NOT NULL AND year != '' AND in_trash=0 ORDER BY year DESC")
                    years = [str(r[0]) for r in cur.fetchall()]
                    self.cb_year.addItems(years)
                    self.cb_heat_year.clear()          # ADD THIS
                    self.cb_heat_year.addItems(years)  # ADD THIS
                    # --- ADD TO REPORT DROPDOWNS ---
                    self.cb_rep_from_year.clear(); self.cb_rep_from_year.addItems(["All"] + years[::-1]) # Oldest first
                    self.cb_rep_to_year.clear(); self.cb_rep_to_year.addItems(["All"] + years)
                    
                self.cb_month.addItem("All")
                if 'month' in cols:
                    cur.execute("SELECT DISTINCT month FROM virtual_fs WHERE month IS NOT NULL AND month != '' AND in_trash=0 ORDER BY month ASC")
                    self.cb_month.addItems([str(r[0]) for r in cur.fetchall()])
                
                self.cb_category.addItem("All")
                if 'category' in cols:
                    cur.execute("SELECT DISTINCT category FROM virtual_fs WHERE category IS NOT NULL AND category != '' AND in_trash=0 ORDER BY category ASC")
                    self.cb_category.addItems([str(r[0]) for r in cur.fetchall()])
                
                self.cb_ext.addItem("All")
                if 'extension' in cols:
                    cur.execute("SELECT DISTINCT extension FROM virtual_fs WHERE extension IS NOT NULL AND extension != '' AND is_folder=0 AND in_trash=0 ORDER BY extension ASC")
                    self.cb_ext.addItems([str(r[0]) for r in cur.fetchall()])
                
                self.cb_size.addItems(["All", "Tiny (< 1MB)", "Medium (1MB - 500MB)", "Huge (> 500MB)"])
                
                self.cb_tag.addItem("All")
                if 'custom_tags' in cols:
                    cur.execute("SELECT custom_tags FROM virtual_fs WHERE custom_tags IS NOT NULL AND custom_tags != '' AND in_trash=0")
                    all_tags = set()
                    for (tags_str,) in cur.fetchall():
                        for t in tags_str.split(','):
                            if t.strip(): all_tags.add(t.strip())
                    self.cb_tag.addItems(sorted(list(all_tags)))
                    
        except Exception as e:
            print(f"Error populating dropdowns: {e}")

    def highlight_month(self, year, month):
        self.calendar.setDateTextFormat(QDate(), QTextCharFormat())
        
        where_clauses = ["is_folder=0", "in_trash=0"]
        params = []
        
        if hasattr(self, 'cb_category') and self.cb_category.currentText() != "All":
            where_clauses.append("category=?"); params.append(self.cb_category.currentText())
        if hasattr(self, 'cb_ext') and self.cb_ext.currentText() != "All":
            where_clauses.append("extension=?"); params.append(self.cb_ext.currentText())
        if hasattr(self, 'cb_tag') and self.cb_tag.currentText() != "All":
            where_clauses.append("custom_tags LIKE ?"); params.append(f"%{self.cb_tag.currentText()}%")
            
        if hasattr(self, 'cb_size'):
            s = self.cb_size.currentText()
            if s == "Tiny (< 1MB)": where_clauses.append("size < 1048576")
            elif s == "Medium (1MB - 500MB)": where_clauses.append("size >= 1048576 AND size <= 524288000")
            elif s == "Huge (> 500MB)": where_clauses.append("size > 524288000")

        base_sql = f"WHERE {' AND '.join(where_clauses)}"
        ym_prefix = f"{year}-{month:02d}-%"
        
        mode = getattr(self, 'calendar_view_mode', "Default (Green Highlight)")
        data_dict = {}
        max_vol = 1
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            cur = conn.cursor()
            
            # --- C+M / BOTH LOGIC ---
            def get_days_for_col(col_name):
                p = tuple(params + [ym_prefix])
                if "Default" in mode:
                    cur.execute(f"SELECT DISTINCT SUBSTR({col_name}, 9, 2) FROM virtual_fs {base_sql} AND {col_name} LIKE ?", p)
                    return {int(r[0]): "default" for r in cur.fetchall() if r[0]}
                elif "Volume" in mode:
                    cur.execute(f"SELECT SUBSTR({col_name}, 9, 2), COUNT(id) FROM virtual_fs {base_sql} AND {col_name} LIKE ? GROUP BY SUBSTR({col_name}, 9, 2)", p)
                    return {int(d): c for d, c in cur.fetchall() if d}
                else:
                    is_cat = "Category" in mode
                    metric = "SUM(size)" if "Size" in mode else "COUNT(id)"
                    target_col = "category" if is_cat else "extension"
                    cur.execute(f"SELECT SUBSTR({col_name}, 9, 2), {target_col}, {metric} FROM virtual_fs {base_sql} AND {col_name} LIKE ? GROUP BY SUBSTR({col_name}, 9, 2), {target_col}", p)
                    temp = {}
                    for d, t_val, val in cur.fetchall():
                        day = int(d)
                        if day not in temp or (val or 0) > temp[day][1]: temp[day] = (t_val, val or 0)
                    return {d: v[0] for d, v in temp.items()}

            if self.date_mode_both.isChecked():
                d_mod = get_days_for_col("modified")
                d_cre = get_days_for_col("creation_date")
                data_dict = {**d_cre, **d_mod} # Union
            elif self.date_mode_cm.isChecked():
                d_mod = get_days_for_col("modified")
                d_cre = get_days_for_col("creation_date")
                common = set(d_mod.keys()).intersection(set(d_cre.keys())) # Intersection
                data_dict = {k: d_mod[k] for k in common} 
            elif self.date_mode_cre.isChecked():
                data_dict = get_days_for_col("creation_date")
            else:
                data_dict = get_days_for_col("modified")
                
            if "Volume" in mode and data_dict:
                max_vol = max(data_dict.values())

        saved_colors = QSettings("vmanOS", "HeatmapColors").value("custom_colors", {})
        if not isinstance(saved_colors, dict): saved_colors = {}
        cat_colors = GLOBAL_CAT_COLORS

        for day, val in data_dict.items():
            fmt = QTextCharFormat()
            fmt.setForeground(QBrush(QColor("white"))); fmt.setFontWeight(QFont.Bold)
            bg_color = "#2ea043" 
            
            if "Volume" in mode:
                intensity = max(0.2, min(1.0, float(val) / float(max_vol)))
                r, g, b = int(22 + (57 - 22) * intensity), int(27 + (211 - 27) * intensity), int(34 + (83 - 34) * intensity)
                bg_color = f"#{r:02x}{g:02x}{b:02x}"
            elif "Category" in mode:
                cat_clean = val if val else "Others"
                bg_color = saved_colors.get(f"Category_{cat_clean}", cat_colors.get(cat_clean, "#8b949e"))
            elif "Extension" in mode:
                ext_clean = str(val).lower() if val else "none"
                key = f"Extension_{ext_clean}"
                if key in saved_colors: bg_color = saved_colors[key]
                else:
                    import hashlib
                    h = int(hashlib.md5(ext_clean.encode()).hexdigest(), 16)
                    bg_color = f"#{min(255, max(100, h & 0xFF)):02x}{min(255, max(100, (h >> 8) & 0xFF)):02x}{min(255, max(100, (h >> 16) & 0xFF)):02x}"

            fmt.setBackground(QBrush(QColor(bg_color)))
            try: self.calendar.setDateTextFormat(QDate(year, month, day), fmt)
            except ValueError: pass


    def load_by_date(self, date):
        date_str = date.toString("yyyy-MM-dd")
        col = "creation_date" 
        if self.date_mode_mod.isChecked() or self.date_mode_cm.isChecked(): col = "modified"
        elif self.date_mode_cre.isChecked(): col = "creation_date"
        else: col = "modified" # 'Both' defaults query to modified for standard fetching
        query = f"SELECT id, name, is_folder, extension, size, parent_path, {col}, custom_tags FROM virtual_fs WHERE {col} LIKE ? AND in_trash=0"
        
        self.execute_search(query, (f"{date_str}%",), update_highlights=False)

        dt_str = date.toString("yyyy-MM-dd")
        col = "creation_date" if self.date_mode_cre.isChecked() else "modified"
        if self.date_mode_both.isChecked() or self.date_mode_cm.isChecked(): col = "modified"
        
        with sqlite3.connect(self.db_path) as conn:
            # FIX HANG: Limit HTML string generation to 150 items
            entries = conn.cursor().execute(f"SELECT SUBSTR({col}, 12, 8), name, parent_path, size, category FROM virtual_fs WHERE {col} LIKE ? AND is_folder=0 AND in_trash=0 ORDER BY {col} ASC LIMIT 150", (f"{dt_str}%",)).fetchall()
        
        html = f"<h1 style='color:#58a6ff; text-align:center;'>📖 System Timeline: {date.toString('dddd, MMMM d, yyyy')}</h1><hr>"
        if not entries: 
            html += "<h3 style='color:#8b949e; text-align:center;'><br><br>No system activity recorded on this day.</h3>"
        else:
            if len(entries) == 150:
                html += f"<p style='color:#e3b341; text-align:center;'><b>Showing first 150 activities. Check Activity Log tab for full list.</b></p><br>"
            else:
                html += f"<p style='color:#c9d1d9; text-align:center;'><b>{len(entries)}</b> files were logged.</p><br>"
            
            html += "<ul style='list-style-type: none; padding-left: 0;'>"
            cat_colors = {"Images": "#a371f7", "Videos": "#f85149", "Audio": "#ff7b72", "Documents": "#d2a8ff", "Code": "#79c0ff", "Others": "#8b949e"}
            action_verb = "Created" if self.date_mode_cre.isChecked() else "Modified"
            
            for time_str, name, pp, size, cat in entries:
                c_color = cat_colors.get(cat, "#8b949e")
                try: safe_size = human_size(size)
                except: safe_size = f"{size} bytes"
                
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

    def execute_search(self, query, params, update_highlights=True):
        self.table.setUpdatesEnabled(False) 
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        
        self.current_chart_query = query
        self.current_chart_params = params
        
        colors_map = {"Red": QColor("#5c2121"), "Orange": QColor("#663c14"), "Gold": QColor("#5c4c21"), "Green": QColor("#215c2b"), "Cyan": QColor("#1b5e5e"), "Blue": QColor("#213c5c"), "Purple": QColor("#43215c"), "Pink": QColor("#5c2144")}

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            cur = conn.cursor()
                       
            try: where_clause = query.split("WHERE ")[1].split(" ORDER BY")[0].split(" LIMIT")[0]
            except: where_clause = "in_trash=0"
                
            # --- UPDATED: Automatically triggers the smart calendar painter ---
            if update_highlights:
                self.highlight_month(self.calendar.yearShown(), self.calendar.monthShown())

            # --- SMART LIMIT & OFFSET ARCHITECTURE ---
            # Instantly count total records
            count_query = "SELECT COUNT(id) FROM virtual_fs WHERE " + where_clause
            total_rows = conn.execute(count_query, params).fetchone()[0]
            
            if total_rows == 0:
                self.table.setUpdatesEnabled(True)
                self.render_charts()
                self.render_monthly_chart()
                return

            limit = min(500, total_rows)
            offset = 0
            
            # Show control prompt if user hit 'Apply Filter' and data is large
            if total_rows > 500:
                dlg = RowLimitDialog(total_rows, self)
                if dlg.exec() == QDialog.Accepted:
                    limit, offset = dlg.get_values()
                else:
                    self.table.setUpdatesEnabled(True)
                    return # User cancelled
            
            prog = QProgressDialog(f"Rendering {limit} rows to UI...", "Cancel", 0, limit, self)
            prog.setWindowTitle("Loading Activity Log")
            prog.setWindowModality(Qt.WindowModal)
            prog.setMinimumDuration(0)
            prog.show(); QApplication.processEvents()

            # Execute the precise slice for the UI Grid ONLY
            table_query = query.split(" LIMIT")[0] + f" LIMIT {limit} OFFSET {offset}"
            mod_query = table_query.replace("custom_tags FROM", "custom_tags, color_tag, category FROM")
            cur.execute(mod_query, params)
            results = cur.fetchall()
            
            self.table.setRowCount(len(results))
            for r, (db_id, name, is_folder, ext, size, path, mod, tags, c_tag, cat) in enumerate(results):
                if prog.wasCanceled(): 
                    self.table.setRowCount(r)
                    break
                    
                if r % 100 == 0:
                    prog.setValue(r)
                    QApplication.processEvents()

                # Fix: Add offset to S.No. so it accurately reflects skipping
                sno_item = QTableWidgetItem(); sno_item.setData(Qt.DisplayRole, r + 1 + offset)
                self.table.setItem(r, 0, sno_item)
                
                name_item = QTableWidgetItem(name)
                if self.parent() and hasattr(self.parent(), '_icon_cache'):
                    ext_clean = str(ext).lower().strip('.') if ext else ""
                    if is_folder: name_item.setIcon(self.parent().style().standardIcon(QStyle.SP_DirIcon))
                    elif ext_clean in self.parent()._icon_cache: name_item.setIcon(self.parent()._icon_cache[ext_clean])
                    else: name_item.setIcon(self.parent().style().standardIcon(QStyle.SP_FileIcon))
                self.table.setItem(r, 1, name_item)
                self.table.setItem(r, 2, QTableWidgetItem("📁 Folder" if is_folder else "📄 File"))
                self.table.setItem(r, 3, QTableWidgetItem(ext if ext else ""))
                self.table.setItem(r, 4, SizeTableWidgetItem(size or 0)) 
                self.table.setItem(r, 5, QTableWidgetItem(str(mod)))
                self.table.setItem(r, 6, QTableWidgetItem(path))
                self.table.setItem(r, 7, QTableWidgetItem(str(tags) if tags else ""))
                
                id_item = QTableWidgetItem(); id_item.setData(Qt.DisplayRole, db_id)
                self.table.setItem(r, 8, id_item)
                
                if c_tag and c_tag in colors_map:
                    bg_brush = QBrush(colors_map[c_tag])
                    for col in range(9):
                        item = self.table.item(r, col)
                        if item: item.setBackground(bg_brush); item.setForeground(QBrush(QColor("white")))

        prog.close()
        self.table.setSortingEnabled(True)
        self.table.setUpdatesEnabled(True) 
        
        # Trigger the optimized SQL chart engines (they will process ALL rows instantly)
        self.render_charts()
        self.render_monthly_chart()
        
    def render_monthly_chart(self):
        self.fig_monthly.clear()
        
        sel_date = self.calendar.selectedDate()
        ym_prefix = f"{sel_date.year()}-{sel_date.month():02d}-%"
        
        metric = self.cb_monthly_metric.currentText()
        ax = self.fig_monthly.add_subplot(111)
        ax.set_facecolor('#0d1117')
        colors = ["#58a6ff", "#3fb950", "#e3b341", "#a371f7", "#f85149", "#d2a8ff", "#79c0ff", "#2ea043"]
        txt_c = "#c9d1d9"
        
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            
            if "Daily Activity" in metric:
                cur.execute("SELECT SUBSTR(modified, 9, 2), COUNT(id), SUM(size) FROM virtual_fs WHERE modified LIKE ? AND in_trash=0 AND is_folder=0 GROUP BY SUBSTR(modified, 9, 2) ORDER BY SUBSTR(modified, 9, 2)", (ym_prefix,))
                data = cur.fetchall()
                if not data:
                    ax.text(0.5, 0.5, "No Activity for this Month", color=txt_c, ha='center'); ax.axis('off')
                else:
                    days = [str(int(r[0])) for r in data]
                    vals = [r[1] if "Count" in metric else (r[2] or 0)/(1024*1024) for r in data]
                    ax.bar(days, vals, color="#3fb950")
                    ax.set_xlabel(f"Days of {sel_date.toString('MMMM yyyy')} (1-31)", color=txt_c, fontweight="bold")
                    ax.set_ylabel("Total Files" if "Count" in metric else "Size (MB)", color=txt_c)
                    ax.set_title(metric, color=txt_c)
                    
            elif "Category" in metric:
                cur.execute("SELECT category, COUNT(id), SUM(size) FROM virtual_fs WHERE modified LIKE ? AND in_trash=0 AND is_folder=0 GROUP BY category ORDER BY SUM(size) DESC", (ym_prefix,))
                data = cur.fetchall()
                if data:
                    vals = [r[1] if "Count" in metric else (r[2] or 0)/(1024*1024) for r in data[:8]]
                    ax.bar([r[0] if r[0] else 'Others' for r in data[:8]], vals, color=colors)
                    ax.set_title(metric + (" (MB)" if "Size" in metric else ""), color=txt_c)
                else: ax.text(0.5, 0.5, "No Data", color=txt_c, ha='center'); ax.axis('off')
                
            elif "Extension" in metric:
                cur.execute("SELECT extension, COUNT(id), SUM(size) FROM virtual_fs WHERE modified LIKE ? AND in_trash=0 AND is_folder=0 GROUP BY extension ORDER BY SUM(size) DESC", (ym_prefix,))
                data = cur.fetchall()
                if data:
                    vals = [r[1] if "Count" in metric else (r[2] or 0)/(1024*1024) for r in data[:10]]
                    ax.bar([str(r[0]).upper() if r[0] else 'NONE' for r in data[:10]], vals, color=colors)
                    ax.set_title(metric + (" (MB)" if "Size" in metric else ""), color=txt_c)
                else: ax.text(0.5, 0.5, "No Data", color=txt_c, ha='center'); ax.axis('off')
                
            elif "Tag" in metric:
                cur.execute("SELECT CASE WHEN custom_tags IS NULL OR custom_tags = '' THEN 'Untagged' ELSE 'Tagged' END, COUNT(id) FROM virtual_fs WHERE modified LIKE ? AND in_trash=0 AND is_folder=0 GROUP BY CASE WHEN custom_tags IS NULL OR custom_tags = '' THEN 'Untagged' ELSE 'Tagged' END", (ym_prefix,))
                data = cur.fetchall()
                if data:
                    ax.pie([r[1] for r in data], labels=[r[0] for r in data], autopct='%1.1f%%', colors=["#30363d", "#58a6ff"], textprops={'color':"white"})
                    ax.set_title("Tag Coverage", color=txt_c)
                else: ax.text(0.5, 0.5, "No Data", color=txt_c, ha='center'); ax.axis('off')
                
            elif "Top 10" in metric:
                cur.execute("SELECT name, size FROM virtual_fs WHERE modified LIKE ? AND in_trash=0 AND is_folder=0 ORDER BY size DESC LIMIT 10", (ym_prefix,))
                data = cur.fetchall()
                if data:
                    names = [str(x[0]).replace('$', '\\$')[:20] + ".." if len(str(x[0])) > 20 else str(x[0]).replace('$', '\\$') for x in data]
                    ax.barh(names, [(x[1] or 0)/1024/1024 for x in data], color="#f85149")
                    ax.invert_yaxis(); ax.set_title("Top 10 Largest Files (MB)", color=txt_c)
                else: ax.text(0.5, 0.5, "No Data", color=txt_c, ha='center'); ax.axis('off')

        if "Tag" not in metric:
            ax.tick_params(colors=txt_c, labelsize=9)
            for spine in ['top', 'right']: ax.spines[spine].set_visible(False)
            for spine in ['bottom', 'left']: ax.spines[spine].set_color('#30363d')
            
        self.fig_monthly.tight_layout()
        self.canvas_monthly.draw()

    def force_chart_redraw(self):
        if self.latest_analytics:
            self.render_charts()

    def render_charts(self):
        self.fig.clear()
        if not hasattr(self, 'current_chart_query'): return
        
        metric = self.cb_chart_metric.currentText()
        ax = self.fig.add_subplot(111)
        ax.set_facecolor('#0d1117')
        colors = ["#58a6ff", "#3fb950", "#e3b341", "#a371f7", "#f85149", "#8b949e", "#79c0ff", "#2ea043"]
        txt_c = "#c9d1d9"
        
        try: where_clause = self.current_chart_query.split("WHERE ")[1].split(" ORDER BY")[0].split(" LIMIT")[0]
        except: where_clause = "in_trash=0"

        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            
            if "Extension" in metric:
                cur.execute(f"SELECT extension, COUNT(id), SUM(size) FROM virtual_fs WHERE {where_clause} AND is_folder=0 GROUP BY extension", self.current_chart_params)
                data = cur.fetchall()
                if data:
                    sorted_d = sorted(data, key=lambda x: x[1] if "Count" in metric else (x[2] or 0), reverse=True)[:8]
                    vals = [x[1] if "Count" in metric else (x[2] or 0)/1024/1024 for x in sorted_d]
                    ax.bar([str(x[0]).upper() if x[0] else 'NONE' for x in sorted_d], vals, color=colors)
                    ax.set_title(metric + (" (MB)" if "Size" in metric else ""), color=txt_c)
                else: ax.text(0.5, 0.5, "No Data", color=txt_c, ha='center'); ax.axis('off')

            elif "Usage by Year" in metric:
                cur.execute(f"SELECT year, SUM(size) FROM virtual_fs WHERE {where_clause} AND year IS NOT NULL AND year != '' GROUP BY year ORDER BY year", self.current_chart_params)
                data = cur.fetchall()
                if data:
                    ax.plot([str(r[0]) for r in data], [(r[1] or 0)/1024/1024 for r in data], marker='o', color="#58a6ff", linewidth=2)
                    ax.fill_between([str(r[0]) for r in data], [(r[1] or 0)/1024/1024 for r in data], color="#58a6ff", alpha=0.2)
                    ax.set_title("Storage Growth by Year (MB)", color=txt_c)
                else: ax.text(0.5, 0.5, "No Data", color=txt_c, ha='center'); ax.axis('off')

            elif "Top 10" in metric:
                cur.execute(f"SELECT name, size FROM virtual_fs WHERE {where_clause} AND is_folder=0 ORDER BY size DESC LIMIT 10", self.current_chart_params)
                data = cur.fetchall()
                if data:
                    names = [str(x[0]).replace('$', '\\$')[:15] + "..." if len(str(x[0])) > 15 else str(x[0]).replace('$', '\\$') for x in data]
                    ax.barh(names, [(x[1] or 0)/1024/1024 for x in data], color="#f85149")
                    ax.invert_yaxis(); ax.set_title("Top 10 Largest Files (MB)", color=txt_c)
                else: ax.text(0.5, 0.5, "No Data", color=txt_c, ha='center'); ax.axis('off')

            elif "Modification Timeline" in metric:
                cur.execute(f"SELECT SUBSTR(modified, 1, 10), COUNT(id) FROM virtual_fs WHERE {where_clause} AND modified IS NOT NULL AND modified != '' GROUP BY SUBSTR(modified, 1, 10) ORDER BY SUBSTR(modified, 1, 10) DESC LIMIT 15", self.current_chart_params)
                data = cur.fetchall()
                if data:
                    data.reverse() # Show chronologically
                    ax.bar([r[0][-5:] for r in data], [r[1] for r in data], color="#3fb950")
                    ax.tick_params(axis='x', rotation=45); ax.set_title("Activity Spikes (Last 15 Active Days)", color=txt_c)
                else: ax.text(0.5, 0.5, "No Data", color=txt_c, ha='center'); ax.axis('off')
                
            elif "Tag" in metric:
                cur.execute(f"SELECT CASE WHEN custom_tags IS NULL OR custom_tags = '' THEN 'Untagged' ELSE 'Tagged' END, COUNT(id) FROM virtual_fs WHERE {where_clause} AND is_folder=0 GROUP BY CASE WHEN custom_tags IS NULL OR custom_tags = '' THEN 'Untagged' ELSE 'Tagged' END", self.current_chart_params)
                data = cur.fetchall()
                if data:
                    ax.pie([r[1] for r in data], labels=[r[0] for r in data], autopct='%1.1f%%', colors=["#30363d", "#58a6ff"], textprops={'color':"white"})
                    ax.set_title("Tag Coverage", color=txt_c)
                else: ax.text(0.5, 0.5, "No Data", color=txt_c, ha='center'); ax.axis('off')
            
            else:
                ax.text(0.5, 0.5, "Metric Supported in Monthly Tab", color=txt_c, ha='center'); ax.axis('off')

        if "Tag" not in metric:
            ax.tick_params(axis='x', colors=txt_c, labelsize=9)
            ax.tick_params(axis='y', colors='#8b949e')
            for spine in ['top', 'right']: ax.spines[spine].set_visible(False)
            for spine in ['bottom', 'left']: ax.spines[spine].set_color('#30363d')
            
        self.fig.tight_layout()
        self.canvas.draw()

    def show_context_menu(self, pos):
        item = self.table.itemAt(pos)
        if not item: return
        row = item.row()
        
        # FIX: Point to column 8 for db_id
        db_id = int(self.table.item(row, 8).text())
        typ = "folder" if "Folder" in self.table.item(row, 2).text() else "file"
        v_path = self.table.item(row, 6).text()
        name = self.table.item(row, 1).text()
        
        full_v_path = f"{v_path}{name}/" if typ == "folder" else f"{v_path}{name}"
        
        menu = QMenu(self)
        act_open = menu.addAction("🚀 Open Native File")
        
        # Grab highlighted rows to update context menu text dynamically
        selected_rows = self.table.selectionModel().selectedRows()
        act_vman = menu.addAction(f"🎞 Open {len(selected_rows)} Highlighted in vman Viewer" if len(selected_rows) > 1 else "🎞 Open in vman Viewer")
        
        act_loc = menu.addAction("📂 Open OS Location")
        act_v_loc = menu.addAction("🌐 Open Virtual Location")
        menu.addSeparator()
        
        # --- NEW COLOR MENU ---
        color_menu = menu.addMenu("🎨 Set Color Tag")
        for color in ["None", "Red", "Orange", "Gold", "Green", "Cyan", "Blue", "Purple", "Pink"]: 
            color_menu.addAction(color, lambda checked=False, c=color: self.bulk_assign_color_tags(c))
            
        act_copy_p = menu.addAction("📋 Copy Virtual Path")
        act_props = menu.addAction("📊 Show Properties")
        act_bulk_tag = menu.addAction("🏷️ Assign Custom Tag")
        menu.addSeparator()
        act_trash = menu.addAction("🗑️ Move to Trash")
        act_perm = menu.addAction("🧨 Delete Permanently")
        
        action = menu.exec(self.table.viewport().mapToGlobal(pos))
        
        if not self.parent(): return
        if action == act_open: self.parent().open_local_file_system(db_id)
        elif action == act_v_loc: self.parent().nav_to_path(v_path)
        elif action == act_vman:
            selected_ids = []
            for idx in selected_rows:
                r = idx.row()
                if "Folder" not in self.table.item(r, 2).text():
                    selected_ids.append((int(self.table.item(r, 8).text()), self.table.item(r, 1).text()))
                    
            if not selected_ids:
                QMessageBox.warning(self, "Viewer", "Please select at least one file (folders are ignored).")
            else:
                playlist = []
                with sqlite3.connect(self.db_path) as conn:
                    cur = conn.cursor()
                    ids_only = [sid[0] for sid in selected_ids]
                    id_to_name = {sid[0]: sid[1] for sid in selected_ids}
                    
                    # Batch fetch in chunks of 900 (prevents SQLite crashes on massive selections)
                    for i in range(0, len(ids_only), 900):
                        chunk = ids_only[i:i+900]
                        cur.execute(f"SELECT id, real_path, extension FROM virtual_fs WHERE id IN ({','.join(['?']*len(chunk))})", chunk)
                        for r_id, rp, ext in cur.fetchall():
                            if rp and os.path.exists(rp):
                                playlist.append({'path': rp, 'name': id_to_name[r_id], 'ext': ext.lower() if ext else ''})
                                
                if not playlist:
                    QMessageBox.warning(self, "Viewer", "No physical files found for the selection.")
                else:
                    new_viewer = vmanViewer(playlist, 0, self.parent())
                    p = self.parent()
                    if p:
                        if not hasattr(p, 'active_viewers'): p.active_viewers = []
                        p.active_viewers.append(new_viewer)
                    new_viewer.show()
                    new_viewer.raise_()
                    new_viewer.activateWindow()
                    
        elif action == act_loc: self.parent().open_file_location(db_id)
        elif action == act_copy_p: QApplication.clipboard().setText(full_v_path)
        elif action == act_props: self.parent().show_properties(typ, full_v_path, db_id)
        elif action == act_bulk_tag: self.bulk_assign_tags()
        elif action == act_trash: self.trash_selected_items(permanent=False)
        elif action == act_perm:
            if QMessageBox.question(self, "Delete", "Permanently delete selected files?", QMessageBox.Yes|QMessageBox.No) == QMessageBox.Yes:
                self.trash_selected_items(permanent=True)

    def open_scanned_file(self, index):
        # FIX: Point to column 8 for db_id
        if self.parent(): self.parent().open_local_file_system(int(self.table.item(index.row(), 8).text()))
  
    def bulk_assign_color_tags(self, color):
        ids = [int(self.table.item(idx.row(), 8).text()) for idx in self.table.selectionModel().selectedRows()]
        if not ids: return
        with sqlite3.connect(self.db_path) as conn:
            conn.cursor().executemany("UPDATE virtual_fs SET color_tag = ? WHERE id = ?", [(color if color != "None" else "", i) for i in ids])
            conn.commit()
        QMessageBox.information(self, "Complete", f"Applied Color Tag: {color} to {len(ids)} items.")
        
        # Re-fetch the current active view to instantly show the painted rows
        self.on_calendar_clicked(self.calendar.selectedDate())
            
    def bulk_assign_tags(self):
        selected_rows = set(idx.row() for idx in self.table.selectedIndexes())
        if not selected_rows: return
        tags, ok = QInputDialog.getText(self, "Assign Tag", "Enter Custom Tag(s) separated by comma:")
        if ok and tags.strip():
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                for r in selected_rows:
                    db_id = int(self.table.item(r, 8).text()) # FIX: Target the correct ID column
                    old = cur.execute("SELECT custom_tags FROM virtual_fs WHERE id=?", (db_id,)).fetchone()[0]
                    new_val = f"{old}, {tags.strip()}".strip(", ") if old else tags.strip()
                    cur.execute("UPDATE virtual_fs SET custom_tags=? WHERE id=?", (new_val, db_id))
                conn.commit()
            QMessageBox.information(self, "Complete", "Tags assigned successfully.")
            self.load_by_filters()

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
        QMessageBox.information(self, "Deletion Complete", f"Successfully {'permanently deleted' if permanent else 'moved to Virtual Trash'} {len(ids_to_del)} items from the VMan Database.")


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
        self.huge_threshold_mb = 500
        self.setWindowTitle("Space & Integrity Analyzer")
        self.resize(1350, 700) 
        self.setMinimumSize(1100, 500)
        self.setWindowFlags(self.windowFlags() | Qt.WindowMaximizeButtonHint | Qt.WindowMinimizeButtonHint)
        
        # --- THEME FIX: Match main app's theme dynamically ---
        if parent and hasattr(parent, 'theme_combo'):
            self.setStyleSheet(THEMES.get(parent.theme_combo.currentText(), THEMES["Dark"]))
        else:
            self.setStyleSheet(THEMES["Dark"])
        
        layout = QVBoxLayout(self)

        folder_lay = QHBoxLayout()
        self.lbl_path = QLabel(f"<b>Scanning:</b> {', '.join(self.scan_roots)}")
        self.lbl_path.setWordWrap(True)
        btn_choose = QPushButton("📂 Set Scan Folders")
        btn_choose.clicked.connect(self.select_scan_folder)
        folder_lay.addWidget(self.lbl_path, stretch=1)
        folder_lay.addWidget(btn_choose)
        layout.addLayout(folder_lay)
        
        # Setup Table (11 Corrected Columns)
        self.table = QTableWidget(0, 11)
        self.table.setHorizontalHeaderLabels(["Select", "S.No.", "Type", "Name", "Location", "Ext", "Size", "Modified Date", "Tags", "SHA-256", "ID"])
        self.table.setSortingEnabled(True) 
        
        self.table.setColumnWidth(0, 50); self.table.setColumnWidth(1, 50); self.table.setColumnWidth(2, 140)
        self.table.setColumnWidth(3, 220); self.table.setColumnWidth(4, 260); self.table.setColumnWidth(5, 70)
        self.table.setColumnWidth(6, 90); self.table.setColumnWidth(7, 140); self.table.setColumnWidth(8, 120); self.table.setColumnWidth(9, 240)
        self.table.setColumnHidden(1, True) # S.No hidden by default
        self.table.setColumnHidden(10, True) # ID hidden

        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)        
        
        # Header Menu Config
        self.table.horizontalHeader().setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.horizontalHeader().customContextMenuRequested.connect(self.show_header_menu)
        
        self.table.doubleClicked.connect(self.open_scanned_file)
        layout.addWidget(self.table)
        
        # Load State
        self.settings = QSettings("vmanOS", "SpaceAnalyzerSettings")
        saved_state = self.settings.value("table_state")
        if saved_state: self.table.horizontalHeader().restoreState(saved_state)

    def closeEvent(self, event):
        self.settings.setValue("table_state", self.table.horizontalHeader().saveState())
        super().closeEvent(event)
        
    def show_header_menu(self, pos):
        menu = QMenu(self)
        for col in range(self.table.columnCount()):
            name = self.table.horizontalHeaderItem(col).text()
            action = menu.addAction(f"Show {name}")
            action.setCheckable(True)
            action.setChecked(not self.table.isColumnHidden(col))
            action.toggled.connect(lambda checked, c=col: self.table.setColumnHidden(c, not checked))
        menu.exec(self.table.horizontalHeader().mapToGlobal(pos))

    def set_huge_threshold(self):
        val, ok = QInputDialog.getInt(self, "Threshold", "Enter huge file threshold (MB):", self.huge_threshold_mb, 10, 50000)
        if ok: self.huge_threshold_mb = val
        
    def invert_checked(self):
        for r in range(self.table.rowCount()):
            it = self.table.item(r, 0) # Fixed to Column 0 (Checkbox)
            if it: it.setCheckState(Qt.Unchecked if it.checkState() == Qt.Checked else Qt.Checked)
            
    def prompt_smart_rule(self):
        rule, ok = QInputDialog.getItem(self, "Smart Selection", "Select Rule:", ["Keep Oldest", "Keep Newest", "Keep files in a specific Virtual Folder..."], 0, False)
        if ok: 
            self.combo_smart_select = QComboBox() 
            self.combo_smart_select.addItem(rule)
            self.apply_smart_selection()
            
    def bulk_tag_items(self, color):
        ids = [int(self.table.item(r, 10).text()) for r in range(self.table.rowCount()) if self.table.item(r, 0).checkState() == Qt.Checked]
        if not ids: return
        with sqlite3.connect(self.db_path) as conn:
            conn.cursor().executemany("UPDATE virtual_fs SET color_tag = ? WHERE id = ?", [(color if color != "None" else "", i) for i in ids])
            conn.commit()
        QMessageBox.information(self, "Complete", f"Applied Color Tag: {color} to {len(ids)} items.")
        
    def bulk_add_custom_tags(self):
        ids = [int(self.table.item(r, 10).text()) for r in range(self.table.rowCount()) if self.table.item(r, 0).checkState() == Qt.Checked]
        if not ids: return
        tags, ok = QInputDialog.getText(self, "Bulk Apply Tags", "Enter tags separated by comma:")
        if not ok or not tags.strip(): return
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            for db_id in ids:
                old_tags = cur.execute("SELECT custom_tags FROM virtual_fs WHERE id=?", (db_id,)).fetchone()[0]
                new_val = f"{old_tags}, {tags.strip()}".strip(", ") if old_tags else tags.strip()
                cur.execute("UPDATE virtual_fs SET custom_tags = ? WHERE id = ?", (new_val, db_id))
            conn.commit()
        QMessageBox.information(self, "Complete", f"Tags applied to {len(ids)} items.")
        

    def apply_smart_selection(self):
        rule = self.combo_smart_select.currentText()
        if self.table.rowCount() == 0: return

        # 1. Group all table rows by their exact Duplicate signature
        groups = {}
        for r in range(self.table.rowCount()):
            typ = self.table.item(r, 1).text()
            if "Duplicate" in typ or "Version" in typ or "Paradox" in typ:
                # Use Hash as primary group key, fallback to Name+Size if no hash exists
                key = self.table.item(r, 7).text() 
                if not key or key == "Not Computed":
                    key = self.table.item(r, 2).text() + "_" + self.table.item(r, 5).text() 
                
                if key not in groups: groups[key] = []
                
                mod_str = self.table.item(r, 6).text()
                loc = self.table.item(r, 3).text()
                try: dt = datetime.strptime(mod_str, "%Y-%m-%d %H:%M:%S")
                except Exception: dt = datetime.min
                
                groups[key].append({'row': r, 'dt': dt, 'loc': loc})
        
        target_folder = ""
        if "specific Virtual Folder" in rule:
            target_folder, ok = QInputDialog.getText(self, "Protect Folder", "Enter the Virtual Folder to KEEP (e.g. /Offline_Archive_Drive/):\n\nAll duplicates outside this folder will be checked for deletion.")
            if not ok or not target_folder.strip(): return
            target_folder = target_folder.strip()

        # 2. Apply rules to each group
        for key, items in groups.items():
            if len(items) < 2: continue # Needs at least 2 to be a duplicate conflict
            
            # Sort items based on the active rule
            if "Keep Oldest" in rule:
                items.sort(key=lambda x: x['dt']) # Oldest is index 0
            elif "Keep Newest" in rule:
                items.sort(key=lambda x: x['dt'], reverse=True) # Newest is index 0
            elif "specific Virtual Folder" in rule:
                # Sort so items inside the protected folder jump to the top (index 0)
                items.sort(key=lambda x: 0 if target_folder in x['loc'] else 1)

            # Keep the 1st item (Uncheck), flag the rest for deletion (Check)
            for i, item_data in enumerate(items):
                chk_state = Qt.Unchecked if i == 0 else Qt.Checked
                self.table.item(item_data['row'], 0).setCheckState(chk_state)
                
        QMessageBox.information(self, "Smart Select Applied", "Checked items based on your rule.\n\nPlease review the selections before clicking Delete.")

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
        menu = QMenu(self)
        
        scan_menu = menu.addMenu("🔍 Run Scanners")
        scan_menu.addAction("🧹 Scan Junk Files", self.scan)
        scan_menu.addAction("🧬 Scan Exact Duplicates", self.scan_hash_duplicates)
        scan_menu.addAction("📝 Scan Version Conflicts", self.scan_version_conflicts)
        scan_menu.addAction("⚠️ Scan Data Anomalies", self.scan_corrupt_files)
        scan_menu.addSeparator()
        scan_menu.addAction(f"⚙️ Set Huge File Threshold (Currently {self.huge_threshold_mb}MB)...", self.set_huge_threshold)
        
        sel_menu = menu.addMenu("☑ Selection")
        sel_menu.addAction("☑ Check All", self.toggle_select_all)
        sel_menu.addAction("✓ Check Highlighted", lambda: self.set_highlighted_state(Qt.Checked))
        sel_menu.addAction("✗ Uncheck Highlighted", lambda: self.set_highlighted_state(Qt.Unchecked))
        sel_menu.addAction("🔄 Invert Checked Items", self.invert_checked)
        sel_menu.addAction("🧠 Apply Smart Rule...", self.prompt_smart_rule)
        
        tag_menu = menu.addMenu("🏷 Tags & Labels (For Checked)")
        color_menu = tag_menu.addMenu("🎨 Set Color Tag")
        for color in ["None", "Red", "Orange", "Gold", "Green", "Cyan", "Blue", "Purple", "Pink"]: 
            color_menu.addAction(color, lambda checked=False, c=color: self.bulk_tag_items(c))
        tag_menu.addAction("📝 Bulk Add Custom Tags...", self.bulk_add_custom_tags)
        
        act_menu = menu.addMenu("🛠 Actions")
        act_menu.addAction("🛡️ Mark Checked as Safe", lambda: self.update_safety_status(True))
        act_menu.addAction("❌ Unmark Checked as Safe", lambda: self.update_safety_status(False))
        act_menu.addAction("👁️ View Safe Files", self.view_safe_files)
        act_menu.addSeparator()
        act_menu.addAction("🗑️ Delete Checked", self.delete_selected)
        
        menu.addSeparator()
        
        if item:
            row = item.row()
            typ = self.table.item(row, 2).text()
            db_id = int(self.table.item(row, 10).text()) # Fixed to Column 10
            
            if any(keyword in typ for keyword in ["Duplicate", "Version", "Paradox", "0-Byte"]):
                act_proof = menu.addAction("⚖️ Compare / Show Proof")
            else: act_proof = None
                
            act_open = menu.addAction("🚀 Open Native File")
            act_loc = menu.addAction("📂 Open File Location")
            
            action = menu.exec(self.table.viewport().mapToGlobal(pos))
            
            if action == act_proof: self.show_proof_dialog(row, typ, db_id)
            elif action == act_open and self.parent(): self.parent().open_local_file_system(db_id)
            elif action == act_loc and self.parent(): self.parent().open_file_location(db_id)
        else:
            menu.exec(self.table.viewport().mapToGlobal(pos))

    def show_proof_dialog(self, row, typ, db_id):
        ext = self.table.item(row, 5).text() # Adjusted Ext index
        sha = self.table.item(row, 9).text() # Adjusted Hash index
        
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            res = cur.execute("SELECT name, size, modified FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
            if not res: return
            true_name, true_size, true_mod = res
            
        query = ""
        query_params = ()
        
        if "Exact" in typ and sha and sha not in ("Not Computed", ""):
            query = "SELECT id, name, parent_path, size, modified, sha256, real_path FROM virtual_fs WHERE sha256=? AND is_folder=0 AND in_trash=0"
            query_params = (sha,)
            
        elif "Duplicate" in typ:
            query = "SELECT id, name, parent_path, size, modified, sha256, real_path FROM virtual_fs WHERE size=? AND extension=? AND is_folder=0 AND in_trash=0"
            query_params = (true_size, ext)
            
        elif "Version" in typ:
            query = "SELECT id, name, parent_path, size, modified, sha256, real_path FROM virtual_fs WHERE name=? AND is_folder=0 AND in_trash=0 ORDER BY modified DESC"
            query_params = (true_name,)
            
        elif "Paradox" in typ:
            query = "SELECT id, name, parent_path, size, modified, sha256, real_path FROM virtual_fs WHERE name=? AND size=? AND modified=? AND is_folder=0 AND in_trash=0"
            query_params = (true_name, true_size, true_mod)
            
        elif "0-Byte" in typ:
            QMessageBox.information(self, "Proof", "This file is exactly 0 bytes. No side-by-side comparison is needed.")
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
            # Column 0 is the Checkbox, Column 10 is the ID
            if self.table.item(r, 0).checkState() == Qt.Checked:
                rows_to_remove.append(r)
                ids_to_update.append(int(self.table.item(r, 10).text()))
                
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

    def scan(self):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        # Removed btn_refresh.setEnabled(False)
        
        huge_bytes = getattr(self, 'huge_threshold_mb', 500) * 1024 * 1024
        self.scanner = SpaceScannerThread(self.db_path, self.scan_roots, huge_bytes, self)
        self.scanner.found.connect(self._add_row)
        
        self.prog_dlg = QProgressDialog(f"Scanning directories...", "Cancel", 0, 100, self)
        self.prog_dlg.setWindowModality(Qt.WindowModal)
        self.prog_dlg.setMinimumDuration(0)
        self.prog_dlg.setValue(0)
        self.prog_dlg.show()
        QApplication.processEvents()
        
        self.scanner.progress.connect(lambda v, t, txt: (self.prog_dlg.setValue(v), self.prog_dlg.setLabelText(txt)))
        self.prog_dlg.canceled.connect(self.scanner.cancel)
        self.scanner.finished_scan.connect(self.on_scan_finished)
        
        self.scanner.start()

    def on_scan_finished(self):
        self.prog_dlg.close()
        # Removed btn_refresh.setEnabled(True)
        self.table.setSortingEnabled(True)


    def view_safe_files(self):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        cond, params = self.get_path_conditions()
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT id, name, parent_path, extension, size, modified, custom_tags, color_tag, sha256 FROM virtual_fs WHERE hash_verified=1 AND ({cond})", params)
            for f in cur.fetchall():
                self._add_row_with_state("🛡️ Verified Safe", f[1], f[2], f[3] or "", f[4] or 0, f[5], f[6] or "", f[7] or "", f[8] or "", f[0], Qt.Unchecked)
        self.table.setSortingEnabled(True)
        if self.table.rowCount() == 0: QMessageBox.information(self, "Result", "No files are currently marked as safe in these locations.")

    def scan_hash_duplicates(self):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        cond, params = self.get_path_conditions()
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT sha256, COUNT(*) as c FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND hash_verified=0 AND ({cond}) AND sha256 IS NOT NULL AND sha256 != '' GROUP BY sha256 HAVING c > 1", params)
            duplicates = cur.fetchall()
            prog = QProgressDialog("Scanning exact hashes...", "Cancel", 0, len(duplicates), self)
            prog.setWindowModality(Qt.WindowModal); prog.show()
            for i, (sha, count) in enumerate(duplicates):
                if prog.wasCanceled(): break
                prog.setValue(i); QApplication.processEvents()
                cur.execute(f"SELECT id, name, parent_path, extension, size, modified, custom_tags, color_tag FROM virtual_fs WHERE sha256=? AND ({cond}) AND is_folder=0 AND in_trash=0 AND hash_verified=0", (sha,) + params)
                for idx, f in enumerate(cur.fetchall()):
                    self._add_row_with_state("Exact Duplicate", f[1], f[2], f[3] or "", f[4] or 0, f[5], f[6] or "", f[7] or "", sha, f[0], Qt.Checked if idx > 0 else Qt.Unchecked)
            prog.close()
        self.table.setSortingEnabled(True)
        if self.table.rowCount() == 0: QMessageBox.information(self, "Result", "No exact SHA-256 duplicates found.")

    def scan_version_conflicts(self):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        cond, params = self.get_path_conditions()
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT name FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND ({cond}) AND sha256 IS NOT NULL AND sha256 != '' AND hash_verified=0 GROUP BY name HAVING COUNT(DISTINCT sha256) > 1", params)
            names = cur.fetchall()
            prog = QProgressDialog("Scanning version conflicts...", "Cancel", 0, len(names), self)
            prog.setWindowModality(Qt.WindowModal); prog.show()
            for i, (name,) in enumerate(names):
                if prog.wasCanceled(): break
                prog.setValue(i); QApplication.processEvents()
                cur.execute(f"SELECT id, name, parent_path, extension, size, modified, custom_tags, color_tag, sha256 FROM virtual_fs WHERE name=? AND ({cond}) AND is_folder=0 AND in_trash=0 AND sha256 IS NOT NULL AND hash_verified=0 ORDER BY modified DESC", (name,) + params)
                for idx, f in enumerate(cur.fetchall()):
                    self._add_row_with_state("📝 Latest Version" if idx == 0 else "🕰️ Older Version", f[1], f[2], f[3] or "", f[4] or 0, f[5], f[6] or "", f[7] or "", f[8] or "", f[0], Qt.Unchecked if idx == 0 else Qt.Checked)
            prog.close()
        self.table.setSortingEnabled(True)
        if self.table.rowCount() == 0: QMessageBox.information(self, "Result", "No version conflicts found.")

    def scan_corrupt_files(self):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        cond, params = self.get_path_conditions()
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT name, size, modified FROM virtual_fs WHERE is_folder=0 AND in_trash=0 AND ({cond}) AND sha256 IS NOT NULL AND sha256 != '' AND hash_verified=0 GROUP BY name, size, modified HAVING COUNT(DISTINCT sha256) > 1", params)
            paradoxes = cur.fetchall()
            prog = QProgressDialog("Scanning anomalies...", "Cancel", 0, len(paradoxes), self)
            prog.setWindowModality(Qt.WindowModal); prog.show()
            for i, (name, size, modified) in enumerate(paradoxes):
                if prog.wasCanceled(): break
                prog.setValue(i); QApplication.processEvents()
                cur.execute(f"SELECT id, name, parent_path, extension, size, modified, custom_tags, color_tag, sha256 FROM virtual_fs WHERE name=? AND size=? AND modified=? AND ({cond}) AND is_folder=0 AND in_trash=0 AND hash_verified=0", (name, size, modified) + params)
                for f in cur.fetchall():
                    self._add_row_with_state("⚠️ Hash Paradox", f[1], f[2], f[3] or "", f[4], f[5], f[6] or "", f[7] or "", f[8] or "", f[0], Qt.Unchecked)
            prog.close()
            cur.execute(f"SELECT id, name, parent_path, extension, size, modified, custom_tags, color_tag, sha256 FROM virtual_fs WHERE size=0 AND is_folder=0 AND in_trash=0 AND ({cond}) AND hash_verified=0", params)
            for f in cur.fetchall():
                self._add_row_with_state("💀 0-Byte File", f[1], f[2], f[3] or "", 0, f[5], f[6] or "", f[7] or "", f[8] or "None", f[0], Qt.Checked)
        self.table.setSortingEnabled(True)
        if self.table.rowCount() == 0: QMessageBox.information(self, "Result", "No corrupted or anomalous files found.")

    def _add_row(self, typ, name, location, ext, size, modified, tags, color_tag, sha256, db_id):
        self._add_row_with_state(typ, name, location, ext, size, modified, tags, color_tag, sha256, db_id, Qt.Unchecked)

    def _add_row_with_state(self, typ, name, location, ext, size, modified, tags, color_tag, sha256, db_id, chk_state):
        row = self.table.rowCount()
        self.table.insertRow(row)
        
        chk = QTableWidgetItem()
        chk.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
        chk.setCheckState(chk_state)
        
        sno = QTableWidgetItem(); sno.setData(Qt.DisplayRole, row + 1)
        
        self.table.setItem(row, 0, chk)
        self.table.setItem(row, 1, sno)
        self.table.setItem(row, 2, QTableWidgetItem(typ))
        self.table.setItem(row, 3, QTableWidgetItem(name))
        self.table.setItem(row, 4, QTableWidgetItem(location))
        self.table.setItem(row, 5, QTableWidgetItem(ext))
        self.table.setItem(row, 6, SizeTableWidgetItem(size or 0))
        self.table.setItem(row, 7, QTableWidgetItem(str(modified)))
        self.table.setItem(row, 8, QTableWidgetItem(str(tags) if tags else ""))
        self.table.setItem(row, 9, QTableWidgetItem(str(sha256) if sha256 else ""))
        
        id_item = QTableWidgetItem(); id_item.setData(Qt.DisplayRole, db_id)
        self.table.setItem(row, 10, id_item)
        
        colors_map = {
            "Red": QColor("#5c2121"), "Orange": QColor("#663c14"), "Gold": QColor("#5c4c21"), "Green": QColor("#215c2b"), 
            "Cyan": QColor("#1b5e5e"), "Blue": QColor("#213c5c"), "Purple": QColor("#43215c"), "Pink": QColor("#5c2144")
        }
        if color_tag and color_tag in colors_map:
            bg_brush = QBrush(colors_map[color_tag])
            # FIX: Start at column 1 to avoid painting over the checkbox
            for col in range(1, 11):
                item = self.table.item(row, col)
                if item: item.setBackground(bg_brush); item.setForeground(QBrush(QColor("white")))
                
    def delete_selected(self):
        # Column 0 is Checkbox, Column 10 is the ID
        ids = [int(self.table.item(r, 10).text()) for r in range(self.table.rowCount()) if self.table.item(r, 0).checkState() == Qt.Checked]
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
                
            QMessageBox.information(self, "Deletion Complete", f"Successfully permanently deleted {len(ids)} items from the VMan Database.")
            
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
        self.combo_action_color.addItems(["None", "Red", "Orange", "Gold", "Green", "Cyan", "Blue", "Purple", "Pink"])
        
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
        prog.setWindowModality(Qt.WindowModal)
        prog.setMinimumDuration(0)
        prog.setValue(0)
        prog.show()
        QApplication.processEvents()
        
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
                if i % 25 == 0: QApplication.processEvents() # <--- Keeps UI responsive
            conn.commit()
            
        self.btn_exec.setEnabled(False)
        self.matched_ids = []
        self.log_box.setHtml(f"<h3 style='color:#3fb950;'>Success! Action applied to {success_count} files.</h3>")
        
        if self.parent():
            self.parent().clear_cache()
            self.parent().refresh_all()


class ThumbnailGeneratorThread(QThread):
    progress = Signal(int)
    def __init__(self, items, parent=None):
        super().__init__(parent)
        self.items = items
        self.is_cancelled = False
    def cancel(self): self.is_cancelled = True
    def run(self):
        for i, (typ, rp, db_id) in enumerate(self.items):
            if self.is_cancelled: break
            if rp and os.path.exists(rp) and rp.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.webp')):
                try:
                    img = QImage(rp)
                    if not img.isNull():
                        scaled = img.scaled(120, 120, Qt.KeepAspectRatio, Qt.FastTransformation)
                        scaled.save(str(THUMBS_DIR / f"{db_id}.png"), "PNG")
                except Exception: pass
            self.progress.emit(i + 1)




class UsageLogDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("App Usage & Session Logs")
        self.resize(600, 400)
        if parent and hasattr(parent, 'styleSheet'): self.setStyleSheet(parent.styleSheet())
        
        layout = QVBoxLayout(self)
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Session Start", "Session End", "Duration"])
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.table)
        
        btn_lay = QHBoxLayout()
        btn_clear = QPushButton("🗑️ Clear All Logs")
        btn_clear.setStyleSheet("background-color: #f85149; color: white; font-weight: bold;")
        btn_clear.clicked.connect(self.clear_logs)
        btn_lay.addStretch()
        btn_lay.addWidget(btn_clear)
        layout.addLayout(btn_lay)
        
        self.log_file = DATA_DIR / "usage.json"
        self.load_logs()

    def load_logs(self):
        self.table.setRowCount(0)
        if not self.log_file.exists(): return
        try:
            with open(self.log_file, "r") as f: logs = json.load(f)
            for r, entry in enumerate(reversed(logs)):
                self.table.insertRow(r)
                self.table.setItem(r, 0, QTableWidgetItem(entry.get("start", "")))
                self.table.setItem(r, 1, QTableWidgetItem(entry.get("end", "")))
                self.table.setItem(r, 2, QTableWidgetItem(entry.get("duration", "")))
        except Exception: pass

    def clear_logs(self):
        if QMessageBox.question(self, "Clear Logs", "Delete all session history?", QMessageBox.Yes|QMessageBox.No) == QMessageBox.Yes:
            if self.log_file.exists(): self.log_file.unlink()
            self.load_logs()

class FloatingClock(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent, Qt.Window | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint | Qt.Tool)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.is_24h = True
        
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(10, 10, 10, 10)
        self.lbl_time = QLabel()
        self.lbl_time.setAlignment(Qt.AlignCenter)
        self.layout.addWidget(self.lbl_time)
        
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_time)
        self.timer.start(1000)
        self.update_time()
        
        # Draggable state
        self._drag_pos = None

    def update_time(self):
        fmt = "%H:%M:%S" if self.is_24h else "%I:%M:%S %p"
        self.lbl_time.setText(datetime.now().strftime(fmt))
        
        is_dark = True
        if self.parent() and hasattr(self.parent(), 'is_dark_mode'): is_dark = self.parent().is_dark_mode
        bg_col = "rgba(13, 17, 23, 0.8)" if is_dark else "rgba(255, 255, 255, 0.8)"
        txt_col = "#58a6ff" if is_dark else "#0969da"
        self.lbl_time.setStyleSheet(f"background-color: {bg_col}; color: {txt_col}; border: 1px solid #30363d; border-radius: 8px; padding: 5px 15px; font-size: 24px; font-weight: bold; font-family: 'Segoe UI';")

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton: self._drag_pos = event.globalPosition().toPoint() - self.frameGeometry().topLeft()
        elif event.button() == Qt.RightButton:
            menu = QMenu(self)
            menu.setStyleSheet("background-color: #161b22; color: white;")
            act_fmt = menu.addAction("Toggle 12h/24h")
            act_close = menu.addAction("Close Clock")
            res = menu.exec(event.globalPosition().toPoint())
            if res == act_fmt: 
                self.is_24h = not self.is_24h; self.update_time()
            elif res == act_close: self.hide()

    def mouseMoveEvent(self, event):
        if self._drag_pos is not None and event.buttons() == Qt.LeftButton:
            self.move(event.globalPosition().toPoint() - self._drag_pos)
            
    def mouseReleaseEvent(self, event): self._drag_pos = None

class QuickTextButton(QPushButton):
    def __init__(self, text, main_app):
        super().__init__(text)
        self.text_val = text
        self.main_app = main_app
        self.setToolTip("Click to Copy")
        self.setStyleSheet("QPushButton { background-color: transparent; border: 1px solid #30363d; border-radius: 4px; padding: 4px 10px; } QPushButton:hover { background-color: #1f6feb; color: white; }")
        self.clicked.connect(self.copy_to_clip)

    def copy_to_clip(self):
        QApplication.clipboard().setText(self.text_val)
        if self.main_app: self.main_app.status.showMessage(f"Copied: {self.text_val}", 2000)

    def mousePressEvent(self, event):
        if event.button() == Qt.RightButton:
            menu = QMenu(self)
            act_del = menu.addAction("🗑️ Delete Quick Text")
            if menu.exec(event.globalPosition().toPoint()) == act_del:
                self.main_app.remove_quick_text(self.text_val)
        else: super().mousePressEvent(event)

class FloatingNotepad(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent, Qt.Window | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint | Qt.Tool)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.resize(260, 300)
        
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(5, 5, 5, 5)
        self.layout.setSpacing(0)
        
        # Draggable Header
        self.header = QWidget()
        self.header.setStyleSheet("background-color: #21262d; border-top-left-radius: 8px; border-top-right-radius: 8px;")
        self.header.setFixedHeight(28)
        h_lay = QHBoxLayout(self.header)
        h_lay.setContentsMargins(10, 0, 5, 0)
        lbl = QLabel("📝 Quick Text Pad")
        lbl.setStyleSheet("color: #8b949e; font-size: 11px; font-weight: bold; border: none;")
        h_lay.addWidget(lbl)
        h_lay.addStretch()
        btn_close = QPushButton("✕")
        btn_close.setFixedSize(20, 20)
        btn_close.setStyleSheet("QPushButton { background: transparent; color: #8b949e; border: none; font-weight: bold; } QPushButton:hover { color: #f85149; }")
        btn_close.clicked.connect(self.hide)
        h_lay.addWidget(btn_close)
        
        # Text Area
        self.text_edit = QPlainTextEdit()
        self.text_edit.setPlaceholderText("Type or paste anything here...\n\n(It will stay on top of everything)")
        self.text_edit.setStyleSheet("QPlainTextEdit { background-color: rgba(13, 17, 23, 0.9); color: #c9d1d9; border: 1px solid #30363d; border-bottom-left-radius: 8px; border-bottom-right-radius: 8px; padding: 8px; font-size: 12px; }")
        
        self.layout.addWidget(self.header)
        self.layout.addWidget(self.text_edit)
        self._drag_pos = None

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton and event.position().y() <= 35:
            self._drag_pos = event.globalPosition().toPoint() - self.frameGeometry().topLeft()

    def mouseMoveEvent(self, event):
        if self._drag_pos is not None and event.buttons() == Qt.LeftButton:
            self.move(event.globalPosition().toPoint() - self._drag_pos)
            
    def mouseReleaseEvent(self, event):
        self._drag_pos = None

class DragScrollTabBar(QTabBar):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._drag_pos = None
        self.setUsesScrollButtons(True) # Keep true so tabs don't squish
        self.setStyleSheet("QTabBar::scroller { width: 0px; }") # Hide the physical buttons natively
        self.setCursor(Qt.OpenHandCursor)

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self._drag_pos = event.position().x()
            self.setCursor(Qt.ClosedHandCursor)
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if self._drag_pos is not None:
            delta = self._drag_pos - event.position().x()
            if abs(delta) > 2: # Send synthetic wheel scroll events for buttery dragging
                import PySide6.QtGui as QtGui
                angle_delta = QtGui.QPoint(0, int(delta * -3))
                wheel_event = QtGui.QWheelEvent(event.position(), event.globalPosition(), QtGui.QPoint(0,0), angle_delta, Qt.NoButton, Qt.NoModifier, Qt.ScrollUpdate, False)
                QApplication.sendEvent(self, wheel_event)
                self._drag_pos = event.position().x()
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        self._drag_pos = None
        self.setCursor(Qt.OpenHandCursor)
        super().mouseReleaseEvent(event)
        
class InteractiveDashboardCard(QFrame):
    clicked = Signal()
    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton: self.clicked.emit()
        super().mousePressEvent(event)

# ---------------- Main Application Window ----------------
class vmanVirtualManager(QMainWindow):
    def __init__(self):
        super().__init__()
        self.show_hidden = False
        self.show_secondary_names = False
        self.current_prefix = "/"
        self.active_db_path = str(DB_FILE)
        
        # --- NEW: App Usage Session Tracker, Floating Clock & Notepad ---
        self.session_start = datetime.now()
        self.floating_clock = FloatingClock(self)
        self.floating_clock.hide() 
        self.floating_notepad = FloatingNotepad(self)
        self.floating_notepad.hide()
        
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
        self.active_viewers = []  # Stores multiple open viewer windows
        
        self.render_timer = QTimer(self)
        self.render_timer.timeout.connect(self._render_chunk)
        self.render_progress = None

        ensure_dirs()
        ICONS_DIR.mkdir(parents=True, exist_ok=True)
        THUMBS_DIR.mkdir(parents=True, exist_ok=True)
        self.db = vmanDB(DB_FILE)
        self.setWindowTitle(APP_TITLE)
        
        self._load_custom_icons() # Load custom ext icons into RAM
        self.resize(1600, 950)
        self.setFont(QFont("Segoe UI", 10))
        self.setWindowIcon(QIcon("icons/vman.png"))
        
        self._build_ui()
        self._setup_shortcuts()
        self.apply_theme("Dark") 
        self.refresh_all()
        
        # ADD THIS BLOCK to restore column sizes and visibility
        saved_table_state = self.settings.value("main_table_state")
        if saved_table_state:
            self.file_table.horizontalHeader().restoreState(saved_table_state)
            
        
        ensure_dirs()
        ICONS_DIR.mkdir(parents=True, exist_ok=True)
        THUMBS_DIR.mkdir(parents=True, exist_ok=True)
        self.db = vmanDB(DB_FILE)
        self.setWindowTitle(APP_TITLE)
        
        self._load_custom_icons() # Load custom ext icons into RAM
        
    def clean_orphaned_thumbnails(self):
        if not THUMBS_DIR.exists(): 
            return QMessageBox.information(self, "Cleanup", "Thumbnails directory does not exist yet.")
            
        thumb_files = list(THUMBS_DIR.glob("*.png"))
        if not thumb_files: 
            return QMessageBox.information(self, "Cleanup", "No thumbnails found to clean.")
        
        db_ids = []
        for f in thumb_files:
            try: db_ids.append(int(f.stem))
            except ValueError: pass
            
        if not db_ids: return
        
        valid_ids = set()
        deleted_count = 0
        try:
            with sqlite3.connect(self.db.path) as conn:
                cur = conn.cursor()
                # Batch in chunks of 900 to respect SQLite's variable limits
                for i in range(0, len(db_ids), 900): 
                    chunk = db_ids[i:i+900]
                    cur.execute(f"SELECT id FROM virtual_fs WHERE id IN ({','.join('?'*len(chunk))})", chunk)
                    valid_ids.update(r[0] for r in cur.fetchall())
                    
            for f in thumb_files:
                try:
                    if int(f.stem) not in valid_ids:
                        f.unlink() # Delete the orphaned thumbnail
                        deleted_count += 1
                except ValueError: pass
                
            QMessageBox.information(self, "Cleanup Complete", f"Successfully cleared {deleted_count} orphaned thumbnails from the drive.")
            self.sys_log(f"Manual cleanup: Removed {deleted_count} orphaned thumbnails.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to clean thumbnails: {e}")

    def show_breadcrumb_menu(self, pos):
        menu = QMenu(self)
        act_type = menu.addAction("📍 Type Virtual Address...")
        if menu.exec(self.breadcrumb_scroll.mapToGlobal(pos)) == act_type:
            self.prompt_type_address()

    def sys_log(self, message, level="INFO"):
        if hasattr(self, 'log_dock'):
            self.log_dock.log(message, level)

    def clear_cache(self): self.view_cache.clear()

    def _build_ui(self):

        tb = QToolBar("Navigation")
        tb.setMovable(False)
        tb.setIconSize(QSize(20, 20))
        tb.setMinimumHeight(40) 
        self.addToolBar(Qt.TopToolBarArea, tb)
        
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
        
        act_timeline = QAction("📅 Timeline", self)
        act_timeline.setToolTip("Timeline Diary")
        act_timeline.triggered.connect(self.open_timeline_diary)
        
        act_analyzer = QAction("🧹 Analyzer", self)
        act_analyzer.triggered.connect(lambda: SpaceAnalyzerDialog(self.active_db_path, self).exec())
        
        act_bulk_del = QAction("🗑 Bulk Operations", self)
        act_bulk_del.triggered.connect(lambda: BulkOperationEngine(self.active_db_path, self).exec())
        
        act_load_ext = QAction("📂 Load DB...", self)
        act_load_ext.triggered.connect(self.load_external_db)
        
        act_csv_lib = QAction("📚 Tags", self)
        act_csv_lib.setToolTip("Tag Library")
        act_csv_lib.triggered.connect(self.open_tag_library)

        act_set_storage = QAction("💾 Set Storage", self)
        act_set_storage.triggered.connect(self.set_storage_capacity)

        self.act_search = QAction("🔍 Search", self)
        self.act_search.triggered.connect(self.open_advanced_search)
 
        
        self.act_view_mode = QAction("🖼 Grid View", self)
        self.act_view_mode.triggered.connect(self.toggle_view_mode)
        
        self.act_fast_mode = QAction("⚡", self)
        self.act_fast_mode.setCheckable(True)
        self.act_fast_mode.setChecked(True) # Default to True for massive DBs
        self.act_fast_mode.setToolTip("Disable disk I/O and deep folder calculations for maximum speed.")
        self.act_fast_mode.triggered.connect(self.clear_cache)
        self.act_fast_mode.triggered.connect(lambda: self.load_directory(self.current_prefix))
        
        self.act_sec_name = QAction("🏷", self)
        self.act_sec_name.setToolTip("Toggle Secondary Names")
        self.act_sec_name.setCheckable(True)
        self.act_sec_name.triggered.connect(self.toggle_secondary_names)
        tb.addAction(self.act_sec_name)
        
        self.act_toggle_sidebar = QAction("📊 Inspector", self)
        self.act_toggle_sidebar.triggered.connect(self.toggle_sidebar)
        
        act_extra = QAction("🛠 Extra", self)
        act_extra.setToolTip("Super User Tools: Disk Analysis, Network Center, Deep Inspector")
        act_extra.triggered.connect(lambda: ExtraFeaturesDialog(self).exec())
        
        self.theme_combo = QComboBox()
        self.theme_combo.addItems(list(THEMES.keys()))
        self.theme_combo.currentTextChanged.connect(self.apply_theme)
        
        act_help = QAction("❓", self)
        act_help.triggered.connect(self.show_help)
        
        tb.addActions([self.act_back, self.act_fwd, act_up])
        tb.addSeparator()
        tb.addActions([act_new_folder, act_new_file])
        tb.addSeparator()
        
 
        tb.addActions([act_analyzer, act_bulk_del, act_timeline, act_csv_lib, act_load_ext, act_set_storage])
        tb.addSeparator()

        # Replaced Inspector with Search, removed Fast Mode from here
        tb.addActions([self.act_view_mode, self.act_search, act_extra])
        
        empty = QWidget()
        empty.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        tb.addWidget(empty)
        
        # Arranged left to right: Help -> Fast Mode -> Theme (Theme is rightmost)
        tb.addAction(act_help)
        tb.addAction(self.act_fast_mode)
        tb.addWidget(self.theme_combo)
        
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
        self.breadcrumb_scroll.setContextMenuPolicy(Qt.CustomContextMenu)
        self.breadcrumb_scroll.customContextMenuRequested.connect(self.show_breadcrumb_menu)
        
        self.local_filter = QLineEdit()
        self.local_filter.setPlaceholderText("Filter ")
        self.local_filter.setMaximumWidth(300)
        self.local_filter.textChanged.connect(self.filter_current_view)
        
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Global Search")
        self.search_box.setMaximumWidth(300)
        self.search_box.returnPressed.connect(self.run_global_search)
        
        # --- NEW: Quick Text Ribbon ---
        self.qt_toolbar = QToolBar("Quick Text Ribbon")
        self.qt_toolbar.setIconSize(QSize(16, 16))
        self.qt_toolbar.setStyleSheet("QToolBar { border-bottom: 1px solid #30363d; spacing: 5px; }")
        self.addToolBar(Qt.TopToolBarArea, self.qt_toolbar)
        self.refresh_quick_text()
        # -----------------------------
        
        nav_row.addWidget(self.breadcrumb_scroll, stretch=1)
        nav_row.addWidget(self.local_filter)
        nav_row.addWidget(self.search_box)
        vbox.addLayout(nav_row) # This line already exists

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
        # ADD THESE TWO LINES:
        self.file_table.horizontalHeader().setContextMenuPolicy(Qt.CustomContextMenu)
        self.file_table.horizontalHeader().customContextMenuRequested.connect(self.show_main_header_menu)
        
        self.view_stack.addWidget(self.file_table)

        self.file_grid = SandboxListView()
        self.file_grid.setModelColumn(1)
        self.file_grid.filesDroppedOS.connect(self.on_files_dropped)
        self.file_grid.internalDrop.connect(self.execute_internal_drop)
        self.file_grid.setContextMenuPolicy(Qt.CustomContextMenu)
        self.file_grid.customContextMenuRequested.connect(lambda pos: self.context_menu(pos, is_grid=True))
        self.file_grid.doubleClicked.connect(self.open_selected)
        self.file_grid.clicked.connect(self.on_grid_click)
        self.file_grid.openRequest.connect(self.open_selected)
        self.view_stack.addWidget(self.file_grid)
        
        vbox.addWidget(self.view_stack)

        self.log_dock = VManConsole(parent=self)
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
        self.ed_tag.addItems(["None", "Red", "Orange", "Gold", "Green", "Cyan", "Blue", "Purple", "Pink"])
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

        self.charts_container = QWidget()
        self.charts_container.setObjectName("ChartsContainer")
        ch_layout = QVBoxLayout(self.charts_container)
        ch_layout.setContentsMargins(0, 0, 0, 0) # No wasted space
        ch_layout.setSpacing(0)

        # Sleek Top Bar
        self.ctrl_bar = QWidget()
        ctrl_lay = QHBoxLayout(self.ctrl_bar)
        ctrl_lay.setContentsMargins(10, 10, 10, 10)

        self.stat_combo = QComboBox()
        self.stat_combo.addItems(["Distribution by Extension (Size)", "Distribution by Extension (Count)", "Top 10 Largest Files", "Storage Ratio (Pie Chart)", "File Count Over Time"])
        self.stat_combo.currentIndexChanged.connect(self.update_statistics)

        self.lbl_inv = QLabel("Investigation:")
        ctrl_lay.addWidget(self.lbl_inv)
        ctrl_lay.addWidget(self.stat_combo, 1)
        ch_layout.addWidget(self.ctrl_bar)

        if MATPLOTLIB_AVAILABLE: 
            # Responsive Figure (no hardcoded figsize)
            self.figure = Figure(dpi=100)
            self.canvas = FigureCanvas(self.figure)
            ch_layout.addWidget(self.canvas, stretch=1) # Expands natively
        else: 
            self.figure = self.canvas = None
            self.lbl_err = QLabel("Matplotlib not installed.")
            self.lbl_err.setAlignment(Qt.AlignCenter)
            ch_layout.addWidget(self.lbl_err, stretch=1)

        self.right_tabs.addTab(self.charts_container, "Charts")

        self.right_dock.setWidget(self.right_tabs)
        self.addDockWidget(Qt.RightDockWidgetArea, self.right_dock)
        
        # Force the Inspector to be slim by default so the main UI has room
        self.resizeDocks([self.right_dock], [380], Qt.Horizontal)
        
        self.status = QStatusBar()
        self.setStatusBar(self.status)

    def prompt_type_address(self):
        path, ok = QInputDialog.getText(self, "Navigate", "Enter Virtual Path (e.g., /Images/Vacation/):", QLineEdit.Normal, self.current_prefix)
        if ok and path:
            if not path.startswith("y_m_f://") and not path.startswith("tags://") and not path.startswith("trash://") and not path.startswith("fav://"):
                if not path.endswith('/'): path += '/'
                if not path.startswith('/'): path = '/' + path
            self.nav_to_path(path)

    def open_timeline_diary(self):
        # Create it once and keep it in memory, just like the Tag Library
        if not hasattr(self, 'timeline_diary_instance') or self.timeline_diary_instance is None:
            self.timeline_diary_instance = TimelineDiaryDialog(self.active_db_path, self)
        else:
            # If the database changed, update it
            if self.timeline_diary_instance.db_path != self.active_db_path:
                self.timeline_diary_instance.db_path = self.active_db_path
                self.timeline_diary_instance.populate_dropdowns()
                
        # Show it as a standard non-blocking window!
        self.timeline_diary_instance.show()
        self.timeline_diary_instance.raise_()
        self.timeline_diary_instance.activateWindow()

    def generate_thumbnails_current_view(self):
        items = []
        model = self.file_table.model()
        for r in range(model.rowCount()):
            data = model.data(model.index(r, 1), Qt.UserRole)
            if data and data[0] == "file":
                items.append(data)
        
        if not items: return
        
        self.thumb_dlg = QProgressDialog("Extracting Thumbnails...", "Cancel", 0, len(items), self)
        self.thumb_dlg.setWindowModality(Qt.WindowModal)
        self.thumb_dlg.show()
        
        self.thumb_worker = ThumbnailGeneratorThread(items, self)
        self.thumb_worker.progress.connect(lambda i: self.thumb_dlg.setValue(i))
        self.thumb_worker.finished.connect(lambda: (self.thumb_dlg.close(), self.clear_cache(), self.refresh_all()))
        self.thumb_dlg.canceled.connect(self.thumb_worker.cancel)
        self._register_worker(self.thumb_worker)
        self.thumb_worker.start()

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
        if not items: return
        
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

        # --- SMART COUNTING LOGIC ---
        file_count = sum(1 for typ, _, _ in items if typ == "file")
        folder_count = sum(1 for typ, _, _ in items if typ == "folder")
        
        msg_parts = []
        if file_count > 0: msg_parts.append(f"{file_count} FILE(S)")
        if folder_count > 0: msg_parts.append(f"{folder_count} FOLDER(S)")
        target_str = " AND ".join(msg_parts)

        # Call YOUR custom flashing dialog, passing the smart string
        warning_dlg = PhysicalDeleteWarningDialog(target_str, self)
        if warning_dlg.exec() != QDialog.Accepted:
            return
        
        deleted = 0
        
        # --- Freeze-Free Progress Dialog ---
        prog = QProgressDialog(f"Preparing to delete {target_str.lower()}...", "Cancel", 0, len(items), self)
        prog.setWindowTitle("Physical Deletion")
        prog.setWindowModality(Qt.WindowModal)
        prog.show()
        
        
        with sqlite3.connect(self.db.path) as conn:
            cur = conn.cursor()
            for i, (typ, path, db_id) in enumerate(items):
                if prog.wasCanceled(): break
                if db_id == -1: continue
                
                # Keep UI responsive and show exactly what is being deleted
                prog.setLabelText(f"Deleting physical data for:\n{path}")
                prog.setValue(i)
                QApplication.processEvents()
                
                # Retrieve all real paths for this item (and its children if it's a folder)
                if typ == "folder":
                    real_paths = cur.execute("SELECT real_path FROM virtual_fs WHERE (parent_path LIKE ? OR id=?) AND real_path != '' AND real_path IS NOT NULL", (f"{path}%", db_id)).fetchall()
                else:
                    real_paths = cur.execute("SELECT real_path FROM virtual_fs WHERE id=? AND real_path != '' AND real_path IS NOT NULL", (db_id,)).fetchall()
                
                for (rpath,) in real_paths:
                    if os.path.exists(rpath):
                        try:
                            if os.path.isdir(rpath):
                                shutil.rmtree(rpath) 
                            else:
                                os.remove(rpath)
                            deleted += 1
                        except Exception as e:
                            print(f"Failed to delete {rpath}: {e}")
                            
                if typ == "file": cur.execute("DELETE FROM virtual_fs WHERE id=?", (db_id,))
                else: cur.execute("DELETE FROM virtual_fs WHERE parent_path LIKE ? OR id=?", (f"{path}%", db_id))
            conn.commit()
            
        # Clean up progress bar
        prog.setValue(len(items))
        prog.close()
        
        self.clear_cache()
        self.refresh_all()
        self.status.showMessage(f"Physically deleted {deleted} items from disk.")
        QMessageBox.information(self, "Physical Deletion Complete", f"Successfully deleted {deleted} physical items directly from your Hard Drive / OS.")
        
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
        VManHelpDialog(self.theme_combo.currentText(), self).exec()

    def filter_current_view(self, text):
        term = text.lower()
        if self.view_stack.currentIndex() == 0:
            for row in range(self.file_table.model().rowCount()):
                name = self.file_table.model().data(self.file_table.model().index(row, 1), Qt.DisplayRole)
                self.file_table.setRowHidden(row, term not in str(name).lower())
        else:
            for row in range(self.file_grid.model().rowCount()):
                name = self.file_grid.model().data(self.file_grid.model().index(row, 1), Qt.DisplayRole)
                self.file_grid.setRowHidden(row, term not in str(name).lower())
                
    def apply_theme(self, theme_name=None):
        if theme_name is None: theme_name = self.theme_combo.currentText()
        
        # 1. Strip the old style to prevent layout/margin caching
        QApplication.instance().setStyleSheet("")
        
        # 2. Apply the new style globally
        css = THEMES.get(theme_name, THEMES["Dark"])
        QApplication.instance().setStyleSheet(css)
        
        self.is_dark_mode = "Light" not in theme_name
        
        # 3. Force the UI to repaint and flush any visual artifacts
        self.style().unpolish(self)
        self.style().polish(self)
        self.update()
        
        self.update_statistics()
        
        # --- FIX: Force the sidebar to redraw so the text turns Black in Light Mode! ---
        self.refresh_tree() 
        
        # Update Search Window Theme instantly if it is open
        if hasattr(self, 'search_instance') and self.search_instance:
            self.search_instance.apply_theme()
        
    def toggle_view_mode(self):
        self.view_stack.setCurrentIndex(1 if self.view_stack.currentIndex() == 0 else 0)
        self.act_view_mode.setText("📄 List View" if self.view_stack.currentIndex() == 1 else "🖼 Grid View")
        
    def toggle_sidebar(self): self.right_dock.setVisible(not self.right_dock.isVisible())

    def _load_custom_icons(self):
        if ICONS_DIR.exists():
            for f in ICONS_DIR.iterdir():
                if f.is_file() and f.suffix.lower() in ('.png', '.ico', '.jpg', '.jpeg'):
                    ext_name = f.stem.lower()
                    self._icon_cache[ext_name] = QIcon(str(f))

    def _get_native_icon(self, real_path: str, is_folder: bool, ext: str = "", db_id: int = -1) -> QIcon:
        if is_folder: 
            return self.style().standardIcon(QStyle.SP_DirIcon)
            
        # 1. Check if a manual thumbnail was generated for this specific file
        if db_id != -1:
            thumb_path = THUMBS_DIR / f"{db_id}.png"
            cache_key = f"thumb_{db_id}"
            
            if cache_key in self._icon_cache:
                return self._icon_cache[cache_key]
                
            if thumb_path.exists():
                self._icon_cache[cache_key] = QIcon(str(thumb_path))
                return self._icon_cache[cache_key]

        # 2. Fallback to extension icons (jpg.png, txt.png)
        ext = str(ext).lower().strip('.')
        if not ext: 
            return self.style().standardIcon(QStyle.SP_FileIcon)

        if ext not in self._icon_cache:
            self._icon_cache[ext] = self.style().standardIcon(QStyle.SP_FileIcon)
            
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
        sys_root = QTreeWidgetItem(self.folder_tree, [f"{db_label}"]); sys_root.setData(0, Qt.UserRole, "/"); sys_root.setIcon(0, self.style().standardIcon(QStyle.SP_DirHomeIcon)); sys_root.setExpanded(True)
        QTreeWidgetItem(sys_root, ["⭐ Favorites"]).setData(0, Qt.UserRole, "fav://")
        QTreeWidgetItem(sys_root, ["🗑 Trash Bin"]).setData(0, Qt.UserRole, "trash://")
        smart = QTreeWidgetItem(sys_root, ["Dynamic Smart Views"]); smart.setIcon(0, self.style().standardIcon(QStyle.SP_FileDialogDetailedView)); smart.setExpanded(True)
        QTreeWidgetItem(smart, ["🏷️ By Custom Tags"]).setData(0, Qt.UserRole, "tags://")
        QTreeWidgetItem(smart, ["🗂 Stat: Year ➔ Month ➔ Folder"]).setData(0, Qt.UserRole, "y_m_f://")
        for proto, cols in SMART_PROTOCOLS.items():
            if proto == "tags://": continue
            QTreeWidgetItem(smart, [f"🗂 {' ➔ '.join([c.capitalize() for c in cols])}"]).setData(0, Qt.UserRole, proto)
        compiled_root = QTreeWidgetItem(self.folder_tree, ["Switch Database..."]); compiled_root.setIcon(0, self.style().standardIcon(QStyle.SP_DriveHDIcon)); compiled_root.setExpanded(True)
        if self.active_db_path != str(DB_FILE):
            node = QTreeWidgetItem(compiled_root, ["⬅ Return to Main DB"]); node.setData(0, Qt.UserRole, "db://main"); node.setIcon(0, self.style().standardIcon(QStyle.SP_ArrowBack))
            
        # --- DB MAKEUP MODE UI, SORTING & HIGHLIGHTING ---
        self.db_makeup_mode = self.settings.value("db_makeup_mode", True, type=bool)
        color_scheme = self.settings.value("db_color_scheme", "Default")
        sort_mode = self.settings.value("db_sort_mode", "Alphabetical")
        
        is_dark = getattr(self, 'is_dark_mode', True)
        base_txt_c = "#161b22" if not is_dark else "#c9d1d9"
        active_accent = "#0969da" if not is_dark else "#58a6ff" # Blue highlight for the Active DB
        opacity = "0.15" 
        
        # 1. Parse and extract sorting metadata for all databases
        db_items = []
        for view_db in VIEWS_DIR.glob("*.db"):
            display_name = view_db.stem
            clean_name = display_name
            date_val = "00000000"
            date_str = ""
            
            if clean_name.startswith("[") and "]" in clean_name:
                end_idx = clean_name.find("]")
                d_val = clean_name[1:end_idx]
                if len(d_val) == 8 and d_val.isdigit():
                    date_val = d_val
                    date_str = f"{d_val[:4]}-{d_val[4:6]}-{d_val[6:]}"
                clean_name = clean_name[end_idx+1:]
                if clean_name.startswith("_"): clean_name = clean_name[1:]
                
            is_hash = False
            if clean_name.lower().startswith("hash_"):
                is_hash = True; clean_name = clean_name[5:]
                
            is_pen = is_ssd = is_hdd = False
            cat_rank = 3 # Default (Uncategorized)
            if clean_name.lower().startswith("pen-"): is_pen = True; cat_rank = 0; clean_name = clean_name[4:]
            elif clean_name.lower().startswith("ssd-") or clean_name.lower().startswith("sdd-"): is_ssd = True; cat_rank = 1; clean_name = clean_name[4:]
            elif clean_name.lower().startswith("hdd-"): is_hdd = True; cat_rank = 2; clean_name = clean_name[4:]
            
            clean_name = clean_name.lstrip('-_ ')
            if not clean_name: clean_name = display_name
            
            db_items.append({
                'path': view_db, 'orig': display_name, 'clean': clean_name, 'date_val': date_val, 'date_str': date_str,
                'is_hash': is_hash, 'is_pen': is_pen, 'is_ssd': is_ssd, 'is_hdd': is_hdd, 'cat_rank': cat_rank
            })
            
        # 2. Sort the database list based on chosen mode
        if sort_mode == "Category":
            db_items.sort(key=lambda x: (x['cat_rank'], x['clean'].lower()))
        elif sort_mode == "Date":
            db_items.sort(key=lambda x: (x['date_val'], x['clean'].lower()), reverse=True) # Newest First
        else: # Alphabetical
            db_items.sort(key=lambda x: x['clean'].lower())
            
        # 3. Render the sorted tree
        active_db_res = str(Path(self.active_db_path).resolve()) if self.active_db_path else ""
        
        for item in db_items:
            view_db = item['path']
            is_active = (str(view_db.resolve()) == active_db_res)
            
            node = QTreeWidgetItem(compiled_root)
            node.setData(0, Qt.UserRole, f"db://{view_db.name}")
            
            if self.db_makeup_mode:
                container = QWidget()
                container.setAttribute(Qt.WA_TransparentForMouseEvents) 
                lay = QHBoxLayout(container)
                lay.setContentsMargins(0, 0, 0, 0); lay.setSpacing(6)
                
                icon_lbl = QLabel()
                icon_lbl.setPixmap(self.style().standardIcon(QStyle.SP_DriveFDIcon).pixmap(16, 16))
                lay.addWidget(icon_lbl)
                
                bg_col = "transparent"
                if color_scheme == "RGB (Blue/Green/Red)":
                    if item['is_pen']: bg_col = f"rgba(65, 105, 225, {opacity})"
                    elif item['is_ssd']: bg_col = f"rgba(50, 205, 50, {opacity})"
                    elif item['is_hdd']: bg_col = f"rgba(220, 20, 60, {opacity})"
                else:
                    if item['is_pen']: bg_col = f"rgba(255, 105, 180, {opacity})"
                    elif item['is_ssd']: bg_col = f"rgba(0, 191, 255, {opacity})"
                    elif item['is_hdd']: bg_col = f"rgba(255, 215, 0, {opacity})"
                
                border = "1px solid #b8860b" if item['is_hash'] else "0px solid transparent"
                txt_c = active_accent if is_active else base_txt_c
                weight = "900" if is_active else "normal"
                
                # OVERRIDE: Give the Active Database a strong glowing blue border to make it obvious!
                if is_active: border = f"2px solid {active_accent}"
                
                text_lbl = QLabel(item['clean'])
                text_lbl.setStyleSheet(f"background-color: {bg_col}; border: {border}; color: {txt_c}; font-weight: {weight}; border-radius: 4px; padding: 2px 4px;")
                
                lay.addWidget(text_lbl)
                lay.addStretch()
                
                node.setText(0, item['clean'])
                node.setForeground(0, QBrush(Qt.transparent)) 
                
                if item['date_str']: node.setToolTip(0, f"Database Date: {item['date_str']}")
                self.folder_tree.setItemWidget(node, 0, container)
            else:
                node.setText(0, item['orig'])
                node.setIcon(0, self.style().standardIcon(QStyle.SP_DriveFDIcon))
                # Highlight active DB even if Makeup is turned off
                if is_active:
                    node.setForeground(0, QBrush(QColor(active_accent)))
                    font = node.font(0); font.setBold(True); node.setFont(0, font)

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
        
        # Fetch the Fast Mode UI state and pass it to the loader
        is_fast = self.act_fast_mode.isChecked() if hasattr(self, 'act_fast_mode') else False
        self.loader_thread = DataLoaderThread(self.active_db_path, target_path, self.show_hidden, is_fast, self)
        
        self.loader_thread.data_ready.connect(lambda f, fl: self._start_ui_render(f, fl, cached=False))
        self.loader_thread.start()

    def _start_ui_render(self, folders, files, cached=False, is_search=False):
        # Prevent search results from overwriting the folder cache!
        if not cached and not is_search: self.view_cache[self.current_prefix] = (folders, files) 
        self.render_queue = folders + files; total = len(self.render_queue); self.table_rows_buffer = []
        
        if total > CHUNK_SIZE:
            self.render_progress = QProgressDialog(f"{'Loading from RAM Cache' if cached else 'Rendering'} {total} items...", "Cancel", 0, total, self)
            self.render_progress.setWindowModality(Qt.WindowModal)
            self.render_progress.setMinimumDuration(0)
            self.render_progress.setValue(0)
            self.render_progress.show()
            QApplication.processEvents()
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
            # 1. ADD S.No. to the headers
            shared_model = vmanTableModel(["S.No.", "Name", "Ext", "Size", "Modified", "Type", "Location", "Labels"], self.table_rows_buffer)
            
            # 2. Assign the model to both views
            self.file_table.setModel(shared_model)
            self.file_grid.setModel(shared_model)
            
            # ---> FIX: Force Grid View back to Column 1 immediately after setting the new model!
            self.file_grid.setModelColumn(1)
            
            # 3. Adjust widths for the new 8-column layout
            self.file_table.setColumnWidth(0, 50)  # S.No.
            self.file_table.setColumnWidth(1, 260) # Name
            self.file_table.setColumnWidth(2, 60)  # Ext
            self.file_table.setColumnWidth(3, 90)  # Size
            self.file_table.setColumnWidth(4, 140) # Modified
            self.file_table.setColumnWidth(5, 120) # Type
            self.file_table.setColumnWidth(6, 350) # Location
            
            self.file_table.setUpdatesEnabled(True); self.file_grid.setUpdatesEnabled(True)
            if self.render_progress: self.render_progress.close()
            self.status.showMessage("Rendering complete.", 3000); self.update_statistics()
            return

        if self.render_progress and self.render_progress.wasCanceled(): self.render_queue.clear(); return

        chunk = self.render_queue[:CHUNK_SIZE]; self.render_queue = self.render_queue[CHUNK_SIZE:]
        dir_icon = self.style().standardIcon(QStyle.SP_DirIcon)
        
        cur = self.db.conn.cursor()

        for item in chunk:
            db_id = item[0]
            tags_res = cur.execute("SELECT custom_tags FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
            t_str = tags_res[0] if tags_res and tags_res[0] else ""

            # Calculate Serial Number
            s_no = len(self.table_rows_buffer) + 1

            if len(item) == 8:
                db_id, pp, f_name, c_tag, sec_n, is_h, count, size = item
                v_path = f"{pp}{f_name}/" if not f_name.endswith("/") else f"{pp}{f_name}"
                disp_name = sec_n if (self.show_secondary_names and sec_n) else (f"{f_name}\n({sec_n})" if sec_n else f"{f_name}")
                
                self.table_rows_buffer.append({"display": [str(s_no), disp_name, "", human_size(size), "N/A", f"Virtual Folder ({count})", pp, t_str], "sort_keys": [(0, s_no), (0, natural_sort_key(f_name)), (0, ""), (0, size), (0, ""), (0, count), (0, natural_sort_key(pp)), (0, t_str)], "user_data": ("folder", v_path, db_id), "color_tag": c_tag, "is_hidden": is_h, "icon": dir_icon})
            else:
                db_id, n, s, ext, rp, mod, c_tag, sec_n, is_h = item[:9]
                icon = self._get_native_icon(rp, False, ext, db_id)
                s_val = s if s else 0
                ext_str = str(ext) if ext else ""
                disp_name = sec_n if (self.show_secondary_names and sec_n) else (f"{n}\n({sec_n})" if sec_n else f"{n}")
                
                self.table_rows_buffer.append({"display": [str(s_no), disp_name, ext_str, human_size(s_val), str(mod), "Virtual File", str(rp) if rp else "", t_str], "sort_keys": [(1, s_no), (1, natural_sort_key(n)), (1, ext_str.lower()), (1, s_val), (1, str(mod)), (1, "Virtual File"), (1, natural_sort_key(rp) if rp else ""), (1, t_str)], "user_data": ("file", str(rp), db_id), "color_tag": c_tag, "is_hidden": is_h, "icon": icon})

        if self.render_progress: self.render_progress.setValue(self.render_progress.maximum() - len(self.render_queue))
        
    def _trigger_preview(self, typ, path, db_id, name):
        if db_id == -1: return 
        self.btn_save_ed.setProperty("db_id", db_id); self.btn_save_ed.setProperty("is_folder", typ == "folder")
        if HAS_MULTIMEDIA and hasattr(self, 'player'): self.player.stop()
        self.preview_image.clear(); self.preview_text.clear()
        
        # 1. Fetch parent_path and name to build the EXACT Virtual Path
        row = self.db.conn.cursor().execute("SELECT parent_path, name, real_path, size, extension, modified, secondary_name, custom_tags, sha256 FROM virtual_fs WHERE id = ?", (db_id,)).fetchone()
        if not row: return
        parent_path, f_name, real_path, size, ext, mod, sec_n, custom_tags, sha256_val = row
        
        # 2. Construct the true Virtual Path so you can copy-paste it
        actual_v_path = f"{parent_path}{f_name}/" if typ == "folder" else f"{parent_path}{f_name}"
        self.ed_v_path.setText(actual_v_path)
        
        self.ed_secondary.setText(str(sec_n) if sec_n else "")
        self.ed_custom_tags.setText(str(custom_tags) if custom_tags else "")
        self.ed_sha.setText(str(sha256_val) if sha256_val else "")
        
        if typ == "folder": 
            self.preview_stack.setCurrentIndex(1)
            self.ed_target.setText(f"Virtual Container: {actual_v_path}")
            self.preview_text.setPlainText(f"Directory Data:\n{actual_v_path}")
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
        if self.file_table.model() and self.file_table.model().data(self.file_table.model().index(index.row(), 1), Qt.UserRole):
            data = self.file_table.model().data(self.file_table.model().index(index.row(), 1), Qt.UserRole)
            if data[2] == -1: return 
            name = self.file_table.model().data(self.file_table.model().index(index.row(), 1), Qt.DisplayRole)
            self.ed_name.setText(str(name).split('\n')[0]); self.ed_tag.setCurrentIndex(max(0, self.ed_tag.findText(self.file_table.model().data(self.file_table.model().index(index.row(), 1), Qt.UserRole + 1))))
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
            
            # Recursive Tagging with Progress Bar
            if new_custom_tags:
                children = cur.execute("SELECT id FROM virtual_fs WHERE parent_path LIKE ?", (f"{pp}{new_name}/%",)).fetchall()
                if children:
                    prog = QProgressDialog("Applying tags to contents...", "Cancel", 0, len(children), self)
                    prog.setWindowModality(Qt.WindowModal)
                    prog.show()
                    for i, (child_id,) in enumerate(children):
                        if prog.wasCanceled(): break
                        cur.execute("UPDATE virtual_fs SET custom_tags=? WHERE id=?", (new_custom_tags, child_id))
                        if i % 50 == 0: prog.setValue(i); QApplication.processEvents()
                    prog.setValue(len(children))
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
            data = model.data(model.index(r, 1), Qt.UserRole); name = model.data(model.index(r, 1), Qt.DisplayRole)
            if data and data[0] == "file" and data[1] and os.path.exists(data[1]):
                playlist.append({'path': data[1], 'name': name.split('\n')[0], 'ext': os.path.splitext(data[1])[1].lower()})
                if data[2] == target_db_id: start_index = len(playlist) - 1
                
        # Launch a new viewer and store it in the active list to allow multiple windows
        new_viewer = vmanViewer(playlist, start_index, self)
        if not hasattr(self, 'active_viewers'): self.active_viewers = []
        self.active_viewers.append(new_viewer)
        new_viewer.show()

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
            for r in range(model.rowCount()): items_to_export.append(model.data(model.index(r, 1), Qt.UserRole))
            
        if not items_to_export: return QMessageBox.warning(self, "Export", "The current view is empty.")
            
        # Using the Smart Collision Checker
        dest_dir = self._get_export_dest_with_check(items_to_export, "Select OS Destination to Materialize")
        if not dest_dir: return
        
        self.export_dlg = QProgressDialog(f"Materializing {len(items_to_export)} selections to physical OS...", "Cancel", 0, len(items_to_export), self)
        self.export_dlg.setWindowTitle("Exporting to OS")
        self.export_dlg.setFixedSize(600, 160)
        label = self.export_dlg.findChild(QLabel)
        if label: label.setWordWrap(True)
        self.export_dlg.setWindowModality(Qt.WindowModal)
        self.export_dlg.show()
        
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
                    writer.writerow(["Type", "File/Folder Name", "Virtual Path", "Physical OS Path", "Size (Bytes)", "Extension", "Modified Date", "SHA-256 Hash", "Custom Tags", "Color Tag", "Secondary Name"])
                    
                    def sanitize(name): return re.sub(r'[\\/*?:"<>|]', '_', str(name))
                    
                    for typ, path_val, db_id in sel_items:
                        if db_id == -1 and typ == "folder" and "://" in path_val:
                            smart_files = resolve_smart_folder(cur, path_val, sanitize)
                            for rel_path, f_id, rp, sz in smart_files:
                                r = cur.execute("SELECT name, parent_path, real_path, size, extension, modified, sha256, custom_tags, color_tag, secondary_name FROM virtual_fs WHERE id=?", (f_id,)).fetchone()
                                if r: writer.writerow(["File", r[0], f"/{rel_path}", r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]])
                        elif db_id == -1: continue
                        elif typ == "file":
                            r = cur.execute("SELECT name, parent_path, real_path, size, extension, modified, sha256, custom_tags, color_tag, secondary_name FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
                            if r: writer.writerow(["File", r[0], r[1] + r[0], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]])
                        else:
                            r = cur.execute("SELECT name, parent_path, modified, custom_tags, color_tag, secondary_name FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
                            if r: writer.writerow(["Folder", r[0], r[1] + r[0] + "/", "N/A (Virtual)", 0, "", r[2], "", r[3], r[4], r[5]])
                            
                            for r in cur.execute("SELECT name, parent_path, real_path, size, extension, modified, sha256, custom_tags, color_tag, secondary_name FROM virtual_fs WHERE parent_path LIKE ? AND is_folder=0", (f"{path_val}%",)).fetchall():
                                writer.writerow(["File", r[0], r[1] + r[0], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]])
                                
            QMessageBox.information(self, "Success", "Rich Database Manifest exported successfully.")
            self.sys_log(f"Exported detailed CSV manifest to: {csv_path}")
        except Exception as e: QMessageBox.warning(self, "Error", str(e))

    def _get_selected_items(self):
        if self.view_stack.currentIndex() == 0:
            return [self.file_table.model().data(self.file_table.model().index(idx.row(), 1), Qt.UserRole) for idx in self.file_table.selectionModel().selectedRows() if self.file_table.model().data(self.file_table.model().index(idx.row(), 1), Qt.UserRole)]
        return [self.file_grid.model().data(idx, Qt.UserRole) for idx in self.file_grid.selectionModel().selectedIndexes() if self.file_grid.model().data(idx, Qt.UserRole)]
        
    def show_main_header_menu(self, pos):
        menu = QMenu(self)
        for col in range(self.file_table.horizontalHeader().count()):
            col_name = self.file_table.model().headerData(col, Qt.Horizontal)
            if col_name:
                action = menu.addAction(f"Show {col_name}")
                action.setCheckable(True)
                action.setChecked(not self.file_table.isColumnHidden(col))
                action.toggled.connect(lambda checked, c=col: self.file_table.setColumnHidden(c, not checked))
                
        menu.exec(self.file_table.horizontalHeader().mapToGlobal(pos))

    def context_menu(self, pos, is_grid=False):
        menu = QMenu(self)

        menu.addAction("☑ Select All (Ctrl+A)", self.cmd_select_all)
        
        all_sel_items = self._get_selected_items()
        real_sel_items = [i for i in all_sel_items if i[2] != -1]
        is_trash = self.current_prefix.startswith("trash://")

        if all_sel_items:
            menu.addSeparator()
            if len(all_sel_items) == 1 and all_sel_items[0][0] == "file":
                menu.addAction("🎞 Open (Ctrl+O)", self.open_selected_vman)

        # --- NEW: Add the Calculate Size option for Smart Views ---
        if self._is_smart_path(self.current_prefix):
            menu.addSeparator()
            menu.addAction("📊 Calculate Sizes & Files (This View)", self.calculate_smart_folder_sizes)
        
        if is_trash:
            menu.addSeparator()
            menu.addAction("🔥 Empty Trash", self.empty_trash)

        if not self._is_smart_path(self.current_prefix) and not is_trash:
            menu.addSeparator()
            create_menu = menu.addMenu("✨ Create / Import")
            create_menu.addAction("📂 Create Virtual Folder (Ctrl+Shift+N)", self.create_folder)
            create_menu.addAction("📥 Import Real Files", self.import_real_files)
            create_menu.addAction("📁 Import Real Folder", self.import_real_folder)
            menu.addAction("🖼️ Generate Thumbnails (Current Folder)", self.generate_thumbnails_current_view)
            menu.addAction("🧹 Clean Orphaned Thumbnails", self.clean_orphaned_thumbnails)

        # --- EXPORT & CLIPBOARD (Available for EVERYTHING, including Smart Views!) ---
        if all_sel_items:
            menu.addSeparator()
            
            # --- NEW: SEND TO DATABASE ---
            send_to_menu = menu.addMenu("📤 Send To Database...")
            db_list = list(VIEWS_DIR.glob("*.db"))
            # Don't let user send to the currently active DB!
            db_list = [db for db in db_list if str(db.resolve()) != str(Path(self.active_db_path).resolve())]
            
            if not db_list:
                act = send_to_menu.addAction("No other databases available")
                act.setEnabled(False)
            else:
                for db_file in db_list:
                    send_to_menu.addAction(db_file.stem, lambda checked=False, tgt=db_file: self.send_to_database(tgt, all_sel_items))
            # -----------------------------
            
            export_menu = menu.addMenu("📤 Export & Extract")
            export_menu.addAction(f"💾 Materialize {len(all_sel_items)} Items to OS", lambda: self.materialize_to_os(all_sel_items))
            export_menu.addAction("📦 Create Dummy (Sparse) Replica...", lambda: self.export_dummy_replica(all_sel_items))
            export_menu.addAction("📄 Create Zero-Byte Replica...", lambda: self.export_zero_byte_replica(all_sel_items))
            export_menu.addAction("🗜️ Export to ZIP...", lambda: self.export_to_zip(all_sel_items))
            export_menu.addAction("📊 Export View to CSV", lambda: self.export_csv(all_sel_items))
            
            menu.addAction("⚙️ Compile to Isolated DB", lambda: self.compile_current_view(all_sel_items))
            
            menu.addSeparator()
            menu.addAction("Copy (Ctrl+C)", self.cmd_copy)
            # Cut is restricted because removing smart queries dynamically is dangerous
            if real_sel_items and len(real_sel_items) == len(all_sel_items):
                menu.addAction("Cut (Ctrl+X)", self.cmd_cut)

        # --- EDIT / PROPERTIES / DELETE (Restricted to REAL items only) ---
        if real_sel_items:
            menu.addSeparator()
            edit_menu = menu.addMenu("✏️ Edit & Modify")
            edit_menu.addAction("Rename (F2)", self.cmd_rename)
            edit_menu.addAction("🧹 Clean '[copy]' Prefix", lambda: self.cmd_remove_copy_prefix(real_sel_items))
            
            if len(real_sel_items) == 1:
                edit_menu.addAction("🏷 Set Secondary Name", self.cmd_set_secondary_name)
                res = self.db.conn.cursor().execute("SELECT is_hidden, is_favorite FROM virtual_fs WHERE id=?", (real_sel_items[0][2],)).fetchone()
                if res:
                    edit_menu.addAction("👁️ Unhide" if res[0] else "🙈 Hide", lambda: self.toggle_item_hidden(real_sel_items[0][2], not res[0]))
                    edit_menu.addAction("💔 Remove Favorite" if res[1] else "⭐ Add Favorite", lambda: self.toggle_item_fav(real_sel_items[0][2], not res[1]))
            
            tag_menu = menu.addMenu("🏷 Tags & Labels")
            color_menu = tag_menu.addMenu("🎨 Set Color Tag")
            for color in ["None", "Red", "Orange", "Gold", "Green", "Cyan", "Blue", "Purple", "Pink"]: 
                color_menu.addAction(color, lambda checked=False, c=color: self.bulk_tag_items(c, real_sel_items))
            tag_menu.addAction("📝 Bulk Add Custom Tags...", lambda: self.bulk_add_custom_tags(real_sel_items))

            sys_menu = menu.addMenu("⚙️ System & Mapping")
            sys_menu.addAction(f"🧬 Compute SHA-256 for {len(real_sel_items)} Item(s)", lambda: self.bulk_compute_hash_selected(real_sel_items))
            if len(real_sel_items) == 1:
                if real_sel_items[0][0] == "folder":
                    sys_menu.addAction("🔗 Map THIS Folder to Physical OS", lambda: self.cmd_map_folder(real_sel_items[0]))
                sys_menu.addAction("🗺️ Map Parent Drive/Mount (Auto-Detect)", lambda: self.cmd_map_parent_drive(real_sel_items[0]))
                
            if len(real_sel_items) == 1:
                menu.addAction("ℹ️ Properties", lambda: self.show_properties(real_sel_items[0][0], real_sel_items[0][1], real_sel_items[0][2]))
            else:
                menu.addAction("ℹ️ Multi-Item Properties", lambda: self.show_multi_properties(real_sel_items))

            menu.addSeparator()
            del_menu = menu.addMenu("🗑️ Delete Options")
            if is_trash:
                del_menu.addAction("♻️ Restore from Trash", self.restore_from_trash)
                del_menu.addAction("🧨 Permanent Delete (Shift+Del)", self.cmd_delete_permanent)
            else:
                del_menu.addAction("🗑️ Move to Trash (Delete)", self.cmd_delete)
                del_menu.addAction("🧨 Permanent Delete (Shift+Del)", self.cmd_delete_permanent)
            del_menu.addSeparator()
            del_menu.addAction("💀 Delete PHYSICAL OS Items", lambda: self.cmd_delete_physical(real_sel_items))

        # View / Global Actions (If no selection)
        menu.addSeparator()
        if not all_sel_items: 
            menu.addAction("📤 Materialize Entire View to OS", lambda: self.materialize_to_os(None))
            menu.addAction("⚙️ Compile View to Isolated DB", lambda: self.compile_current_view(None))

        menu.addSeparator()
        view_menu = menu.addMenu("🖥️ Window Views")
        view_menu.addAction("📝 Toggle Console", lambda: self.log_dock.setVisible(not self.log_dock.isVisible()))
        view_menu.addAction("🗂️ Toggle Data Engine View", lambda: self.tree_dock.setVisible(not self.tree_dock.isVisible()))
        view_menu.addAction("📊 Toggle Inspector", lambda: self.right_dock.setVisible(not self.right_dock.isVisible()))
        
        # --- NEW: Floating Widgets & Usage Log ---
        view_menu.addSeparator()
        
        act_clock = view_menu.addAction("🕒 Toggle Floating Clock")
        act_clock.setCheckable(True)
        act_clock.setChecked(self.floating_clock.isVisible())
        act_clock.triggered.connect(lambda: self.floating_clock.setVisible(not self.floating_clock.isVisible()))
        
        act_notepad = view_menu.addAction("📝 Toggle Floating Notepad")
        act_notepad.setCheckable(True)
        act_notepad.setChecked(self.floating_notepad.isVisible())
        act_notepad.triggered.connect(lambda: self.floating_notepad.setVisible(not self.floating_notepad.isVisible()))
        
        view_menu.addAction("⏱️ View App Usage Logs", lambda: UsageLogDialog(self).exec())
        # --------------------------------
        
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

    def copy_vpath_to_clipboard(self, db_id, typ):
        if db_id == -1: return
        try:
            with sqlite3.connect(self.db.path) as conn:
                res = conn.cursor().execute("SELECT parent_path, name FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
                if res:
                    # Constructs the perfect 100% accurate virtual path
                    v_path = f"{res[0]}{res[1]}/" if typ == "folder" else f"{res[0]}{res[1]}"
                    QApplication.clipboard().setText(v_path)
                    self.status.showMessage("Virtual path copied to clipboard.", 3000)
        except Exception as e:
            QMessageBox.warning(self, "Copy Error", str(e))

    def cmd_copy(self):
        items = self._get_selected_items() # Uses ALL items
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
        is_permanent = force_permanent or self.current_prefix.startswith("trash://")
        
        msg = "Permanently delete from VMan? (This cannot be undone!)" if is_permanent else "Move selected items to Virtual Trash?"
        if QMessageBox.question(self, "Delete", msg, QMessageBox.Yes|QMessageBox.No) != QMessageBox.Yes: return
        
        with sqlite3.connect(self.db.path) as conn:
            cur = conn.cursor()
            prog = QProgressDialog(f"Deleting {len(clean_items)} items...", "Cancel", 0, len(clean_items), self)
            prog.setWindowModality(Qt.WindowModal)
            prog.setMinimumDuration(0)
            prog.setValue(0)
            prog.show()
            QApplication.processEvents()
            
            for i, (typ, path, db_id) in enumerate(clean_items):
                if prog.wasCanceled(): break
                if is_permanent:
                    if typ == "file": cur.execute("DELETE FROM virtual_fs WHERE id = ?", (db_id,))
                    else: cur.execute("DELETE FROM virtual_fs WHERE parent_path LIKE ? OR id = ?", (f"{path}%", db_id))
                else:
                    if typ == "file": cur.execute("UPDATE virtual_fs SET in_trash = 1 WHERE id = ?", (db_id,))
                    else: cur.execute("UPDATE virtual_fs SET in_trash = 1 WHERE parent_path LIKE ? OR id = ?", (f"{path}%", db_id))
                prog.setValue(i+1)
                if i % 25 == 0: QApplication.processEvents() # <--- Keeps UI responsive
            conn.commit()
            
        self.clear_cache(); self.refresh_all(); self.sys_log(f"{'Permanently deleted' if is_permanent else 'Trashed'} {len(clean_items)} items.")
        QMessageBox.information(self, "Deletion Complete", f"Successfully {'permanently deleted' if is_permanent else 'moved to Virtual Trash'} {len(clean_items)} items from the VMan Database.")

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
        if not self._current_drag_items or dest_path.startswith("trash://") or self._is_smart_path(dest_path): return
        
        def get_copy_name(name):
            clean_name = name
            while clean_name.startswith("[copy]"): clean_name = clean_name[6:]
            return f"[copy]{clean_name}"

        def sanitize(name): return re.sub(r'[\\/*?:"<>|]', '_', str(name))

        # --- FIX: Show initializing dialog BEFORE counting to prevent white screen ---
        prog = QProgressDialog(f"Initializing transfer...", "Cancel", 0, 0, self)
        prog.setWindowTitle("Virtual Transfer")
        prog.setWindowModality(Qt.WindowModal)
        prog.setMinimumDuration(0)
        prog.show()
        QApplication.processEvents(); QThread.msleep(50); QApplication.processEvents()

        # Pre-calculate progress
        total_items = 0
        with sqlite3.connect(self.db.path) as conn:
            cur = conn.cursor()
            for typ, path, db_id in self._current_drag_items:
                if db_id == -1 and typ == "folder" and "://" in path:
                    total_items += len(resolve_smart_folder(cur, path, sanitize))
                elif db_id != -1:
                    if typ == "file": total_items += 1
                    else: total_items += cur.execute("SELECT COUNT(id) FROM virtual_fs WHERE parent_path LIKE ? AND is_folder=0", (f"{path}%",)).fetchone()[0]
        
        if total_items == 0: total_items = 1
        
        # Update progress bar to real limits
        prog.setMaximum(total_items)
        prog.setLabelText(f"{'Copying' if is_copy else 'Moving'} {total_items} items...")
        QApplication.processEvents()

        processed = 0
        with sqlite3.connect(self.db.path) as conn:
            cur = conn.cursor()
            for typ, path, db_id in self._current_drag_items:
                if prog.wasCanceled(): break
                             
                if db_id == -1 and typ == "folder" and "://" in path:
                    if not is_copy: continue # Cannot cut dynamic Smart Views
                    base_f = get_copy_name(sanitize(path.strip('/').split('/')[-1])) if path.strip('/').replace('://', '') else ""
                    smart_files = resolve_smart_folder(cur, path, sanitize)
                    created_folders = set()
                    
                    for rel_path, f_id, rp, sz in smart_files:
                        row = cur.execute("SELECT name, is_folder, real_path, size, extension, modified, color_tag, is_hidden, category, year, month, custom_tags, creation_date, hash_verified FROM virtual_fs WHERE id=?", (f_id,)).fetchone()
                        if not row: continue
                        
                        full_rel = f"{base_f}/{rel_path}" if base_f else rel_path
                        parts = full_rel.split('/')
                        f_name = parts[-1]
                        parent_dirs = parts[:-1]
                        
                        curr_parent = dest_path
                        for part in parent_dirs:
                            if curr_parent + part + "/" not in created_folders:
                                cur.execute("INSERT OR IGNORE INTO virtual_fs (parent_path, name, is_folder, modified) VALUES (?, ?, 1, ?)", (curr_parent, part, now_ts()))
                                created_folders.add(curr_parent + part + "/")
                            curr_parent += part + "/"
                            
                        cur.execute("INSERT INTO virtual_fs (parent_path, name, is_folder, real_path, size, extension, modified, color_tag, is_hidden, category, year, month, custom_tags, creation_date, hash_verified) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (curr_parent, f_name, *row[1:]))
                        processed += 1; 
                        if processed % 50 == 0: prog.setValue(processed); QApplication.processEvents()
                
                elif db_id != -1:
                    if typ == "file":
                        if is_copy:
                            row = cur.execute("SELECT name, is_folder, real_path, size, extension, modified, color_tag, is_hidden, category, year, month, custom_tags, creation_date, hash_verified FROM virtual_fs WHERE id = ?", (db_id,)).fetchone()
                            if row: 
                                new_name = get_copy_name(row[0])
                                cur.execute("INSERT INTO virtual_fs (parent_path, name, is_folder, real_path, size, extension, modified, color_tag, is_hidden, category, year, month, custom_tags, creation_date, hash_verified) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (dest_path, new_name, *row[1:]))
                        else: cur.execute("UPDATE virtual_fs SET parent_path = ? WHERE id = ?", (dest_path, db_id))
                        processed += 1; 
                        if processed % 50 == 0: prog.setValue(processed); QApplication.processEvents()
                    else:
                        row = cur.execute("SELECT name FROM virtual_fs WHERE id = ?", (db_id,)).fetchone()
                        if not row: continue
                        if is_copy:
                            new_base_name = get_copy_name(row[0])
                            cur.execute("INSERT INTO virtual_fs (parent_path, name, is_folder, modified) VALUES (?, ?, 1, ?)", (dest_path, new_base_name, now_ts()))
                            for r in cur.execute("SELECT name, is_folder, real_path, size, extension, modified, color_tag, is_hidden, parent_path, category, year, month, custom_tags, creation_date, hash_verified FROM virtual_fs WHERE parent_path LIKE ?", (f"{path}%",)).fetchall(): 
                                cur.execute("INSERT INTO virtual_fs (parent_path, name, is_folder, real_path, size, extension, modified, color_tag, is_hidden, category, year, month, custom_tags, creation_date, hash_verified) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (f"{dest_path}{new_base_name}/" + r[8][len(path):], *r[:8], r[9], r[10], r[11], r[12], r[13], r[14]))
                                processed += 1
                                if processed % 50 == 0: prog.setValue(processed); QApplication.processEvents()
                        else:
                            cur.execute("UPDATE virtual_fs SET parent_path = ? WHERE id = ?", (dest_path, db_id))
                            cur.execute("UPDATE virtual_fs SET parent_path = ? || SUBSTR(parent_path, LENGTH(?) + 1) WHERE parent_path LIKE ?", (f"{dest_path}{row[0]}/", path, f"{path}%"))
            conn.commit()
        
        prog.setValue(max(1, total_items)); prog.close()
        self._current_drag_items = []; self.clear_cache(); self.refresh_all(); self.sys_log(f"Internal Transfer executed to '{dest_path}'")

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
        if self._is_smart_path(self.current_prefix) or self.current_prefix.startswith("trash://"): 
            return QMessageBox.warning(self, "Error", "Cannot import directly into Smart Views.")
            
        # 1. Setup the UI Progress Dialog
        self.import_dlg = QProgressDialog("Importing files into Virtual Sandbox...", "Cancel", 0, 100, self)
        self.import_dlg.setWindowModality(Qt.WindowModal)
        self.import_dlg.setMinimumDuration(0)
        self.import_dlg.setValue(0)
        self.import_dlg.show()
        QApplication.processEvents()
        
        # 2. Initialize the background thread
        self.import_thread = ImportFilesThread(str(self.db.path), self.current_prefix, paths, self)
        
        # 3. Connect ALL the necessary signals
        self.import_thread.progress.connect(lambda c, t, m: (self.import_dlg.setMaximum(max(1, t)), self.import_dlg.setValue(c), self.import_dlg.setLabelText(m)))
        self.import_dlg.canceled.connect(self.import_thread.cancel)
        
        def on_import_finished(f_cnt, d_cnt):
            self.import_dlg.close()
            self.clear_cache()
            self.refresh_all()
            self.status.showMessage(f"Imported {f_cnt} files and {d_cnt} folders.", 5000)
            self.sys_log(f"Successfully Sandbox Imported {f_cnt} files.")
            
        self.import_thread.finished_import.connect(on_import_finished)
        self.import_thread.error.connect(lambda e: (self.import_dlg.close(), QMessageBox.critical(self, "Import Error", e)))
        
        # 4. Start the background thread!
        self._register_worker(self.import_thread)
        self.import_thread.start()
        
    def compile_current_view(self, sel_items=None):
        if not sel_items:
            sel_items = self._get_selected_items()
            if not sel_items:
                sel_items = [("folder", self.current_prefix, -1)]

        name, ok = QInputDialog.getText(self, "Compile DB View", "Enter name for new separate database (e.g., 'Project_Backup'):")
        if not ok or not name.strip(): return
        target_path = VIEWS_DIR / f"{name.strip().replace(' ', '_')}.db"

        self.compile_dlg = QProgressDialog("Compiling standalone database...", "Cancel", 0, 100, self)
        self.compile_dlg.setWindowModality(Qt.WindowModal)
        self.compile_dlg.setFixedSize(600, 160)
        label = self.compile_dlg.findChild(QLabel)
        if label: label.setWordWrap(True)
        self.compile_dlg.show()
        
        self.compiler = CompilerThread(str(self.db.path), str(target_path), sel_items, self)
        self.compiler.progress.connect(lambda c, t, msg: (self.compile_dlg.setValue(int((c/max(1,t))*100)), self.compile_dlg.setLabelText(msg)))
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
        usage_pct = (stats['used_bytes'] / self.max_virtual_storage) * 100 if self.max_virtual_storage else 0
        html = f"<h3 style='color:#58a6ff;'>System Analytics ({Path(self.active_db_path).name})</h3><hr><b>Total Virtual Files:</b> {stats['files']}<br><b>Total Virtual Folders:</b> {stats['folders']}<br><b>Simulated Storage Used:</b> {human_size(stats['used_bytes'])}<br><b>Average File Size:</b> {human_size(stats['avg_bytes'])}<br><b>System Allocation:</b> {usage_pct:.4f}%<br><hr><b>Oldest Mod Date:</b> {stats['oldest']}<br><b>Newest Mod Date:</b> {stats['newest']}<br><hr><h4 style='color:#58a6ff;'>Top Largest Managed Files:</h4><ul style='list-style-type: square; margin-left: -20px;'>"
        for f in stats['top_files'][:5]: html += f"<li>{f[0]} <span style='color:#8b949e;'>({human_size(f[1])})</span></li>"
        html += "</ul>"
        self.lbl_stats_txt.setHtml(html)
        
        if not self.figure: return
        self.figure.clear(); mode = self.stat_combo.currentText(); ax = self.figure.add_subplot(111)
        
        # Color definitions
        is_dark = getattr(self, 'is_dark_mode', True)
        bg_c, txt_c = ('#0d1117', '#c9d1d9') if is_dark else ('#ffffff', '#24292f')
        bar_bg = '#161b22' if is_dark else '#f6f8fa'
        border_c = '#30363d' if is_dark else '#d0d7de'
        
        # Apply dynamic themes and beautiful rounded corners to the UI Wrappers
        if hasattr(self, 'charts_container'):
            self.charts_container.setStyleSheet(f"#ChartsContainer {{ background-color: {bg_c}; border-radius: 8px; border: 1px solid {border_c}; }}")
            self.ctrl_bar.setStyleSheet(f"background-color: {bar_bg}; border-bottom: 1px solid {border_c}; border-top-left-radius: 8px; border-top-right-radius: 8px;")
            self.stat_combo.setStyleSheet(f"QComboBox {{ padding: 4px; border: 1px solid {border_c}; border-radius: 4px; background: {bg_c}; color: {txt_c}; }}")
            self.lbl_inv.setStyleSheet(f"color: {txt_c}; font-weight: bold; border: none; background: transparent;")
            if hasattr(self, 'lbl_err'): self.lbl_err.setStyleSheet(f"color: {txt_c}; background: transparent;")

        # Apply to Matplotlib canvas
        self.figure.patch.set_facecolor(bg_c); ax.set_facecolor(bg_c); ax.tick_params(colors=txt_c, labelsize=9)
        
        grid_color = '#30363d' if is_dark else '#e1e4e8'

        if "Line Chart" in mode or "Over Time" in mode:
            data = stats["time_series"]
            if not data: ax.text(0.5, 0.5, "No temporal data available", color=txt_c, ha='center')
            else:
                dates = [datetime.strptime(d[0], "%Y-%m") for d in data if len(d[0]) == 7]
                if "Count" in mode: vals = [d[1] for d in data if len(d[0]) == 7]; ylabel = "Files Modified"; color = '#58a6ff'
                else: vals = [d[2] / (1024*1024) for d in data if len(d[0]) == 7]; ylabel = "Storage (MB)"; color = '#e3b341'
                if dates and vals:
                    ax.plot(dates, vals, marker='o', linestyle='-', color=color, linewidth=2, markersize=5)
                    ax.fill_between(dates, vals, color=color, alpha=0.15)
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
                    self.figure.autofmt_xdate(rotation=30)
                    ax.set_ylabel(ylabel, color=txt_c, fontweight='bold')
                    ax.set_title(mode, color=txt_c, fontweight='bold', pad=15)
                    ax.grid(True, color=grid_color, linestyle='--', alpha=0.5)

        elif "Distribution by Extension" in mode:
            data = stats["distribution"]
            if not data: ax.text(0.5, 0.5, "No distribution data available", color=txt_c, ha='center')
            else:
                clean_data = [(str(d[0]).upper() if d[0] else 'NONE', d[1], d[2]) for d in data]
                if "Size" in mode: 
                    sorted_d = sorted(clean_data, key=lambda x: x[2], reverse=True)[:10]
                    ax.bar([x[0] for x in sorted_d], [x[2] / (1024*1024) for x in sorted_d], color='#3fb950', edgecolor=bg_c)
                    ax.set_ylabel("Storage Size (MB)", color=txt_c, fontweight='bold')
                else: 
                    sorted_d = sorted(clean_data, key=lambda x: x[1], reverse=True)[:10]
                    ax.bar([x[0] for x in sorted_d], [x[1] for x in sorted_d], color='#58a6ff', edgecolor=bg_c)
                    ax.set_ylabel("File Count", color=txt_c, fontweight='bold')
                ax.set_title(mode, color=txt_c, fontweight='bold', pad=15)
                ax.tick_params(axis='x', rotation=30)
                ax.grid(axis='y', color=grid_color, linestyle='--', alpha=0.5)

        elif "Largest Files" in mode:
            data = stats["top_files"]
            if not data: ax.text(0.5, 0.5, "No files available", color=txt_c, ha='center')
            else:
                names = [d[0][:15] + ".." if len(d[0])>15 else d[0] for d in data]
                sizes = [d[1] / (1024*1024) for d in data]
                ax.barh(names, sizes, color='#a371f7', edgecolor=bg_c)
                ax.set_xlabel("Size (MB)", color=txt_c, fontweight='bold')
                ax.set_title(mode, color=txt_c, fontweight='bold', pad=15)
                ax.invert_yaxis()
                ax.grid(axis='x', color=grid_color, linestyle='--', alpha=0.5)
        
        elif "Ratio" in mode:
            labels = ['Used Storage', 'Free Space']
            sizes = [stats['used_bytes'], max(0, self.max_virtual_storage - stats['used_bytes'])]
            colors = ['#e3b341', '#3fb950']         
            ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, colors=colors, textprops={'color': txt_c, 'fontweight': 'bold'}, wedgeprops={'edgecolor': bg_c, 'linewidth': 1.5})
            ax.set_title("Simulated Storage Allocation", color=txt_c, fontweight='bold', pad=15)

        # Cleanup Spines for Modern Minimalist Look
        if "Ratio" not in mode:
            for spine in ['top', 'right']: ax.spines[spine].set_visible(False)
            for spine in ['bottom', 'left']: ax.spines[spine].set_color(grid_color)

        self.figure.tight_layout(pad=1.5)
        self.canvas.draw()

    def export_to_zip(self, items_to_export):
        zip_path, _ = QFileDialog.getSaveFileName(self, "Compile to ZIP", "", "ZIP Files (*.zip)")
        if zip_path:
            self.export_dlg = QProgressDialog("Compiling ZIP...", "Cancel", 0, 100, self)
            self.export_dlg.setWindowTitle("Exporting to ZIP")
            self.export_dlg.setFixedSize(600, 160)
            label = self.export_dlg.findChild(QLabel)
            if label: label.setWordWrap(True)
            self.export_dlg.setWindowModality(Qt.WindowModal)
            self.export_dlg.show()
            
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
        # --- NEW: Save Session Usage Time ---
        try:
            end_time = datetime.now()
            duration = end_time - self.session_start
            hours, remainder = divmod(duration.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            dur_str = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
            
            log_file = DATA_DIR / "usage.json"
            logs = []
            if log_file.exists():
                with open(log_file, "r") as f: logs = json.load(f)
            logs.append({"start": self.session_start.strftime("%Y-%m-%d %H:%M:%S"), "end": end_time.strftime("%Y-%m-%d %H:%M:%S"), "duration": dur_str})
            with open(log_file, "w") as f: json.dump(logs, f)
        except Exception: pass
        # ------------------------------------

        self.settings.setValue("main_table_state", self.file_table.horizontalHeader().saveState())
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
        menu = QMenu(self)
        
        # --- MASTER MAKEUP TOGGLE ---
        act_makeup = menu.addAction("🎨 Toggle DB Name Makeup")
        act_makeup.setCheckable(True)
        act_makeup.setChecked(getattr(self, 'db_makeup_mode', True))
        
        def toggle_db_makeup(checked):
            self.settings.setValue("db_makeup_mode", checked)
            self.db_makeup_mode = checked
            self.refresh_tree()
            
        act_makeup.toggled.connect(toggle_db_makeup)
        
        # --- COLOR SCHEME TOGGLE ---
        scheme_menu = menu.addMenu("🌈 Drive Color Scheme")
        act_def_color = scheme_menu.addAction("Default (Pink/Cyan/Yellow)")
        act_def_color.setCheckable(True)
        act_def_color.setChecked(self.settings.value("db_color_scheme", "Default") == "Default")
        act_def_color.triggered.connect(lambda: (self.settings.setValue("db_color_scheme", "Default"), self.refresh_tree()))
        
        act_rgb_color = scheme_menu.addAction("RGB (Blue/Green/Red)")
        act_rgb_color.setCheckable(True)
        act_rgb_color.setChecked(self.settings.value("db_color_scheme", "Default") == "RGB (Blue/Green/Red)")
        act_rgb_color.triggered.connect(lambda: (self.settings.setValue("db_color_scheme", "RGB (Blue/Green/Red)"), self.refresh_tree()))
        
        # --- NEW: SORT DATABASES MENU ---
        sort_menu = menu.addMenu("🔤 Sort Databases By...")
        
        act_sort_alpha = sort_menu.addAction("Alphabetical (A-Z)")
        act_sort_alpha.setCheckable(True)
        act_sort_alpha.setChecked(self.settings.value("db_sort_mode", "Alphabetical") == "Alphabetical")
        act_sort_alpha.triggered.connect(lambda: (self.settings.setValue("db_sort_mode", "Alphabetical"), self.refresh_tree()))
        
        act_sort_cat = sort_menu.addAction("Category (PEN, SSD, HDD)")
        act_sort_cat.setCheckable(True)
        act_sort_cat.setChecked(self.settings.value("db_sort_mode", "Alphabetical") == "Category")
        act_sort_cat.triggered.connect(lambda: (self.settings.setValue("db_sort_mode", "Category"), self.refresh_tree()))
        
        act_sort_date = sort_menu.addAction("Date (Newest First)")
        act_sort_date.setCheckable(True)
        act_sort_date.setChecked(self.settings.value("db_sort_mode", "Alphabetical") == "Date")
        act_sort_date.triggered.connect(lambda: (self.settings.setValue("db_sort_mode", "Date"), self.refresh_tree()))
        
        menu.addSeparator()
        
        if not item: 
            menu.exec(self.folder_tree.viewport().mapToGlobal(pos))
            return
            
        path = item.data(0, Qt.UserRole)
        
        if path and path.startswith("db://") and path != "db://main":
            db_name = path.replace("db://", "")
            db_file = VIEWS_DIR / db_name
            
            act_rename = menu.addAction("✏️ Rename Database")
            act_delete = menu.addAction("🗑️ Delete Database")
            act_merge = menu.addAction("📥 Merge into Active Database")
            menu.addSeparator()
            act_compare = menu.addAction("⚖️ Compare Drives (Bitrot)")
            
            action = menu.exec(self.folder_tree.viewport().mapToGlobal(pos))
            
            if action == act_rename:
                new_name, ok = QInputDialog.getText(self, "Rename Database", "New name (without .db):", QLineEdit.Normal, db_file.stem)
                if ok and new_name.strip():
                    new_file = VIEWS_DIR / f"{new_name.strip().replace(' ', '_')}.db"
                    if new_file.exists(): return QMessageBox.warning(self, "Error", "A database with this name already exists.")
                    try:
                        if self.active_db_path == str(db_file):
                            self.db.close(); db_file.rename(new_file)
                            self.active_db_path = str(new_file); self.db = vmanDB(Path(self.active_db_path))
                            self.status.showMessage(f"Renamed active database to {new_file.name}")
                        else: db_file.rename(new_file)
                        self.refresh_tree(); self.sys_log(f"Renamed database '{db_name}' to '{new_file.name}'")
                    except Exception as e: QMessageBox.critical(self, "Error", f"Failed to rename: {e}")
            elif action == act_delete:
                if QMessageBox.question(self, "Delete Database", f"Are you sure you want to permanently delete '{db_name}'?\nThis cannot be undone.", QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
                    try:
                        if self.active_db_path == str(db_file):
                            self.db.close(); self.active_db_path = str(DB_FILE); self.db = vmanDB(Path(self.active_db_path))
                            self.status.showMessage("Reconnected to Main System DB. Active isolated DB was deleted.")
                            self.nav_to_path("/")
                        if db_file.exists(): os.remove(db_file)
                        self.refresh_tree(); self.sys_log(f"Deleted database '{db_name}'")
                    except Exception as e: QMessageBox.critical(self, "Error", f"Failed to delete: {e}")
            elif action == act_merge:
                if QMessageBox.question(self, "Merge Database", f"Copy all missing files and folders from '{db_name}' into the current database?", QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
                    try:
                        self.db.conn.commit()
                        cur = self.db.conn.cursor()
                        try:
                            cur.execute(f"ATTACH DATABASE '{db_file}' AS source_db")
                            cols = "parent_path, name, is_folder, real_path, size, extension, modified, color_tag, secondary_name, is_hidden, in_trash, is_favorite, sha256, category, year, month, custom_tags, hash_verified, creation_date"
                            cur.execute(f"INSERT INTO virtual_fs ({cols}) SELECT {cols} FROM source_db.virtual_fs WHERE source_db.virtual_fs.parent_path || source_db.virtual_fs.name NOT IN (SELECT parent_path || name FROM virtual_fs)")
                            self.db.conn.commit()
                        finally: cur.execute("DETACH DATABASE source_db")
                        QMessageBox.information(self, "Merge Complete", "Database merged successfully. No original files were deleted.")
                        self.clear_cache(); self.refresh_all(); self.sys_log(f"Merged isolated database '{db_name}' into active database.")
                    except Exception as e: QMessageBox.critical(self, "Merge Error", f"Failed to merge database:\n{e}")
            elif action == act_compare:
                DriveComparatorDialog(self.active_db_path, str(db_file), self).exec()
        else:
            menu.exec(self.folder_tree.viewport().mapToGlobal(pos))

    def toggle_hidden_files(self):
        self.show_hidden = not self.show_hidden
        self.status.showMessage(f"Hidden items are now {'VISIBLE' if self.show_hidden else 'HIDDEN'}.", 3000)
        self.clear_cache()
        self.load_directory(self.current_prefix)
        
    def bulk_compute_hash_selected(self, sel_items):
        ids_to_hash = []
        with sqlite3.connect(self.db.path) as conn:
            for typ, path, db_id in sel_items:
                if db_id == -1: continue
                if typ == "file":
                    ids_to_hash.append(db_id)
                else:
                    children = conn.cursor().execute("SELECT id FROM virtual_fs WHERE parent_path LIKE ? AND is_folder=0", (f"{path}%",)).fetchall()
                    ids_to_hash.extend([c[0] for c in children])
                    
        if not ids_to_hash: return QMessageBox.information(self, "Empty", "No files found to hash in selection.")
        
        self.hash_dlg = QProgressDialog(f"Scanning & Hashing {len(ids_to_hash)} files...", "Cancel", 0, len(ids_to_hash), self)
        self.hash_dlg.setWindowModality(Qt.WindowModal); self.hash_dlg.setMinimumDuration(0); self.hash_dlg.show()
        QApplication.processEvents()
        
        self.bulk_hasher = BulkHashCalculator(self.active_db_path, target_ids=ids_to_hash, parent=self)
        self.bulk_hasher.progress.connect(lambda c,t,m: (self.hash_dlg.setMaximum(t), self.hash_dlg.setValue(c), self.hash_dlg.setLabelText(m)))
        self.hash_dlg.canceled.connect(self.bulk_hasher.cancel)
        self.bulk_hasher.finished.connect(lambda count: (self.hash_dlg.close(), QMessageBox.information(self, "Complete", f"Successfully computed and stored SHA-256 hashes for {count} files."), self.clear_cache(), self.refresh_all()))
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
        
        # Using the Smart Collision Checker
        dest_dir = self._get_export_dest_with_check(items, "Select Physical OS Destination")
        if not dest_dir: return

        self.mat_dlg = QProgressDialog("Analyzing structure for export...", "Cancel", 0, 100, self)
        self.mat_dlg.setWindowTitle("Materializing Files")
        self.mat_dlg.setFixedSize(600, 160)
        label = self.mat_dlg.findChild(QLabel)
        if label: label.setWordWrap(True)
        self.mat_dlg.setWindowModality(Qt.WindowModal)
        self.mat_dlg.show()

        self.mat_thread = MaterializeThread(self.active_db_path, items, dest_dir, self)
        self.mat_thread.progress.connect(lambda v, t, m: (self.mat_dlg.setMaximum(t), self.mat_dlg.setValue(v), self.mat_dlg.setLabelText(m)))
        self.mat_dlg.canceled.connect(self.mat_thread.cancel)
        self.mat_thread.finished.connect(lambda count: (self.mat_dlg.close(), QMessageBox.information(self, "Materialize Complete", f"Successfully exported {count} files to the OS.")))
        self.mat_thread.error.connect(lambda err: (self.mat_dlg.close(), QMessageBox.warning(self, "Export Error", err)))
        self._register_worker(self.mat_thread); self.mat_thread.start()
        
    def _get_export_dest_with_check(self, items, title):
        """Asks for destination and checks if files/folders already exist to prevent blind overwrites."""
        while True:
            dest_dir = QFileDialog.getExistingDirectory(self, title)
            if not dest_dir: return None
            
            collision = False
            with sqlite3.connect(self.active_db_path) as conn:
                for typ, path_val, db_id in items:
                    base_name = ""
                    if typ == "folder":
                        base_name = path_val.strip('/').split('/')[-1] if path_val.strip('/') else ""
                    elif db_id != -1:
                        res = conn.execute("SELECT name FROM virtual_fs WHERE id=?", (db_id,)).fetchone()
                        if res: base_name = res[0]
                    
                    if base_name:
                        safe_name = re.sub(r'[\\/*?:"<>|]', '_', str(base_name))
                        if os.path.exists(os.path.join(dest_dir, safe_name)):
                            collision = True
                            break
                            
            if collision:
                msg = QMessageBox(self)
                msg.setIcon(QMessageBox.Warning)
                msg.setWindowTitle("Overwrite Warning")
                msg.setText(f"One or more items already exist in:\n{dest_dir}")
                msg.setInformativeText("Do you want to OVERWRITE them, or choose another location?")
                
                btn_over = msg.addButton("Overwrite", QMessageBox.AcceptRole)
                btn_another = msg.addButton("Choose Another Location", QMessageBox.ActionRole)
                msg.addButton("Cancel", QMessageBox.RejectRole)
                msg.exec()
                
                if msg.clickedButton() == btn_over:
                    return dest_dir       # User chose to overwrite
                elif msg.clickedButton() == btn_another:
                    continue              # Loops back to open the folder picker again
                else:
                    return None           # User clicked Cancel
            else:
                return dest_dir           # No collision, safe to proceed
                
    def export_dummy_replica(self, items):
        if not items: return
        # Using the new smart checker that prompts on collision!
        dest_dir = self._get_export_dest_with_check(items, "Select Physical OS Destination for Dummy Replicas")
        if not dest_dir: return
        

        # --- FILESYSTEM SPARSE SUPPORT TEST ---
        # We must ensure the drive supports sparse files to prevent massive SSD wear.
        test_file = Path(dest_dir) / ".vman_sparse_test.tmp"
        sparse_supported = True
        fail_reason = "Unknown Error"

        try:
            if sys.platform == "win32":
                with open(test_file, "wb") as f: pass
                # Attempt to set sparse flag. Will fail on FAT32/exFAT.
                res = subprocess.run(["fsutil", "sparse", "setflag", str(test_file)], capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
                if res.returncode != 0:
                    sparse_supported = False
                    fail_reason = "Your target drive format (e.g., FAT32 or exFAT) does not support Windows Sparse Files."
            else:
                # Unix/Mac Test: Truncate to 50MB and check if OS wrote physical sectors.
                with open(test_file, "wb") as f: f.truncate(50 * 1024 * 1024)
                st = os.stat(test_file)
                # st_blocks represents physical 512-byte blocks. If it allocated more than 1000 blocks, it's writing zeros!
                if hasattr(st, 'st_blocks') and st.st_blocks > 1000:
                    sparse_supported = False
                    fail_reason = "Your target drive format does not support native sparse files. Continuing would cause extreme SSD wear."
        except Exception as e:
            sparse_supported = False
            fail_reason = f"System test failed: {str(e)}"
        finally:
            if test_file.exists(): test_file.unlink()

        # If it fails, give the user the exact reason and abort.
        # --- FILESYSTEM SPARSE SUPPORT TEST ---
        if not sparse_supported:
            msg = f"Cannot create Dummy Replicas here.\n\nREASON: {fail_reason}\n\nPlease select an NTFS drive (Windows) or APFS/ext4 drive (Mac/Linux) to safely create these fake files without SSD wear.\n\nAlternatively, use the 'Create Zero-Byte Replica' option."
            QMessageBox.warning(self, "Unsupported Filesystem", msg)
            return

        # --- UPDATED PROGRESS DIALOG (FIXED SIZE & WORD WRAP) ---
        self.mat_dlg = QProgressDialog("Analyzing structure for sparse replication...", "Cancel", 0, 100, self)
        self.mat_dlg.setWindowTitle("Creating Dummy Replicas")
        
        # 1. Lock the size so it NEVER shape-shifts
        self.mat_dlg.setFixedSize(600, 160)
        
        # 2. Force the internal label to wrap long file paths to the next line
        label = self.mat_dlg.findChild(QLabel)
        if label:
            label.setWordWrap(True)
            
        self.mat_dlg.setWindowModality(Qt.WindowModal)
        self.mat_dlg.show()

        self.dummy_rep_thread = DummyReplicaThread(self.active_db_path, items, dest_dir, zero_byte_mode=False, parent=self)
        self.dummy_rep_thread.progress.connect(lambda v, t, m: (self.mat_dlg.setMaximum(t), self.mat_dlg.setValue(v), self.mat_dlg.setLabelText(m)))
        self.mat_dlg.canceled.connect(self.dummy_rep_thread.cancel)
        self.dummy_rep_thread.finished.connect(lambda count: (self.mat_dlg.close(), QMessageBox.information(self, "Replica Complete", f"Successfully created {count} zero-space sparse file replicas."), self.sys_log(f"Created Dummy Replicas for {count} files.")))
        self.dummy_rep_thread.error.connect(lambda err: (self.mat_dlg.close(), QMessageBox.warning(self, "Replica Error", err)))
        self._register_worker(self.dummy_rep_thread)
        self.dummy_rep_thread.start()
    
    def export_zero_byte_replica(self, items):
        if not items: return
        # Using the new smart checker that prompts on collision!
        dest_dir = self._get_export_dest_with_check(items, "Select Physical OS Destination for 0-Byte Replicas")
        if not dest_dir: return

        # --- UPDATED PROGRESS DIALOG (FIXED SIZE & WORD WRAP) ---
        self.mat_dlg = QProgressDialog("Creating 0-byte structural replica...", "Cancel", 0, 100, self)
        self.mat_dlg.setWindowTitle("Creating Zero-Byte Replicas")
        
        # 1. Lock the size so it NEVER shape-shifts
        self.mat_dlg.setFixedSize(600, 160)
        
        # 2. Force the internal label to wrap long file paths to the next line
        label = self.mat_dlg.findChild(QLabel)
        if label:
            label.setWordWrap(True)
            
        self.mat_dlg.setWindowModality(Qt.WindowModal)
        self.mat_dlg.show()

        # Note the zero_byte_mode=True flag
        self.dummy_rep_thread = DummyReplicaThread(self.active_db_path, items, dest_dir, zero_byte_mode=True, parent=self)
        self.dummy_rep_thread.progress.connect(lambda v, t, m: (self.mat_dlg.setMaximum(t), self.mat_dlg.setValue(v), self.mat_dlg.setLabelText(m)))
        self.mat_dlg.canceled.connect(self.dummy_rep_thread.cancel)
        self.dummy_rep_thread.finished.connect(lambda count: (self.mat_dlg.close(), QMessageBox.information(self, "Replica Complete", f"Successfully created {count} zero-byte files."), self.sys_log(f"Created Zero-Byte Replicas for {count} files.")))
        self.dummy_rep_thread.error.connect(lambda err: (self.mat_dlg.close(), QMessageBox.warning(self, "Replica Error", err)))
        self._register_worker(self.dummy_rep_thread)
        self.dummy_rep_thread.start()  
 
    def open_advanced_search(self):
        from search import AdvancedSearchWindow
        
        # Require an active database to search
        if not hasattr(self, 'active_db_path') or not self.active_db_path:
            return QMessageBox.warning(self, "No Database", "Please load a database first.")
            
        # Create or update the Window
        if not hasattr(self, 'search_instance') or self.search_instance is None:
            self.search_instance = AdvancedSearchWindow(self.active_db_path, self)
        else:
            self.search_instance.active_db = self.active_db_path
            
        # Display Window
        self.search_instance.show()
        self.search_instance.raise_()
        self.search_instance.activateWindow()
        
    def calculate_smart_folder_sizes(self):
        if not self._is_smart_path(self.current_prefix): return
        
        prog = QProgressDialog("Calculating smart folder sizes...", "Cancel", 0, self.file_table.model().rowCount(), self)
        prog.setWindowModality(Qt.WindowModal)
        prog.setMinimumDuration(0) # Force immediate display
        prog.show()
        
        # --- FIX: Force OS to paint the window before CPU locks up ---
        QApplication.processEvents(); QThread.msleep(50); QApplication.processEvents()
        
        with sqlite3.connect(self.db.path) as conn:
            cur = conn.cursor()
            for r in range(self.file_table.model().rowCount()):
                if prog.wasCanceled(): break
                data = self.file_table.model().data(self.file_table.model().index(r, 1), Qt.UserRole)
                if not data or data[0] != "folder" or data[2] != -1: continue
                
                v_path = data[1]
                smart_files = resolve_smart_folder(cur, v_path, lambda x: x)
                total_files = len(smart_files)
                total_size = sum((sz or 0) for _, _, _, sz in smart_files)
                
                row_dict = self.file_table.model().all_rows[r]
                row_dict["display"][3] = human_size(total_size)
                row_dict["display"][5] = f"Virtual Folder ({total_files})"
                
                row_dict["sort_keys"][3] = (0, total_size)
                row_dict["sort_keys"][5] = (0, total_files)
                
                prog.setValue(r + 1)
                
        self.file_table.viewport().update()
        self.file_grid.viewport().update()
        prog.close()   

    def send_to_database(self, target_db_path, items):
        if not items: return
        
        self.compile_dlg = QProgressDialog(f"Sending items to {target_db_path.stem}...", "Cancel", 0, 100, self)
        self.compile_dlg.setWindowModality(Qt.WindowModal)
        self.compile_dlg.setFixedSize(600, 160)
        self.compile_dlg.setMinimumDuration(0)
        self.compile_dlg.show()
        
        # Force OS Paint
        QApplication.processEvents(); QThread.msleep(50); QApplication.processEvents()
        
        self.compiler = CompilerThread(str(self.db.path), str(target_db_path), items, append_mode=True, parent=self)
        self.compiler.progress.connect(lambda c, t, msg: (self.compile_dlg.setValue(int((c/max(1,t))*100)), self.compile_dlg.setLabelText(msg)))
        self.compile_dlg.canceled.connect(self.compiler.cancel)
        def on_compile_finished(db_res):
            self.compile_dlg.close()
            QMessageBox.information(self, "Success", f"Files successfully sent to:\n{Path(db_res).name}")
            self.sys_log(f"Appended items to Database: {Path(db_res).name}")
        self.compiler.finished.connect(on_compile_finished)
        self.compiler.error.connect(lambda e: (self.compile_dlg.close(), QMessageBox.critical(self, "Error", e)))
        self._register_worker(self.compiler)
        self.compiler.start()            

    def refresh_quick_text(self):
        self.qt_toolbar.clear()
        json_file = DATA_DIR / "text.json"
        saved_texts = []
        if json_file.exists():
            try:
                with open(json_file, "r", encoding="utf-8") as f: saved_texts = json.load(f)
            except Exception: pass
            
        lbl = QLabel(" ⚡ Quick Text: ")
        lbl.setStyleSheet("font-weight: bold; color: #8b949e;")
        self.qt_toolbar.addWidget(lbl)
        
        btn_add = QToolButton()
        btn_add.setText("➕ Add Text")
        btn_add.setStyleSheet("color: #3fb950; font-weight: bold;")
        btn_add.clicked.connect(self.add_quick_text)
        self.qt_toolbar.addWidget(btn_add)
        self.qt_toolbar.addSeparator()
        
        for t in saved_texts:
            self.qt_toolbar.addWidget(QuickTextButton(t, self))

    def add_quick_text(self):
        text, ok = QInputDialog.getText(self, "Add Quick Text", "Enter text to save to ribbon:")
        if ok and text.strip():
            json_file = DATA_DIR / "text.json"
            saved = []
            if json_file.exists():
                try:
                    with open(json_file, "r", encoding="utf-8") as f: saved = json.load(f)
                except Exception: pass
            if text.strip() not in saved: saved.append(text.strip())
            with open(json_file, "w", encoding="utf-8") as f: json.dump(saved, f)
            self.refresh_quick_text()

    def remove_quick_text(self, text):
        json_file = DATA_DIR / "text.json"
        if json_file.exists():
            try:
                with open(json_file, "r", encoding="utf-8") as f: saved = json.load(f)
                if text in saved: saved.remove(text)
                with open(json_file, "w", encoding="utf-8") as f: json.dump(saved, f)
                self.refresh_quick_text()
            except Exception: pass
 
        
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName(APP_TITLE)
    win = vmanVirtualManager()
   
    
    #------splash----
    # 2. Initialize and show the splash screen
    splash = PremiumSplash()
    splash.start_fade_in()
    
    # Optional: If you want to show it loading for a couple of seconds visually
    splash.update_text("Booting Core Engine...")
    app.processEvents()
    time.sleep(0.5) 
    
    splash.update_text("Loading Databases...")
    app.processEvents()
    time.sleep(0.5)

    splash.update_text("Starting UI...")
    app.processEvents()

    # 3. Initialize your main window
    win = vmanVirtualManager()
    splash.start_fade_out()
    #------splash----
    
    
    win.show()
    sys.exit(app.exec())
