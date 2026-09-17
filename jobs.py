import sys
import os
import shutil
import subprocess
import re
import stat
import hashlib
import random
import string
from pathlib import Path
from datetime import datetime

from PySide6.QtWidgets import (
    QApplication, QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QFileDialog,
    QTextBrowser, QProgressBar, QTabWidget, QWidget, QLabel, 
    QMessageBox, QSpinBox, QFormLayout, QGroupBox, QComboBox, 
    QLineEdit, QCheckBox, QPlainTextEdit, QRadioButton
)
from PySide6.QtCore import QThread, Signal, Qt
from PySide6.QtGui import QFont

# ----------------------
# Helpers
# ----------------------
def get_system_drives():
    if sys.platform == "win32":
        from ctypes import windll
        drives = []
        bitmask = windll.kernel32.GetLogicalDrives()
        for letter in string.ascii_uppercase:
            if bitmask & 1: drives.append(f"{letter}:\\")
            bitmask >>= 1
        return drives
    else:
        drives = ["/"]
        for mount in ["/Volumes", "/media", "/mnt"]:
            if os.path.exists(mount):
                try:
                    for d in os.listdir(mount): drives.append(os.path.join(mount, d))
                except Exception: pass
        return drives

def format_bytes(size):
    try: size = float(size)
    except (ValueError, TypeError): return "Unknown"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0: return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

# ----------------------
# Background Threads
# ----------------------
class HardlinkFinderThread(QThread):
    log_signal = Signal(str)
    finished_signal = Signal()

    def __init__(self, target_file, is_dark=True):
        super().__init__()
        self.target_file = Path(target_file)
        self.is_dark = is_dark

    def run(self):
        accent = "#58a6ff" if self.is_dark else "#0969da"
        self.log_signal.emit(f"<hr><h3 style='color:{accent};'>🔍 Searching for Hardlink Siblings...</h3>")
        
        try:
            if sys.platform == "win32":
                # Windows Native Fast Search
                cmd = ["fsutil", "hardlink", "list", str(self.target_file)]
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                
                out = subprocess.check_output(cmd, text=True, startupinfo=startupinfo)
                drive = self.target_file.anchor[:2]  # Extracts "C:"
                
                siblings = []
                for line in out.splitlines():
                    line = line.strip()
                    if line:
                        # fsutil outputs paths relative to the drive (e.g., \Users\...)
                        full_path = line if line.startswith(drive) else f"{drive}{line}"
                        
                        if Path(full_path) != self.target_file:
                            siblings.append(full_path)
                
                if siblings:
                    self.log_signal.emit(f"<span style='color:#3fb950;'>Found {len(siblings)} sibling(s):</span>")
                    for sib in siblings:
                        self.log_signal.emit(f"<span style='color:#d29922;'> &nbsp;&nbsp;&rarr; {sib}</span>")
                else:
                    self.log_signal.emit("<span style='color:#8b949e;'>No siblings found on this volume.</span>")

            else:
                # Linux / Mac Native Fast Search via Inode
                stat_info = self.target_file.stat()
                inode = stat_info.st_ino
                
                cmd = ["find", self.target_file.anchor, "-inum", str(inode)]
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
                
                siblings = []
                for line in process.stdout:
                    line = line.strip()
                    if line and Path(line) != self.target_file:
                        siblings.append(line)
                        self.log_signal.emit(f"<span style='color:#d29922;'> &nbsp;&nbsp;&rarr; {line}</span>")
                        
                if not siblings:
                    self.log_signal.emit("<span style='color:#8b949e;'>No siblings found on this volume.</span>")
                    
        except Exception as e:
            self.log_signal.emit(f"<span style='color:#f85149;'>Search failed. Elevated permissions may be required.<br>Error: {e}</span>")
            
        self.log_signal.emit(f"<br><span style='color:#a371f7;'><b>Search completed.</b></span><hr>")
        self.finished_signal.emit()

class OSCommandThread(QThread):
    log_signal = Signal(str, str)
    finished_signal = Signal()

    def __init__(self, command: list, is_network=False, is_dark=True):
        super().__init__()
        self.command = command; self.is_network = is_network; self.is_dark = is_dark
        self.ip_pattern = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')
        self.mac_pattern = re.compile(r'(?:[0-9a-fA-F]{2}[:-]){5}[0-9a-fA-F]{2}')
        self.process = None

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            self.log_signal.emit("<br><span style='color:#e3b341; font-weight:bold;'>[Process Terminated by User]</span><br>", "")

    def run(self):
        cmd_str = ' '.join(self.command)
        accent = "#58a6ff" if self.is_dark else "#0969da"
        self.log_signal.emit(f"<span style='color:{accent}; font-weight:bold; font-size:14px;'>&gt; Executing: {cmd_str}</span><br>", cmd_str)
        try:
            startupinfo = None
            if sys.platform == "win32":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            self.process = subprocess.Popen(self.command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors='replace', startupinfo=startupinfo)
            for line in self.process.stdout:
                if line.strip(): self.log_signal.emit(self.format_line(line.strip()), line.strip())
            self.process.wait()
            
            if self.process.poll() is not None and self.process.returncode != 15:
                status_color = "#3fb950" if self.process.returncode == 0 else "#f85149"
                self.log_signal.emit(f"<br><span style='color:{status_color}; font-weight:bold;'>[Process completed with code {self.process.returncode}]</span><br><br>", "")
        except Exception as e:
            self.log_signal.emit(f"<br><span style='color:#f85149; font-weight:bold;'>Execution Failed: {e}</span><br><br>", str(e))
        finally:
            self.finished_signal.emit()

    def format_line(self, line):
        html = line.replace('<', '&lt;').replace('>', '&gt;')
        if self.is_network:
            ip_col = "#2ea043" if self.is_dark else "#116329"
            mac_col = "#a371f7" if self.is_dark else "#8250df"
            key_col = "#d29922" if self.is_dark else "#9a6700"
            html = self.ip_pattern.sub(f'<span style="color:{ip_col}; font-weight:bold;">\\g<0></span>', html)
            html = self.mac_pattern.sub(f'<span style="color:{mac_col};">\\g<0></span>', html)
            for keyword in ["IPv4 Address", "Subnet Mask", "Default Gateway", "DNS Servers", "Physical Address", "Description", "Reply from", "Pinging", "Tracing route", "Name:", "Address:", "TCP", "UDP"]:
                if keyword.lower() in html.lower():
                    html = re.sub(f"(?i){keyword}", f"<span style='color:{key_col}; font-weight:bold;'>{keyword}</span>", html)
            if any(x in html.lower() for x in ["timed out", "unreachable", "failure", "error", "could not find", "request timed out"]):
                html = f"<span style='color:#f85149; font-weight:bold;'>{html}</span>"
        return html

class DriveInfoThread(QThread):
    result_signal = Signal(dict)
    finished_signal = Signal()

    def __init__(self, drive):
        super().__init__()
        self.drive = drive

    def run(self):
        info = {}
        try:
            if sys.platform == "win32":
                drive_letter = self.drive.strip("\\")
                cmd = ["wmic", "logicaldisk", "where", f"DeviceID='{drive_letter}'", "get", "FreeSpace,Size,VolumeName,FileSystem,VolumeSerialNumber,Description,DriveType,ProviderName", "/format:list"]
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                out = subprocess.check_output(cmd, text=True, startupinfo=startupinfo)
                for line in out.splitlines():
                    if "=" in line:
                        k, v = line.split("=", 1)
                        info[k.strip()] = v.strip()
            else:
                out = subprocess.check_output(["df", "-Th", self.drive], text=True)
                lines = out.splitlines()
                if len(lines) > 1:
                    parts = lines[1].split()
                    if len(parts) >= 7:
                        info["FileSystem"] = parts[1]; info["Size_HR"] = parts[2]; info["Used_HR"] = parts[3]
                        info["FreeSpace_HR"] = parts[4]; info["Use%"] = parts[5]; info["VolumeName"] = parts[6]
                        info["Description"] = "Mounted Volume"
        except Exception as e: info["Error"] = str(e)
        self.result_signal.emit(info)
        self.finished_signal.emit()

class SmartDummyThread(QThread):
    log_signal = Signal(str)
    progress_signal = Signal(int)
    finished_signal = Signal()

    def __init__(self, target_dir, syntax):
        super().__init__()
        self.target_dir = target_dir
        self.syntax = syntax

    def parse_size(self, size_str):
        m = re.match(r'^([\d\.]+)([a-zA-Z]*)$', size_str.strip())
        if not m: return 0
        val = float(m.group(1))
        unit = m.group(2)
        if unit == 'b': return int(val / 8) # bits to bytes
        elif unit == 'B': return int(val)
        unit = unit.upper()
        mult = 1
        if unit == 'KB': mult = 1024
        elif unit == 'MB': mult = 1024**2
        elif unit == 'GB': mult = 1024**3
        return int(val * mult)

    def run(self):
        m = re.match(r'^(.+):(.+)x(.+)$', self.syntax.strip(), re.IGNORECASE)
        if not m:
            self.log_signal.emit("<span style='color:#f85149;'>[ERROR] Invalid Syntax. Use format: NamePattern:SizeRangexCount</span>")
            self.finished_signal.emit(); return

        name_part = m.group(1).strip()
        size_part = m.group(2).strip()
        count_part = m.group(3).strip().lower()

        # Parse Sizes
        if '-' in size_part:
            min_s, max_s = size_part.split('-')
            min_sz = self.parse_size(min_s); max_sz = self.parse_size(max_s)
        else:
            min_sz = max_sz = self.parse_size(size_part)

        # Parse Advanced <auto> Logic
        is_auto = False
        target_alloc = 0
        count = 0
        
        try: total_free = shutil.disk_usage(self.target_dir).free
        except Exception: total_free = 0

        try:
            if count_part.startswith("<auto"):
                is_auto = True
                if ">." in count_part:
                    leave_str = count_part.split(">.")[1]
                    leave_bytes = self.parse_size(leave_str)
                    target_alloc = max(0, total_free - leave_bytes)
                elif "=" in count_part:
                    exact_str = count_part.split("=")[1].strip(">")
                    target_alloc = self.parse_size(exact_str)
                else:
                    target_alloc = total_free
            else:
                count = int(count_part)
        except ValueError:
            self.log_signal.emit("<span style='color:#f85149;'>[ERROR] Invalid count syntax. Expected a number or an &lt;auto&gt; command.</span>")
            self.finished_signal.emit()
            return

        # Parse Base Names & Array
        arr_match = re.search(r'\[(.*?)\]', name_part)
        bases = []
        if arr_match:
            bases = [x.strip() for x in arr_match.group(1).split(',')]
            ext_pattern = name_part.replace(arr_match.group(0), '')
        else:
            ext_pattern = name_part

        exts_map = {
            'i': ['.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp'],
            'v': ['.mp4', '.mkv', '.avi', '.mov'],
            'a': ['.mp3', '.wav', '.ogg', '.flac'],
            'd': ['.pdf', '.docx', '.txt', '.xlsx'],
            'c': ['.py', '.js', '.html', '.cpp', '.json']
        }

        def get_rand_ext(pat):
            m_ext = re.search(r'<([a-z]+)>', pat.lower())
            if m_ext:
                code = m_ext.group(1)
                choices = []
                if code == 'all':
                    for e in exts_map.values(): choices.extend(e)
                else:
                    for char in code:
                        if char in exts_map: choices.extend(exts_map[char])
                if choices: return random.choice(choices)
            return pat.replace('<filename>', '') if '<filename>' in pat else pat

        allocated = 0; i = 0
        while True:
            if not is_auto and i >= count: break

            # Calculate remaining allocation for <auto>
            if is_auto:
                remaining_to_allocate = target_alloc - allocated
                if remaining_to_allocate <= 0:
                    self.log_signal.emit(f"<span style='color:#e3b341;'>Target volume threshold reached. Stopping.</span>")
                    break

            # Process Name
            if bases:
                base = bases[i % len(bases)]
                if '<' in ext_pattern: filename = f"{base}{get_rand_ext(ext_pattern)}"
                else: filename = f"{base}{ext_pattern}"
            elif '<filename>' in name_part:
                rand_name = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
                filename = f"{rand_name}{get_rand_ext(ext_pattern)}"
            else:
                base, dot_ext = os.path.splitext(name_part)
                if '<' in dot_ext: dot_ext = get_rand_ext(dot_ext)
                filename = f"{base}{i+1}{dot_ext}"

            filepath = os.path.join(self.target_dir, filename)

            # Process Size with Natural Extension Limits
            ext = os.path.splitext(filename)[1].lower()
            natural_ranges = {
                # Images
                '.png': (50*1024, 15*1024**2), '.jpg': (50*1024, 10*1024**2), '.jpeg': (50*1024, 10*1024**2),
                '.gif': (10*1024, 15*1024**2), '.bmp': (100*1024, 20*1024**2), '.webp': (10*1024, 5*1024**2),
                # Video
                '.mp4': (10*1024**2, 2*1024**3), '.mkv': (50*1024**2, 3*1024**3), 
                '.avi': (10*1024**2, 1*1024**3), '.mov': (10*1024**2, 2*1024**3),
                # Audio
                '.mp3': (1*1024**2, 15*1024**2), '.wav': (5*1024**2, 50*1024**2), 
                '.ogg': (1*1024**2, 10*1024**2), '.flac': (10*1024**2, 100*1024**2),
                # Documents
                '.pdf': (100*1024, 50*1024**2), '.docx': (10*1024, 20*1024**2), '.xlsx': (10*1024, 15*1024**2),
                # Code / Text
                '.txt': (100, 2*1024**2), '.py': (100, 500*1024), '.js': (100, 2*1024**2), 
                '.html': (100, 5*1024**2), '.cpp': (100, 1*1024**2), '.json': (100, 20*1024**2)
            }
            
            calc_min, calc_max = min_sz, max_sz
            if ext in natural_ranges:
                nat_min, nat_max = natural_ranges[ext]
                # Bound the random size to the natural limits, unless user forced a strict override
                calc_min = max(min_sz, nat_min)
                calc_max = min(max_sz, nat_max)
                if calc_min > calc_max: 
                    calc_min, calc_max = min_sz, max_sz # Fallback to strict user bounds
                    
            f_size = random.randint(calc_min, calc_max)
            if is_auto: f_size = min(f_size, remaining_to_allocate)

            try:
                # 1. Create empty file first
                with open(filepath, "wb") as f: 
                    pass 
                
                # 2. Mark it as sparse on Windows to prevent physical disk writes (Zero SSD Wear)
                if sys.platform == "win32":
                    subprocess.run(["fsutil", "sparse", "setflag", filepath], creationflags=subprocess.CREATE_NO_WINDOW)
                
                # 3. Native logical truncation. This forces the OS to report the massive file size 
                # but because it's flagged as sparse, it physically writes nothing to the drive.
                with open(filepath, "r+b") as f: 
                    f.truncate(f_size)
                    
                allocated += f_size
                self.log_signal.emit(f"<span style='color:#3fb950;'>[SPARSE ALLOCATED]</span> {filename} ({format_bytes(f_size)})")
            except Exception as e:
                self.log_signal.emit(f"<span style='color:#f85149;'>[ERROR] {e}</span>")
                if is_auto: break
            
            i += 1
            if not is_auto: self.progress_signal.emit(int((i / count) * 100))
            else: self.progress_signal.emit(int((allocated / target_alloc) * 100) if target_alloc > 0 else 100)

        self.log_signal.emit(f"<br><span style='color:#a371f7; font-weight:bold;'>Total Logical Data Allocated: {format_bytes(allocated)}</span>")
        self.progress_signal.emit(100)
        self.finished_signal.emit()

class AdvancedOpsThread(QThread):
    progress = Signal(int)
    log_signal = Signal(str)
    finished_signal = Signal()

    def __init__(self, op_type, source, dest, args):
        super().__init__()
        self.op_type = op_type; self.source = source; self.dest = dest; self.args = args

    def run(self):
        try:
            if self.op_type == "CLONE": self.run_clone()
            elif self.op_type == "WIPE": self.run_wipe()
            elif self.op_type == "WIPE_FREE": self.run_wipe_free()
            elif self.op_type == "RECENT": self.run_recent_scan()
        except Exception as e:
            self.log_signal.emit(f"<span style='color:#f85149;'><b>[ERROR]</b> {e}</span>")
        finally:
            self.finished_signal.emit()

    def run_clone(self):
        base = Path(self.source).name
        name_suffix = ""
        if self.args.get('timestamp'): name_suffix += f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        target_root = Path(self.dest) / f"{base}{name_suffix}"
        
        if self.args.get('versioned'):
            existing = [p.name for p in Path(self.dest).iterdir() if p.is_dir() and p.name.startswith(target_root.name)]
            v = 1
            while f"{target_root.name}_v{v}" in existing: v += 1
            target_root = Path(self.dest) / f"{target_root.name}_v{v}"
            
        target_root.mkdir(parents=True, exist_ok=True)
        self.log_signal.emit(f"<span style='color:#58a6ff;'><b>[START]</b> Versioning to: {target_root}</span>")
        
        msg = self.args.get('commit', '').strip()
        if msg:
            with open(target_root / "vman_version_manifest.txt", "w", encoding="utf-8") as f:
                f.write(f"Version Source: {self.source}\nDate: {datetime.now()}\n\nCommit Notes:\n{msg}\n")

        files = [p for p in Path(self.source).rglob("*") if p.is_file()]
        if self.args.get('ignore_hidden'): files = [f for f in files if not f.name.startswith('.')]
        
        total = len(files) or 1
        for count, f in enumerate(files, 1):
            tgt = target_root / f.relative_to(self.source)
            tgt.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.link(f, tgt); self.log_signal.emit(f"<span style='color:#3fb950;'>[LINKED]</span> {tgt.name}")
            except Exception:
                shutil.copy2(f, tgt); self.log_signal.emit(f"<span style='color:#d29922;'>[COPIED]</span> {tgt.name}")
            self.progress.emit(int(count / total * 100))

    def run_wipe(self):
        paths, passes = self.source, self.args.get('passes', 3)
        all_files = []
        for p in paths:
            if p.is_file(): all_files.append(p)
            elif p.is_dir(): all_files.extend([f for f in p.rglob("*") if f.is_file()])

        total = len(all_files) or 1
        for count, f in enumerate(all_files, 1):
            try:
                size = f.stat().st_size
                with open(f, "r+b") as file_obj:
                    for _ in range(passes):
                        file_obj.seek(0)
                        remaining = size
                        while remaining > 0:
                            to_write = min(1024 * 1024, remaining)
                            file_obj.write(os.urandom(to_write))
                            remaining -= to_write
                        file_obj.flush()
                        os.fsync(file_obj.fileno())
                f.unlink()
                self.log_signal.emit(f"<span style='color:#f85149;'>[SHREDDED]</span> {f.name}")
            except Exception as e: self.log_signal.emit(f"<span style='color:#e3b341;'>[FAILED]</span> {f.name} : {e}")
            self.progress.emit(int(count / total * 100))
        for p in paths:
            if p.is_dir(): shutil.rmtree(p, ignore_errors=True)

    def run_wipe_free(self):
        drive = self.source
        self.log_signal.emit(f"<span style='color:#58a6ff;'><b>[START]</b> Securing free space on {drive}...</span>")
        dummy_file = os.path.join(drive, f"WIPE_FREE_SPACE_{int(datetime.now().timestamp())}.tmp")
        try:
            with open(dummy_file, "wb") as f:
                while True:
                    f.write(os.urandom(1024 * 1024 * 50)) 
                    self.log_signal.emit(f"<span style='color:#a371f7;'>[WRITING]</span> Shredding 50MB block of free sectors...")
        except OSError: pass 
        finally:
            if os.path.exists(dummy_file): os.remove(dummy_file)
        self.log_signal.emit(f"<span style='color:#3fb950;'><b>[COMPLETE]</b> Free space securely wiped and released.</span>")
        self.progress.emit(100)

    def run_recent_scan(self):
        self.log_signal.emit("<h3 style='color:#a371f7;'>Scanning for recently modified files...</h3>")
        folder = Path(self.source)
        try:
            files = []
            for root, _, f_names in os.walk(folder):
                for f in f_names:
                    p = Path(root) / f
                    try: files.append((p, p.stat().st_mtime))
                    except Exception: pass
            
            files.sort(key=lambda x: x[1], reverse=True)
            top_50 = files[:50]
            
            if not top_50:
                self.log_signal.emit("No files found.")
                return
                
            html = "<table width='100%' style='border-collapse:collapse; font-size:13px;'>"
            for p, mtime in top_50:
                dt = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
                html += f"<tr><td style='padding:4px; border-bottom:1px solid #444; color:#3fb950;'>{dt}</td><td style='padding:4px; border-bottom:1px solid #444;'>{p}</td></tr>"
            html += "</table>"
            self.log_signal.emit(html)
        except Exception as e:
            self.log_signal.emit(f"<span style='color:#f85149;'>Scan Failed: {e}</span>")

class DirCompareThread(QThread):
    import filecmp
    log_signal = Signal(str)
    finished_signal = Signal()

    def __init__(self, dir1: Path, dir2: Path):
        super().__init__()
        self.dir1 = dir1; self.dir2 = dir2

    def run(self):
        self.log_signal.emit(f"<h3 style='color:#58a6ff;'>⚖️ Diffing Versions</h3>")
        self.log_signal.emit(f"<b>[A] Older:</b> {self.dir1}<br><b>[B] Newer:</b> {self.dir2}<hr>")
        diff_found = False
        def compare_trees(d1, d2, relative_path=""):
            nonlocal diff_found
            dc = self.filecmp.dircmp(d1, d2)
            if dc.left_only:
                diff_found = True; self.log_signal.emit(f"<div style='color:#f85149; margin-top: 5px;'><b>[-] Deleted in Newer Version:</b></div>")
                for f in dc.left_only: self.log_signal.emit(f"<span style='color:#f85149;'>&nbsp;&nbsp; - {os.path.join(relative_path, f)}</span>")
            if dc.right_only:
                diff_found = True; self.log_signal.emit(f"<div style='color:#3fb950; margin-top: 5px;'><b>[+] Added in Newer Version:</b></div>")
                for f in dc.right_only: self.log_signal.emit(f"<span style='color:#3fb950;'>&nbsp;&nbsp; + {os.path.join(relative_path, f)}</span>")
            if dc.diff_files:
                diff_found = True; self.log_signal.emit(f"<div style='color:#d29922; margin-top: 5px;'><b>[~] Modified Files:</b></div>")
                for f in dc.diff_files: self.log_signal.emit(f"<span style='color:#d29922;'>&nbsp;&nbsp; ~ {os.path.join(relative_path, f)}</span>")
            for common_dir in dc.common_dirs: compare_trees(os.path.join(d1, common_dir), os.path.join(d2, common_dir), os.path.join(relative_path, common_dir))
        try:
            compare_trees(self.dir1, self.dir2)
            if not diff_found: self.log_signal.emit("<br><span style='color:#3fb950;'><b>✓ Both versions are completely identical.</b></span>")
        except Exception as e: self.log_signal.emit(f"<br><span style='color:#f85149;'><b>Error during comparison:</b> {e}</span>")
        self.log_signal.emit("<hr><b style='color:#58a6ff;'>Comparison Complete.</b>")
        self.finished_signal.emit()

# ----------------------
# Extra Features UI
# ----------------------
class ExtraFeaturesDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Extra Jobs")
        
        
        if sys.platform == "win32":
            self.resize(1030, 600)
        elif sys.platform == "darwin":
            self.resize(1035, 600) 
        
        self.is_dark = getattr(parent, 'is_dark_mode', True) if parent else True
        if parent and hasattr(parent, 'styleSheet'): self.setStyleSheet(parent.styleSheet())
            
        self.active_threads = []
        layout = QVBoxLayout(self)
        
        self.tabs = QTabWidget()
        self.tabs.addTab(self._build_drive_tab(), "💽 Volume Manager")
        self.tabs.addTab(self._build_network_tab(), "🌐 Network Suite")
        self.tabs.addTab(self._build_inspector_tab(), "📄 File Inspector")
        self.tabs.addTab(self._build_dummy_tab(), "📦 Dummy Files")
        self.tabs.addTab(self._build_clone_tab(), "🔗 Version Control")
        self.tabs.addTab(self._build_junction_tab(), "🔀 Links & Junctions")
        self.tabs.addTab(self._build_attr_tab(), "🛡️ Attributes & Recent")
        self.tabs.addTab(self._build_wipe_tab(), "🧨 Secure Wipe")
        
        layout.addWidget(self.tabs, stretch=1)

    def _create_button(self, text, icon=""):
        btn = QPushButton(f"{icon} {text}")
        btn.setMinimumHeight(35)
        return btn

    def _open_folder(self, line_edit):
        path = line_edit.text().strip()
        if os.path.exists(path):
            if sys.platform == "win32": os.startfile(path)
            elif sys.platform == "darwin": subprocess.Popen(["open", path])
            else: subprocess.Popen(["xdg-open", path])

    def _build_drive_tab(self):
        w = QWidget(); lay = QVBoxLayout(w)
        ctrl = QHBoxLayout()
        ctrl.addWidget(QLabel("<b>Target Logical Drive:</b>"))
        self.combo_drives = QComboBox()
        self.combo_drives.addItems(get_system_drives())
        ctrl.addWidget(self.combo_drives, stretch=1)
        
        btn_info = self._create_button("Analyze Volume Info", "📊")
        btn_info.clicked.connect(self.run_drive_info)
        ctrl.addWidget(btn_info)
        lay.addLayout(ctrl)
        
        self.drive_console = QTextBrowser(); self.drive_console.setFont(QFont("Consolas", 11))
        lay.addWidget(self.drive_console, stretch=1)
        return w

    def _build_network_tab(self):
        w = QWidget(); lay = QVBoxLayout(w)
        ctrl = QHBoxLayout()
        self.combo_net_tool = QComboBox()
        self.combo_net_tool.addItems(["Ping", "Traceroute", "DNS Lookup (nslookup)", "Network Stats (netstat)", "ARP Table"])
        ctrl.addWidget(self.combo_net_tool)
        
        self.txt_ping = QLineEdit("8.8.8.8")
        self.txt_ping.setPlaceholderText("Target IP or Domain...")
        ctrl.addWidget(self.txt_ping, stretch=1)
        
        btn_run = self._create_button("Execute Tool", "📡")
        btn_run.clicked.connect(self.run_network_tool)
        
        self.btn_stop_net = self._create_button("Stop", "🛑")
        self.btn_stop_net.clicked.connect(self.stop_network_tool)
        self.btn_stop_net.setEnabled(False)
        
        btn_ip = self._create_button("Local IP Config", "🖥️")
        btn_ip.clicked.connect(self.run_ipconfig)
        
        ctrl.addWidget(btn_run); ctrl.addWidget(self.btn_stop_net); ctrl.addWidget(btn_ip)
        lay.addLayout(ctrl)
        
        self.net_console = QTextBrowser(); self.net_console.setFont(QFont("Consolas", 10))
        bg = "#010409" if self.is_dark else "#f6f8fa"
        self.net_console.setStyleSheet(f"background: {bg}; border-radius: 4px; padding: 10px;")
        lay.addWidget(self.net_console, stretch=1)
        return w

    def _build_inspector_tab(self):
        w = QWidget(); lay = QVBoxLayout(w)
        ctrl = QHBoxLayout()
        btn_sel = self._create_button("Select File for Deep Analysis", "📂")
        btn_sel.clicked.connect(self.run_file_inspector)
        self.btn_find_links = self._create_button("Find Hardlink Siblings", "🔍")
        self.btn_find_links.clicked.connect(self.find_hardlinks)
        self.btn_find_links.setEnabled(False)
        ctrl.addWidget(btn_sel); ctrl.addWidget(self.btn_find_links)
        lay.addLayout(ctrl)
        
        self.inspector_view = QTextBrowser()
        self.inspector_view.setHtml("<div style='text-align:center; font-size:16px; margin-top:40px;'>Select a file to extract OS-level metadata, inodes, and security checksums.</div>")
        lay.addWidget(self.inspector_view, stretch=1)
        return w

    def _build_dummy_tab(self):
        w = QWidget(); lay = QVBoxLayout(w)
        lay.addWidget(QLabel("<b>Smart Dummy File Generator (Sparse Allocation)</b><br><span style='color:#808080;'>Instantly allocate zero-fill files without SSD wear.</span>"))
        
        self.dummy_tabs = QTabWidget()
        
        # Simple Mode Tab
        t_simple = QWidget(); f_simple = QFormLayout(t_simple)
        self.txt_dummy_dir = QLineEdit(); self.txt_dummy_dir.textChanged.connect(self.update_dummy_stats)
        btn_dir = self._create_button("Browse"); btn_dir.clicked.connect(lambda: (self.txt_dummy_dir.setText(QFileDialog.getExistingDirectory(self, "Output Directory")), self.update_dummy_stats()))
        btn_open_s = self._create_button("📂 Open"); btn_open_s.clicked.connect(lambda: self._open_folder(self.txt_dummy_dir))
        dir_lay = QHBoxLayout(); dir_lay.addWidget(self.txt_dummy_dir); dir_lay.addWidget(btn_dir); dir_lay.addWidget(btn_open_s)
        f_simple.addRow("Target Directory:", dir_lay)
        self.txt_dummy_name = QLineEdit("payload_data")
        f_simple.addRow("Base File Name:", self.txt_dummy_name)
        self.spin_dummy_count = QSpinBox(); self.spin_dummy_count.setRange(1, 100000); self.spin_dummy_count.setValue(10)
        self.spin_dummy_count.valueChanged.connect(self.update_dummy_stats)
        f_simple.addRow("Number of Files:", self.spin_dummy_count)
        self.spin_dummy_size = QSpinBox(); self.spin_dummy_size.setRange(1, 1024000); self.spin_dummy_size.setValue(100)
        self.spin_dummy_size.setSuffix(" MB")
        self.spin_dummy_size.valueChanged.connect(self.update_dummy_stats)
        f_simple.addRow("Size per File:", self.spin_dummy_size)
        self.dummy_tabs.addTab(t_simple, "Simple Mode")

        # Smart Mode Tab
        t_smart = QWidget(); f_smart = QFormLayout(t_smart)
        self.txt_dummy_dir_sm = QLineEdit()
        btn_dir_sm = self._create_button("Browse"); btn_dir_sm.clicked.connect(lambda: self.txt_dummy_dir_sm.setText(QFileDialog.getExistingDirectory(self, "Output Directory")))
        btn_open_sm = self._create_button("📂 Open"); btn_open_sm.clicked.connect(lambda: self._open_folder(self.txt_dummy_dir_sm))
        dir_lay_sm = QHBoxLayout(); dir_lay_sm.addWidget(self.txt_dummy_dir_sm); dir_lay_sm.addWidget(btn_dir_sm); dir_lay_sm.addWidget(btn_open_sm)
        f_smart.addRow("Target Directory:", dir_lay_sm)
        self.txt_dummy_cmd = QLineEdit("[Sonia, Rahul].<iva>:24kb-5mbx<auto>.1gb")
        f_smart.addRow("Smart Command:", self.txt_dummy_cmd)
        
        help_html = """
        <div style='font-size:12px; color:#8b949e; background:#252526; padding:5px; border-radius:4px;'>
        <b>Syntax: NamePattern:SizeRange x Count</b><br>
        - file.txt:24kbx5 &rarr; file1.txt...file5.txt (24KB each)<br>
        - file.&lt;i&gt;:25kb-5mbx20 &rarr; 20 files with random image exts & sizes<br>
        - &lt;filename&gt;.&lt;iva&gt;:54B-1gbx40 &rarr; Random names, random img/vid/aud exts (b=bits, B=bytes)<br>
        - file.&lt;all&gt;:23b-1gbx&lt;auto&gt; &rarr; Fill entire disk automatically<br>
        - [Sonia, Rahul].&lt;dc&gt;:1mbx&lt;auto&gt;.34gb &rarr; Loop array names, fill disk until exactly 34GB free left<br>
        - file.bin:5mbx&lt;auto=2gb&gt; &rarr; Fill exactly 2GB worth of files
        </div>
        """
        f_smart.addRow("", QLabel(help_html))
        self.dummy_tabs.addTab(t_smart, "Smart Command Mode")
        lay.addWidget(self.dummy_tabs)
        
        self.lbl_dummy_stats = QLabel("<b>Disk Free Space:</b> N/A | <b>Projected Total Size:</b> 1000.00 MB")
        lay.addWidget(self.lbl_dummy_stats)
        
        self.dummy_progress = QProgressBar(); self.dummy_progress.setValue(0)
        lay.addWidget(self.dummy_progress)
        
        btn_gen = self._create_button("⚡ Execute Dummy Allocation")
        btn_gen.clicked.connect(self.run_dummy_files)
        lay.addWidget(btn_gen)
        
        self.dummy_console = QTextBrowser(); self.dummy_console.setFont(QFont("Consolas", 9))
        lay.addWidget(self.dummy_console, stretch=1)
        self.update_dummy_stats()
        return w

    def _build_clone_tab(self):
        w = QWidget(); lay = QVBoxLayout(w)
        grp = QGroupBox("Version Control & Hardlink Cloning")
        c_lay = QVBoxLayout(grp)
        c_lay.addWidget(QLabel("Create folder replicas instantly using Zero-Space Hardlinks. Perfect for managing versions without using extra storage."))
        
        opts = QHBoxLayout()
        self.chk_version = QCheckBox("Auto-Increment Version (_v1, _v2)")
        self.chk_timestamp = QCheckBox("Append Date/Timestamp")
        self.chk_hidden = QCheckBox("Ignore Hidden Files")
        opts.addWidget(self.chk_version); opts.addWidget(self.chk_timestamp); opts.addWidget(self.chk_hidden); opts.addStretch()
        c_lay.addLayout(opts)
        
        c_lay.addWidget(QLabel("<b>Commit Message / Backup Notes:</b>"))
        self.txt_commit = QPlainTextEdit()
        self.txt_commit.setPlaceholderText("Write notes regarding this version. A 'vman_version_manifest.txt' will be created in the destination folder.")
        self.txt_commit.setMaximumHeight(60)
        c_lay.addWidget(self.txt_commit)
        
        btn_lay = QHBoxLayout()
        btn_clone = self._create_button("🔗 Execute Hardlink Version")
        btn_clone.clicked.connect(self.trigger_clone)
        btn_compare = self._create_button("⚖️ Compare Versions (Diff)")
        btn_compare.clicked.connect(self.trigger_compare)
        btn_lay.addWidget(btn_clone); btn_lay.addWidget(btn_compare)
        c_lay.addLayout(btn_lay)
        lay.addWidget(grp)
        
        self.clone_progress = QProgressBar(); self.clone_progress.setValue(0)
        lay.addWidget(self.clone_progress)
        self.clone_console = QTextBrowser(); self.clone_console.setFont(QFont("Consolas", 9))
        lay.addWidget(self.clone_console, stretch=1)
        return w

    def _build_junction_tab(self):
        w = QWidget(); lay = QVBoxLayout(w)
        lay.addWidget(QLabel("<h2>Directory Junctions & Symlinks</h2><p>Create powerful OS-level shortcuts that trick applications into thinking a folder exists elsewhere.</p>"))
        
        form = QFormLayout()
        self.txt_junc_src = QLineEdit()
        btn_junc_src = self._create_button("Browse"); btn_junc_src.clicked.connect(lambda: self.txt_junc_src.setText(QFileDialog.getExistingDirectory(self, "Select Original Target Folder")))
        btn_op1 = self._create_button("📂 Open"); btn_op1.clicked.connect(lambda: self._open_folder(self.txt_junc_src))
        src_lay = QHBoxLayout(); src_lay.addWidget(self.txt_junc_src); src_lay.addWidget(btn_junc_src); src_lay.addWidget(btn_op1)
        form.addRow("Original Target (Source):", src_lay)
        
        self.txt_junc_dst = QLineEdit()
        btn_junc_dst = self._create_button("Browse"); btn_junc_dst.clicked.connect(lambda: self.txt_junc_dst.setText(QFileDialog.getExistingDirectory(self, "Select Where Link Will Be Placed")))
        btn_op2 = self._create_button("📂 Open"); btn_op2.clicked.connect(lambda: self._open_folder(self.txt_junc_dst))
        dst_lay = QHBoxLayout(); dst_lay.addWidget(self.txt_junc_dst); dst_lay.addWidget(btn_junc_dst); dst_lay.addWidget(btn_op2)
        form.addRow("Link Location (Where shortcut goes):", dst_lay)
        
        self.txt_junc_name = QLineEdit("MyJunction")
        form.addRow("Name of Link:", self.txt_junc_name)
        
        self.combo_link_type = QComboBox()
        if sys.platform == "win32": self.combo_link_type.addItems(["Directory Junction (/J) - Recommended", "Directory Symbolic Link (/D)", "File Symbolic Link"])
        else: self.combo_link_type.addItems(["Symbolic Link (Soft Link)", "Hard Link"])
        form.addRow("Link Type:", self.combo_link_type)
        
        lay.addLayout(form)
        btn_create = self._create_button("🔀 Create Link")
        btn_create.clicked.connect(self.create_junction)
        lay.addWidget(btn_create)
        
        self.junc_console = QTextBrowser(); self.junc_console.setFont(QFont("Consolas", 9))
        lay.addWidget(self.junc_console, stretch=1)
        return w

    def _build_attr_tab(self):
        w = QWidget(); lay = QVBoxLayout(w)
        
        grp_attr = QGroupBox("File & Folder Attributes")
        a_lay = QVBoxLayout(grp_attr)
        a_lay.addWidget(QLabel("<i>Applying to a folder recursively applies to all its contents.</i>"))
        
        a_form = QFormLayout()
        self.txt_attr_tgt = QLineEdit()
        btn_attr_sel = self._create_button("Select File")
        btn_attr_sel.clicked.connect(lambda: self.txt_attr_tgt.setText(QFileDialog.getOpenFileName(self, "Select File")[0]))
        btn_attr_sel_d = self._create_button("Select Folder")
        btn_attr_sel_d.clicked.connect(lambda: self.txt_attr_tgt.setText(QFileDialog.getExistingDirectory(self, "Select Folder")))
        btn_op3 = self._create_button("📂 Open"); btn_op3.clicked.connect(lambda: self._open_folder(self.txt_attr_tgt))
        
        t_lay = QHBoxLayout(); t_lay.addWidget(self.txt_attr_tgt); t_lay.addWidget(btn_attr_sel); t_lay.addWidget(btn_attr_sel_d); t_lay.addWidget(btn_op3)
        a_form.addRow("Target:", t_lay)
        
        self.combo_attr = QComboBox()
        self.combo_attr.addItems([
            "Lock (Make Undeletable & Unmodifiable)", 
            "Unlock (Restore Full Access)", 
            "Make Read-Only (Basic Flag)", 
            "Make Normal (Reset Basic Flags)", 
            "Hide (Act as System File)", 
            "Unhide"
        ])
        a_form.addRow("Action:", self.combo_attr)
        a_lay.addLayout(a_form)
        
        btn_apply_attr = self._create_button("🛡️ Apply Attribute (Recursive)")
        btn_apply_attr.clicked.connect(self.apply_attributes)
        a_lay.addWidget(btn_apply_attr)
        lay.addWidget(grp_attr)
        
        grp_rec = QGroupBox("Recent Files Scanner")
        r_lay = QVBoxLayout(grp_rec)
        r_lay.addWidget(QLabel("Scan any directory to find the 50 most recently modified files."))
        r_form = QFormLayout()
        self.txt_rec_dir = QLineEdit()
        btn_rec_dir = self._create_button("Browse"); btn_rec_dir.clicked.connect(lambda: self.txt_rec_dir.setText(QFileDialog.getExistingDirectory(self, "Select Directory to Scan")))
        btn_op4 = self._create_button("📂 Open"); btn_op4.clicked.connect(lambda: self._open_folder(self.txt_rec_dir))
        rec_d_lay = QHBoxLayout(); rec_d_lay.addWidget(self.txt_rec_dir); rec_d_lay.addWidget(btn_rec_dir); rec_d_lay.addWidget(btn_op4)
        r_form.addRow("Directory to Scan:", rec_d_lay)
        r_lay.addLayout(r_form)
        
        btn_scan_rec = self._create_button("🔍 Scan Recent Files")
        btn_scan_rec.clicked.connect(self.scan_recent_files)
        r_lay.addWidget(btn_scan_rec)
        lay.addWidget(grp_rec)
        
        self.attr_console = QTextBrowser(); self.attr_console.setFont(QFont("Consolas", 9))
        lay.addWidget(self.attr_console, stretch=1)
        return w

    def _build_wipe_tab(self):
        w = QWidget(); lay = QVBoxLayout(w)
        grp = QGroupBox("Military-Grade Secure Wipe")
        w_lay = QVBoxLayout(grp)
        w_lay.addWidget(QLabel("Physically overwrite disk sectors multiple times before unlinking to prevent forensic recovery."))
        form = QFormLayout()
        self.spin_passes = QSpinBox(); self.spin_passes.setRange(1, 15); self.spin_passes.setValue(3); self.spin_passes.setFixedWidth(100)
        form.addRow("Overwrite Passes:", self.spin_passes)
        w_lay.addLayout(form)
        
        btns = QHBoxLayout()
        btn_wipe = self._create_button("🧨 Secure Wipe Target (File/Folder)")
        btn_wipe.clicked.connect(self.trigger_wipe)
        btn_wipe_free = self._create_button("🧹 Shred Free Space on Drive")
        btn_wipe_free.clicked.connect(self.trigger_wipe_free)
        btns.addWidget(btn_wipe); btns.addWidget(btn_wipe_free); btns.addStretch()
        w_lay.addLayout(btns)
        lay.addWidget(grp)
        
        self.wipe_progress = QProgressBar(); self.wipe_progress.setValue(0)
        lay.addWidget(self.wipe_progress)
        self.wipe_console = QTextBrowser(); self.wipe_console.setFont(QFont("Consolas", 9))
        lay.addWidget(self.wipe_console, stretch=1)
        return w

    # ----------------------
    # Execution Logic
    # ----------------------
    def run_drive_info(self):
        drive = self.combo_drives.currentText()
        self.drive_console.setHtml("<h3>Scanning Volume...</h3>")
        t = DriveInfoThread(drive)
        def on_res(info):
            if "Error" in info:
                self.drive_console.setHtml(f"<h3 style='color:#f85149;'>Failed to read volume: {info['Error']}</h3>")
                return
            size = int(info.get('Size', 0)) if 'Size' in info else 0
            free = int(info.get('FreeSpace', 0)) if 'FreeSpace' in info else 0
            used = size - free
            if "Size_HR" in info: hr_size, hr_free, hr_used = info['Size_HR'], info['FreeSpace_HR'], info['Used_HR']
            else: hr_size, hr_free, hr_used = format_bytes(size), format_bytes(free), format_bytes(used)
            pct = (used / size * 100) if size > 0 else 0
            border = "#555" if self.is_dark else "#ccc"
            html = f"""
            <h2 style='color:#58a6ff; margin-bottom:5px;'>💽 Volume Profile: {drive}</h2><br>
            <table width='100%' style='border-collapse: collapse; font-size: 15px;'>
                <tr><td style='padding: 10px; border-bottom: 1px solid {border};'><b>Description:</b></td><td style='border-bottom: 1px solid {border};'>{info.get('Description', 'Local Fixed Disk')}</td></tr>
                <tr><td style='padding: 10px; border-bottom: 1px solid {border};'><b>Volume Name:</b></td><td style='border-bottom: 1px solid {border};'>{info.get('VolumeName', 'None')}</td></tr>
                <tr><td style='padding: 10px; border-bottom: 1px solid {border};'><b>File System:</b></td><td style='border-bottom: 1px solid {border};'>{info.get('FileSystem', 'Unknown')}</td></tr>
                <tr><td style='padding: 10px; border-bottom: 1px solid {border};'><b>Serial Number:</b></td><td style='border-bottom: 1px solid {border};'><span style='color:#d29922;'>{info.get('VolumeSerialNumber', 'N/A')}</span></td></tr>
                <tr><td style='padding: 10px; border-bottom: 1px solid {border};'><b>Total Capacity:</b></td><td style='border-bottom: 1px solid {border};'><b>{hr_size}</b></td></tr>
                <tr><td style='padding: 10px; border-bottom: 1px solid {border};'><b>Used Space:</b></td><td style='border-bottom: 1px solid {border};'><span style='color:#f85149;'>{hr_used} ({pct:.1f}%)</span></td></tr>
                <tr><td style='padding: 10px; border-bottom: 1px solid {border};'><b>Free Space:</b></td><td style='border-bottom: 1px solid {border};'><span style='color:#3fb950;'>{hr_free}</span></td></tr>
            </table>
            """
            self.drive_console.setHtml(html)
        t.result_signal.connect(on_res)
        self.active_threads.append(t)
        t.finished_signal.connect(lambda: self.active_threads.remove(t) if t in self.active_threads else None)
        t.start()

    def run_network_tool(self):
        tool = self.combo_net_tool.currentText()
        target = self.txt_ping.text().strip()
        self.net_console.clear()
        cmd = []
        if tool == "Ping":
            if not target: return
            cmd = ["ping", "-n" if sys.platform == "win32" else "-c", "4", target]
        elif tool == "Traceroute":
            if not target: return
            cmd = ["tracert" if sys.platform == "win32" else "traceroute", target]
        elif tool == "DNS Lookup (nslookup)":
            if not target: return
            cmd = ["nslookup", target]
        elif tool == "Network Stats (netstat)":
            cmd = ["netstat", "-an"]
        elif tool == "ARP Table":
            cmd = ["arp", "-a"]
            
        if cmd:
            self.btn_stop_net.setEnabled(True)
            t = OSCommandThread(cmd, is_network=True, is_dark=self.is_dark)
            t.log_signal.connect(lambda html, raw: self.net_console.append(html))
            self.active_threads.append(t)
            t.finished_signal.connect(lambda: self.btn_stop_net.setEnabled(False))
            t.finished_signal.connect(lambda t=t: self.active_threads.remove(t) if t in self.active_threads else None)
            t.start()

    def stop_network_tool(self):
        for t in self.active_threads:
            if isinstance(t, OSCommandThread) and t.is_network: t.stop()
        self.btn_stop_net.setEnabled(False)

    def run_ipconfig(self):
        self.net_console.clear()
        cmd = ["ipconfig", "/all"] if sys.platform == "win32" else ["ifconfig"]
        self._spawn_command(cmd, self.net_console, is_network=True)

    def run_file_inspector(self):
        path, _ = QFileDialog.getOpenFileName(self, "Deep Inspect File")
        if not path: return
        self.inspected_file = Path(path)
        try:
            s = self.inspected_file.stat()
            md5, sha1, sha256, sha512 = hashlib.md5(), hashlib.sha1(), hashlib.sha256(), hashlib.sha512()
            with open(self.inspected_file, 'rb') as f:
                for block in iter(lambda: f.read(65536), b""):
                    md5.update(block); sha1.update(block); sha256.update(block); sha512.update(block)
            
            box_bg = "#3a2d00" if self.is_dark else "#fff8dc"
            box_border = "#d29922" if self.is_dark else "#daa520"
            txt_head = "#e3b341" if self.is_dark else "#8b6508"
            border = "#555" if self.is_dark else "#ccc"
            
            html = f"""
            <div style='background-color:{box_bg}; padding: 10px; border-left: 5px solid {box_border}; margin-bottom: 15px;'>
                <h2 style='margin:0; color:{txt_head};'>📄 FILE INSPECTOR: {self.inspected_file.name}</h2>
            </div>
            <table width='100%' style='border-collapse: collapse; font-size: 14px;'>
                <tr><td style='padding: 8px; border-bottom: 1px solid {border};' width='25%'><b>Absolute Path:</b></td><td style='border-bottom: 1px solid {border};'>{self.inspected_file.absolute()}</td></tr>
                <tr><td style='padding: 8px; border-bottom: 1px solid {border};'><b>Exact Size:</b></td><td style='border-bottom: 1px solid {border};'><b style='color:#58a6ff;'>{s.st_size} bytes</b> ({format_bytes(s.st_size)})</td></tr>
                <tr><td style='padding: 8px; border-bottom: 1px solid {border};'><b>Created:</b></td><td style='border-bottom: 1px solid {border};'>{datetime.fromtimestamp(s.st_ctime)}</td></tr>
                <tr><td style='padding: 8px; border-bottom: 1px solid {border};'><b>Modified:</b></td><td style='border-bottom: 1px solid {border};'>{datetime.fromtimestamp(s.st_mtime)}</td></tr>
                <tr><td style='padding: 8px; border-bottom: 1px solid {border};'><b>OS Inode:</b></td><td style='border-bottom: 1px solid {border};'><span style='color:#a371f7;'>{s.st_ino}</span></td></tr>
                <tr><td style='padding: 8px; border-bottom: 1px solid {border};'><b>Volume Device ID:</b></td><td style='border-bottom: 1px solid {border};'>{s.st_dev}</td></tr>
                <tr><td style='padding: 8px; border-bottom: 1px solid {border};'><b>Hardlink Count:</b></td><td style='border-bottom: 1px solid {border};'><b style='color:#3fb950;'>{s.st_nlink}</b></td></tr>
                <tr><td style='padding: 8px; border-bottom: 1px solid {border};'><b>Permissions:</b></td><td style='border-bottom: 1px solid {border};'>{stat.filemode(s.st_mode)} (Octal: {oct(stat.S_IMODE(s.st_mode))})</td></tr>
            </table>
            <h3 style='color:#58a6ff; margin-top:20px;'>Security Checksums</h3>
            <table width='100%' style='border-collapse: collapse; font-size: 13px; font-family: Consolas, monospace;'>
                <tr><td style='padding: 6px; border-bottom: 1px solid {border};' width='15%'><b>MD5:</b></td><td style='border-bottom: 1px solid {border};'>{md5.hexdigest()}</td></tr>
                <tr><td style='padding: 6px; border-bottom: 1px solid {border};'><b>SHA-1:</b></td><td style='border-bottom: 1px solid {border};'>{sha1.hexdigest()}</td></tr>
                <tr><td style='padding: 6px; border-bottom: 1px solid {border};'><b>SHA-256:</b></td><td style='border-bottom: 1px solid {border}; color:#3fb950;'><b>{sha256.hexdigest()}</b></td></tr>
                <tr><td style='padding: 6px; border-bottom: 1px solid {border};'><b>SHA-512:</b></td><td style='border-bottom: 1px solid {border}; color:#a371f7;'>{sha512.hexdigest()}</td></tr>
            </table>
            """
            self.inspector_view.setHtml(html)
            self.btn_find_links.setEnabled(s.st_nlink > 1)
            self.btn_find_links.setText(f"🔍 Find {s.st_nlink - 1} Hardlink Sibling(s)" if s.st_nlink > 1 else "🔍 No Other Hardlinks Exist")
        except Exception as e: self.inspector_view.setHtml(f"<h3 style='color:#f85149;'>Failed to read file: {e}</h3>")

    def find_hardlinks(self):
        if not hasattr(self, 'inspected_file'): return
        t = HardlinkFinderThread(self.inspected_file, self.is_dark)
        t.log_signal.connect(self.inspector_view.append)
        self.active_threads.append(t)
        t.finished_signal.connect(lambda t=t: self.active_threads.remove(t) if t in self.active_threads else None)
        t.start()

    def update_dummy_stats(self):
        if self.dummy_tabs.currentIndex() == 0:
            target_dir = self.txt_dummy_dir.text().strip()
            count = self.spin_dummy_count.value()
            size_bytes = self.spin_dummy_size.value() * 1024 * 1024
            proj_total = count * size_bytes
        else:
            target_dir = self.txt_dummy_dir_sm.text().strip()
            proj_total = 0
            
        free_space = 0
        if target_dir and os.path.exists(target_dir):
            try: free_space = shutil.disk_usage(target_dir).free
            except Exception: pass
            
        remaining = free_space - proj_total
        rem_color = "#f85149" if remaining < 0 else ("#3fb950" if self.is_dark else "#116329")
        
        if self.dummy_tabs.currentIndex() == 0:
            msg = f"<b>Disk Free Space:</b> {format_bytes(free_space) if free_space else 'N/A'} | <b>Projected Size:</b> {format_bytes(proj_total)} | <b>Remaining:</b> <span style='color:{rem_color};'>{format_bytes(remaining) if remaining > 0 else 'EXCEEDS CAPACITY'}</span>"
        else:
            msg = f"<b>Disk Free Space:</b> {format_bytes(free_space) if free_space else 'N/A'} | Smart Parsing Mode Active."
            
        self.lbl_dummy_stats.setText(msg)

    def run_dummy_files(self):
        self.dummy_console.clear()
        self.dummy_progress.setValue(0)
        
        if self.dummy_tabs.currentIndex() == 0:
            # Simple Mode translation to syntax
            target_dir = self.txt_dummy_dir.text().strip()
            base = self.txt_dummy_name.text().strip() or "payload"
            size = f"{self.spin_dummy_size.value()}mb"
            count = self.spin_dummy_count.value()
            syntax = f"{base}:{size}x{count}"
        else:
            target_dir = self.txt_dummy_dir_sm.text().strip()
            syntax = self.txt_dummy_cmd.text().strip()

        if not target_dir or not os.path.exists(target_dir): return QMessageBox.warning(self, "Invalid Path", "Please select a valid target directory.")
        
        t = SmartDummyThread(target_dir, syntax)
        t.log_signal.connect(self.dummy_console.append)
        t.progress_signal.connect(self.dummy_progress.setValue)
        self.active_threads.append(t)
        t.finished_signal.connect(lambda t=t: self.active_threads.remove(t) if t in self.active_threads else None)
        t.start()

    def trigger_clone(self):
        src = QFileDialog.getExistingDirectory(self, "Select Source Folder to Backup")
        if not src: return
        dst = QFileDialog.getExistingDirectory(self, "Select Destination Parent")
        if not dst: return
        
        src_anchor = Path(src).anchor
        dst_anchor = Path(dst).anchor
        if src_anchor != dst_anchor:
            msg = "Hardlinking requires the source and destination to be on the EXACT SAME drive/volume.\n\nYou have selected different volumes. The operation will fall back to a standard file copy (duplicating disk space).\n\nDo you want to proceed with a standard copy?"
            if QMessageBox.warning(self, "Different Volumes Detected", msg, QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes: return
        else:
            test_src = Path(dst) / ".vman_hl_test_src.tmp"
            test_lnk = Path(dst) / ".vman_hl_test_lnk.tmp"
            hl_supported = True
            try:
                test_src.touch()
                os.link(test_src, test_lnk)
            except OSError: hl_supported = False
            finally:
                if test_lnk.exists(): test_lnk.unlink()
                if test_src.exists(): test_src.unlink()
                
            if not hl_supported:
                msg = "The target file system (e.g., FAT32, exFAT) does NOT support Hardlinks.\n\nThe operation will fall back to a standard file copy (duplicating disk space).\n\nDo you want to proceed with a standard copy?"
                if QMessageBox.warning(self, "Filesystem Limitation", msg, QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes: return
        
        self.clone_console.clear()
        self.clone_progress.setValue(0)
        args = {
            'versioned': self.chk_version.isChecked(),
            'timestamp': self.chk_timestamp.isChecked(),
            'ignore_hidden': self.chk_hidden.isChecked(),
            'commit': self.txt_commit.toPlainText()
        }
        t = AdvancedOpsThread("CLONE", src, dst, args)
        t.progress.connect(self.clone_progress.setValue)
        t.log_signal.connect(self.clone_console.append)
        self.active_threads.append(t)
        t.finished_signal.connect(lambda t=t: (
            self.clone_progress.setValue(100),
            QMessageBox.information(self, "Complete", f"Version Control Clone Processed!"),
            self.active_threads.remove(t) if t in self.active_threads else None
        ))
        t.start()

    def trigger_compare(self):
        dir1 = QFileDialog.getExistingDirectory(self, "Select Version A (Older Backup)")
        if not dir1: return
        dir2 = QFileDialog.getExistingDirectory(self, "Select Version B (Newer Backup)")
        if not dir2: return
        self.clone_console.clear()
        t = DirCompareThread(Path(dir1), Path(dir2))
        t.log_signal.connect(self.clone_console.append)
        self.active_threads.append(t)
        t.finished_signal.connect(lambda t=t: self.active_threads.remove(t) if t in self.active_threads else None)
        t.start()

    def create_junction(self):
        src = os.path.normpath(self.txt_junc_src.text().strip())
        dst_folder = os.path.normpath(self.txt_junc_dst.text().strip())
        name = self.txt_junc_name.text().strip()
        link_type = self.combo_link_type.currentText()
        
        if not src or not dst_folder or not name: return QMessageBox.warning(self, "Input Error", "Please fill out all paths.")
        if not os.path.exists(src): return QMessageBox.warning(self, "Input Error", "Source does not exist.")
        dst_path = os.path.join(dst_folder, name)
        if os.path.exists(dst_path): return QMessageBox.warning(self, "Input Error", f"The destination link '{name}' already exists.")
        
        self.junc_console.clear()
        try:
            if sys.platform == "win32":
                if "Directory Junction" in link_type: cmd_str = f'mklink /J "{dst_path}" "{src}"'
                elif "Directory Symbolic" in link_type: cmd_str = f'mklink /D "{dst_path}" "{src}"'
                else: cmd_str = f'mklink "{dst_path}" "{src}"'
                out = subprocess.check_output(cmd_str, shell=True, text=True, stderr=subprocess.STDOUT)
                self.junc_console.append(f"<span style='color:#3fb950;'>[SUCCESS] {out.strip()}</span>")
            else:
                if "Hard Link" in link_type: os.link(src, dst_path)
                else: os.symlink(src, dst_path)
                self.junc_console.append(f"<span style='color:#3fb950;'>[SUCCESS] Link created: {dst_path} -> {src}</span>")
        except Exception as e:
            self.junc_console.append(f"<span style='color:#f85149;'>[FAILED] Elevated privileges may be required.<br>{e}</span>")

    def apply_attributes(self):
        target = os.path.normpath(self.txt_attr_tgt.text().strip())
        action = self.combo_attr.currentText()
        if not target or not os.path.exists(target): return QMessageBox.warning(self, "Input Error", "Invalid Target.")
        
        try:
            if sys.platform == "win32":
                if "Lock" in action:
                    subprocess.run(["icacls", target, "/deny", "Everyone:(DE,DC,W,WD,AD,WEA,WA)", "/T", "/C", "/Q"], check=False, creationflags=subprocess.CREATE_NO_WINDOW)
                    subprocess.run(["attrib", "+R", target], check=False, creationflags=subprocess.CREATE_NO_WINDOW)
                    subprocess.run(["attrib", "+R", os.path.join(target, "*"), "/S", "/D"], check=False, creationflags=subprocess.CREATE_NO_WINDOW)
                elif "Unlock" in action:
                    subprocess.run(["icacls", target, "/remove:d", "Everyone", "/T", "/C", "/Q"], check=False, creationflags=subprocess.CREATE_NO_WINDOW)
                    subprocess.run(["attrib", "-R", target], check=False, creationflags=subprocess.CREATE_NO_WINDOW)
                    subprocess.run(["attrib", "-R", os.path.join(target, "*"), "/S", "/D"], check=False, creationflags=subprocess.CREATE_NO_WINDOW)
                elif "Read-Only" in action:
                    subprocess.run(["attrib", "+R", target], check=False, creationflags=subprocess.CREATE_NO_WINDOW)
                    subprocess.run(["attrib", "+R", os.path.join(target, "*"), "/S", "/D"], check=False, creationflags=subprocess.CREATE_NO_WINDOW)
                elif "Normal" in action:
                    subprocess.run(["attrib", "-R", "-H", "-S", target], check=False, creationflags=subprocess.CREATE_NO_WINDOW)
                    subprocess.run(["attrib", "-R", "-H", "-S", os.path.join(target, "*"), "/S", "/D"], check=False, creationflags=subprocess.CREATE_NO_WINDOW)
                elif "Hide" in action:
                    subprocess.run(["attrib", "+H", "+S", target], check=False, creationflags=subprocess.CREATE_NO_WINDOW)
                    subprocess.run(["attrib", "+H", "+S", os.path.join(target, "*"), "/S", "/D"], check=False, creationflags=subprocess.CREATE_NO_WINDOW)
                elif "Unhide" in action:
                    subprocess.run(["attrib", "-H", "-S", target], check=False, creationflags=subprocess.CREATE_NO_WINDOW)
                    subprocess.run(["attrib", "-H", "-S", os.path.join(target, "*"), "/S", "/D"], check=False, creationflags=subprocess.CREATE_NO_WINDOW)
            else:
                if "Lock" in action: 
                    cmd = ["chflags", "-R", "uchg", target] if platform.system() == "Darwin" else ["chattr", "-R", "+i", target]
                    subprocess.run(cmd, check=True)
                elif "Unlock" in action:
                    cmd = ["chflags", "-R", "nouchg", target] if platform.system() == "Darwin" else ["chattr", "-R", "-i", target]
                    subprocess.run(cmd, check=True)
                elif "Read-Only" in action: subprocess.run(["chmod", "-R", "444", target])
                elif "Normal" in action: subprocess.run(["chmod", "-R", "755", target])
                elif "Hide" in action:
                    if platform.system() == "Darwin": subprocess.run(["chflags", "hidden", target])
                    else:
                        p = Path(target)
                        if not p.name.startswith('.'): p.rename(p.parent / f".{p.name}")
                elif "Unhide" in action:
                    if platform.system() == "Darwin": subprocess.run(["chflags", "nohidden", target])
                    else:
                        p = Path(target)
                        if p.name.startswith('.'): p.rename(p.parent / p.name[1:])
            self.attr_console.append(f"<span style='color:#3fb950;'>[SUCCESS] Applied '{action}' to: {target} (Recursive)</span>")
        except Exception as e:
            self.attr_console.append(f"<span style='color:#f85149;'>[FAILED] Elevated privileges may be required.<br>{e}</span>")

    def scan_recent_files(self):
        target_dir = self.txt_rec_dir.text().strip()
        if not target_dir or not os.path.exists(target_dir): return QMessageBox.warning(self, "Input Error", "Select a valid directory to scan.")
        self.attr_console.clear()
        t = AdvancedOpsThread("RECENT", target_dir, None, {})
        t.log_signal.connect(self.attr_console.append)
        self.active_threads.append(t)
        t.finished_signal.connect(lambda t=t: self.active_threads.remove(t) if t in self.active_threads else None)
        t.start()

    def trigger_wipe(self):
        path_str, _ = QFileDialog.getOpenFileName(self, "Select File to Secure Wipe")
        if not path_str: path_str = QFileDialog.getExistingDirectory(self, "Or Select Folder to Secure Wipe")
        if not path_str: return
        if QMessageBox.warning(self, "Secure Wipe", f"CRITICAL WARNING:\nYou are about to overwrite and permanently destroy:\n{path_str}\n\nContinue?", QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes: return
        self.wipe_console.clear()
        self.wipe_progress.setValue(0)
        t = AdvancedOpsThread("WIPE", [Path(path_str)], None, {'passes': self.spin_passes.value()})
        self._run_wipe_thread(t, "Target has been securely overwritten and destroyed.")

    def trigger_wipe_free(self):
        drive = self.combo_drives.currentText()
        if QMessageBox.warning(self, "Wipe Free Space", f"This will create massive dummy files on {drive} until it is completely full, then delete them to shred deleted data.\n\nThis may take a long time and temporarily trigger 'Disk Full' OS warnings. Continue?", QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes: return
        self.wipe_console.clear()
        self.wipe_progress.setValue(0)
        t = AdvancedOpsThread("WIPE_FREE", drive, None, {})
        self._run_wipe_thread(t, f"Free space on {drive} has been securely overwritten.")

    def _run_wipe_thread(self, thread, success_msg):
        thread.progress.connect(self.wipe_progress.setValue)
        thread.log_signal.connect(self.wipe_console.append)
        self.active_threads.append(thread)
        thread.finished_signal.connect(lambda t=thread: (
            QMessageBox.information(self, "Complete", success_msg),
            self.wipe_progress.setValue(100),
            self.active_threads.remove(t) if t in self.active_threads else None
        ))
        thread.start()

    def _spawn_command(self, cmd, target_console, is_network=False):
        thread = OSCommandThread(cmd, is_network, self.is_dark)
        thread.log_signal.connect(lambda html, raw, console=target_console: console.append(html))
        self.active_threads.append(thread)
        thread.finished_signal.connect(lambda t=thread: self.active_threads.remove(t) if t in self.active_threads else None)
        thread.start()
        
        
        