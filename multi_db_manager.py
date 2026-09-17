# multi_db_manager.py
"""
VMan OS Multi-Database Cross-Auditor & Unified Search Explorer
High-performance, debounced multi-database explorer with comprehensive duplicate control.
"""
import os
import re
import sqlite3
import subprocess
import csv
from pathlib import Path
from collections import defaultdict
from PySide6.QtCore import Qt, QThread, Signal, QTimer
from PySide6.QtGui import QIcon, QColor, QBrush, QFont
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QLineEdit, QComboBox, QTableWidget, QTableWidgetItem, QHeaderView,
    QMessageBox, QMenu, QAbstractItemView, QProgressDialog, QGroupBox,
    QFileDialog, QApplication, QSplitter, QListWidget, QListWidgetItem, QWidget,
    QProgressBar  # <-- Added QProgressBar here
)
from themes import THEMES

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

class MultiDBLoaderThread(QThread):
    progress = Signal(int, int, str)
    finished_loading = Signal(list, dict)

    def __init__(self, db_paths, parent=None):
        super().__init__(parent)
        self.db_paths = db_paths
        self.is_cancelled = False

    def cancel(self): self.is_cancelled = True

    def run(self):
        all_records = []
        signature_map = defaultdict(list)
        total_dbs = len(self.db_paths)

        for i, db_path in enumerate(self.db_paths):
            if self.is_cancelled: return
            db_name = Path(db_path).stem
            self.progress.emit(i + 1, total_dbs, f"Indexing ({i+1}/{total_dbs}): {db_name}")

            if not os.path.exists(db_path): continue

            try:
                with sqlite3.connect(db_path, timeout=30) as conn:
                    cur = conn.cursor()
                    cur.execute("PRAGMA table_info(virtual_fs)")
                    cols = [row[1] for row in cur.fetchall()]

                    sha_col = "sha256" if "sha256" in cols else ("hash" if "hash" in cols else "''")
                    cat_col = "category" if "category" in cols else "'Others'"
                    tag_col = "custom_tags" if "custom_tags" in cols else "''"

                    query = f"SELECT id, name, parent_path, is_folder, size, extension, modified, real_path, {sha_col}, {cat_col}, {tag_col} FROM virtual_fs WHERE in_trash=0"
                    cur.execute(query)

                    while True:
                        if self.is_cancelled: return
                        rows = cur.fetchmany(10000)
                        if not rows: break

                        for r in rows:
                            db_id, name, p_path, is_fldr, sz, ext, mod, rp, sha, cat, tags = r
                            size_val = sz or 0
                            sha_val = str(sha).strip() if sha else ""

                            if sha_val and sha_val not in ("", "Not Computed"):
                                sig = f"sha:{sha_val}"
                            else:
                                sig = f"name_size:{name.lower()}_{size_val}"

                            record = {
                                'id': db_id, 'name': name, 'parent_path': p_path,
                                'is_folder': is_fldr, 'size': size_val, 'ext': ext or "",
                                'mod': mod or "", 'real_path': rp or "", 'sha256': sha_val,
                                'category': cat or "Others", 'tags': tags or "",
                                'db_name': db_name, 'db_path': db_path, 'sig': sig
                            }
                            all_records.append(record)
                            signature_map[sig].append(db_name)
            except Exception as e:
                print(f"Error loading {db_path}: {e}")

        processed_sigs = {}
        for sig, db_list in signature_map.items():
            distinct_dbs = sorted(list(set(db_list)))
            processed_sigs[sig] = {
                'count': len(distinct_dbs),
                'total_occurrences': len(db_list),
                'databases': distinct_dbs
            }

        self.finished_loading.emit(all_records, processed_sigs)


class MultiDBExplorerDialog(QDialog):
    def __init__(self, views_dir, main_db_path, parent=None):
        super().__init__(parent)
        self.views_dir = Path(views_dir)
        self.main_db_path = str(main_db_path)
        self.main_app = parent
        self.setWindowTitle("🌐 Multi-Database Cross-Auditor & Unified Search Explorer")
        self.resize(1350, 780)
        self.setWindowFlags(self.windowFlags() | Qt.WindowMaximizeButtonHint | Qt.WindowMinimizeButtonHint)

        if parent and hasattr(parent, 'theme_combo'):
            self.setStyleSheet(THEMES.get(parent.theme_combo.currentText(), THEMES["Dark"]))
        else:
            self.setStyleSheet(THEMES.get("Dark", ""))

        self.all_records = []
        self.processed_sigs = {}
        self.filtered_records = []

        # Debounce timer for instant responsiveness when searching large DBs
        self.search_debounce = QTimer(self)
        self.search_debounce.setSingleShot(True)
        self.search_debounce.setInterval(300)
        self.search_debounce.timeout.connect(self.apply_filters)

        self._build_ui()
        self._populate_db_list()

    def _build_ui(self):
        main_layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Horizontal)

        left_panel = QWidget(); left_layout = QVBoxLayout(left_panel); left_layout.setContentsMargins(0, 0, 5, 0)

        db_grp = QGroupBox("🗄️ Select Databases")
        db_lay = QVBoxLayout(db_grp)
        self.db_list_widget = QListWidget()
        self.db_list_widget.setSelectionMode(QAbstractItemView.NoSelection)

        btn_db_bar = QHBoxLayout()
        btn_sel_all = QPushButton("☑ All"); btn_sel_all.clicked.connect(lambda: self._toggle_all_dbs(True))
        btn_sel_none = QPushButton("☐ None"); btn_sel_none.clicked.connect(lambda: self._toggle_all_dbs(False))
        btn_add_ext = QPushButton("➕ External..."); btn_add_ext.clicked.connect(self._add_external_db)
        btn_db_bar.addWidget(btn_sel_all); btn_db_bar.addWidget(btn_sel_none); btn_db_bar.addWidget(btn_add_ext)

        self.btn_load_dbs = QPushButton("⚡ Load Selected Databases")
        self.btn_load_dbs.setStyleSheet("background-color: #1f6feb; color: white; font-weight: bold; padding: 8px;")
        self.btn_load_dbs.clicked.connect(self.load_selected_databases)

        db_lay.addWidget(self.db_list_widget); db_lay.addLayout(btn_db_bar); db_lay.addWidget(self.btn_load_dbs)
        left_layout.addWidget(db_grp)

        # Filters
        filter_grp = QGroupBox("🔍 Display Filters")
        f_lay = QVBoxLayout(filter_grp)
        self.combo_repeat_filter = QComboBox()
        self.combo_repeat_filter.addItems(["All Files & Folders", "🔥 Repeated Across Databases (>1 DB)", "⭐ Unique to Single Database (Only 1 DB)", "Files Only (Exclude Folders)"])
        self.combo_repeat_filter.currentIndexChanged.connect(self.apply_filters)
        self.combo_filter_db = QComboBox(); self.combo_filter_db.addItem("All Loaded Databases"); self.combo_filter_db.currentIndexChanged.connect(self.apply_filters)
        self.txt_ext_filter = QLineEdit(); self.txt_ext_filter.setPlaceholderText("Filter Extension (e.g. .jpg)"); self.txt_ext_filter.textChanged.connect(lambda: self.search_debounce.start())

        f_lay.addWidget(QLabel("<b>Repetition Filter:</b>")); f_lay.addWidget(self.combo_repeat_filter)
        f_lay.addWidget(QLabel("<b>Origin Database:</b>")); f_lay.addWidget(self.combo_filter_db)
        f_lay.addWidget(QLabel("<b>Extension Filter:</b>")); f_lay.addWidget(self.txt_ext_filter)
        left_layout.addWidget(filter_grp)

        # Actions
        act_grp = QGroupBox("🛠️ Bulk Action Center")
        act_lay = QVBoxLayout(act_grp)
        
        btn_sel_duplicates = QPushButton("🎯 Check All Duplicate Copies (Keep 1st Safe)")
        btn_sel_duplicates.clicked.connect(self.select_duplicate_copies)
        
        tag_lay = QHBoxLayout()
        self.txt_bulk_tag = QLineEdit(); self.txt_bulk_tag.setPlaceholderText("Tag...")
        btn_bulk_tag = QPushButton("🏷️ Apply Tag"); btn_bulk_tag.clicked.connect(self.bulk_apply_tags)
        tag_lay.addWidget(self.txt_bulk_tag); tag_lay.addWidget(btn_bulk_tag)
        
        col_lay = QHBoxLayout()
        self.combo_bulk_color = QComboBox(); self.combo_bulk_color.addItems(["None", "Red", "Orange", "Gold", "Green", "Cyan", "Blue", "Purple", "Pink"])
        btn_bulk_color = QPushButton("🎨 Apply Color"); btn_bulk_color.clicked.connect(self.bulk_apply_color)
        col_lay.addWidget(self.combo_bulk_color); col_lay.addWidget(btn_bulk_color)

        btn_delete_checked = QPushButton("🗑️ Delete Checked from Database")
        btn_delete_checked.setStyleSheet("background-color: #8b0000; color: white; font-weight: bold;")
        btn_delete_checked.clicked.connect(self.delete_checked_from_db)

        act_lay.addWidget(btn_sel_duplicates); act_lay.addLayout(tag_lay); act_lay.addLayout(col_lay); act_lay.addWidget(btn_delete_checked)
        left_layout.addWidget(act_grp)
        splitter.addWidget(left_panel)

        # ---- RIGHT PANEL ----
        right_panel = QWidget(); right_layout = QVBoxLayout(right_panel); right_layout.setContentsMargins(5, 0, 0, 0)
        
        search_row = QHBoxLayout()
        self.txt_search = QLineEdit(); self.txt_search.setPlaceholderText("🔍 Fast Multi-Database Filter (Name, Path, Ext, Tag)...")
        self.txt_search.textChanged.connect(lambda: self.search_debounce.start())
        btn_search_engine = QPushButton("🔍 Open in VMan Search Engine")
        btn_search_engine.setStyleSheet("background-color: #238636; color: white; font-weight: bold; padding: 6px 12px;")
        btn_search_engine.clicked.connect(self.open_in_search_engine)
        search_row.addWidget(self.txt_search, stretch=1); search_row.addWidget(btn_search_engine)
        right_layout.addLayout(search_row)

        self.lbl_stats = QLabel("Select databases and click 'Load Selected Databases'.")
        self.lbl_stats.setStyleSheet("color: #58a6ff; font-weight: bold; padding: 3px 0;")
        right_layout.addWidget(self.lbl_stats)
        
        # --- NEW PROGRESS BAR ---
        self.action_prog = QProgressBar(); self.action_prog.setVisible(False); self.action_prog.setFixedHeight(12)
        right_layout.addWidget(self.action_prog)

        self.table = QTableWidget(0, 9)
        self.table.setHorizontalHeaderLabels(["Select", "Status / Repeat Count", "File / Folder Name", "From Database", "Present in DBs", "Size", "Virtual Location", "SHA-256", "DB Path"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive); self.table.setColumnWidth(0, 50); self.table.setColumnWidth(1, 150)
        self.table.setColumnWidth(2, 230); self.table.setColumnWidth(3, 130); self.table.setColumnWidth(4, 180); self.table.setColumnWidth(5, 90)
        self.table.setColumnWidth(6, 220); self.table.setColumnWidth(7, 160); self.table.setColumnHidden(8, True); self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows); self.table.setSortingEnabled(True); self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu); self.table.doubleClicked.connect(self.on_table_double_clicked)
        right_layout.addWidget(self.table)

        bottom_bar = QHBoxLayout()
        btn_sel_all_tbl = QPushButton("Toggle Checks"); btn_sel_all_tbl.clicked.connect(self.toggle_table_checks)
        btn_export_csv = QPushButton("📥 Export Audit Report (CSV)"); btn_export_csv.clicked.connect(self.export_csv_report)
        bottom_bar.addWidget(btn_sel_all_tbl); bottom_bar.addWidget(btn_export_csv); bottom_bar.addStretch()
        right_layout.addLayout(bottom_bar)

        splitter.addWidget(right_panel); splitter.setSizes([350, 1000]); main_layout.addWidget(splitter)

    def apply_filters(self):
        search_txt = self.txt_search.text().strip().lower()
        ext_txt = self.txt_ext_filter.text().strip().lower()
        repeat_mode = self.combo_repeat_filter.currentText()
        selected_db = self.combo_filter_db.currentText()

        filtered, total_repeated, total_unique = [], 0, 0

        for r in self.all_records:
            sig_info = self.processed_sigs.get(r['sig'], {'count': 1, 'databases': [r['db_name']]})
            db_count = sig_info['count']
            is_repeated = db_count > 1

            if is_repeated: total_repeated += 1
            else: total_unique += 1

            if selected_db != "All Loaded Databases" and r['db_name'] != selected_db: continue
            if "Repeated" in repeat_mode and not is_repeated: continue
            if "Unique" in repeat_mode and is_repeated: continue
            if "Files Only" in repeat_mode and r['is_folder']: continue
            if ext_txt and ext_txt not in r['ext'].lower(): continue

            if search_txt:
                match = (search_txt in r['name'].lower() or search_txt in r['parent_path'].lower() or search_txt in r['ext'].lower() or search_txt in r['tags'].lower() or search_txt in r['db_name'].lower())
                if not match: continue

            filtered.append((r, sig_info))

        self.filtered_records = filtered
        self._populate_table()

        total_loaded = len(self.all_records)
        self.lbl_stats.setText(f"Loaded {total_loaded:,} items | 🔥 Repeated: {total_repeated:,} | ⭐ Unique: {total_unique:,} | Matches: {len(filtered):,}")

    def bulk_apply_tags(self):
        checked_rows = [r for r in range(self.table.rowCount()) if self.table.item(r, 0).checkState() == Qt.Checked]
        tag_text = self.txt_bulk_tag.text().strip()
        if not checked_rows or not tag_text: return QMessageBox.warning(self, "Error", "Check items and enter a tag.")
        
        self.action_prog.setVisible(True); self.action_prog.setMaximum(len(checked_rows)); self.action_prog.setValue(0)
        
        db_updates = defaultdict(list)
        for r in checked_rows:
            rec, _ = self.filtered_records[r]
            db_updates[rec['db_path']].append((rec['id'], rec['tags']))

        processed = 0
        for db_path, items in db_updates.items():
            try:
                with sqlite3.connect(db_path, timeout=30) as conn:
                    cur = conn.cursor()
                    for db_id, old_tags in items:
                        new_val = f"{old_tags}, {tag_text}".strip(", ") if old_tags else tag_text
                        cur.execute("UPDATE virtual_fs SET custom_tags=? WHERE id=?", (new_val, db_id))
                        processed += 1
                        if processed % 50 == 0: self.action_prog.setValue(processed); QApplication.processEvents()
                    conn.commit()
            except Exception as e: print(e)
            
        self.action_prog.setVisible(False)
        QMessageBox.information(self, "Success", f"Tags applied to {processed} records.")
        self.load_selected_databases()

    def bulk_apply_color(self):
        checked_rows = [r for r in range(self.table.rowCount()) if self.table.item(r, 0).checkState() == Qt.Checked]
        color = self.combo_bulk_color.currentText()
        if not checked_rows: return QMessageBox.warning(self, "Error", "Check items to apply color.")
        
        self.action_prog.setVisible(True); self.action_prog.setMaximum(len(checked_rows)); self.action_prog.setValue(0)
        
        db_updates = defaultdict(list)
        for r in checked_rows:
            rec, _ = self.filtered_records[r]
            db_updates[rec['db_path']].append(rec['id'])

        processed = 0
        for db_path, ids in db_updates.items():
            try:
                with sqlite3.connect(db_path, timeout=30) as conn:
                    cur = conn.cursor()
                    col_val = "" if color == "None" else color
                    for i in range(0, len(ids), 900):
                        chunk = ids[i:i+900]
                        cur.execute(f"UPDATE virtual_fs SET color_tag=? WHERE id IN ({','.join(['?']*len(chunk))})", [col_val] + chunk)
                        processed += len(chunk)
                        self.action_prog.setValue(processed); QApplication.processEvents()
                    conn.commit()
            except Exception as e: print(e)
            
        self.action_prog.setVisible(False)
        QMessageBox.information(self, "Success", f"Colors applied to {processed} records.")

    def _populate_db_list(self):
        self.db_list_widget.clear()
        if os.path.exists(self.main_db_path):
            item = QListWidgetItem("Main System DB (vman_vfs.db)")
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked)
            item.setData(Qt.UserRole, self.main_db_path)
            self.db_list_widget.addItem(item)

        if self.views_dir.exists():
            for db_file in sorted(list(self.views_dir.glob("*.db"))):
                if str(db_file.resolve()) == str(Path(self.main_db_path).resolve()): continue
                item = QListWidgetItem(db_file.name)
                item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
                item.setCheckState(Qt.Checked)
                item.setData(Qt.UserRole, str(db_file))
                self.db_list_widget.addItem(item)

    def _toggle_all_dbs(self, checked):
        state = Qt.Checked if checked else Qt.Unchecked
        for i in range(self.db_list_widget.count()):
            self.db_list_widget.item(i).setCheckState(state)

    def _add_external_db(self):
        path, _ = QFileDialog.getOpenFileName(self, "Add External Database", "", "Database (*.db)")
        if path:
            item = QListWidgetItem(f"[Ext] {Path(path).name}")
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked)
            item.setData(Qt.UserRole, path)
            self.db_list_widget.addItem(item)

    def get_selected_db_paths(self):
        paths = []
        for i in range(self.db_list_widget.count()):
            it = self.db_list_widget.item(i)
            if it.checkState() == Qt.Checked:
                paths.append(it.data(Qt.UserRole))
        return paths

    def load_selected_databases(self):
        paths = self.get_selected_db_paths()
        if not paths:
            return QMessageBox.warning(self, "No Databases Selected", "Please tick at least one database.")

        self.btn_load_dbs.setEnabled(False)
        prog = QProgressDialog("Loading and indexing databases...", "Cancel", 0, len(paths), self)
        prog.setWindowModality(Qt.WindowModal)
        prog.show()

        self.loader = MultiDBLoaderThread(paths, parent=self)
        self.loader.progress.connect(lambda cur, tot, msg: (prog.setMaximum(tot), prog.setValue(cur), prog.setLabelText(msg)))
        prog.canceled.connect(self.loader.cancel)

        def on_loaded(records, sigs):
            prog.close()
            self.btn_load_dbs.setEnabled(True)
            self.all_records = records
            self.processed_sigs = sigs

            loaded_dbs = sorted(list(set(r['db_name'] for r in records)))
            self.combo_filter_db.blockSignals(True)
            self.combo_filter_db.clear()
            self.combo_filter_db.addItem("All Loaded Databases")
            self.combo_filter_db.addItems(loaded_dbs)
            self.combo_filter_db.blockSignals(False)

            self.apply_filters()

        self.loader.finished_loading.connect(on_loaded)
        self.loader.start()

    

    def _populate_table(self):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)

        # Cap UI rendering to 2500 rows to keep navigation completely freeze-free
        limit = min(2500, len(self.filtered_records))
        display_records = self.filtered_records[:limit]

        for row_idx, (r, sig_info) in enumerate(display_records):
            self.table.insertRow(row_idx)
            db_count = sig_info['count']
            is_repeat = db_count > 1
            dbs_str = ", ".join(sig_info['databases'])

            chk = QTableWidgetItem()
            chk.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
            chk.setCheckState(Qt.Unchecked)
            self.table.setItem(row_idx, 0, chk)

            status_text = f"🔥 Repeated in {db_count} DBs" if is_repeat else "⭐ Unique (1 DB)"
            item_status = QTableWidgetItem(status_text)
            if is_repeat:
                item_status.setForeground(QBrush(QColor("#f85149")))
                item_status.setFont(QFont("Segoe UI", 9, QFont.Bold))
            else:
                item_status.setForeground(QBrush(QColor("#3fb950")))

            item_name = QTableWidgetItem(r['name'])
            item_orig_db = QTableWidgetItem(r['db_name'])
            item_orig_db.setForeground(QBrush(QColor("#58a6ff")))
            item_all_dbs = QTableWidgetItem(dbs_str)
            item_size = QTableWidgetItem(human_size(r['size']) if not r['is_folder'] else "--")
            item_size.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            item_path = QTableWidgetItem(f"{r['parent_path']}{r['name']}" + ("/" if r['is_folder'] else ""))
            item_sha = QTableWidgetItem(r['sha256'] if r['sha256'] else "Not Computed")
            item_db_path = QTableWidgetItem(r['db_path'])

            self.table.setItem(row_idx, 1, item_status)
            self.table.setItem(row_idx, 2, item_name)
            self.table.setItem(row_idx, 3, item_orig_db)
            self.table.setItem(row_idx, 4, item_all_dbs)
            self.table.setItem(row_idx, 5, item_size)
            self.table.setItem(row_idx, 6, item_path)
            self.table.setItem(row_idx, 7, item_sha)
            self.table.setItem(row_idx, 8, item_db_path)

        self.table.setSortingEnabled(True)
        if len(self.filtered_records) > 2500:
            self.lbl_stats.setText(self.lbl_stats.text() + f"  [UI Capped to {limit:,} rows. Export CSV for all {len(self.filtered_records):,}]")

    def toggle_table_checks(self):
        state = Qt.Checked if not getattr(self, '_all_checked', False) else Qt.Unchecked
        self._all_checked = not getattr(self, '_all_checked', False)
        for r in range(self.table.rowCount()):
            it = self.table.item(r, 0)
            if it: it.setCheckState(state)

    def select_duplicate_copies(self):
        """Keep the 1st instance of any repeated file unchecked, check all extra duplicate copies."""
        seen_sigs = set()
        checked_count = 0
        self.table.setSortingEnabled(False)
        for r in range(self.table.rowCount()):
            record, sig_info = self.filtered_records[r]
            sig = record['sig']
            if sig_info['count'] > 1:
                if sig in seen_sigs:
                    self.table.item(r, 0).setCheckState(Qt.Checked)
                    checked_count += 1
                else:
                    self.table.item(r, 0).setCheckState(Qt.Unchecked)
                    seen_sigs.add(sig)
            else:
                self.table.item(r, 0).setCheckState(Qt.Unchecked)
        self.table.setSortingEnabled(True)
        QMessageBox.information(self, "Duplicates Flagged", f"Checked {checked_count} duplicate file copies.\nThe first copy of each file has been left safe and unchecked.")

    def delete_checked_from_db(self):
        checked_rows = [r for r in range(self.table.rowCount()) if self.table.item(r, 0).checkState() == Qt.Checked]
        if not checked_rows:
            return QMessageBox.warning(self, "No Selection", "Please check the items you wish to remove from their database.")

        if QMessageBox.question(self, "Confirm Delete", f"Permanently remove {len(checked_rows)} checked records from their databases?", QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes:
            return

        db_deletions = defaultdict(list)
        for r in checked_rows:
            rec, _ = self.filtered_records[r]
            db_deletions[rec['db_path']].append(rec['id'])

        deleted_total = 0
        for db_path, ids in db_deletions.items():
            try:
                with sqlite3.connect(db_path, timeout=30) as conn:
                    cur = conn.cursor()
                    for i in range(0, len(ids), 900):
                        chunk = ids[i:i+900]
                        cur.execute(f"DELETE FROM virtual_fs WHERE id IN ({','.join(['?']*len(chunk))})", chunk)
                    conn.commit()
                    deleted_total += len(ids)
            except Exception as e:
                QMessageBox.critical(self, "Delete Error", f"Error on {Path(db_path).name}: {e}")

        QMessageBox.information(self, "Deleted", f"Successfully removed {deleted_total} records from their databases.")
        self.load_selected_databases()

    def on_table_double_clicked(self, item):
        row = item.row()
        record, _ = self.filtered_records[row]
        rp = record.get('real_path')
        if rp and os.path.exists(rp):
            try:
                if os.name == 'nt': os.startfile(rp)
                else: subprocess.Popen(['xdg-open', rp])
            except Exception as e:
                QMessageBox.warning(self, "Open Error", str(e))
        else:
            QMessageBox.information(self, "Virtual Entry", f"File: {record['name']}\nOrigin DB: {record['db_name']}\nVirtual Path: {record['parent_path']}\nPhysical file not found.")

    def show_context_menu(self, pos):
        item = self.table.itemAt(pos)
        if not item: return
        row = item.row()
        record, sig_info = self.filtered_records[row]

        menu = QMenu(self)
        act_open = menu.addAction("🚀 Open Native File")
        act_loc = menu.addAction("📂 Open Location in OS")
        menu.addSeparator()
        act_copy_name = menu.addAction("📋 Copy File Name")
        act_copy_path = menu.addAction("📋 Copy Virtual Path")
        act_copy_dbs = menu.addAction("📋 Copy Databases containing this file")
        menu.addSeparator()
        act_switch_db = menu.addAction(f"🔀 Switch Active DB in VMan to '{record['db_name']}'")

        action = menu.exec(self.table.viewport().mapToGlobal(pos))
        rp = record.get('real_path')

        if action == act_open:
            if rp and os.path.exists(rp):
                if os.name == 'nt': os.startfile(rp)
                else: subprocess.Popen(['xdg-open', rp])
            else: QMessageBox.warning(self, "Not Available", "Physical file not found.")
        elif action == act_loc:
            if rp and os.path.exists(rp):
                if os.name == 'nt': subprocess.Popen(['explorer', '/select,', os.path.normpath(rp)])
                else: subprocess.Popen(['xdg-open', os.path.dirname(rp)])
            else: QMessageBox.warning(self, "Not Available", "Local location not available.")
        elif action == act_copy_name: QApplication.clipboard().setText(record['name'])
        elif action == act_copy_path: QApplication.clipboard().setText(f"{record['parent_path']}{record['name']}")
        elif action == act_copy_dbs: QApplication.clipboard().setText(", ".join(sig_info['databases']))
        elif action == act_switch_db:
            if self.main_app and hasattr(self.main_app, 'active_db_path'):
                self.main_app.active_db_path = record['db_path']
                from database import vmanDB
                self.main_app.db.close()
                self.main_app.db = vmanDB(Path(record['db_path']))
                self.main_app.clear_cache()
                self.main_app.refresh_tree()
                self.main_app.nav_to_path("/")
                QMessageBox.information(self, "Switched", f"Active database switched to: {record['db_name']}")

    def open_in_search_engine(self):
        paths = self.get_selected_db_paths()
        if not paths:
            return QMessageBox.warning(self, "No Databases", "Please select at least one database.")

        try:
            from search import AdvancedSearchWindow
            search_win = AdvancedSearchWindow(paths[0], self.main_app if self.main_app else self)
            search_win.active_db_list = paths
            search_win.show()
            search_win.raise_()
            search_win.activateWindow()
            if self.txt_search.text().strip():
                search_win.txt_name.setText(self.txt_search.text().strip())
                search_win.trigger_search()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to launch Search Engine:\n{e}")

    def export_csv_report(self):
        if not self.filtered_records:
            return QMessageBox.information(self, "No Data", "No records to export.")

        path, _ = QFileDialog.getSaveFileName(self, "Export Cross-Database Report", "VMan_MultiDB_Report.csv", "CSV Files (*.csv)")
        if not path: return

        try:
            with open(path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "File Name", "Status", "Database Count", "Origin Database",
                    "All Databases Containing File", "Size (Bytes)", "Human Size",
                    "Virtual Path", "SHA-256", "Physical Path", "Tags"
                ])
                for r, sig_info in self.filtered_records:
                    writer.writerow([
                        r['name'],
                        f"Repeated ({sig_info['count']} DBs)" if sig_info['count'] > 1 else "Unique (1 DB)",
                        sig_info['count'], r['db_name'],
                        "; ".join(sig_info['databases']),
                        r['size'], human_size(r['size']),
                        f"{r['parent_path']}{r['name']}",
                        r['sha256'], r['real_path'], r['tags']
                    ])
            QMessageBox.information(self, "Exported", f"Successfully exported {len(self.filtered_records):,} records.")
        except Exception as e:
            QMessageBox.critical(self, "Export Error", str(e))
