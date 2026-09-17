#usage_logger.py

import json
from pathlib import Path
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem, QAbstractItemView, QPushButton, QMessageBox

class UsageLogDialog(QDialog):
    def __init__(self, log_file_path, parent=None):
        super().__init__(parent)
        self.setWindowTitle("App Usage & Session Logs")
        self.resize(600, 400)
        
        # Inherit the exact Light/Dark theme from the main app
        if parent and hasattr(parent, 'styleSheet'): 
            self.setStyleSheet(parent.styleSheet())
        
        self.log_file = Path(log_file_path)
        
        layout = QVBoxLayout(self)
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Session Start", "Session End", "Duration"])
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.table)
        
        btn_lay = QHBoxLayout()
        btn_clear = QPushButton("🗑️ Clear All Logs")
        btn_clear.setStyleSheet("background-color: #f85149; color: white; font-weight: bold; padding: 6px 12px;")
        btn_clear.clicked.connect(self.clear_logs)
        btn_lay.addStretch()
        btn_lay.addWidget(btn_clear)
        layout.addLayout(btn_lay)
        
        self.load_logs()

    def load_logs(self):
        self.table.setRowCount(0)
        if not self.log_file.exists(): return
        try:
            with open(self.log_file, "r", encoding="utf-8") as f: 
                logs = json.load(f)
            # Reverse so the newest sessions appear at the top
            for r, entry in enumerate(reversed(logs)):
                self.table.insertRow(r)
                self.table.setItem(r, 0, QTableWidgetItem(entry.get("start", "")))
                self.table.setItem(r, 1, QTableWidgetItem(entry.get("end", "")))
                self.table.setItem(r, 2, QTableWidgetItem(entry.get("duration", "")))
        except Exception: pass

    def clear_logs(self):
        if QMessageBox.question(self, "Clear Logs", "Delete all session history?", QMessageBox.Yes|QMessageBox.No) == QMessageBox.Yes:
            if self.log_file.exists(): 
                self.log_file.unlink()
            self.load_logs()