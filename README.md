

# 🌌 Nexus OS Virtual File Manager

Nexus OS is a high-performance, SQLite-backed virtual file management system. It decouples your file organization from your physical hard drive, allowing you to build, tag, map, and explore massive directory structures virtually without altering your host OS—until you want to. 

Featuring an asynchronous rendering engine, Auto-Expanding Miller Columns for tag exploration, and a built-in Media Engine, Nexus OS acts as a completely isolated sandbox for your data.

## ✨ Key Features

### 🗂️ Virtual Sandbox & OS Bridge
* **Virtual File System (VFS):** Create folders, files, and hierarchies in a local SQLite database (`nexus_vfs.db`) without touching your host OS.
* **Physical OS Mapping:** Right-click any virtual folder to securely map it to a physical path on your hard drive. 
* **Materialize to OS:** Select any virtual file/folder structure and "Materialize" it to physically copy the files and recreate the exact folder tree on your Windows, Mac, or Linux machine.
* **Import Real Files/Folders:** Drag and drop or use the import dialog to ingest physical files into the virtual sandbox.

### 📚 Auto-Expanding Tag Library (Miller Columns)
* **Infinite Depth Navigation:** Navigate deeply nested folders through a dynamic, horizontally expanding Miller Column interface.
* **Universal Tag Engine:** Add custom tags to files and folders.
* **Smart Reverse-Filtering:** Click any tag in the master list to instantly reconstruct the exact directory paths that lead to that tag across all columns.
* **CSV Tree Import/Export:** Import complex directory structures and tags via CSV. Optionally command Nexus to instantly generate the physical folders on your real hard drive.

### 🧠 Deep Smart Views & Analytics
* **Dynamic Protocols:** Navigate your VFS using smart URIs like `tags://` (sort by custom tags) or `y_m_f://` (sort by Year ➔ Month ➔ Folder).
* **Timeline Diary:** An interactive calendar view that tracks exactly when files were added, modified, or logged in the system.
* **Advanced Space Analyzer:** Asynchronously scans the database to flag Junk Files (`.tmp`, `.bak`, `.cache`), Huge Files (>500MB), and Exact Duplicates.
* **Rich Dashboards:** Matplotlib-powered charts showing storage ratios, file distribution by extension, and modification timelines.

### 🎞️ Integrated Nexus Media Engine
* **Native Previews:** Instantly preview Images, Text, Code, Audio, and Video files directly within the Inspector dock.
* **Theater Mode (`Ctrl+O`):** A dedicated pop-out media player with playlist support, zoom controls, and media scrubbing for uninterrupted consumption.

### 🚀 Zero-Lag Async Engine
* Built with `QThread` and an asynchronous chunk-rendering system. Nexus OS can handle thousands of virtual files in a single directory without freezing the UI.

---

## 🛠️ Installation & Requirements

Nexus OS is built on **Python 3.8+** and uses the **PySide6** framework for its GUI. 

### Core Dependencies
```bash
pip install PySide6
```

### Optional (Highly Recommended) Dependencies
For the interactive Analytics Dashboard (Charts) and Advanced Data Export:
```bash
pip install pandas matplotlib
```

### Running the App
```bash
python nexus_os.py
```
*(The app will automatically generate a `nexus_data` folder in its current directory to store the main `nexus_vfs.db` and any compiled view databases.)*

---

## ⌨️ Keyboard Shortcuts

| Shortcut | Action |
| :--- | :--- |
| `Backspace` | Navigate Back in History |
| `Shift + Backspace` | Navigate Forward in History |
| `Alt + Up` | Go Up One Directory |
| `Ctrl + F` | Focus Global Search |
| `Ctrl + N` | Create New Virtual File |
| `Ctrl + Shift + N` | Create New Virtual Folder |
| `F2` | Rename (Select multiple files to Bulk Rename sequentially!) |
| `Delete` | Send to Virtual Trash / Permanently Delete |
| `Ctrl + C / X / V` | Copy, Cut, Paste (Virtual Sandbox) |
| `Ctrl + O` | Open selected files in the Nexus Media Viewer |
| `Enter` | Open selected folder or launch file in Host OS |

### Media Viewer Controls
* `Spacebar`: Play / Pause Media
* `Left / Right Arrow`: Previous / Next Item in Playlist
* `Up / Down Arrow`: Volume Control
* `Escape`: Close Viewer

---

## ⚙️ Architecture Highlights

* **WAL Mode SQLite:** The underlying database utilizes `PRAGMA journal_mode=WAL` for high-concurrency read/write access without locking up the UI.
* **RAM Caching:** Navigating previously visited directories is instantaneous due to dictionary-based RAM caching of the database tree.
* **Modular Multi-threading:** Database loading, space analyzing, hash calculation (SHA-256), and file materialization are all strictly offloaded to isolated QThreads.
* **QStackedWidgets & QSplitters:** The UI is designed dynamically, tearing down and rebuilding QListWidgets on the fly to support the infinite-depth Tag Library.

---

## 📋 License & Disclaimer
*Nexus OS Virtual File Manager* is a customized data engine. Always ensure you have backups of your physical files before utilizing the "Bulk Operations" or "Materialize" features.
