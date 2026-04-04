

# Nexus OS Virtual File Manager

**Timeline Diary, Deep Smart Views, Zero-Lag Async Engine, Custom Tags, Multi-Themes, and OS Hooks.**

Nexus OS Data Engine is a powerful, SQLite-backed virtual file manager built with Python and PySide6. It allows you to ingest, tag, analyze, and reorganize your massive file collections in a secure "virtual sandbox" without altering the physical files on your hard drive until you choose to "Materialize" them.

---

## ✨ Key Features

### 🗂️ Virtual File System (VFS)
* **Non-Destructive Organization:** Drag and drop files from your OS into Nexus to create a virtual tracking map. Move, rename, and tag them virtually without breaking your actual hard drive paths.
* **Materialize to OS:** Once your files are perfectly organized in the virtual environment, compile and "Materialize" the structure back to physical folders on your OS or export it as a compiled ZIP archive.
* **Multi-Database Support:** Compile specific virtual views into isolated standalone SQLite databases (`.db` files) and hot-swap between them.

### 🧠 Deep Smart Views & Protocols
Navigate your data through dynamic, database-driven URI protocols rather than static folders:
* `tags://` - Browse your entire system dynamically organized by your custom tags.
* `y_m_f://` - Browse by Year ➔ Month ➔ Folder.
* `category://` - Browse by media type (Images, Videos, Audio, Documents, Code).
* `trash://` and `fav://` - Built-in quick access to deleted and favorite items.

### ⚡ Zero-Lag Async Engine
* All heavy processing (Hash calculations, bulk imports, deep OS scans, zip compressions, and file materialization) runs on dedicated background `QThreads`. Your UI will never freeze, even when processing gigabytes of data.

### 📊 Built-in Visual Analytics
* **Executive Dashboard:** Get instant metrics on virtual folder counts, storage usage, and tag density.
* **Matplotlib Integrations:** View dynamic Pie Charts, Bar Charts, and Scatter plots of your storage usage, extension distributions, and temporal file activity.

### 🛠️ Advanced Tooling
* **Timeline Diary:** A calendar-based view to see exactly which files were modified on any given day, color-coded by category.
* **Space & Integrity Analyzer:** Scan for junk files, duplicate files (via SHA-256 exact hash matching), version conflicts, and 0-byte anomalies.
* **Bulk Operations Engine:** Run complex queries (e.g., "Find all files older than 30 days over 500MB") and apply bulk actions like tagging, moving, or trashing.
* **Native Media Engine:** Built-in viewer for Images, Code/Text, and Multimedia (Video/Audio) using `QtMultimedia`.

---

## 🚀 Installation & Setup

### Prerequisites
* **Python 3.8+**
* Ensure you have a working audio/video backend if you intend to use the QtMultimedia features (e.g., LAV Filters on Windows, GStreamer on Linux).

### Dependencies
Install the required Python packages:

```bash
pip install PySide6 pandas matplotlib
```

*(Note: `pandas` and `matplotlib` are technically optional for the core file manager, but are required to unlock the Visual Analytics charts).*

### Running the Application

Save the script as `nexus_os.py` and run:

```bash
python nexus_os.py
```

Upon first launch, the application will automatically generate a `nexus_data` directory containing the `nexus_vfs.db` database and a `compiled_views` folder in your current working directory.

---

## 🖥️ UI Navigation

* **Left Dock (Data Engine & Views):** Navigate the Main System DB, Favorites, Trash, and Deep Smart Views. You can also swap to isolated databases here.
* **Center Area (View Stack):** Toggle between Grid View and Table View. Drag and drop items here from your OS. 
* **Right Dock (Inspector):** Instant previews of selected files, property editing, custom tagging, and analytics charts.
* **Bottom Dock (Live System Console):** Real-time readout of background thread operations, SQL queries, and system hooks.

---

## ⌨️ Keyboard Shortcuts

| Shortcut | Action |
| :--- | :--- |
| `Enter` | Open selected item (Virtual Folder or native OS File) |
| `Ctrl + F` | Focus Global Search |
| `Ctrl + N` | Create a New Virtual File |
| `Ctrl + Shift + N` | Create a New Virtual Folder |
| `Ctrl + O` | Open selected files in the internal Nexus Media Viewer |
| `Ctrl + H` | Toggle visibility of hidden files |
| `F2` | Rename selected item(s). *(Supports Bulk Renaming)* |
| `Delete` | Move to Virtual Trash |
| `Shift + Delete` | Permanently delete from Virtual Database |
| `Alt + Up` | Go Up one directory |
| `Backspace` | Go Back |
| `Shift + Backspace`| Go Forward |
| `F11` | Toggle Fullscreen |

---

## 📂 Architecture Note

* **Virtual Mapping:** The database stores `real_path` which points to the physical file on your OS. Deleting a file in Nexus OS **does not** delete the file on your physical hard drive unless explicitly coded to do so in the Analyzer tools.
* **Safety Overrides:** If a physical file is moved outside of Nexus OS, the link will break. Use the "Mark Safe" or "Map View to OS Folder" tools to repair broken links.
