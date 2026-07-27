**VMan** (Virtual File Manager) is a database-driven, sandbox-style file management system.

Instead of relying on your operating system’s rigid folder structure, VMan creates a **Virtual File System (VFS)** using a high-speed SQLite database. It allows you to ingest, tag, reorganize, and analyze thousands of files virtually—without actually moving or altering the physical files on your hard drive until you explicitly choose to do so.

Here is your high-level, comprehensive guide to the VMan ecosystem.

---

## 🧠 The Core Philosophy: Virtual vs. Physical

To master VMan, you must understand how it tracks data:

* **Virtual Path:** Where the file "lives" inside VMan (e.g., `/Projects/Active/video.mp4`). This is just a database record.
* **Physical Path (`real_path`):** Where the file actually lives on your hard drive (e.g., `C:/Users/Downloads/video.mp4`).
* **The Sandbox Rule:** Moving a file, renaming it, or deleting it inside VMan alters the database, not your hard drive. The only time VMan alters your physical drive is if you explicitly right-click and choose "Delete PHYSICAL OS Items", or if you use the "Materialize" export tools.

---

## 🖥️ The Main Interface Architecture

The main application window (`vmanVirtualManager`) is divided into several dynamic docks and panels.

### 1. The Top Navigation Toolbar

* **Breadcrumb Navigation:** A clickable, interactive path bar allowing you to jump instantly to higher directories.
* **View Toggles:** Switch instantly between the **📄 List View** (detailed table) and **🖼 Grid View** (large thumbnail icons).
* **⚡ Fast Mode:** Disables deep folder size calculations and thumbnail generation for instant loading when navigating massive databases.
* **Global Search:** A search bar (Ctrl+F) that instantly queries the database across all virtual folders for names, secondary names, and custom tags.

### 2. Left Dock: Data Engine & Views

This tree acts as your primary compass.

* **💽 Main System DB:** Your standard virtual folder hierarchy.
* **⭐ Favorites & 🗑 Trash:** Quick access to pinned or deleted virtual items.
* **💡 Dynamic Smart Views:** Instead of navigating folders, clicking these automatically groups your files by metadata. For example, `y_m_f://` automatically sorts your entire database into Year ➔ Month ➔ Folder hierarchies regardless of where the files actually live.
* **📦 Switch Database:** VMan supports multiple isolated databases. You can compile a view into a separate `.db` file and switch to it here for focused, siloed workspaces.

### 3. Right Dock: The Inspector

A multi-tabbed panel that provides instant context on whatever file you select in the center view.

* **Preview:** Automatically renders text files, displays images, or acts as a mini Audio Engine for `.mp3`/`.wav` files.
* **Properties:** Allows you to rapidly edit a file's Virtual Name, Secondary Name (description), Custom Tags, Color Label, or compute its SHA-256 cryptographic hash.
* **Analytics & Charts:** Instantly generates localized pie charts and bar graphs for the specific folder you are currently viewing.

### 4. Bottom Dock: Live System Console

A read-only log tracking every action you take (renames, deletions, imports, tag updates) with timestamps, giving you a transparent audit trail of your session.

---

## 🧰 Key Features & Sub-Engines

Beyond basic file browsing, VMan houses several advanced engines.

### 1. The Media Engine (vmanViewer)

Pressing **Ctrl+O** on media files opens a custom, borderless viewing engine.

* **Standard Mode:** Plays videos, renders images with rotation/flip controls, and reads text/code files.
* **🖼 Webpage Mode (Vertical Strip):** Takes an entire folder of images and stacks them vertically into an infinitely scrolling feed, exactly like browsing a webpage or comic reader.
* **Slideshow:** Features a native progress bar and adjustable speed intervals (1s to 10s) for hands-free viewing.

### 2. The Heavy Modules

VMan includes four dedicated popup engines for mass database manipulation:

* **Space & Integrity Analyzer:** Scans for junk, 0-byte dead files, version conflicts, and exact cryptographic duplicates across the virtual system.
* **Bulk Operations Engine:** A query builder to isolate thousands of files by rule (e.g., size, age, tag) and mass-move, delete, or color-code them.
* **Timeline Diary:** Turns your file modifications into a daily calendar heatmap and chronological HTML activity reports.
* **Universal Tag Library:** A macOS Finder-style "Miller Column" browser to navigate tags as if they were physical folders, complete with high-level executive dashboards.

### 3. Materialization & Exporting

Because VMan is a sandbox, you eventually need to get your sorted files out of it.

* **Materialize to OS:** Select any number of virtual folders or files, right-click, and "Materialize". VMan will read the virtual structure and literally build those folders on your Windows/Mac hard drive, copying the physical files into their new, perfectly organized physical locations.
* **Compile to ZIP:** Instantly grabs the physical files associated with your virtual selection and packs them into a compressed ZIP archive.
* **Rich CSV Manifest:** Exports your virtual hierarchy, including all physical paths, tags, hashes, and sizes, into a spreadsheet for external auditing.

---

## ⚠️ Overall System Cautions

To maintain the health of your VMan database, keep these rules in mind:

1. **The "Disconnected" State:** If you move or rename a file on your actual Windows/Mac hard drive using the normal OS file explorer, VMan will not know about it. The file in VMan will show a physical path of "Disconnected" and will fail to open. You must use the **🔗 Map THIS Folder to Physical OS** tool to relink broken structures.
2. **Matplotlib Dependency:** All visual charts across the main window Inspector, Timeline Diary, and Tag Library require the Python `matplotlib` library. If this is not installed in the host environment, the analytics tabs will fallback to standard text.
3. **Database Size Limits:** While SQLite is incredibly robust, the `vman_vfs.db` file can grow rapidly if you import hundreds of thousands of files. Use the **Compile View to Isolated DB** tool to break massive projects into smaller, faster database files.