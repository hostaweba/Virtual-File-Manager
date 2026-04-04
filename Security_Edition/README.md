# Nexus OS Virtual File Manager

**Nexus OS Data Engine** is an advanced, encrypted, zero-lag Virtual File System (VFS). It allows you to logically organize, tag, analyze, and manage your files without altering their physical locations on your hard drive.

Featuring military-grade SQLCipher encryption, interactive Matplotlib data dashboards, dynamic smart views, and a dedicated timeline diary, Nexus OS acts as a powerful meta-layer over your existing OS file system.

-----

## ✨ Key Features

  * **🔒 Zero-Leakage Security Engine:** Database encrypted via SQLCipher3 (AES-256) with PBKDF2-HMAC-SHA256 key derivation (600,000 iterations). Your metadata, tags, and virtual paths remain completely locked without the Master Password.
  * **⚡ Zero-Lag Async Engine:** Fully multi-threaded architecture (via `QThread`). Loading directories, calculating SHA-256 hashes, and exporting files happen seamlessly in the background.
  * **🧠 Deep Smart Views:** Navigate dynamic virtual protocols like `tags://` or `y_m_f://` (Year ➔ Month ➔ Folder) that automatically group your files based on their metadata.
  * **📖 Timeline Diary:** A built-in calendar interface that maps your file modifications chronologically. Click any day to see a generated HTML diary of your system activity.
  * **🧹 Space & Integrity Analyzer:** Detect 0-byte paradoxes, exact SHA-256 duplicates, version conflicts, and massive junk files using the built-in scanner.
  * **📈 Executive Dashboard & Analytics:** Generates real-time horizontal bar, pie, and scatter charts of your file distribution, storage usage, and custom tag frequencies using `matplotlib` and `pandas`.
  * **⚙️ Advanced Bulk Operations:** Mass-rename, move, tag, color-code, or materialize thousands of virtual files at once.
  * **🎞️ Nexus Media Viewer:** Native support for viewing images, reading code/text files, and playing Audio/Video (via `QtMultimedia`).
  * **📤 Materialize to OS:** Export your virtual folder structures back into reality. Nexus will safely recreate the folders and copy the physical files to any target drive, or compress them directly into a `.zip`.

-----

## 🛠️ Prerequisites & Installation

Nexus OS is built on Python and PySide6. To run the application, you need to install the required dependencies.

### 1\. Install Python Dependencies

```bash
pip install PySide6 pandas matplotlib
```

### 2\. Install SQLCipher3 (Crucial for Security)

Nexus relies on `sqlcipher3` for database encryption.

  * **Windows:** You may need pre-compiled binaries or to build SQLCipher from source. Alternatively, use a drop-in replacement if testing locally without encryption (though not recommended for the intended Nexus experience).
  * **Linux/macOS:**
    ```bash
    sudo apt-get install sqlcipher libsqlcipher-dev
    pip install sqlcipher3
    ```

### 3\. Run the Engine

```bash
python nexus_os.py
```

-----

## 🚀 Getting Started

1.  **First Boot (Initialization):** On the first run, Nexus will ask you to create a **Master Password**. This derives the raw hexadecimal AES key that creates your encrypted `nexus_vfs.db`. *Do not lose this password; your virtual structure cannot be recovered without it.*
2.  **Importing Files:** Right-click inside the main grid/table or use the native toolbar to **Import Real Files** or **Import Real Folder**. Nexus logs their locations and creates a virtual sandbox representation.
3.  **Navigating:** Use the interactive breadcrumb bar at the top or the left-hand dock (Data Engine & Views) to jump between Root, Favorites, Trash, and Smart Views.
4.  **Tagging & Colors:** Select files, right-click, and add custom tags or color labels. You can then instantly browse these in the `tags://` Smart View.

-----

## ⌨️ Keyboard Shortcuts

| Shortcut | Action |
| :--- | :--- |
| `Ctrl + F` | Focus Global Search / Filter |
| `Ctrl + T` | Focus Tag Search (in Tag Library) |
| `Ctrl + N` | Create New Virtual File |
| `Ctrl + Shift + N` | Create New Virtual Folder |
| `F2` | Rename File/Folder (Works for Bulk Renaming\!) |
| `Delete` | Send selected to Virtual Trash |
| `Shift + Delete` | Permanently Delete selected items |
| `Ctrl + C / X / V` | Copy, Cut, Paste virtual items |
| `Ctrl + H` | Toggle Hidden Files visibility |
| `Ctrl + O` | Open selected item in Nexus Media Viewer |
| `Backspace` | Navigate Back |
| `Shift + Backspace` | Navigate Forward |
| `Alt + Up` | Navigate Up one directory |
| `F11` | Toggle Fullscreen |

-----

## 🧩 Core Modules Breakdown

### 1\. Sandbox Views (Main Window)

Toggle between Grid View and detailed List View. Supports rich internal drag-and-drop. You can drop files directly from your native OS (Windows Explorer / macOS Finder) into the Nexus grid to instantly import them.

### 2\. Tag Library (`act_csv_lib`)

A comprehensive tag management interface. View horizontal hierarchies of your file structures, edit tag names, map virtual columns globally to physical OS paths, and import/export CSV manifests of your entire database.

### 3\. The Compiler (`db://`)

Have a massive project? You can "Compile" any specific virtual folder (or Smart View) into a standalone, isolated SQLite database file. Switch contexts instantly via the left sidebar without muddying your main system DB.

### 4\. Nexus Viewer

Press `Ctrl+O` on any file.

  * **Code/Text:** Renders in a clean Consolas text window.
  * **Images:** Opens in the `AdvancedImageViewer` with Ctrl+Scroll zooming.
  * **Media:** Plays natively if `PySide6.QtMultimedia` is active.

-----

## 📂 File System Mappings

  * **Virtual Path:** The path as it exists *inside* Nexus OS (e.g., `/Projects/TopSecret/file.txt`).
  * **Real OS Path:** The actual physical location on your SSD/HDD (e.g., `C:\Users\Name\Documents\file.txt`).
  * Nexus strictly maps the Virtual Path to the Real OS Path. Deleting an item in Nexus **does not** delete the physical file from your hard drive—it only removes the virtual link.

-----

## 🎨 Theming System

Switch themes instantly from the top-right dropdown:

  * **Dark Nexus:** A high-contrast, GitHub-inspired dark mode (Default).
  * **Light Clean:** A crisp, high-visibility light mode.