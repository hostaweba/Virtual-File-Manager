# Nexus OS — Virtual File Manager

Nexus OS is an advanced, SQLite-powered virtual file manager. It features Deep Smart Views for dynamic file categorization without altering physical paths. Packed with a zero-lag UI, native media viewer, bulk renamer, space analyzer, custom tagging, and the ability to seamlessly materialize virtual structures directly to your real OS.

---

## ✨ Key Features

### 🧠 Dynamic Smart Views

Instantly restructure your entire file system without moving a single physical file. Access dynamically generated hierarchies:

* **Statistical Clustering:** `Year ➔ Month ➔ Original Folder`
* **Deep Hierarchies:** `Year ➔ Category ➔ Month ➔ Extension` or `Category ➔ Year ➔ Month`
* **Custom Tags:** Tag files with custom labels (e.g., `#urgent`, `#project_x`) and browse them through the `🏷️ By Custom Tags` view.

### ⚡ Zero-Lag Asynchronous Engine

Built for massive file processing. The background `DataLoaderThread` and chunked UI rendering system ensure the interface never freezes, even when displaying or filtering thousands of files. View caching guarantees instantaneous back/forward navigation.

### 🎞️ Nexus Media Engine

Press `Ctrl+O` on any file to open the dedicated Nexus Viewer.

* **Images:** High-performance rendering with panning and Ctrl+Scroll zoom.
* **Text/Code:** Built-in syntax preview.
* **Media:** Full audio and video playback with volume and timeline controls.
* **Context-Aware:** Automatically loads the surrounding directory into a playlist for seamless Left/Right arrow navigation.

### 📤 Materialize to OS

Select any Virtual Folder or dynamically generated Smart View and export it. Nexus OS will physically recreate your custom virtual hierarchy on your actual Windows/Mac/Linux drive and copy the files into it.

### 🛠️ Advanced Professional Tools

* **Space & Junk Analyzer:** Scan your database for massive files (>500MB), duplicates, and temporary junk files (`.tmp`, `.cache`, `.bak`).
* **Bulk Serial Renamer:** Select multiple items and press `F2` to cleanly rename them sequentially (e.g., `Vacation (1).jpg`, `Vacation (2).jpg`).
* **Bulk Deleter:** Purge thousands of files instantly by extension or naming pattern.
* **Database Compiler:** Right-click any folder or view to extract it into its own isolated, portable SQLite `.db` file.
* **SHA-256 Hashing:** Instantly compute cryptographic checksums for data integrity verification.

---

## 🚀 Installation & Setup

**Prerequisites:**
Ensure you have Python 3.8+ installed on your system.

**1. Install Required Dependencies:**
Open your terminal or command prompt and run:

```bash
pip install PySide6 pandas matplotlib

```

**2. Run the Application:**

```bash
python nexus_os.py

```

---

## ⌨️ Keyboard Shortcuts

### System Navigation

* **Enter:** Open selected folder or execute the real OS file natively.
* **Ctrl + O:** Open selected file in the Nexus Media Engine.
* **Ctrl + F:** Focus the Global Search bar.
* **Backspace:** Go Back.
* **Shift + Backspace:** Go Forward.
* **Alt + Up:** Go up one directory level.

### File Operations

* **Ctrl + N:** Create a new Virtual File.
* **Ctrl + Shift + N:** Create a new Virtual Folder.
* **F2:** Rename item (Select multiple items for Bulk Serial Renaming).
* **Delete:** Send to Virtual Trash (Permanently deletes if inside the Trash view).
* **Ctrl + C / X / V:** Copy, Cut, and Paste virtual files.
* **Shift + F10:** Open Context Menu.

### Nexus Media Engine

* **Spacebar:** Play / Pause audio and video.
* **Left / Right Arrows:** Navigate to the Previous / Next file in the directory.
* **Up / Down Arrows:** Increase / Decrease media volume.
* **Ctrl + Scroll Wheel:** Zoom in/out on images.
* **Escape:** Close the viewer safely.

---

## 📊 Analytics & Inspector

Toggle the Inspector panel from the toolbar to access:

* **Properties:** Edit names, assign colors, apply custom tags, and view the real physical path of your virtual files.
* **Text Analytics:** View real-time calculations of folder sizes, total item counts, and system allocation percentages.
* **Data Visualization:** Leverage Matplotlib to view line charts (modifications over time), bar charts (extension distributions), and storage pie charts.

