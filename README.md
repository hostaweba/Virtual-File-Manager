
# 🌌 Nexus OS Virtual File Manager

**Master Professional Edition** A state-of-the-art, SQLite-backed virtual file manager and data analytics engine built with Python and PySide6. Nexus OS allows you to organize, tag, analyze, and manipulate massive file structures virtually without altering your underlying physical hard drive until you choose to "Materialize" them.

---

## ✨ Core Features

* **🗃️ Virtual File System (VFS):** Create folders, import files, and build complex directory structures entirely in a virtual sandbox.

* **💡 Deep Smart Views:** Dynamic, auto-generated views based on custom protocols (`tags://`, `y_m_f://` for Year/Month/Folder, etc.).

* **📊 Universal Tag Engine & Analytics:** A dedicated workspace for deep data investigation. Features a multi-level hierarchy browser (Parent > Level 1 > Level 2) and paginating Matplotlib charts (Bar, Pie, Scatter) to analyze tag and folder distribution.

* **📅 Timeline Diary:** A built-in calendar view that tracks file modifications and system actions over time.

* **🧹 Advanced Space Analyzer:** Instantly scan your virtual database for duplicate files, massive files (>500MB), and system junk/cache files.

* **🎞️ Nexus Media Engine:** Built-in viewer for text, images, and native audio/video playback utilizing FFmpeg and PySide6 Multimedia.

* **⚡ Zero-Lag Async Engine:** Heavy operations (importing, compiling, materializing, hashing) are handled on background threads with real-time UI progress bars.

* **📤 OS Hooks & Materialization:** Export your virtual folder structures back to your physical OS, compile views into isolated standalone SQLite databases, or export entire virtual structures directly to a ZIP archive.

---

## 🛠️ Prerequisites & Installation

Ensure you have **Python 3.8+** installed. The application relies on PySide6 for its UI and multimedia engine, alongside a few data processing libraries.

1. **Clone or Download the Repository:**
   ```bash
   git clone [https://github.com/hostaweba/Virtual-File-Manager.git](https://github.com/hostaweba/Virtual-File-Manager.git)
   cd Virtual-File-Manager
   ```

2.  **Install Required Dependencies:**

    ```bash
    pip install PySide6 pandas matplotlib
    ```

    *(Note: FFmpeg is bundled with PySide6 in newer versions to handle audio/video playback).*

-----

## 🚀 How to Run

Simply execute the main Python script from your terminal:

```bash
python main.py
```

*(If you named your script something else, replace `main.py` with your filename).*

Upon first launch, the app will automatically generate a `nexus_data` folder in the root directory to safely store your virtual file system database (`nexus_vfs.db`) and compiled views.

-----

## ⌨️ Keyboard Shortcuts & Navigation

Nexus OS is built for power users. Here are the global hotkeys:

| Shortcut | Action |
| :--- | :--- |
| `Backspace` | Navigate Back |
| `Shift + Backspace` | Navigate Forward |
| `Alt + Up` | Go up one directory level |
| `Ctrl + F` | Focus Global Search |
| `Ctrl + Shift + N` | Create New Virtual Folder |
| `Ctrl + N` | Create New Virtual File |
| `F2` | Rename (Works sequentially on multi-selections) |
| `Delete` | Send to Virtual Trash (or Permanently Delete) |
| `Ctrl + C / X / V` | Virtual Copy, Cut, and Paste |
| `Ctrl + O` | Open selected files in the internal **Nexus Media Engine** |
| `Enter` | Open selected virtual folder, or open real file in native OS app |

**Media Engine Controls (When inside Nexus Viewer):**

  * `Spacebar`: Play / Pause Media
  * `Left / Right Arrows`: Previous / Next Item in playlist
  * `Up / Down Arrows`: Volume Up / Down
  * `Escape`: Close Viewer

-----

## 📂 Project Architecture

  * **Database Engine:** SQLite3 with WAL mode enabled for high-concurrency read/writes.
  * **UI Framework:** PySide6 (Qt for Python).
  * **Threading:** Operations are offloaded using custom `QThread` workers (`DataLoaderThread`, `SpaceScannerThread`, `MaterializeThread`, etc.) to keep the UI perfectly responsive.
  * **Data Analytics:** Powered by `pandas` for data structuring and `matplotlib.backends.backend_qtagg` for native Qt chart embedding.

-----

## 🎨 Theming

Nexus OS supports dynamic theming. You can switch between **Dark Nexus** (Default) and **Light Clean** seamlessly using the dropdown in the main navigation toolbar.

-----

## 📜 License

This project is licensed under the MIT License.
