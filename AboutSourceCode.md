
# 📖 About the Source Code: Nexus OS Data Engine

This document provides a technical overview of the Nexus OS Virtual File Manager's internal architecture. The application is built using **Python 3** and the **PySide6 (Qt for Python)** framework, utilizing **SQLite** as its high-speed virtual file system backend.

## 🏗️ High-Level Architecture

Nexus OS follows a heavily asynchronous, database-driven architecture to prevent UI freezing (hanging) when dealing with massive datasets. The architecture is divided into four main layers:

1. **The Database Layer (`NexusDB`):** Handles all persistent state, metadata, and structural logic using SQLite.
2. **The Async Worker Layer (`QThread` classes):** Offloads heavy I/O, SQL querying, and image decoding from the main event loop.
3. **The UI Rendering Engine (`QTimer` Chunking):** A specialized loop that takes data from the worker threads and injects it into the GUI in small batches to maintain 60 FPS responsiveness.
4. **The Presentation Layer (PySide6):** The visual interface, custom widgets, and media playback integrations.

---

## 🗄️ Database Schema & Indexing

The core of the application relies on a single SQLite table named `virtual_fs`. Instead of moving real files, Nexus OS updates string values in this table.

### Table: `virtual_fs`

* `id` (INTEGER PRIMARY KEY): Unique identifier.
* `parent_path` (TEXT): The virtual directory path (e.g., `/Projects/Design/`).
* `name` (TEXT): The display name of the file/folder.
* `is_folder` (INTEGER): Boolean flag (1 for folder, 0 for file).
* `real_path` (TEXT): The absolute OS path to the actual file (empty for virtual folders).
* `size` (INTEGER): File size in bytes.
* `extension` (TEXT): Lowercase file extension (e.g., `.png`).
* `modified` (TEXT): Timestamp of last modification.
* `color_tag` (TEXT): UI color grouping.
* `secondary_name` (TEXT): User-defined alias or description.
* `is_hidden`, `in_trash`, `is_favorite` (INTEGER): State flags.
* `category` (TEXT): Auto-generated media type (e.g., 'Images', 'Code').
* `year`, `month` (TEXT): Extracted from the `modified` timestamp for Smart Views.
* `custom_tags` (TEXT): Comma-separated user-defined tags.

### B-Tree Indexing

To ensure instantaneous loading of Smart Views, the schema enforces compound indexes on frequently queried columns:

* `idx_vfs_parent ON virtual_fs(parent_path)`
* `idx_vfs_ycme ON virtual_fs(year, category, month, extension)`
* `idx_vfs_tags ON virtual_fs(custom_tags)`

---

## 🧠 The Smart View Engine

Smart Views allow dynamic restructuring of files without altering the database. This is achieved through a custom URI protocol parser in the `DataLoaderThread`.

When the app navigates to a path like `y_m_f://2025/12/`, the engine:

1. Detects the `y_m_f://` prefix.
2. Splits the path by `/` to determine the depth.
3. Dynamically constructs a `SELECT` statement based on the depth (e.g., if depth is 2, it queries for unique folders modified in that year and month).
4. Returns virtual `(db_id, parent_path, name, color_tag, secondary_name, is_hidden, count, size)` tuples to the UI, making the database rows *appear* as physical folders.

---

## ⚡ The Anti-Freeze Engine (Chunked Rendering)

Standard file managers crash or hang when attempting to draw 10,000 icons simultaneously. Nexus OS solves this using a **Producer-Consumer** pattern:

1. **Producer (`DataLoaderThread`):** Runs the SQL query in the background. When finished, it emits a `data_ready` signal containing a standard Python list of the results.
2. **Consumer (`_render_chunk` via `QTimer`):** * The main UI thread pauses its layout engine (`setUpdatesEnabled(False)`).
* A `QTimer` fires every 1 millisecond, popping a `CHUNK_SIZE` (default 150) batch of items from the list and instantiating `QListWidgetItem` objects.
* Once the queue is empty, the layout engine is re-enabled, and the UI visually pops into existence instantly.



---

## 🧵 Thread Management & Safety

The app implements strict thread safety. GUI elements are **never** manipulated directly from background threads.

### Key Threads:

* **`DataLoaderThread`:** Resolves Smart Views and fetches directory contents.
* **`SpaceScannerThread`:** Recursively analyzes sizes and hashes for the Space Analyzer dialog.
* **`CompilerThread`:** Extracts a subset of the `virtual_fs` table into a new, isolated `.db` file using `executemany` batches.
* **`MaterializeThread`:** Copies real OS files into physical folders based on the virtual hierarchy on the screen.
* **`ImageLoader`:** Reads heavy image files via `QImage` (which is thread-safe, unlike `QPixmap`) and resizes them in the background before passing them to the main thread for the Preview pane.

### Thread Cancellation

Every background thread checks an `self.is_cancelled` flag within its loop. When a user clicks "Cancel" on a `QProgressDialog`, the main thread sets this flag, allowing the background thread to exit gracefully without corrupting data or leaving open database connections.

---

## 🎞️ Nexus Media Engine (`NexusViewer`)

The custom popup viewer integrates `PySide6.QtMultimedia` for audio/video and a custom `QGraphicsView` for images.

* **Context-Awareness:** When `Ctrl+O` is pressed, the main UI passes a *playlist* (the entire contents of the current Grid/Table view) to the `NexusViewer`. This allows the viewer to natively handle "Next" and "Previous" operations without needing to query the database again.
* **Memory Management:** The media player calls `.stop()` internally whenever the viewer is closed or the file changes, preventing audio overlap or memory leaks.

---

## 🎨 UI / UX Design Patterns

* **QStackedWidget:** Used heavily in both the main view (to switch between `SandboxTableView` and `SandboxListWidget`) and the Preview panel (to switch between Image, Text, and Media modes).
* **Event Overrides (`keyPressEvent`, `dropEvent`):** Custom widget classes (`SandboxTableView`, `SandboxListWidget`) override native Qt events to capture Drag & Drop payloads and route specific keystrokes (like `Enter` to open folders) to the main window via custom `Signals`.
* **Stylesheet (CSS):** The entire application relies on a global, brutalist dark-mode CSS string (`FUTURISTIC_THEME`) applied at the `QMainWindow` level, ensuring uniformity across all popups and tables.
