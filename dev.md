# Nexus OS Data Engine: Developer Documentation

This document outlines the internal architecture, database schema, and asynchronous threading model of the Nexus OS Data Engine. It is intended for developers looking to extend, debug, or maintain the application.

---

## 1. High-Level Architecture

Nexus OS follows a robust multi-threaded architecture separating the UI (PySide6), the Data Layer (SQLite3), and the Execution Engine (QThreads). 

* **Presentation Layer (UI):** Built with PySide6 (`QMainWindow`, `QGraphicsView`, `QTableWidget`). It remains strictly synchronous and purely handles user inputs and data rendering.
* **Execution Layer (Workers):** Heavy operations (hashing, file copying, OS scanning, database compiling) are delegated to isolated `QThread` classes.
* **Data Layer (SQLite):** A local SQLite database running in WAL (Write-Ahead Logging) mode handles all state, metadata, and virtual pathing.

---

## 2. The Virtual File System (VFS) Concept

Nexus OS does not physically move files upon import. It creates a **Virtual Sandbox**. 

* **Virtual Paths:** Managed by the `parent_path` and `name` columns. Example: `/Projects/2026/`
* **Physical Anchors:** The `real_path` column holds the absolute OS path (e.g., `C:/Users/Data/file.txt`).
* **Materialization:** When a user "exports" or "materializes", the app reads the virtual structure and uses `shutil.copy2` to recreate that exact structure physically using the `real_path` as the source.

---

## 3. Database Schema (`nexus_vfs.db`)

The entire state is maintained in a single table: `virtual_fs`.

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | INTEGER | Primary Key. |
| `parent_path` | TEXT | The virtual directory containing the item (always ends with `/`). |
| `name` | TEXT | The virtual filename or folder name. |
| `is_folder` | INTEGER | `1` if directory, `0` if file. |
| `real_path` | TEXT | The absolute physical path on the host OS. Null/Empty if purely virtual. |
| `size` | INTEGER | File size in bytes. |
| `extension` | TEXT | File extension (e.g., `.png`). |
| `modified` | TEXT | Timestamp (`YYYY-MM-DD HH:MM:SS`). |
| `color_tag` | TEXT | UI color label (Red, Blue, Green, Gold). |
| `secondary_name` | TEXT | Optional user-defined description. |
| `is_hidden` | INTEGER | `1` if hidden from standard views. |
| `in_trash` | INTEGER | `1` if virtually deleted. |
| `is_favorite` | INTEGER | `1` if starred. |
| `sha256` | TEXT | Pre-computed hash for duplicate/integrity checking. |
| `category` | TEXT | Auto-assigned based on extension (Images, Videos, Code, etc.). |
| `year` / `month` | TEXT | Extracted from `modified` for rapid temporal grouping. |
| `custom_tags` | TEXT | Comma-separated user tags. |
| `hash_verified` | INTEGER | `1` if explicitly marked safe by the user in the Space Analyzer. |

### Key Indexes
To maintain zero-lag navigation across millions of rows, the following indexes are critical:
* `idx_vfs_parent` on `(parent_path)`
* `idx_vfs_ycme` on `(year, category, month, extension)`
* `idx_vfs_tags` on `(custom_tags)`

---

## 4. Smart View Resolution (URI Protocols)

Instead of standard paths, Nexus OS utilizes custom URI protocols intercepted by the `DataLoaderThread` to generate dynamic SQL queries on the fly.

**Registered Protocols:**
* `tags://[tag_name]/`
* `y_m_f://[year]/[month]/[folder_name]`
* `trash://`
* `fav://`

**Resolution Logic (`DataLoaderThread.run`):**
When the UI requests a path (e.g., `tags://urgent/`), the thread splits the URI, dynamically constructs a `WHERE custom_tags LIKE '%urgent%'` SQL query, and emits the resulting tuple lists back to the UI.

---

## 5. Concurrency & Event Loop (QThreads)

To prevent UI freezing, **no database writes or heavy reads occur on the main thread.** ### Standard Worker Lifecycle:
1.  **Instantiate:** Main UI creates a subclass of `QThread` (e.g., `SpaceScannerThread`).
2.  **Connect Signals:** UI methods are mapped to the thread's signals (e.g., `scanner.progress.connect(update_ui)`).
3.  **Execution:** Thread runs `run()` method.
4.  **Yielding:** The thread uses `.emit()` to push data chunks back to the main event loop.
5.  **Cleanup:** The thread emits `finished()`, and the UI calls `.deleteLater()` via the `_cleanup_worker` orchestrator.

### The Render Queue
For massive directories, the UI does not render all objects instantly. The `DataLoaderThread` fetches the data, but the main UI consumes it using a `QTimer` (`self.render_timer`) triggering `_render_chunk()`. It populates the `QTableView` in batches of 150 (`CHUNK_SIZE`) to maintain 60FPS UI performance during load.

---

## 6. Extending the Application

### Adding a New File Category
Modify the global `FILE_CATEGORIES` dictionary at the top of the file. The `get_category_for_ext()` function handles the rest automatically during database import.

```python
FILE_CATEGORIES = {
    "Images": ['.png', '.jpg', ...],
    "3D Models": ['.obj', '.fbx', '.stl', '.blend'] # Add your new category here
}
```

### Adding a New Smart Protocol
1.  Register the protocol and its corresponding database columns in the `SMART_PROTOCOLS` dictionary.
    ```python
    SMART_PROTOCOLS = {
        "ext://": ["extension"] # Example: browse purely by extension
    }
    ```
2.  Update the `InteractiveBreadcrumb` class to assign it a custom icon/label.
3.  The generic logic inside `DataLoaderThread` will automatically map the depth of the URI slashes to the specified columns.
