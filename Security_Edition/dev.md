# Nexus OS - Architecture & Developer Guide

This document outlines the high-level architecture, design patterns, and data flow of the **Nexus OS Virtual File Manager** to assist developers in understanding, maintaining, and extending the codebase.

---

## 1. High-Level Architecture

Nexus OS is a **thick-client desktop application** built with a monolithic architecture. It heavily utilizes the **Model-View-Delegate** pattern provided by Qt (PySide6) for its UI, backed by a highly concurrent, thread-safe encrypted SQLite database (SQLCipher3).

The application acts as a **Virtual File System (VFS) abstraction layer**. It does not store physical files within its database; instead, it stores metadata, custom tags, and virtual folder hierarchies that map (`real_path`) to physical files on the host operating system.

### Core Technology Stack
* **GUI Framework:** PySide6 (Qt for Python)
* **Database:** SQLCipher3 (SQLite with AES-256 encryption)
* **Concurrency:** `PySide6.QtCore.QThread` and Signal/Slot architecture
* **Analytics:** Pandas & Matplotlib (QtAgg backend)
* **Media:** PySide6.QtMultimedia

---

## 2. Core Components

### A. Security Engine (`SecurityEngine`)
Acts as the gatekeeper for the application. 
* **Mechanism:** Uses `hashlib.pbkdf2_hmac` (600,000 iterations) with a locally stored 32-byte salt (`.salt` file) to derive a raw 256-bit hex key from the user's Master Password.
* **Zero-Leakage:** Passes the raw hex key directly to SQLCipher via `PRAGMA key="x'...'"`. 
* **Singleton:** The raw key is held in RAM within the `SecurityEngine` singleton to allow background threads instant DB connections without recalculating the PBKDF2 hash, preventing CPU bottlenecks.

### B. Database Layer (`NexusDB`)
Wraps the SQLCipher connection and manages the schema and raw SQL queries.
* **Journaling:** Uses `PRAGMA journal_mode=WAL;` to allow concurrent reads (UI rendering) and writes (background hashing/importing).
* **Schema:** The entire VFS is flattened into a single table: `virtual_fs`.

### C. The Async Engine (Background Threads)
To ensure the GUI never freezes, all heavy I/O and DB operations are offloaded to `QThread` subclasses.
* **Pattern:** Threads emit PySide6 `Signal`s (`progress`, `finished`, `error`, `data_ready`) which are caught by the main UI thread to safely update widgets.
* **Key Threads:**
    * `DataLoaderThread`: Queries the DB and formats data for the VFS views.
    * `SpaceScannerThread` / `BulkHashCalculator`: Traverses physical files and computes SHA-256 hashes.
    * `ImportFilesThread`: Walks host OS directories and inserts metadata rows into the DB.
    * `MaterializeThread` / `ExportZipThread`: Translates virtual paths back into physical OS copies or ZIP archives.

### D. UI & Presentation Layer
* **`NexusVirtualManager`:** The `QMainWindow`. Manages the central stack (Grid/List), dock widgets (Inspector, Tree, Log), and global state (`current_prefix`).
* **`NexusTableModel`:** A subclass of `QAbstractTableModel` that acts as the bridge between the SQLite data (`table_rows_buffer`) and the `SandboxTableView`/`SandboxListView`. It handles sorting, icon rendering, and data chunking (`CHUNK_SIZE = 150`) for infinite scrolling performance.

---

## 3. Database Schema (`virtual_fs`)

The entire virtual file system is structured via adjacency list logic in a single SQLite table.

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | INTEGER | Primary Key. |
| `parent_path` | TEXT | The virtual directory path (e.g., `/Projects/Beta/`). |
| `name` | TEXT | Virtual name of the file or folder (e.g., `draft.docx`). |
| `is_folder` | INTEGER | `1` for virtual directories, `0` for files. |
| `real_path` | TEXT | The absolute path on the host OS (e.g., `C:\Users\Doc.docx`). |
| `size` | INTEGER | File size in bytes. |
| `extension` | TEXT | Lowercase extension (e.g., `.docx`). |
| `modified` | TEXT | Timestamp (`YYYY-MM-DD HH:MM:SS`). |
| `color_tag` | TEXT | UI highlight color (`Red`, `Blue`, etc.). |
| `secondary_name`| TEXT | User-defined description or alias. |
| `is_hidden` | INTEGER | Visibility toggle (`0` or `1`). |
| `in_trash` | INTEGER | Soft-delete flag (`0` or `1`). |
| `is_favorite` | INTEGER | Bookmark flag (`0` or `1`). |
| `sha256` | TEXT | Computed hash for deduplication/integrity. |
| `custom_tags` | TEXT | Comma-separated tags (e.g., `urgent, review`). |
| `hash_verified` | INTEGER | Safety flag set by the Space Analyzer (`0` or `1`). |

---

## 4. Virtual Routing & Smart Protocols

Nexus OS uses URI-style prefixes to dynamically route data to the views. The `DataLoaderThread` intercepts these prefixes and dynamically generates standard file/folder models without them actually existing in the DB hierarchy.

* **Standard Routing:** `/Path/To/Folder/` -> Standard `LIKE` query on `parent_path`.
* **Smart Protocols (`SMART_PROTOCOLS`):**
    * `tags://[tag_name]/` -> Intercepts and groups files by the `custom_tags` column.
    * `y_m_f://[year]/[month]/` -> Intercepts and dynamically builds virtual folders based on the `year` and `month` columns.
* **Special Bins:**
    * `trash://` -> Queries where `in_trash = 1`.
    * `fav://` -> Queries where `is_favorite = 1`.

---

## 5. Sub-System Modules

### Analytics & Timelines (`TimelineDiaryDialog`, `NexusTagLibraryDialog`)
* **Data Aggregation:** SQL `GROUP BY` queries and Python dictionaries are used to aggregate metadata (counts, sizes, extensions).
* **Visualization:** Data is passed to `PaginatingChartWidget`, which utilizes Matplotlib to render static images (`FigureCanvas`) directly into PySide6 layouts.

### Space & Integrity Analyzer (`SpaceAnalyzerDialog`)
* Executes complex SQL to find data paradoxes:
    * **Duplicates:** `GROUP BY size, extension HAVING COUNT(*) > 1`.
    * **Exact Duplicates:** Filters by `sha256`.
    * **Version Conflicts:** Same `name`, different `sha256`, sorted by `modified`.

### Materialization Engine (`MaterializeThread`)
* Reverses the VFS logic. It takes a list of virtual IDs, reads their `real_path` and virtual `parent_path`, uses `os.makedirs` to recreate the virtual folder tree on the host OS, and `shutil.copy2` to move the physical files into the newly created structure.

---

## 6. Developer Guidelines & Gotchas

1.  **Never Block the Main Thread:** Any function interacting with `os.walk`, `hashlib.sha256`, or large `cur.executemany` batches *must* be encapsulated in a `QThread`. Use the `_register_worker` pattern in `NexusVirtualManager` to prevent garbage collection crashes.
2.  **Thread Connection Scope:** SQLite connections cannot easily be shared across threads. Every `QThread.run()` method must instantiate its own DB connection using `SecurityEngine.connect()`.
3.  **Sanitize Output Paths:** When working with the Materialization or Export threads, always use `self.sanitize_filename()` to prevent illegal OS characters (e.g., `<, >, |, ?, *`) stored in the virtual DB from crashing `os.makedirs`.
4.  **Signal/Slot Thread Safety:** Only pass standard Python types (int, str, list, dict) through PySide6 signals between background threads and the UI. Do not pass SQLite cursor objects or file handles.