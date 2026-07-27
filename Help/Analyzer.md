## 🛠️ Core Scan Engines

The Analyzer contains four distinct scanning modules, each targeting a different type of file clutter.

### 1. 🔄 Scan Junk (Standard Cleanup)

This is a fast, surface-level scan that does not require heavy computational power. It flags three things:

* **Junk Files:** Any file with the extension `.tmp`, `.bak`, `.log`, `.cache`, or with "cache" in its name.
* **Huge Files:** Any single file exceeding 500 MB.
* **Surface Duplicates:** Files that share the exact same extension and exact same byte size. *(Note: Because it doesn't read the file contents, these might be false positives).*

### 2. 🧬 Exact Duplicates (Deep Hash Scan)

This scan finds mathematically identical files regardless of what they are named.

* **How it works:** It queries files that share the exact same **SHA-256 hash**.
* **Requirement:** This only works on files that have already been hashed. You must run the **"Compute SHA-256 for all Contents"** tool (via the main menu) before this scan will yield results.

### 3. 📝 Version Conflicts

This helps you clean up messy iteration histories (e.g., `Document_v1.docx`, `Document_v2.docx` imported with the same virtual name).

* **How it works:** It looks for files with the **exact same name**, but **different SHA-256 hashes**.
* **Auto-Selection:** It automatically flags the oldest versions for deletion (`🕰️ Older Version`) and leaves the newest one unchecked (`📝 Latest Version`).

### 4. ⚠️ Data Anomalies

This engine looks for broken or logically impossible file states in your database.

* **Type A - The Hash Paradox:** Files with the exact same name, size, and modified date—but different cryptographic hashes. This usually indicates silent file corruption or incomplete syncs.
* **Type B - 0-Byte Dead Files:** Files that contain absolutely zero data.

---

## 🧠 Smart Rule Engine & Safe Marking

Manually reviewing hundreds of duplicates is tedious. The Analyzer includes tools to automate the selection process safely.

### The Smart Select Tool

Instead of manually checking boxes, you can apply logical rules to groups of conflicting files:

* **Keep Oldest:** Unchecks the oldest file in a duplicate group and checks the newer ones for deletion.
* **Keep Newest:** Unchecks the newest file and checks the older ones.
* **Keep Specific Virtual Folder:** Prompts you for a target folder (e.g., `/Main_Archive/`). It will protect duplicates inside that folder and flag the copies residing anywhere else.

### 🛡️ Mark Safe (Ignoring Intentional Duplicates)

Sometimes you *want* identical files in different places (e.g., boilerplate code files, blank template documents).

* Select these items and click **Mark Safe**.
* This flags them in the database (`hash_verified = 1`), rendering them invisible to all future Analyzer scans. You can view or undo this using the **View Safe Files** and **Unmark Safe** buttons.

### ⚖️ Compare / Show Proof

If you are unsure why the Analyzer flagged files as duplicates, right-click the item and select **Compare / Show Proof**. This opens a side-by-side collision dialog showing the virtual path, physical OS path, byte size, date, and hash, allowing you to manually verify the conflict before deleting.

---

## 🛑 Cautions and Warnings

Because the Analyzer deals with mass deletion and deduplication, you must use it with care. Keep these cautions in mind:

1. **Virtual Deletion vs. Physical Deletion:**
* Clicking **"🗑️ Delete Checked"** inside the Analyzer dialog *only deletes the files from the VMan database*. It untracks them.
* It **does not** physically delete the files from your Windows/Mac hard drive. If you want to physically reclaim space on your hard drive, you must select the files in the *Main Application Window*, right-click, and choose **💀 Delete PHYSICAL OS Items**.


2. **Beware of 0-Byte Files:**
* While the anomaly scanner flags 0-byte files as dead data, some software uses empty files intentionally (e.g., `.gitkeep`, `.nomedia`, or lockfiles). Do not blindly delete them if you are mapping OS-critical directories.


3. **The "Scan Junk" False Positive Risk:**
* The "Scan Junk" duplicate finder only checks if two files have the same extension and exact same byte count. Two completely different `1024-byte` text files will be flagged as duplicates. Always use **Compare / Show Proof** or run the exact SHA-256 hash scan if you aren't sure.


4. **CPU Overhead for Hashes:**
* The deeper, more accurate scans require SHA-256 hashes. Running the Bulk Hash computation on a directory with thousands of large files will cause heavy disk I/O and CPU usage. Run hashes during downtime.

