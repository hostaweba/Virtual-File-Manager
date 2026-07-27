## The Scanning Engines

The Analyzer features four distinct scanning modes tailored to different types of storage waste:

| Scan Type | What It Finds | Best Used For |
| --- | --- | --- |
| **Scan Junk** | `.tmp`, `.bak`, `.cache`, and cache folders. | Quick spring cleaning of temporary system waste. |
| **Exact Duplicates** | Files with identical **SHA-256 Hashes**. | Finding exact 1:1 copies across different drives. |
| **Version Conflicts** | Files with the *same name* but *different hashes*. | Finding updated or altered versions of documents. |
| **Data Anomalies** | 0-byte files or Hash Paradoxes. | Identifying corrupted files or interrupted transfers. |

---

## Recommended De-Duplication Workflow

To safely clean a newly inserted pendrive against your master database without risking your offline archives, follow this exact sequence:

1. **Run a Target Scan:**
Click **📂 Set Scan Folders** and input the paths you want to analyze (e.g., `/` to scan everything, or `/New_Pendrive/` for a specific drive), then run your preferred Scan engine.


2. **Verify with the Proof Engine:**
Before making decisions, right-click any conflict and select **⚖️ Compare / Show Proof**. This opens a side-by-side breakdown of sizes, dates, and physical locations.


3. **Mark Intentional Duplicates as Safe:**
Highlight any files you intentionally want to keep as backups across multiple drives. Click **🛡️ Mark Safe**. VMan will instantly hide them and completely ignore them in all future scans.


4. **Apply a Smart Rule:**
Select a rule from the dropdown (e.g., *Rule: Keep files in a specific Virtual Folder...*) and click **Apply Rule**. VMan will automatically check the disposable copies and uncheck the ones you want to protect.


5. **Execute the Deletion:**
Review the checked items. Once verified, click **🗑️ Delete Checked** to purge them from the database.


---

## ⚠️ Critical Cautions & Data Safety

Because VMan manages both virtual representations and physical files, you must understand how the deletion mechanisms work.

* **Virtual Deletion vs. Physical Deletion:** The red **🗑️ Delete Checked** button at the bottom of the Analyzer *only deletes the records from the VMan Database*. It does not erase the files from your physical pendrive. To erase real files from your physical disk, you must highlight them, right-click, and select **💀 Delete PHYSICAL OS Items**.
* **The "Keep Oldest/Newest" Rule Trap:** When using the Smart Rules for Version Conflicts, remember that "Newest" means the most recently modified. If an older, finished document was accidentally overwritten by a newer, blank document, "Keep Newest" will destroy the finished work. Always use the **Compare / Show Proof** tool on documents before automating deletion.
* **Safe Marking is Permanent:** When you click **🛡️ Mark Safe**, that file is permanently flagged as an intentional duplicate. It will never appear in a scan again unless you explicitly click **👁️ View Safe Files**, highlight it, and click **❌ Unmark Safe**.
* **Disconnected Physical Paths:** If you run a physical deletion on a file located on a pendrive that is currently unplugged, VMan will throw a warning and safely skip the physical deletion, though it may still clear the virtual database record if instructed.