## ⚙️ The 4-Step Bulk Workflow

The engine is designed with a strict linear workflow to prevent accidental data loss. You cannot execute an action without completing the steps in order.

### Step 1: Set the Target Folder

By default, the engine scopes its search to the absolute root (`/`). If you only want to organize a specific drive or project, click **📂 Change Target** and input a specific virtual path (e.g., `/Downloads/` or `/Projects/Active/`). The engine will recursively search that folder and all its subfolders.

### Step 2: Define the Match Condition

This determines *which* files inside your target folder will be affected. You can filter by:

* **Extension equals:** Targets specific file types (e.g., `.tmp`, `.jpg`).
* **Name contains / starts with / ends with:** Great for grabbing files with specific naming conventions like `[copy]` or `backup_`.
* **Size greater than (MB):** Finds files over a certain threshold to quickly tag or move heavy assets.
* **Older than (Days):** Targets aging files. It calculates this based on the file's `modified` date compared to today.
* **Has Custom Tag:** Grabs files that already have a specific tag (e.g., finding all `urgent` files to move them to an active folder).

### Step 3: Choose the Action

Once the files are isolated, what do you want to do with them?

* **Send to Virtual Trash:** Safely hides them from your main views (sets `in_trash = 1`).
* **Delete Permanently:** Wipes them from the VMan database entirely.
* **Move to Folder:** Changes their virtual parent path. *Note: If the destination folder doesn't exist, VMan will automatically create it. If it does exist, VMan will prompt you to confirm if you want to merge the files into it.*
* **Add Custom Tag:** Appends a new text tag (e.g., `archived`) to the files without erasing their existing tags.
* **Set Color Tag:** Replaces the current color label (Red, Green, Blue, Gold, None) for visual sorting.

### Step 4: Preview and Execute

You **cannot** click Execute until you preview the changes.
Clicking **🔍 Preview Matches** runs a safe simulation of your query. The console box will list up to 50 matching files so you can visually verify that your condition didn't accidentally grab the wrong files. Only after a successful preview will the **⚡ Execute Bulk Action** button unlock.

---

## 🛑 Cautions and Limitations

Because this engine alters database records at high speed, you should keep the following in mind:

1. **Virtual Actions Only:**
Like the Analyzer's delete function, the Bulk Operations Engine only manipulates your *virtual database*. Moving a file to `/Archive/` in VMan **does not** move the physical file on your Windows/Mac hard drive. Permanently deleting files here only untracks them.
2. **No Undo (Ctrl+Z) for Bulk Moves/Deletes:**
There is no global "undo" button for bulk operations. If you accidentally bulk-tag 10,000 files with the wrong color, you will have to run another bulk operation to set their color back to "None".
3. **Smart View Restrictions:**
You cannot set a Smart View (like `tags://` or `y_m_f://`) as your Target Folder. The engine strictly requires standard virtual directory paths (e.g., `/Home/`).
4. **Format Strictness:**
* When filtering by **Extension**, you don't need to type the dot (typing `png` or `.png` both work, the engine auto-corrects it).
* When filtering by **Older than (Days)** or **Size**, ensure you type purely numeric values. Entering text here will cause the query to fail.