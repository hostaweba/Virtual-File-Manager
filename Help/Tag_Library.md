## 🧭 The Left Panel: Miller-Style Tag Browser

The `Library Browser` tab operates entirely on a cascading column view that translates your virtual paths into an infinitely deep, horizontal hierarchy.

### 1. The Column Hierarchy

* **How it Works:** You start at a Base View (by default, the Root `/`). The first column shows the immediate contents. Clicking any folder dynamically spawns the next column to the right, showing its sub-contents, and so on.
* **Custom Hierarchy Names:** Clicking the **⚙️ Custom Names** button in the toolbar allows you to define what each depth level represents. For example, if you structure your folders as `Client -> Project -> Year`, you can name Level 1 "Client," Level 2 "Project," etc. These names will be saved persistently and update the column headers and analytics tabs.
* **Seamless Navigation:** You can use your keyboard's Left and Right arrow keys to quickly drill into and back out of deep virtual structures.

### 2. The Universal Tag List

Pinned to the far right of the column view is the Tag List.

* **Highlighting Matches:** Clicking any tag in this list instantly highlights (in bold blue text) every file and folder in the column view that carries that tag. This allows you to visually track a project's assets scattered across completely different folder trees.
* **Double-Click to Isolate:** Double-clicking a tag sends a command to the main VMan window to instantly open a Smart View (`tags://[tagname]`), isolating all tagged files into a clean grid/list.

---

## 📊 The Executive Dashboards

The remaining tabs in the Tag Library transform your virtual structure into high-level business logic and analytics.

### 1. 🏠 Executive Dashboard

This is a high-level metric board utilizing colorful KPI cards. It gives you instant readouts on:

* Total Virtual Items, Folders, and Files.
* The total number of unique tags currently in use.
* The number of Multi-Tagged items (files that cross-reference multiple concepts).
* Volume metrics based on your custom hierarchy names (e.g., Total "Clients", Total "Projects").

### 2. Paginating Chart Analytics

Each level of your folder depth (and your custom tags) gets its own dedicated analytics tab.

* **The Engine:** Because some levels might have hundreds of items (e.g., 500 different tags), the engine uses a paginating chart system. You can set the items "Per Page" to keep the visual charts clean and cycle through the data using Next/Prev buttons.
* **Dynamic Visuals:** You can instantly pivot the `matplotlib` data between a Horizontal Bar chart (to view top volume items), a Pie chart (for strict distribution ratios), and a Scatter plot (for identifying outliers).

---

## ⚙️ Mass Linkage & Integrations

The Tag Library isn't just for looking at data; it has powerful administrative tools in its top toolbar.

### 1. 🔗 Global Mapping

If you move an entire folder of files on your physical Windows/Mac hard drive, VMan will lose the connection. The **"Map View to OS Folder"** button allows you to select a new physical root folder. VMan will recursively traverse your virtual database and intelligently remap every single file underneath it to the new physical path.

### 2. 📥 Mass Import / 📤 Mass Export (CSV Manifests)

* **Export:** You can generate a rich `.csv` manifest containing every virtual path and its associated tags.
* **Import:** You can import a `.csv` file with a list of paths and tags. VMan will automatically build the virtual folders, populate the files, and apply the tags.
* **Physical Syncing:** When importing a CSV, VMan will prompt you: *"Do you want to actually generate physical folders on your hard drive?"* If you click Yes, VMan will use the CSV blueprint to build an actual, physical directory tree on your OS and drop a `tag.txt` file inside each folder containing the imported tags!

---

## 🛑 Cautions and Limitations

1. **RAM Consumption on Deep Caching:**
* To make the Miller columns and global search lightning fast, the Tag Library pre-loads your entire selected Base View into a Python dictionary (`self.tag_cache`) when it opens. If your VMan database contains hundreds of thousands of files, this might take a few seconds to build and will consume system RAM.


2. **External DB Edits:**
* If you leave the Tag Library open and change file tags or move folders in the *main* VMan window, the Tag Library won't know about it automatically. You must click the **🔄 Refresh** button in the Tag Library toolbar to sync its memory cache back up with the SQLite database.


3. **Matplotlib Requirement:**
* Just like the Timeline Diary, Tabs 2-8 will only function if the `matplotlib` Python package is installed on the host machine. If it is missing, the tabs will cleanly fail over to a "Matplotlib not installed" text label to prevent crashing.