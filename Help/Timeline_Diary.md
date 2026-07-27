
## 📅 The Left Panel: Time & Filters

The left side of the window is your control center for drilling down into specific datasets.

### 1. The Interactive Calendar

This isn't just a date picker; it’s a heatmap of your system activity.

* **Green Highlights:** Any day that has file activity is highlighted in bright green. You can instantly see which days were highly active and which were dormant.
* **Created vs. Modified Toggle:** Just above the calendar, you can swap the engine's focus.
* *Modified:* Highlights days when files were last edited or changed.
* *Created:* Highlights days when files were born (or first logged).


* **Instant Load:** Clicking any highlighted date instantly generates a daily report in the right panel.

### 2. Deep Data Filters

If you don't want to browse day-by-day, you can use the advanced filters to grab specific swaths of time or types of files. You can filter by:

* **Year & Month:** To look at broader historical trends.
* **Category:** (e.g., Images, Video, Code).
* **Type/Ext:** Drill down to specific formats like `.pdf` or `.py`.
* **Size Buckets:** Filter by Tiny (<1MB), Medium, or Huge (>500MB).
* **Tags:** Look at the timeline of files carrying a specific custom tag.

---

## 📑 The Right Panel: The 3-Tab Interface

Once you select a date or apply a filter, the results are populated across three distinct, synchronized tabs.

### Tab 1: 📖 Daily Diary (HTML Report)

When you click a specific date on the calendar, this tab generates a highly readable, human-friendly timeline of the day.

* **Color-Coded Timeline:** Files are listed chronologically by the hour and minute they were created/modified.
* **Visual Categories:** The entries are color-coded based on their category (e.g., Purple for Images, Red for Videos, Blue for Code) so you can scan the day's activity at a glance.

### Tab 2: 📋 Activity Log (Data Table)

This is the actionable view of your filtered data.

* **Grid View:** Displays Name, Type, Extension, Size, Virtual Location, and Database ID.
* **Actionable:** Just like the Space Analyzer, you can right-click items here to open them natively, view their properties, or delete them.

### Tab 3: 📊 Visual Analytics

This tab takes whatever data you've filtered and turns it into dynamic `matplotlib` charts. The dropdown at the top lets you pivot the data into different visual metrics:

* **Distribution by Extension:** See whether `.jpg` or `.mp4` is eating up the most space or file count.
* **Age Distribution:** A bar chart grouping files by age (e.g., `<1wk`, `<1mo`, `>1yr`).
* **Top 10 Largest Files:** A horizontal bar chart of the heaviest files in your filtered selection.
* **Activity Spikes:** Shows the last 15 active days so you can spot your busiest workflows.
* **Tag Coverage:** A pie chart showing the ratio of tagged vs. untagged files in this dataset.

---

## 🛑 Cautions and Limitations

Keep these things in mind when using the Timeline Engine:

1. **Matplotlib Dependency:**
* The Visual Analytics tab relies completely on the Python library `matplotlib`. If this library is not installed in your Python environment, Tab 3 will simply display an error message and the charts will fail to render.


2. **Deletion is Real (Virtually):**
* If you use the right-click menu in the Activity Log (Tab 2) to "Move to Trash" or "Permanent Delete", you are altering the VMan database. This isn't just a read-only log; it's a live management view.
* *(Reminder: This still only deletes them from the virtual database, not the physical OS, unless you specifically use the Physical OS deletion command).*


3. **Date Formatting Strictness:**
* The calendar and filtering engine rely heavily on SQLite string matching for dates (e.g., `YYYY-MM-DD HH:MM:SS`). If files were imported with missing, corrupted, or non-standard timestamps, they will not appear correctly on the calendar heatmap.


4. **Performance on Broad Filters:**
* If you have a massive database and click "Apply Filters" while leaving everything set to "All", the engine will pull your entire virtual file system into RAM to generate the table and charts. This can cause a temporary UI freeze.